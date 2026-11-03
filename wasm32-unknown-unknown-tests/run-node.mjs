// TODO(WASM): This would be better in Typescript. I didn't have time to set up
// all the machinery around that, but would like to try to do it with tsrs once
// it stabilizes.

// Runs the harness module with Node's WebAssembly API, supplying its `host`
// imports, and no bindings.
//
// Usage: node run-node.mjs <module.wasm> <server-address>
//
// <server-address> is the IP address and port of a running s3-mock-server,
// such as `127.0.0.1:49152`, that the module's scenarios accept.
//
// Checks that the module imports exactly the `host` interface that its crate
// documentation describes. Runs each scenario in a fresh instance, sending
// every request the module passes to `http_send` to the server with
// `node:http`, and drives the scenario with `scenario_step` until it passes.
// The wasmtime-runner crate runs the same loop under Wasmtime. Finally checks
// that constructing a client with the default runtime mode or without a
// sleep implementation traps with the documented message.
//
// Prints `wasm32-unknown-unknown-tests: <count> scenarios under Node` first,
// then `PASS <scenario>: <summary>` for each scenario that passes: the
// module's report for the module's scenarios, then one line for each trap.
// Prints `<count> scenarios passed` last, only if every scenario passed.

import { readFile } from "node:fs/promises";
import http from "node:http";

// The module's imports from the module `host`.
const IMPORTS = ["http_cancel", "http_send", "monotonic_now", "wall_clock_now"];

// Exports that must trap, and the message each must report.
const TRAPS = [
  ["client_without_runtime", "requires RuntimeConfig::wasm"],
  ["client_without_sleep", "requires an AsyncSleep implementation"],
];

// How long one scenario may run.
const SCENARIO_TIMEOUT_MS = 60_000;

// `scenario_step` results.
const PASSED = -1n;
const NO_DEADLINE = 0x7fff_ffff_ffff_ffffn;

// Response headers that describe the connection rather than the response.
const HOP_BY_HOP = new Set(["connection", "keep-alive", "transfer-encoding"]);

const [path, server] = process.argv.slice(2);
if (!path || !server) {
  console.error("usage: node run-node.mjs <module.wasm> <server-address>");
  process.exit(2);
}
const module = await WebAssembly.compile(await readFile(path));

const imports = WebAssembly.Module.imports(module)
  .map(({ module, name, kind }) => `${module}.${name} (${kind})`)
  .sort();
const expected = IMPORTS.map((name) => `host.${name} (function)`);
if (JSON.stringify(imports) !== JSON.stringify(expected)) {
  console.error(
    `the module must import exactly ${expected.join(", ")}; it imports ${imports.join(", ")}`,
  );
  process.exit(1);
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function message(instance) {
  const { memory, message_ptr, message_len } = instance.exports;
  return decoder.decode(
    new Uint8Array(memory.buffer, message_ptr() >>> 0, message_len() >>> 0),
  );
}

// Copies `bytes` into the module's host buffer.
function writeBuffer(exports, bytes) {
  const ptr = exports.host_buffer(bytes.length) >>> 0;
  new Uint8Array(exports.memory.buffer, ptr, bytes.length).set(bytes);
}

function indexOf(bytes, pair) {
  for (let i = 0; i + 1 < bytes.length; i++) {
    if (bytes[i] === pair[0] && bytes[i + 1] === pair[1]) return i;
  }
  return -1;
}

// Parses a request in the module's wire format: `METHOD URI\n`, header
// lines, a blank line, and the body. Returns the headers as name and value
// pairs in order.
function parseRequest(bytes) {
  const split = indexOf(bytes, [10, 10]);
  if (split < 0) throw new Error("request has no blank line after its head");
  const [requestLine, ...lines] = decoder
    .decode(bytes.subarray(0, split))
    .split("\n");
  const space = requestLine.indexOf(" ");
  if (space < 0) throw new Error(`invalid request line ${requestLine}`);
  const headers = lines.map((line) => {
    const colon = line.indexOf(": ");
    if (colon < 0) throw new Error(`invalid request header ${line}`);
    return [line.slice(0, colon), line.slice(colon + 2)];
  });
  return {
    method: requestLine.slice(0, space),
    url: new URL(requestLine.slice(space + 1)),
    headers,
    body: bytes.subarray(split + 2),
  };
}

// Serializes a response in the module's wire format.
function serializeResponse(response, body) {
  let head = `${response.statusCode}\n`;
  const raw = response.rawHeaders;
  for (let i = 0; i < raw.length; i += 2) {
    if (!HOP_BY_HOP.has(raw[i].toLowerCase()))
      head += `${raw[i]}: ${raw[i + 1]}\n`;
  }
  return Buffer.concat([encoder.encode(`${head}\n`), body]);
}

// The `host` imports for one instance. Sends requests only to `server`.
class Host {
  // Requests sent and neither answered nor cancelled, by id.
  inFlight = new Map();
  // Responses of in-flight requests, in completion order.
  completed = [];
  // The first error the host observed.
  failure = null;
  memory = null;
  #wake = null;

  constructor(server) {
    this.server = server;
  }

  imports() {
    return {
      host: {
        monotonic_now: () => process.hrtime.bigint(),
        wall_clock_now: () => BigInt(Date.now()) * 1_000_000n,
        http_send: (id, ptr, len) => this.#send(id, ptr >>> 0, len >>> 0),
        http_cancel: (id) => this.#cancel(id),
      },
    };
  }

  #fail(failure) {
    this.failure ??= failure;
    this.#wake?.();
  }

  #send(id, ptr, len) {
    let request;
    try {
      request = parseRequest(
        new Uint8Array(this.memory.buffer, ptr, len).slice(),
      );
    } catch (error) {
      this.#fail(`request ${id}: ${error.message}`);
      return;
    }
    const { method, url, headers, body } = request;
    if (url.protocol !== "http:" || url.host !== this.server) {
      this.#fail(
        `request ${id} is for ${url.origin}, not http://${this.server}`,
      );
      return;
    }
    if (this.inFlight.has(id)) {
      this.#fail(`request ${id} was sent twice`);
      return;
    }
    // Node adds no Host header when the headers are an array, and no
    // Content-Length header for an empty body.
    const declared = (wanted) =>
      headers.some(([name]) => name.toLowerCase() === wanted);
    if (!declared("host")) headers.push(["host", url.host]);
    if (
      !declared("content-length") &&
      !declared("transfer-encoding") &&
      (body.length !== 0 || method === "POST" || method === "PUT")
    ) {
      headers.push(["content-length", String(body.length)]);
    }
    const controller = new AbortController();
    this.inFlight.set(id, controller);
    const outgoing = http.request(
      url,
      { method, headers: headers.flat(), signal: controller.signal },
      (response) => {
        const chunks = [];
        response.on("data", (chunk) => chunks.push(chunk));
        response.on("end", () => {
          if (!this.inFlight.has(id)) return;
          this.completed.push({
            id,
            bytes: serializeResponse(response, Buffer.concat(chunks)),
          });
          this.#wake?.();
        });
      },
    );
    outgoing.on("error", (error) => {
      if (!controller.signal.aborted)
        this.#fail(`request ${id}: ${error.message}`);
    });
    outgoing.end(body);
  }

  #cancel(id) {
    const controller = this.inFlight.get(id);
    if (!controller) {
      this.#fail(`request ${id} was cancelled but is not in flight`);
      return;
    }
    this.inFlight.delete(id);
    this.completed = this.completed.filter((response) => response.id !== id);
    controller.abort();
  }

  // Waits until a response completes, the host fails, `next` passes on the
  // monotonic clock, or `timeoutAt` passes on `performance.now()`.
  async wait(next, timeoutAt) {
    if (this.completed.length !== 0 || this.failure) return;
    let delay = timeoutAt - performance.now();
    if (next !== NO_DEADLINE) {
      delay = Math.min(delay, Number(next - process.hrtime.bigint()) / 1e6);
    }
    await new Promise((resolve) => {
      const timer = setTimeout(resolve, Math.max(0, delay));
      this.#wake = () => {
        clearTimeout(timer);
        resolve();
      };
    });
    this.#wake = null;
  }

  // Removes and returns the completed responses.
  takeCompleted() {
    const completed = this.completed;
    this.completed = [];
    for (const { id } of completed) this.inFlight.delete(id);
    return completed;
  }

  // Aborts every request in flight.
  close() {
    for (const controller of this.inFlight.values()) controller.abort();
    this.inFlight.clear();
  }
}

// Runs scenario `index` in a fresh instance. Returns the module's report, and
// a failure if the scenario did not pass.
async function runScenario(index) {
  const host = new Host(server);
  const instance = await WebAssembly.instantiate(module, host.imports());
  const { exports } = instance;
  host.memory = exports.memory;
  const timeoutAt = performance.now() + SCENARIO_TIMEOUT_MS;
  try {
    writeBuffer(exports, encoder.encode(`http://${server}`));
    exports.scenario_start(index);
    for (;;) {
      const next = exports.scenario_step();
      if (host.failure)
        return { failure: host.failure, report: message(instance) };
      if (next === PASSED) return { report: message(instance) };
      if (host.inFlight.size === 0 && next === NO_DEADLINE) {
        return {
          failure: "the module awaits responses but no request is in flight",
          report: message(instance),
        };
      }
      await host.wait(next, timeoutAt);
      if (host.failure)
        return { failure: host.failure, report: message(instance) };
      for (const { id, bytes } of host.takeCompleted()) {
        writeBuffer(exports, bytes);
        if (exports.host_respond(id) !== 1) {
          return {
            failure: `the module dropped request ${id} without cancelling it`,
            report: message(instance),
          };
        }
      }
      if (performance.now() >= timeoutAt) {
        return {
          failure: `did not finish within ${SCENARIO_TIMEOUT_MS} ms`,
          report: message(instance),
        };
      }
    }
  } catch (error) {
    if (!(error instanceof WebAssembly.RuntimeError)) throw error;
    return { failure: `trapped: ${error.message}`, report: message(instance) };
  } finally {
    host.close();
  }
}

// Calls export `name` in a fresh instance.
async function invoke(name) {
  const instance = await WebAssembly.instantiate(
    module,
    new Host(server).imports(),
  );
  try {
    return { result: instance.exports[name](), message: message(instance) };
  } catch (error) {
    if (!(error instanceof WebAssembly.RuntimeError)) throw error;
    return { trap: error, message: message(instance) };
  }
}

let failed = false;

const scenarios = (await invoke("scenario_count")).result;
const count = scenarios + TRAPS.length;
console.log(`wasm32-unknown-unknown-tests: ${count} scenarios under Node`);

for (let index = 0; index < scenarios; index++) {
  const { failure, report } = await runScenario(index);
  if (failure) {
    console.error(`FAIL scenario ${index}: ${failure}`);
    process.stderr.write(report);
    failed = true;
  } else {
    process.stdout.write(report);
  }
}

for (const [name, expected] of TRAPS) {
  const outcome = await invoke(name);
  if (outcome.trap && outcome.message.includes(expected)) {
    console.log(`PASS ${name}: trapped reporting "${expected}"`);
  } else {
    console.error(`FAIL ${name}: expected a trap mentioning "${expected}"`);
    process.stderr.write(outcome.message);
    failed = true;
  }
}

if (!failed) console.log(`${count} scenarios passed`);
process.exit(failed ? 1 : 0);
