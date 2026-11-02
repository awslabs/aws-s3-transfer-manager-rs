/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Runs the `wasm32-unknown-unknown` harness module under Wasmtime,
//! supplying its `host` imports.
//!
//! Usage: `wasmtime-runner <module.wasm> <server-address>`, where
//! `<server-address>` is the IP address and port of a running
//! `s3-mock-server` that the module's scenarios accept, such as
//! `127.0.0.1:49152`.
//!
//! The runner checks that the module imports exactly the `host` interface
//! that the harness crate documents, then runs each scenario in a fresh
//! instance. A tokio runtime sends each request that the module passes to
//! `http_send` to the server over HTTP/1.1. The main thread drives the
//! scenario with `scenario_step` and, between calls, waits for a response or
//! the deadline that the call returned. `run-node.mjs` runs the same loop
//! under Node. Finally the runner checks that constructing a client with the
//! default runtime mode or without a sleep implementation traps with the
//! documented message.
//!
//! It prints `wasm32-unknown-unknown-tests: <count> scenarios under
//! Wasmtime` first, then `PASS <scenario>: <summary>` for each scenario that
//! passes, and `<count> scenarios passed` last, only if every scenario
//! passed. It reports each failure on standard error and then exits with
//! status 1.

use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::Path;
use std::process::ExitCode;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use wasmtime::{Caller, Engine, Instance, Linker, Memory, Module, Store, Trap, TypedFunc};

const USAGE: &str = "usage: wasmtime-runner <module.wasm> <server-address>";

/// The module's imports from the module `host`.
const IMPORTS: [&str; 4] = [
    "http_cancel",
    "http_send",
    "monotonic_now",
    "wall_clock_now",
];

/// Exports that must trap, and the message each must report.
const TRAPS: [(&str, &str); 2] = [
    ("client_without_runtime", "requires RuntimeMode::Wasm"),
    (
        "client_without_sleep",
        "requires an AsyncSleep implementation",
    ),
];

/// How long one scenario may run.
const SCENARIO_TIMEOUT: Duration = Duration::from_secs(60);

/// `scenario_step` results.
const PASSED: i64 = -1;
const NO_DEADLINE: i64 = i64::MAX;

/// Response headers that describe the connection rather than the response.
const HOP_BY_HOP: [&str; 3] = ["connection", "keep-alive", "transfer-encoding"];

type HttpClient = Client<HttpConnector, Full<Bytes>>;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [path, server] = args.as_slice() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let Ok(server) = server.parse::<SocketAddr>() else {
        eprintln!("{USAGE}\n<server-address> must be an IP address and port: {server}");
        return ExitCode::from(2);
    };
    match run(Path::new(path), server) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("{e:?}");
            ExitCode::FAILURE
        }
    }
}

/// Runs every scenario and trap check. Returns whether all passed.
fn run(path: &Path, server: SocketAddr) -> wasmtime::Result<bool> {
    let engine = Engine::default();
    let module = Module::from_file(&engine, path)?;
    check_imports(&module)?;
    let tokio = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let runner = Runner {
        linker: linker(&engine)?,
        engine,
        module,
        server: server.to_string(),
        epoch: Instant::now(),
        tokio: tokio.handle().clone(),
        client: Client::builder(TokioExecutor::new()).build_http(),
    };

    let mut passed = true;
    let scenarios = runner.scenario_count()?;
    let count = scenarios + TRAPS.len();
    println!("wasm32-unknown-unknown-tests: {count} scenarios under Wasmtime");
    for index in 0..scenarios {
        match runner.run_scenario(index)? {
            Outcome::Passed { report } => print!("{report}"),
            Outcome::Failed { failure, report } => {
                eprintln!("FAIL scenario {index}: {failure}");
                eprint!("{report}");
                passed = false;
            }
        }
    }
    for (name, expected) in TRAPS {
        let (trap, report) = runner.invoke(name)?;
        if trap && report.contains(expected) {
            println!("PASS {name}: trapped reporting \"{expected}\"");
        } else {
            eprintln!("FAIL {name}: expected a trap mentioning \"{expected}\"");
            eprint!("{report}");
            passed = false;
        }
    }
    if passed {
        println!("{count} scenarios passed");
    }
    Ok(passed)
}

/// Fails unless the module imports exactly [`IMPORTS`], as functions.
fn check_imports(module: &Module) -> wasmtime::Result<()> {
    let imports: BTreeSet<String> = module
        .imports()
        .map(|import| {
            let kind = if import.ty().func().is_some() {
                "function"
            } else {
                "non-function"
            };
            format!("{}.{} ({kind})", import.module(), import.name())
        })
        .collect();
    let expected: BTreeSet<String> = IMPORTS
        .iter()
        .map(|name| format!("host.{name} (function)"))
        .collect();
    if imports != expected {
        wasmtime::bail!("the module must import exactly {expected:?}; it imports {imports:?}");
    }
    Ok(())
}

/// A request's serialized response, or the reason it failed.
struct Completion {
    id: u32,
    response: Result<Vec<u8>, String>,
}

/// State of the `host` imports for one instance.
struct Host {
    /// The server's address, such as `127.0.0.1:49152`.
    server: String,
    epoch: Instant,
    tokio: tokio::runtime::Handle,
    client: HttpClient,
    completions: Sender<Completion>,
    /// Requests sent and neither answered nor cancelled, by id.
    in_flight: HashMap<u32, tokio::task::AbortHandle>,
    /// The first error the imports observed.
    failure: Option<String>,
}

impl Host {
    fn fail(&mut self, failure: String) {
        self.failure.get_or_insert(failure);
    }

    fn send(&mut self, id: u32, bytes: &[u8]) {
        let request = match parse_request(bytes, &self.server) {
            Ok(request) => request,
            Err(e) => return self.fail(format!("request {id}: {e}")),
        };
        if self.in_flight.contains_key(&id) {
            return self.fail(format!("request {id} was sent twice"));
        }
        let client = self.client.clone();
        let completions = self.completions.clone();
        let task = self.tokio.spawn(async move {
            let response = perform(&client, request).await;
            // The receiver is gone only once the scenario has ended.
            let _ = completions.send(Completion { id, response });
        });
        self.in_flight.insert(id, task.abort_handle());
    }

    fn cancel(&mut self, id: u32) {
        match self.in_flight.remove(&id) {
            Some(task) => task.abort(),
            None => self.fail(format!("request {id} was cancelled but is not in flight")),
        }
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        for task in self.in_flight.values() {
            task.abort();
        }
    }
}

/// Defines the `host` imports.
fn linker(engine: &Engine) -> wasmtime::Result<Linker<Host>> {
    let mut linker = Linker::new(engine);
    linker.func_wrap("host", "monotonic_now", |caller: Caller<'_, Host>| {
        nanos(caller.data().epoch.elapsed())
    })?;
    linker.func_wrap("host", "wall_clock_now", || {
        nanos(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default(),
        )
    })?;
    linker.func_wrap(
        "host",
        "http_send",
        |mut caller: Caller<'_, Host>, id: u32, ptr: u32, len: u32| {
            let memory = caller
                .get_export("memory")
                .and_then(|export| export.into_memory());
            let bytes = memory.and_then(|memory| {
                let start = usize::try_from(ptr).ok()?;
                let end = start.checked_add(usize::try_from(len).ok()?)?;
                memory.data(&caller).get(start..end).map(<[u8]>::to_vec)
            });
            let host = caller.data_mut();
            match bytes {
                Some(bytes) => host.send(id, &bytes),
                None => host.fail(format!("request {id} lies outside the module's memory")),
            }
        },
    )?;
    linker.func_wrap(
        "host",
        "http_cancel",
        |mut caller: Caller<'_, Host>, id: u32| {
            caller.data_mut().cancel(id);
        },
    )?;
    Ok(linker)
}

fn nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

/// Parses a request in the module's wire format: `METHOD URI\n`, header lines,
/// a blank line, and the body. Fails unless the URI is on `server`.
fn parse_request(bytes: &[u8], server: &str) -> Result<hyper::Request<Full<Bytes>>, String> {
    let split = bytes
        .windows(2)
        .position(|window| window == b"\n\n")
        .ok_or("request has no blank line after its head")?;
    let head = std::str::from_utf8(&bytes[..split]).map_err(|e| format!("request head: {e}"))?;
    let mut lines = head.split('\n');
    let request_line = lines.next().unwrap_or_default();
    let (method, uri) = request_line
        .split_once(' ')
        .ok_or_else(|| format!("invalid request line {request_line:?}"))?;
    let uri: hyper::Uri = uri
        .parse()
        .map_err(|e| format!("invalid URI {uri:?}: {e}"))?;
    if uri.scheme_str() != Some("http")
        || uri.authority().map(|authority| authority.as_str()) != Some(server)
    {
        return Err(format!("request is for {uri}, not http://{server}"));
    }
    let mut builder = hyper::Request::builder().method(method).uri(uri);
    for line in lines {
        let (name, value) = line
            .split_once(": ")
            .ok_or_else(|| format!("invalid request header {line:?}"))?;
        builder = builder.header(name, value);
    }
    builder
        .body(Full::new(Bytes::copy_from_slice(&bytes[split + 2..])))
        .map_err(|e| format!("invalid request: {e}"))
}

/// Sends `request` and serializes its response in the module's wire format.
async fn perform(
    client: &HttpClient,
    request: hyper::Request<Full<Bytes>>,
) -> Result<Vec<u8>, String> {
    let response = client.request(request).await.map_err(|e| chain(&e))?;
    let (parts, body) = response.into_parts();
    let body = body.collect().await.map_err(|e| chain(&e))?.to_bytes();
    let mut head = format!("{}\n", parts.status.as_u16());
    for (name, value) in &parts.headers {
        if HOP_BY_HOP.contains(&name.as_str()) {
            continue;
        }
        let value = value
            .to_str()
            .map_err(|_| format!("response header {name} is not visible ASCII"))?;
        writeln!(head, "{name}: {value}").expect("writing to a string cannot fail");
    }
    head.push('\n');
    let mut serialized = head.into_bytes();
    serialized.extend_from_slice(&body);
    Ok(serialized)
}

/// Formats `error` and its sources, outermost first.
fn chain(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        write!(message, ": {error}").expect("writing to a string cannot fail");
        source = error.source();
    }
    message
}

/// The result of one scenario.
enum Outcome {
    Passed { report: String },
    Failed { failure: String, report: String },
}

/// The exports that the runner calls.
struct Exports {
    memory: Memory,
    scenario_start: TypedFunc<i32, ()>,
    scenario_step: TypedFunc<(), i64>,
    host_buffer: TypedFunc<u32, u32>,
    host_respond: TypedFunc<u32, i32>,
    message_ptr: TypedFunc<(), u32>,
    message_len: TypedFunc<(), u32>,
}

struct Runner {
    engine: Engine,
    module: Module,
    linker: Linker<Host>,
    server: String,
    epoch: Instant,
    tokio: tokio::runtime::Handle,
    client: HttpClient,
}

impl Runner {
    /// Instantiates the module with fresh `host` state. Returns the store, the
    /// instance, its exports, and the channel on which requests complete.
    fn instantiate(
        &self,
    ) -> wasmtime::Result<(Store<Host>, Instance, Exports, Receiver<Completion>)> {
        let (completions, completed) = mpsc::channel();
        let mut store = Store::new(
            &self.engine,
            Host {
                server: self.server.clone(),
                epoch: self.epoch,
                tokio: self.tokio.clone(),
                client: self.client.clone(),
                completions,
                in_flight: HashMap::new(),
                failure: None,
            },
        );
        let instance = self.linker.instantiate(&mut store, &self.module)?;
        let exports = Exports {
            memory: instance
                .get_memory(&mut store, "memory")
                .ok_or_else(|| wasmtime::format_err!("the module exports no memory"))?,
            scenario_start: instance.get_typed_func(&mut store, "scenario_start")?,
            scenario_step: instance.get_typed_func(&mut store, "scenario_step")?,
            host_buffer: instance.get_typed_func(&mut store, "host_buffer")?,
            host_respond: instance.get_typed_func(&mut store, "host_respond")?,
            message_ptr: instance.get_typed_func(&mut store, "message_ptr")?,
            message_len: instance.get_typed_func(&mut store, "message_len")?,
        };
        Ok((store, instance, exports, completed))
    }

    fn scenario_count(&self) -> wasmtime::Result<usize> {
        let (mut store, instance, ..) = self.instantiate()?;
        let count = instance
            .get_typed_func::<(), i32>(&mut store, "scenario_count")?
            .call(&mut store, ())?;
        Ok(usize::try_from(count)?)
    }

    /// Calls export `name` in a fresh instance. Returns whether it trapped,
    /// and the module's report.
    fn invoke(&self, name: &str) -> wasmtime::Result<(bool, String)> {
        let (mut store, instance, exports, _) = self.instantiate()?;
        let export = instance.get_typed_func::<(), i32>(&mut store, name)?;
        let trapped = match export.call(&mut store, ()) {
            Ok(_) => false,
            Err(e) if e.downcast_ref::<Trap>().is_some() => true,
            Err(e) => return Err(e),
        };
        Ok((trapped, message(&mut store, &exports)))
    }

    /// Runs scenario `index` in a fresh instance.
    fn run_scenario(&self, index: usize) -> wasmtime::Result<Outcome> {
        let (mut store, _, exports, completed) = self.instantiate()?;
        let outcome = self.drive(&mut store, &exports, &completed, index);
        let report = message(&mut store, &exports);
        Ok(match outcome {
            Ok(()) => Outcome::Passed { report },
            Err(failure) => Outcome::Failed { failure, report },
        })
    }

    /// Starts scenario `index` and steps it until it passes or fails.
    fn drive(
        &self,
        store: &mut Store<Host>,
        exports: &Exports,
        completed: &Receiver<Completion>,
        index: usize,
    ) -> Result<(), String> {
        let index = i32::try_from(index).map_err(|e| e.to_string())?;
        write_buffer(store, exports, format!("http://{}", self.server).as_bytes())?;
        exports
            .scenario_start
            .call(&mut *store, index)
            .map_err(trapped)?;
        let timeout_at = Instant::now() + SCENARIO_TIMEOUT;
        loop {
            let next = exports
                .scenario_step
                .call(&mut *store, ())
                .map_err(trapped)?;
            if let Some(failure) = store.data_mut().failure.take() {
                return Err(failure);
            }
            if next == PASSED {
                return Ok(());
            }
            if next == NO_DEADLINE && store.data().in_flight.is_empty() {
                return Err("the module awaits responses but no request is in flight".into());
            }
            let mut wait = timeout_at.saturating_duration_since(Instant::now());
            if next != NO_DEADLINE {
                let remaining = next.saturating_sub(nanos(self.epoch.elapsed()));
                wait = wait.min(Duration::from_nanos(u64::try_from(remaining).unwrap_or(0)));
            }
            let mut ready = Vec::new();
            match completed.recv_timeout(wait) {
                Ok(completion) => ready.push(completion),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    unreachable!("the store's host holds a sender")
                }
            }
            ready.extend(completed.try_iter());
            for Completion { id, response } in ready {
                // A cancelled request is never answered.
                if store.data_mut().in_flight.remove(&id).is_none() {
                    continue;
                }
                let response = response.map_err(|e| format!("request {id}: {e}"))?;
                write_buffer(store, exports, &response)?;
                let delivered = exports
                    .host_respond
                    .call(&mut *store, id)
                    .map_err(trapped)?;
                if delivered != 1 {
                    return Err(format!(
                        "the module dropped request {id} without cancelling it"
                    ));
                }
            }
            if Instant::now() >= timeout_at {
                return Err(format!(
                    "did not finish within {}s",
                    SCENARIO_TIMEOUT.as_secs()
                ));
            }
        }
    }
}

fn trapped(error: wasmtime::Error) -> String {
    match error.downcast_ref::<Trap>() {
        Some(trap) => format!("trapped: {trap}"),
        None => format!("{error:?}"),
    }
}

/// Copies `bytes` into the module's host buffer.
fn write_buffer(store: &mut Store<Host>, exports: &Exports, bytes: &[u8]) -> Result<(), String> {
    let len = u32::try_from(bytes.len()).map_err(|e| e.to_string())?;
    let ptr = exports
        .host_buffer
        .call(&mut *store, len)
        .map_err(trapped)?;
    exports
        .memory
        .write(&mut *store, ptr as usize, bytes)
        .map_err(|e| format!("write the host buffer: {e}"))
}

/// The module's report, or an empty string if it cannot be read.
fn message(store: &mut Store<Host>, exports: &Exports) -> String {
    let (Ok(ptr), Ok(len)) = (
        exports.message_ptr.call(&mut *store, ()),
        exports.message_len.call(&mut *store, ()),
    ) else {
        return String::new();
    };
    let start = ptr as usize;
    exports
        .memory
        .data(&*store)
        .get(start..start.saturating_add(len as usize))
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
        .unwrap_or_default()
}
