# Buffer-pool fuzzing

The `buffer-pool-operations` target checks public buffer-pool state
transitions against an independent accounting and ownership model.

```text
 checked-in seeds ─┐
                   ├─> libFuzzer mutations ─> 8-byte records ─> Operation values
 evolving corpus ──┘                                           │
                                                               v
                                          ┌──────────── real BufferPool
                                          │
                                          └──────────── PoolModel
                                                               │
                                                               v
                                  compare bytes, handles, metrics, and audit
                                         after every operation
                                                               │
                                                               v
                                  drop all owners and require zero teardown
```

Property tests generate typed `Operation` values directly. Fuzzing and
checked-in corpus replay first decode bytes into the same values, then use the
same runner, reference model, per-step audit, and teardown checks. The decoder
accepts at most 128 eight-byte records, which bounds work for every input.

The transfer-manager entry point exists only under `cfg(s3_tm_fuzz)`.
Ordinary builds do not expose it or depend on `libfuzzer-sys`.

## Running a campaign

Install `cargo-fuzz`, then run from this directory:

```sh
mkdir -p corpus/buffer-pool-operations

RUSTFLAGS="-Dwarnings --cfg s3_tm_fuzz" \
    cargo +nightly-2026-09-10 fuzz run buffer-pool-operations \
    corpus/buffer-pool-operations \
    ../aws-sdk-s3-transfer-manager/src/runtime/buffer_pool/tests/corpus/buffer-pool-operations \
    -- -max_len=1024 -timeout=10
```

The first path is cargo-fuzz's default writable corpus. It is ignored by Git
and may accumulate locally or in the scheduled CI cache. The second path
contains reviewed, checked-in seeds and is input only. Keeping them separate
prevents a campaign from modifying regression fixtures.

CI and repository validation use `.github/scripts/run-fuzz-targets.sh` from the
repository root. It takes one mode argument:

- `build` compiles every target without executing it;
- `smoke` executes every target for a deterministic, bounded number of runs;
  and
- `campaign` explores every target for a bounded amount of time.

Run `.github/scripts/run-fuzz-targets.sh --help` for the complete command-line
reference. Common invocations are:

```sh
.github/scripts/run-fuzz-targets.sh build
FUZZ_RUNS=64 .github/scripts/run-fuzz-targets.sh smoke
FUZZ_MAX_TOTAL_TIME=60 .github/scripts/run-fuzz-targets.sh campaign
```

The runner has no additional positional arguments. Configure its execution
with these environment variables:

| Variable | Modes | Default | Purpose |
| --- | --- | --- | --- |
| `FUZZ_TARGET_TRIPLE` | all | `x86_64-unknown-linux-gnu` | Native compilation target |
| `FUZZ_RUNS` | `smoke` | `4096` | Deterministic execution count |
| `FUZZ_SEED` | `smoke` | `1` | Deterministic libFuzzer seed |
| `FUZZ_MAX_LEN` | `smoke`, `campaign` | `1024` | Maximum input length |
| `FUZZ_TIMEOUT` | `smoke`, `campaign` | `10` | Per-input timeout in seconds |
| `FUZZ_MAX_TOTAL_TIME` | `campaign` | `900` | Per-target campaign duration in seconds |

Counts, lengths, and timeouts must be positive decimal integers. The seed may
be zero. Set `FUZZ_TARGET_TRIPLE` to the installed host target for a local
campaign when it differs from the Linux CI default.

See the [cargo-fuzz book](https://rust-fuzz.github.io/book/introduction.html)
for installation, engine options, minimization, and coverage workflows.

## Checked-in seeds

A checked-in seed is a small reviewed input that ordinary tests replay without
starting libFuzzer. The current seeds cover distinct lifecycle, queue-return,
aliasing, and growth scenarios.

Check in another seed when it:

- reproduces a fixed defect;
- preserves a state transition that is difficult to reach deterministically;
  or
- adds materially distinct coverage after corpus minimization.

Do not check in every input produced by a campaign. Minimize a crash with
`cargo fuzz tmin`, or reduce a non-crashing coverage corpus with
`cargo fuzz cmin`, then inspect the decoded sequence. A checked-in input must
contain complete eight-byte records and remain meaningful across supported
page sizes. The decoder reserves semantic selectors for "all remaining" and
"grow by one carrier" so checked-in scenarios do not depend on modulo results
from one host geometry. Replay tests require every named seed to reach its
documented milestones with 4 KiB, 16 KiB, and 64 KiB carrier geometry when the
runtime page size can represent them.

To add a seed:

1. Copy the minimized input into
   `src/runtime/buffer_pool/tests/corpus/buffer-pool-operations/` in the
   transfer-manager package.
2. Add its name and `include_bytes!` entry to `tests/fuzz_replay.rs`.
3. Run the replay test and a short native campaign.

The manifest test requires the checked-in directory and replay table to match
exactly. Keeping the seeds inside the package also makes source packages
self-contained.

## CI coverage

Required pull-request CI:

- runs every library test in release mode, including typed property sequences,
  decoder tests, and curated replay;
- checks and lints the isolated fuzz package with its lockfile;
- builds every target returned by `cargo fuzz list`; and
- runs a deterministic 4,096-input smoke campaign with a separate writable
  corpus.

The package check verifies the hidden fuzz build shape. Release tests execute
the model and checked-in regressions without libFuzzer. The target build then
validates native libFuzzer instrumentation and linking, and the smoke step
actually executes the engine for a fixed number of deterministic inputs.

Normal debug tests, Miri, ASan, and TSan provide complementary coverage. Miri
skips generated proptest cases and native guard-page faults but replays the
deterministic sequences and checked-in seeds. cargo-fuzz uses ASan for native
campaigns.

The scheduled workflow runs daily at 07:17 UTC and may also be dispatched
manually. It restores an evolving writable corpus from the previous run,
fuzzes each registered target, saves the updated corpus, and uploads both the
corpus and crash artifacts for 30 days. GitHub Actions caches are evictable
and are not source control. CI never commits generated inputs. When a campaign
fails, minimize and review the artifact before adding it as a checked-in
regression seed.

The fuzz package has its own checked-in lockfile because it is excluded from
the main workspace and adds libFuzzer-specific dependencies. Its resolution is
intentionally independent: generated inputs are compatibility evidence across
that graph, while promoted regression seeds must also replay under the main
workspace lock.
