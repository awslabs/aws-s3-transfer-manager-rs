# Developer tool

`tools/tmdev` is the private Rust developer command. Run it through
`just tmdev <command>`, or use `just release` for its release subcommand.
The recipe invokes `cargo build --quiet --locked` before running the executable.
Cargo checks build freshness on each invocation and recompiles only when inputs
have changed. Git and Cargo subprocesses operate on the product repository, not
the tool workspace.

The tool is a separate Cargo workspace, excluded from the product workspace.
Its committed `Cargo.lock` pins developer-tool dependencies independently of
SDK/runtime dependencies. Builds use `--locked`; dependency changes update this
lockfile explicitly. The tool is not published.

## Release commands

`release.toml` contains only the package versions intended for that release.
It is not a workspace inventory. A one-component release contains one entry
when its required dependency versions are already published. The initial
publication can contain all publishable packages. Completed uploads stay in
the intended set when retrying a partial release.

| Command | Checks and effects | Default source ref |
|---|---|---|
| `audit` | Checks published dependency requirements across all publishable workspace members; does not read the release record or write a report | No ref argument; uses the checkout's manifests |
| `prepare` | Runs the audit and prepares or retains the reviewed release record; does not create a Git tag | Checkout HEAD |
| `check` | Runs the audit, validates the source and record, and writes pending/completed registry state | `main` |
| `tag` | Runs audit/source/record checks and creates the recorded local annotated tag; `--push` additionally sends that tag to origin | `HEAD` |
| `publish` | Runs audit/source/record checks, writes registry state, and passes only pending packages to Cargo | `main` |

`audit` evaluates the registry requirements of occupied current dependent
versions against current managed path dependencies. It can run when no release
is being prepared. `check` additionally evaluates a specific reviewed record:
versions must match manifests, omitted required dependencies must be available,
and completed versions must identify the selected clean source commit.

`--ref` is a source assertion, not a checkout operation. Its commit must equal
checkout HEAD. A different commit returns an error; no fallback or branch switch
occurs. Use `--ref HEAD` on a preparation branch. Actual publication requires
`--ref <recorded-tag>` and a clean checkout. `check --require-tag` applies that
same publication source requirement. The workflow first checks out its input
ref and then supplies that ref to the tool.

The commands perform their own prerequisites; running `audit` before `check` or
`publish` is optional. The complete operator sequence is in
[releases.md](releases.md).

## Record lifecycle

Publication leaves `release.toml` unchanged. A completed tagged record remains
until the next preparation. After updating selected manifests, `prepare` checks
that the prior tagged versions are available and not yanked, then writes a new
tag and unpublished-version subset. If nothing new is unpublished, it retains
the old record. Tagged incomplete releases must finish first.

An untagged record is an active preparation: repeated preparation preserves it
and validates its versions against the manifests. Checking a completed record
against later manifests or another source commit can fail; inspect or retry
that release at its original tag. The next release begins with `prepare` rather
than trying to publish the old record at the newer commit.

## Release report

`check` and `publish` generate `target/release/plan.json`; `--output` changes its
location. The file is output, not a publication input or a recovery journal.
Every invocation recomputes state from Cargo manifests, `release.toml`, the
selected source commit, and the registry. Deleting or editing a prior report
does not change package selection.

| Field | Meaning |
|---|---|
| `tag` | Reviewed release tag |
| `source_revision` | Checkout commit verified against the requested ref |
| `inventory` | All publishable workspace package versions |
| `intended` | Full package/version subset from the release record |
| `pending` | Intended versions confirmed absent from the registry |
| `completed` | Intended versions verified against the selected clean commit and package directory |

`check` overwrites the report after successful validation. `publish` overwrites
it before invoking Cargo and after each successful visibility poll following
upload. Dry-run leaves the pre-publication snapshot because it uploads nothing.
An error before the next write leaves the last snapshot, which can still list
successfully uploaded versions as pending. The next invocation rechecks the
registry and replaces that snapshot.

The workflow reads `pending` from the check report to decide whether publishing
credentials are needed, then invokes `publish`, which rechecks registry state.
It saves the report as a run artifact. Report/TOML key order has no selection or
execution meaning; Cargo determines dependency publication order.

Repeated publication of the same reviewed tag skips source-verified completed
versions. A partial retry uploads only the remaining versions; a completed
retry invokes no Cargo publisher and the workflow requests no publishing
credentials. Conflicting source metadata or yanked versions return errors.
This permits resumable publication without making the upload batch atomic.

## Layout

- `src/main.rs`: top-level argument parsing and process exit.
- `src/diagnostics.rs`: stderr progress and opt-in debug output.
- `src/release.rs`: release commands, audit, selection and publication policy.
- `src/release/workspace.rs`: resolved Cargo metadata and the reviewed record.
- `src/release/registry.rs`: registry versions and checksum-verified source metadata.
- `src/release/requirements.rs`: pure published-dependency requirement checks.
- `src/release/git.rs`: source checks and immutable local/remote tags.
- `src/release/test_util.rs`: fake registry, clock, publisher, and local fixtures.
- `tests/release.rs`: compiled CLI checks with real Git/Cargo fixtures.

[UPSTREAM.md](../../tools/tmdev/UPSTREAM.md) records sources and adaptations for
Smithy logic and tests. Update that provenance when bringing across upstream
changes.

## Output and diagnostics

Default stderr records include UTC timestamps with millisecond precision and
report release phases, registry discovery, request retries, and visibility
progress. Registry discovery names the highest non-yanked stable version, or
the highest non-yanked prerelease if no stable version exists. It distinguishes
an absent crate, all-yanked versions, and whether each requested version is
published, yanked, or absent. These summaries do not select release versions.

Results on stdout identify
the release tag, source commit, each intended package's pending/completed state,
and the JSON report path. Cargo verification and publication output streams
through unchanged.

Use `--verbose` or `-v` for developer diagnostics:

```sh
just release check --ref HEAD --verbose
```

Debug output includes command arguments and working directories, captured command
status and output sizes, registry URLs and attempts, occupied/absent version
classifications, and expected versus archived source metadata. Successful
captured command stderr is included. HTTP dependency logs are excluded; request
headers, tokens, and subprocess environments are not logged by the tool.

Failures print the complete error chain and return a nonzero exit status. Failed
captured commands include exit status, working directory, stdout, and stderr.
Publication failure identifies the retained report; visibility timeout names
remaining packages and the report path. Retry the same reviewed record and tag.

## Development

```sh
cargo fmt --manifest-path tools/tmdev/Cargo.toml -- --check
cargo clippy --locked --manifest-path tools/tmdev/Cargo.toml \
    --target-dir tools/tmdev/target --all-targets --no-deps -- -D warnings
cargo test --locked --manifest-path tools/tmdev/Cargo.toml \
    --target-dir tools/tmdev/target --all-targets
```

The required release-tool CI job runs these checks on every PR. Root workspace
checks do not include the tool. Both publishing and test CI cache its separate
build directory using its lockfile and toolchain.

Use the ordinary development profile. Dependency features select the required
CLI and blocking HTTP/TLS functionality. Registry and policy tests run without
credentials or registry uploads; Git push tests target local bare repositories
under the tool's ignored `target/` directory.
