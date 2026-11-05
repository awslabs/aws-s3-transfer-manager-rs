# Workspace releases

The repository publishes independently versioned crates under a date-based
release tag. `release.toml` records the intended package versions. Cargo
manifests define package versions and dependency requirements; the release
record must match those manifests at the selected source commit.

The record contains only the package versions intended for that release, not
all workspace members. A component-only release can contain one entry; the
initial publication can include every publishable package. Required unpublished
local dependencies must also be included. Completed uploads remain in the
record when retrying.

Use `just release` to prepare, audit, check, tag, or publish a release. It wraps
`tmdev release`, building the private developer tool with its own committed
lockfile. The command requires just and Cargo supporting workspace publication.
Release CI uses the workspace's Rust minimum version. Calling `just release`
without arguments displays the supported commands. See [tmdev.md](tmdev.md)
for tool maintenance, output, and `--verbose` diagnostics.

## Source and command checks

`audit` checks published requirements across the workspace without reading the
release record. `check` runs that audit and additionally validates the selected
source, record versions, dependency availability, and completed package source
identity. `prepare`, `tag`, and `publish` also run the audit; separate audit/check
invocations are not required before local publication.

The CLI defaults `check` and `publish` to `--ref main`, and `tag` to `--ref HEAD`.
A requested ref must resolve to checkout HEAD; a mismatch returns an error.
The tool does not check out another ref or fall back to HEAD. Preparation
branches use `--ref HEAD`; actual publication uses the recorded tag. The
publishing workflow checks out its input ref before invoking the tool.

## Prepare a release

1. Update the package versions, workspace dependency requirements, lockfiles,
   and package-labelled changelog entries. If changing the extension's exact
   TM requirement, also bump the extension's package version.
2. Run the dependency audit. It checks published normal/build requirements
   against current managed workspace versions, including optional, renamed,
   and target-specific dependencies. An occupied dependent version with an
   incompatible requirement needs a new unpublished version; editing only
   its current requirement cannot repair published metadata.

   ```sh
   just release audit
   ```

3. Generate the record:

   ```sh
   just release prepare
   ```

   Preparation selects a UTC date tag from local and origin tags:
   `release-YYYY-MM-DD`, then `.2`, `.3`, and so on. Remote lookup failure is
   an error. Use `--tag release-YYYY-MM-DD` to specify a planned date.
   Preparation does not reserve the tag on origin.

4. Review `release.toml` with the version and changelog changes. Preparation
   selects publishable workspace versions not yet present on crates.io.
   Remove a package from the record to hold it back; required unpublished
   local dependencies must remain included.
5. Validate and verify the packages from the preparation branch:

   ```sh
   just release check --ref HEAD
   just release publish --ref HEAD --dry-run
   ```

6. Merge the preparation PR. In a clean checkout of its reviewed commit,
   create and push the recorded annotated tag:

   ```sh
   just release tag --ref HEAD --push
   ```

   Omit `--push` to create the local tag for inspection. Repeating the command
   succeeds when existing tags identify the same commit; conflicting tags
   fail. Pushing sends only the recorded tag and never uses force. Prepare a
   draft GitHub release using that tag and list every intended package/version
   from the record.

For example:

```toml
[release]
tag = "release-2026-11-03"

[release.crates]
aws-smithy-topology = "0.1.0"
aws-smithy-execution = "0.1.0"
aws-smithy-managed-http = "0.1.0"
aws-sdk-s3-transfer-manager = "0.4.0"
aws-sdk-s3-transfer-manager-ext = "0.1.0"
```

Automatic preparation preserves an existing untagged record across repeated
invocations and date changes. Its recorded versions must still match Cargo
metadata. Publication retries use the same tag and record.
If updating a preparation record before tagging, edit it in the reviewed PR
or prepare a different tag with `--tag`. A tagged previous release must finish
uploading its intended set before preparation of another release.

When nothing new is unpublished, preparation leaves the existing record alone.
Manual version review determines which changes need a new version; the helper
does not infer version bumps from source changes or audit SemVer compatibility.

Local verification of uncommitted changes can use:

```sh
just release publish --ref HEAD --dry-run --allow-dirty
```

`--allow-dirty` is accepted only for dry-run. Publication requires a clean
checkout.

## Publish

Dispatch **Publish to crates.io** with:

```yaml
ref: release-2026-11-03
dry_run: true
```

After verification succeeds, repeat with `dry_run: false`. Actual publication
requires the input to be the tag in `release.toml`, and that tag must resolve
to the checkout's HEAD. Dry-run also accepts a branch or commit.

The workflow validates the source and record before fetching publishing
credentials. It uses the existing AWS role and registry-token secret only
when publication has pending packages. Publication runs are serialized without
cancelling an upload already in progress.

The helper passes pending packages to native Cargo workspace publishing.
Cargo orders dependencies and verifies packaged code. Verification enables
managed HTTP's `client` feature when that package is selected; optional feature
configurations are covered by feature CI. Credentials are unnecessary for
dry-run.

`target/release/plan.json` records the source commit, the full workspace
version inventory, the intended subset, and completed/pending entries. CI saves
this report as an artifact. `check` writes it after successful validation;
`publish` writes it before Cargo and after each successful post-upload visibility
poll. Dry-run leaves the pre-upload snapshot. A failure can leave that snapshot
listing versions that were subsequently uploaded as pending.

The report is never read by the tool to select packages or resume publication;
every invocation rechecks source and registry state. The workflow reads its
pending list to decide whether publishing credentials are needed. Key order
has no publication meaning; Cargo orders selected dependencies. `release.toml`
and the source tag remain the durable release definition. See [tmdev.md](tmdev.md)
for the report fields and output contract.

After publication:

- Confirm every intended version is on crates.io and its documentation builds
  on docs.rs. Managed HTTP's documentation includes its `client` module.
- Check a consumer that resolves the released crates from the registry.
- For a new crate name, establish the normal SDK registry ownership. Package
  author metadata does not configure registry owners.
- Open the saved draft GitHub release, check its tag and complete package/version
  list against the record, and choose **Publish release** after those checks pass.
  This publishes release notes; the workflow has already uploaded the crates.

## After a release and before the next one

Leave `release.toml` unchanged after publication. It records the last intended
release, not a queue of unpublished work. Do not clear its entries or write
completed/pending state back into it. The release tag preserves that record and
its matching manifests.

To prepare the next release:

1. Update selected package versions, dependency requirements, lockfiles, and
   changelog entries.
2. Run `just release prepare`. For a tagged previous record, it checks that the
   recorded versions are available and not yanked. It then replaces the record
   with a new tag and current publishable versions absent from the registry.
3. Review the new subset and run `check` and dry-run verification as described
   in *Prepare a release*.

If no current versions are unpublished, preparation reports that there is
nothing new and leaves the old record unchanged. A tagged incomplete release
must finish before another is prepared. An untagged record remains an active
preparation: repeated preparation preserves it and requires its versions to
match the manifests.

An old completed record can fail `check` against a newer main commit because
its versions or published source metadata no longer match that checkout.
Retry or inspect that release at its original tag. Ordinary PR CI runs the
dependency audit and validates/verifies the record only when the record changes.
Changing package versions during development does not require rewriting the
last completed record on every PR.

## Independent component releases

A component release uses the same record and workflow. For example, bumping
execution from 0.1.0 to 0.1.1 while all other current versions remain published
produces a one-package record. TM and the other crates retain their versions.
The new date tag still contains the entire repository.

A compatible dependency requirement can remain unchanged. A requirement that
must change is updated in its Cargo manifest or workspace declaration; releasing
that changed dependent also requires a new package version. The extension's
exact TM dependency must name the TM version released with it or already
available on the registry.

Keep package/version-labelled entries in the root changelog. The GitHub
announcement lists the intended release subset.

## Resume a partial release

If publication stops after uploading some packages, retry the same tag with the
same record. The helper checks registry state and omits completed entries from
the next Cargo invocation. An entirely completed set succeeds without invoking
Cargo or fetching publishing credentials. Repeated workflow runs at the same
reviewed tag therefore skip completed uploads. The report is regenerated on
each run and need not be retained for recovery; the upload batch is not atomic.

A recorded version is treated as completed only when the registry package's
Cargo VCS metadata identifies the release commit, the expected package directory,
and a clean worktree. Conflicting or missing Cargo VCS metadata fails, as does a yanked recorded
version; an occupied version cannot be uploaded again. Registry request failures do not
mean that a version is absent.

Retain the successful uploads. Do not move an already-used tag or regenerate a
smaller record to describe only the remainder. The announcement continues to
list the complete intended set. After an upload succeeds, the registry index
can take time to expose it; retry the same release after visibility converges.

## CI coverage

Every PR formats, lints and tests the locked developer tool and audits published
workspace dependency requirements. A PR changing `release.toml` also checks
its versions and performs credential-free Cargo package verification. Ordinary
source/version updates do not require rewriting the last release's record.
The publication workflow always checks the record at its requested source ref.
