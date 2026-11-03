# Workspace releases

The repository publishes independently versioned crates under a date-based
release tag. `release.toml` records the intended package versions. Cargo
manifests define package versions and dependency requirements; the release
record must match those manifests at the selected source commit.

Use `just release` to prepare, check, or publish a release. The command requires
Python 3.11 or newer and Cargo supporting workspace publication. Release CI
uses the workspace's Rust minimum version. Calling `just release` without
arguments displays the supported commands.

## Prepare a release

1. Update the package versions, workspace dependency requirements, lockfiles,
   and package-labelled changelog entries. If changing the extension's exact
   TM requirement, also bump the extension's package version.
2. Choose a UTC release date. Use `release-YYYY-MM-DD`; subsequent releases
   on that day use `.2`, `.3`, and so on.
3. Generate the record:

   ```sh
   just release prepare --tag release-2026-11-03
   ```

4. Review `release.toml` with the version and changelog changes. Preparation
   selects publishable workspace versions not yet present on crates.io.
   Remove a package from the record to hold it back; required unpublished
   local dependencies must remain included.
5. Validate and verify the packages from the preparation branch:

   ```sh
   just release check --ref HEAD
   just release publish --ref HEAD --dry-run
   ```

6. Merge the preparation PR, then create the recorded tag at the reviewed
   commit. Prepare a draft GitHub release using that existing tag and list
   every intended package/version from the record.

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

Preparation preserves an existing record with the same tag, including during
partial publication. Its recorded versions must still match Cargo metadata.
If updating a preparation record before tagging, edit it in the reviewed PR
or prepare a different tag. A tagged previous release must finish uploading
its intended set before preparation of another release.

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
this report as an artifact. `release.toml` and the source tag remain the durable
release definition.

After publication:

- Confirm every intended version is on crates.io and its documentation builds
  on docs.rs. Managed HTTP's documentation includes its `client` module.
- Check a consumer that resolves the released crates from the registry.
- For a new crate name, establish the normal SDK registry ownership. Package
  author metadata does not configure registry owners.
- Publish the GitHub announcement after the intended set is available.

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
Cargo or fetching publishing credentials.

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

Every PR runs the release-tool tests. A PR changing `release.toml` also checks
its versions and performs credential-free Cargo package verification. Ordinary
source/version updates do not require rewriting the last release's record.
The publication workflow always checks the record at its requested source ref.
