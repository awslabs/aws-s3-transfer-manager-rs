/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// The comparisons sync ships with.
//
// Each mode is a unit struct, and the directions it serves are the impls it has. A mode whose
// answer turns on which side is local writes one impl per direction; a mode that ignores
// direction writes one blanket impl and serves all of them.
//
// All four here serve every direction, so nothing yet relies on the type parameters to refuse a
// pairing. They earn their place for the mode that will: a rule making sense only on a download
// can be written as one impl for that pair of side types, and handing it an upload fails to
// build. A blanket impl
// also spells local-to-local, which the walk never constructs — it builds one local side against
// one listing — so the exclusion comes from there and not from these types.
//
// Timestamps arrive as whole seconds, truncated where the entry was read, so a mode never sees
// the fraction a local filesystem records and S3 does not. That is what makes a pair that once
// agreed keep agreeing: comparing finer local precision against a listing's seconds would find a
// difference on every run.

use aws_sdk_s3::types::Object;

use crate::io::key::stream::Entry;
use crate::io::walk::FsEntry;
use crate::operation::sync::compare::{Compare, Decision, Described, TransferReason, Verdict};

// Send when the sizes differ, or when the source was written later than the destination.
//
// The time test is the one `aws s3 sync` applies, and it reads the same way in both directions:
// send when the local file is newer than the object. For uploading that is what anyone would
// want. For downloading it is backwards — a newer object is exactly what should come down, and
// this is the test that skips it, while a local file someone just edited is overwritten by an
// older object. Kept because parity with the CLI is the baseline, and `ExactTimestamps` is the
// way out.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SizeAndTime;

// Send when the sizes differ, whatever the timestamps say.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SizeOnly;

// Like `SizeAndTime`, except a download leaves a key alone only when the two times are exactly
// equal, so an object written at any other second comes down. Uploads and copies keep
// `SizeAndTime`'s rule.
//
// This mode cannot be used for downloads today, and choosing it costs a full re-transfer of
// every key on every run. Equality becomes reachable once a download stamps the object's
// last-modified time onto the file it writes.
// Nothing here does that, so a downloaded file carries the time it was written, the two sides
// never match, and every key is sent on every run — the case this mode is meant to cure.
//
// Uploading and copying can never reach it at all: S3 stamps its own time when it stores an
// object and never carries the source's across. That is why those two keep the default rule.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ExactTimestamps;

// Never write over a key the destination already holds. Only keys the destination lacks are sent.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct NoOverwrite;

// Which built-in comparison a run uses.
//
// The trait is how a caller plugs in their own; this is the shortcut for callers wanting one of
// ours, chosen from a flag or a config value without naming a type.
//
// A match on this is exhaustive, so adding a mode breaks every match until each one says what it
// does with the new variant. That is what is wanted while the type reaches nobody outside this
// crate. Publishing it means saying the opposite — that a caller's match has to expect variants it
// has not seen — and comparing checksums and comparing properties a caller picks are both waiting
// to be added.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Mode {
    #[default]
    SizeAndTime,
    SizeOnly,
    ExactTimestamps,
    NoOverwrite,
}

// Picking one, per direction.
//
// `Mode` deliberately does not implement `Compare`. An impl would have to forward each method, and
// a method added later would go unforwarded — `Mode` would answer the trait's default while the
// selected mode's override sat unused, which is the bug this selector exists to avoid, one level
// up. Handing back the mode itself means every method it defines is the one that runs, now and
// after the trait grows.
macro_rules! picker {
    ($picker:ident, $source:ty, $destination:ty, $doc:literal) => {
        impl Mode {
            #[doc = $doc]
            pub(crate) fn $picker(&self) -> &'static dyn Compare<$source, $destination> {
                match self {
                    Self::SizeAndTime => &SizeAndTime,
                    Self::SizeOnly => &SizeOnly,
                    Self::ExactTimestamps => &ExactTimestamps,
                    Self::NoOverwrite => &NoOverwrite,
                }
            }
        }
    };
}

picker!(
    uploading,
    FsEntry,
    Object,
    "The comparison this mode makes for an upload."
);
picker!(
    downloading,
    Object,
    FsEntry,
    "The comparison this mode makes for a download."
);
picker!(
    copying,
    Object,
    Object,
    "The comparison this mode makes for a copy between two buckets."
);

// How much later the destination was written than the source. Negative when the source is newer.
fn delta<S, D>(source: &Described<'_, S>, destination: &Described<'_, D>) -> i64 {
    destination.last_modified_secs() - source.last_modified_secs()
}

fn by_size(source: u64, destination: u64) -> Verdict {
    if source == destination {
        unchanged()
    } else {
        send(TransferReason::SizeDiffers)
    }
}

fn send(reason: TransferReason) -> Verdict {
    Verdict::decided(Decision::transfer(reason))
}

fn unchanged() -> Verdict {
    Verdict::decided(Decision::skip_unchanged())
}

fn destination_exists() -> Verdict {
    Verdict::decided(Decision::skip_destination_exists())
}

// Size first, then the time test.
//
// A pair differing in both is reported as differing in size, which is the answer a caller can act
// on: two sizes settle it without knowing anything about clocks.
fn by_size_then<S, D>(
    source: &Described<'_, S>,
    destination: &Described<'_, D>,
    leave_alone: impl Fn(i64) -> bool,
) -> Verdict {
    if source.size() != destination.size() {
        send(TransferReason::SizeDiffers)
    } else if leave_alone(delta(source, destination)) {
        unchanged()
    } else {
        send(TransferReason::TimeDiffers)
    }
}

// Uploading: leave it while the object is at least as new as the file.
impl Compare<FsEntry, Object> for SizeAndTime {
    fn compare_described(
        &self,
        source: Described<'_, FsEntry>,
        destination: Described<'_, Object>,
    ) -> Verdict {
        by_size_then(&source, &destination, |delta| delta >= 0)
    }
}

// Copying: the same rule, with an object on both sides.
impl Compare<Object, Object> for SizeAndTime {
    fn compare_described(
        &self,
        source: Described<'_, Object>,
        destination: Described<'_, Object>,
    ) -> Verdict {
        by_size_then(&source, &destination, |delta| delta >= 0)
    }
}

// Downloading: leave it while the object is at least as new as the file. That is the same relation
// uploading tests, and the sign flips because the object is the source here — which is what keeps a
// newer object on the service.
impl Compare<Object, FsEntry> for SizeAndTime {
    fn compare_described(
        &self,
        source: Described<'_, Object>,
        destination: Described<'_, FsEntry>,
    ) -> Verdict {
        by_size_then(&source, &destination, |delta| delta <= 0)
    }
}

// One impl for every direction, since a size means the same thing whichever entry reported it.
impl<S, D> Compare<S, D> for SizeOnly {
    fn compare_described(
        &self,
        source: Described<'_, S>,
        destination: Described<'_, D>,
    ) -> Verdict {
        by_size(source.size(), destination.size())
    }

    // A timestamp nobody could read says nothing to a mode that ignores timestamps, so the sizes
    // still settle it. Sending the key here would re-send it on every run over a field this mode
    // never consults.
    fn compare_undescribed(&self, source: &Entry<S>, destination: &Entry<D>) -> Verdict {
        match (source.meta.size, destination.meta.size) {
            (Some(source), Some(destination)) => by_size(source, destination),
            // The size itself is what went unread, so there is nothing left to compare.
            _ => send(TransferReason::Undescribable),
        }
    }
}

// Unchanged from the default rule, by deferring to it: changing that rule changes this one.
impl Compare<FsEntry, Object> for ExactTimestamps {
    fn compare_described(
        &self,
        source: Described<'_, FsEntry>,
        destination: Described<'_, Object>,
    ) -> Verdict {
        SizeAndTime.compare_described(source, destination)
    }

    fn compare_undescribed(&self, source: &Entry<FsEntry>, destination: &Entry<Object>) -> Verdict {
        SizeAndTime.compare_undescribed(source, destination)
    }
}

// Unchanged from the default rule, by deferring to it: changing that rule changes this one.
impl Compare<Object, Object> for ExactTimestamps {
    fn compare_described(
        &self,
        source: Described<'_, Object>,
        destination: Described<'_, Object>,
    ) -> Verdict {
        SizeAndTime.compare_described(source, destination)
    }

    fn compare_undescribed(&self, source: &Entry<Object>, destination: &Entry<Object>) -> Verdict {
        SizeAndTime.compare_undescribed(source, destination)
    }
}

// The one direction this mode changes.
impl Compare<Object, FsEntry> for ExactTimestamps {
    fn compare_described(
        &self,
        source: Described<'_, Object>,
        destination: Described<'_, FsEntry>,
    ) -> Verdict {
        by_size_then(&source, &destination, |delta| delta == 0)
    }
}

impl<S, D> Compare<S, D> for NoOverwrite {
    // Both sides hold something, so the destination has this key and it stays as it is. Neither
    // size nor time is read.
    fn compare_described(&self, _: Described<'_, S>, _: Described<'_, D>) -> Verdict {
        destination_exists()
    }

    // Neither entry is read. A mode comparing two sides sends a pair it could not describe,
    // which is safe when nothing can show they match. Here the destination holds the key, and
    // what went unread about it changes nothing.
    fn compare_undescribed(&self, _: &Entry<S>, _: &Entry<D>) -> Verdict {
        destination_exists()
    }
}

#[cfg(test)]
mod tests {
    use crate::io::key::stream::{Entry, EntryMeta, KeysLost};
    use crate::operation::sync::compare::SkipReason;
    use crate::operation::sync::walk::{Pairing, SideState};

    use super::*;

    fn object(size: u64, secs: i64) -> Entry<Object> {
        described(Some(size), Some(secs))
    }

    fn described(size: Option<u64>, secs: Option<i64>) -> Entry<Object> {
        Entry {
            key: "a.txt".to_string(),
            meta: EntryMeta {
                size,
                last_modified_secs: secs,
                obstruction: None,
            },
            source: Object::builder().key("a.txt").build(),
        }
    }

    // Copying puts an object on both sides, so every mode with a blanket impl and both rules
    // shared by uploads can be asked without touching a filesystem.
    fn copying(source: Entry<Object>, destination: Entry<Object>) -> Pairing<Object, Object> {
        Pairing::new(
            "a.txt".to_string(),
            SideState::Present(source),
            SideState::Present(destination),
        )
    }

    #[test]
    fn a_pair_of_the_same_size_and_time_is_left_alone() {
        assert_eq!(
            SizeAndTime.compare(&copying(object(1, 100), object(1, 100))),
            unchanged()
        );
    }

    #[test]
    fn a_size_difference_is_reported_ahead_of_a_time_difference() {
        // The times here say leave it alone — the destination was written later — and the sizes
        // disagree. That combination is what makes the order matter: asking about the times first
        // would answer "unchanged" and leave a destination of the wrong length that way for good.
        // A pair where both tests point the same way proves nothing about which one ran.
        assert_eq!(
            SizeAndTime.compare(&copying(object(1, 50), object(2, 100))),
            send(TransferReason::SizeDiffers),
            "a length that disagrees is a difference no timestamp can excuse"
        );
    }

    #[test]
    fn a_newer_source_is_sent() {
        // delta is 50 - 100, below zero, so the test fails and the key goes.
        assert_eq!(
            SizeAndTime.compare(&copying(object(1, 100), object(1, 50))),
            send(TransferReason::TimeDiffers)
        );
    }

    #[test]
    fn a_newer_destination_is_left_alone() {
        assert_eq!(
            SizeAndTime.compare(&copying(object(1, 50), object(1, 100))),
            unchanged()
        );
    }

    #[test]
    fn size_only_ignores_a_time_difference_in_either_direction() {
        assert_eq!(
            SizeOnly.compare(&copying(object(1, 100), object(1, 50))),
            unchanged()
        );
        assert_eq!(
            SizeOnly.compare(&copying(object(1, 50), object(1, 100))),
            unchanged()
        );
    }

    #[test]
    fn size_only_sends_a_size_difference() {
        assert_eq!(
            SizeOnly.compare(&copying(object(1, 100), object(2, 100))),
            send(TransferReason::SizeDiffers)
        );
    }

    #[test]
    fn size_only_still_compares_sizes_when_a_timestamp_went_unread() {
        // A timestamp nobody read says nothing to a mode ignoring timestamps. Sending the key
        // instead would re-send it on every run over a field this mode never consults.
        assert_eq!(
            SizeOnly.compare(&copying(object(7, 100), described(Some(7), None))),
            unchanged()
        );
        assert_eq!(
            SizeOnly.compare(&copying(object(7, 100), described(Some(9), None))),
            send(TransferReason::SizeDiffers)
        );
    }

    #[test]
    fn size_only_sends_a_pair_whose_size_went_unread() {
        // The one field this mode reads is the one missing, so there is nothing left to compare.
        assert_eq!(
            SizeOnly.compare(&copying(object(7, 100), described(None, Some(100)))),
            send(TransferReason::Undescribable)
        );
    }

    #[test]
    fn a_mode_reading_both_fields_sends_a_pair_with_an_unread_timestamp() {
        // The same pairing size-only skips. Two modes differ here because one consults the field
        // that went missing.
        assert_eq!(
            SizeAndTime.compare(&copying(object(7, 100), described(Some(7), None))),
            send(TransferReason::Undescribable)
        );
    }

    #[test]
    fn exact_timestamps_keeps_the_upload_rule_where_nothing_is_downloaded() {
        // A newer destination is still left alone for a copy, because a copy can never reach an
        // exact match: S3 stamps its own time on what it stores.
        assert_eq!(
            ExactTimestamps.compare(&copying(object(1, 50), object(1, 100))),
            unchanged()
        );
    }

    #[test]
    fn no_overwrite_leaves_a_key_the_destination_holds() {
        assert_eq!(
            NoOverwrite.compare(&copying(object(1, 50), object(9, 100))),
            destination_exists(),
            "neither size nor time is consulted"
        );
    }

    #[test]
    fn no_overwrite_sends_a_key_the_destination_lacks() {
        let pairing: Pairing<Object, Object> = Pairing::new(
            "a.txt".to_string(),
            SideState::Present(object(1, 100)),
            SideState::Absent,
        );
        assert_eq!(NoOverwrite.compare(&pairing), send(TransferReason::Missing));
    }

    #[test]
    fn no_overwrite_leaves_a_pair_it_could_not_describe() {
        // Every other mode sends this, which is safe when a comparison cannot show a match. Here
        // it would write over a key the destination holds, which is what this mode prevents.
        let unread = described(None, Some(100));
        assert_eq!(
            NoOverwrite.compare(&copying(object(1, 100), unread.clone())),
            destination_exists()
        );
        assert_eq!(
            SizeAndTime.compare(&copying(object(1, 100), unread)),
            send(TransferReason::Undescribable),
            "what every other mode answers for the same pair"
        );
    }

    #[test]
    fn no_overwrite_still_reports_a_key_nobody_could_read() {
        // Answering one case differently must not cost the answers shared with every mode.
        let pairing: Pairing<Object, Object> = Pairing::new(
            "a.txt".to_string(),
            SideState::Unknown(KeysLost::UnknownRange),
            SideState::Present(object(1, 100)),
        );
        let Verdict::Decided(Decision::Skip(skip)) = NoOverwrite.compare(&pairing) else {
            panic!("expected a skip")
        };
        assert_eq!(skip.reason(), SkipReason::Unknown);
        assert_eq!(skip.keys_lost(), Some(KeysLost::UnknownRange));
    }

    // A real local entry, because `FsEntry` holds filesystem metadata and only a walk can make
    // one. What each test varies is the meta beside it, which is what a mode reads; the entry
    // itself is here so the pairing has the local side's type.
    async fn a_local_entry() -> (tempfile::TempDir, FsEntry) {
        use crate::io::walk::{FsWalkContext, FsWalker};

        let dir = tempfile::tempdir().expect("a temp dir");
        std::fs::write(dir.path().join("a.txt"), b"x").expect("a regular file");
        let mut walk = FsWalker::builder()
            .build()
            .walk(FsWalkContext::builder().root(dir.path()).build());
        let entry = walk
            .next()
            .await
            .expect("the file")
            .expect("this tree reads cleanly");
        (dir, entry)
    }

    fn local(size: u64, secs: i64, source: FsEntry) -> Entry<FsEntry> {
        Entry {
            key: "a.txt".to_string(),
            meta: EntryMeta {
                size: Some(size),
                last_modified_secs: Some(secs),
                obstruction: None,
            },
            source,
        }
    }

    fn downloading(source: Entry<Object>, destination: Entry<FsEntry>) -> Pairing<Object, FsEntry> {
        Pairing::new(
            "a.txt".to_string(),
            SideState::Present(source),
            SideState::Present(destination),
        )
    }

    fn uploading(source: Entry<FsEntry>, destination: Entry<Object>) -> Pairing<FsEntry, Object> {
        Pairing::new(
            "a.txt".to_string(),
            SideState::Present(source),
            SideState::Present(destination),
        )
    }

    #[tokio::test]
    async fn downloading_leaves_a_newer_object_on_the_service() {
        // The backwards half of the default rule, and the reason `ExactTimestamps` exists. The
        // object was written later than the file, and this is the test that skips it.
        let (_dir, fs) = a_local_entry().await;
        assert_eq!(
            SizeAndTime.compare(&downloading(object(1, 200), local(1, 100, fs))),
            unchanged()
        );
    }

    #[tokio::test]
    async fn downloading_overwrites_a_newer_file_with_an_older_object() {
        // The other half: a file someone just edited is replaced by an older object.
        let (_dir, fs) = a_local_entry().await;
        assert_eq!(
            SizeAndTime.compare(&downloading(object(1, 50), local(1, 100, fs))),
            send(TransferReason::TimeDiffers)
        );
    }

    #[tokio::test]
    async fn exact_timestamps_brings_a_newer_object_down() {
        // The same pairing the default rule skips.
        let (_dir, fs) = a_local_entry().await;
        assert_eq!(
            ExactTimestamps.compare(&downloading(object(1, 200), local(1, 100, fs))),
            send(TransferReason::TimeDiffers)
        );
    }

    #[tokio::test]
    async fn exact_timestamps_leaves_a_download_whose_times_match() {
        // The state is built here by hand and no run produces it: a downloaded file carries the
        // time it was written, so the two sides never agree until a download stamps the object's
        // time onto the file. This pins the rule for when that arrives.
        let (_dir, fs) = a_local_entry().await;
        assert_eq!(
            ExactTimestamps.compare(&downloading(object(1, 100), local(1, 100, fs))),
            unchanged()
        );
    }

    #[tokio::test]
    async fn a_size_difference_decides_even_where_the_times_say_leave_it() {
        // The same precedence on the upload path, which reaches it through its own impl. The copy
        // test above carries why the order matters.
        let (_dir, fs) = a_local_entry().await;
        assert_eq!(
            SizeAndTime.compare(&uploading(local(1, 100, fs), object(9, 200))),
            send(TransferReason::SizeDiffers),
            "a length that disagrees is a difference no timestamp can excuse"
        );
    }

    #[tokio::test]
    async fn uploading_sends_a_newer_file() {
        let (_dir, fs) = a_local_entry().await;
        assert_eq!(
            SizeAndTime.compare(&uploading(local(1, 200, fs), object(1, 100))),
            send(TransferReason::TimeDiffers)
        );
    }

    #[tokio::test]
    async fn uploading_leaves_a_file_older_than_the_object() {
        let (_dir, fs) = a_local_entry().await;
        assert_eq!(
            SizeAndTime.compare(&uploading(local(1, 50, fs), object(1, 100))),
            unchanged()
        );
    }

    #[test]
    fn picking_a_mode_by_name_answers_what_that_mode_answers() {
        // Sizes match, the source is newer, and the destination holds the key. Three answers,
        // because exact timestamps keeps the default rule for a copy — so this pairing cannot
        // tell those two apart, and `picking_exact_timestamps_by_name_changes_a_download` is what
        // separates them.
        let pairing = || copying(object(1, 100), object(1, 50));
        for (mode, expected) in [
            (Mode::SizeAndTime, send(TransferReason::TimeDiffers)),
            (Mode::SizeOnly, unchanged()),
            (Mode::ExactTimestamps, send(TransferReason::TimeDiffers)),
            (Mode::NoOverwrite, destination_exists()),
        ] {
            assert_eq!(mode.copying().compare(&pairing()), expected, "{mode:?}");
        }
    }

    #[test]
    fn picking_no_overwrite_by_name_keeps_what_it_answers_for_an_unreadable_field() {
        // The case it decides differently from every other mode. Forwarding only the first trait
        // method would send this pair, overwriting the key the mode exists to protect, and the
        // test above would still pass.
        let unread = described(None, Some(100));
        assert_eq!(
            Mode::NoOverwrite
                .copying()
                .compare(&copying(object(1, 100), unread.clone())),
            destination_exists()
        );
        assert_eq!(
            Mode::SizeAndTime
                .copying()
                .compare(&copying(object(1, 100), unread)),
            send(TransferReason::Undescribable),
            "what the other modes answer for the same pair"
        );
    }

    #[tokio::test]
    async fn picking_exact_timestamps_by_name_changes_a_download() {
        // The only thing this mode changes, and the one direction it changes it in. A copy cannot
        // show it, because there the mode keeps the default rule by design — so wiring this
        // variant to the default would pass every other selector test.
        let (_dir, fs) = a_local_entry().await;
        let newer_object = downloading(object(1, 200), local(1, 100, fs));
        assert_eq!(
            Mode::ExactTimestamps.downloading().compare(&newer_object),
            send(TransferReason::TimeDiffers),
            "a newer object comes down"
        );
        assert_eq!(
            Mode::SizeAndTime.downloading().compare(&newer_object),
            unchanged(),
            "where the default rule leaves it on the service"
        );
    }

    #[tokio::test]
    async fn a_mode_picked_by_name_serves_every_direction() {
        let (_down_dir, down) = a_local_entry().await;
        assert_eq!(
            Mode::SizeOnly
                .downloading()
                .compare(&downloading(object(1, 200), local(1, 100, down))),
            unchanged(),
            "sizes match, and this mode reads nothing else"
        );
        let (_up_dir, up) = a_local_entry().await;
        assert_eq!(
            Mode::SizeOnly
                .uploading()
                .compare(&uploading(local(1, 200, up), object(2, 100))),
            send(TransferReason::SizeDiffers)
        );
    }

    #[test]
    fn the_default_mode_is_the_one_the_cli_applies() {
        assert_eq!(Mode::default(), Mode::SizeAndTime);
    }

    #[tokio::test]
    async fn equal_times_leave_an_upload_and_a_download_alone() {
        // The boundary both predicates turn on. Narrowing either to a strict comparison would send
        // every key whose sides already agree, on every run, for as long as the pair exists.
        let (_up_dir, up) = a_local_entry().await;
        assert_eq!(
            SizeAndTime.compare(&uploading(local(1, 100, up), object(1, 100))),
            unchanged(),
            "an upload whose object carries the file's own second"
        );
        let (_down_dir, down) = a_local_entry().await;
        assert_eq!(
            SizeAndTime.compare(&downloading(object(1, 100), local(1, 100, down))),
            unchanged(),
            "and a download, once a written file carries the object's second"
        );
    }

    #[tokio::test]
    async fn exact_timestamps_leaves_an_upload_the_default_rule_leaves() {
        // This mode's upload impl, which no other test reaches. It delegates, so what it must show
        // is that the delegation is wired: a newer object is left alone, as the default rule says.
        let (_dir, fs) = a_local_entry().await;
        assert_eq!(
            ExactTimestamps.compare(&uploading(local(1, 50, fs), object(1, 100))),
            unchanged()
        );
    }

    #[test]
    fn a_pair_that_agrees_still_agrees_on_a_second_run() {
        // Timestamps are whole seconds by the time they arrive, so no mode can find a difference
        // that was absent the first time. Without that, an unchanged pair flips between sending
        // and skipping forever.
        let at = 1_700_000_000;
        for _ in 0..2 {
            assert_eq!(
                SizeAndTime.compare(&copying(object(7, at), object(7, at))),
                unchanged()
            );
            assert_eq!(
                SizeOnly.compare(&copying(object(7, at), object(7, at))),
                unchanged()
            );
            assert_eq!(
                ExactTimestamps.compare(&copying(object(7, at), object(7, at))),
                unchanged()
            );
        }
    }
}
