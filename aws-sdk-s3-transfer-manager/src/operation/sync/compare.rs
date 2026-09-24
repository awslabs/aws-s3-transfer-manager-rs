/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// Turning what two sides hold at one key into what should happen to it.
//
// A comparison is handed a key and what each side holds there, and answers with an action and
// the reason it chose it. The reason travels with the action because a run reports per key: a
// caller asking why a file was sent again has to be answered from the plan itself.
//
// Deciding one key reads that key alone, so this is a plain function with no I/O reachable from
// it. Comparing bytes or asking the service about an object would need a request per key, which
// is what the answer type leaves room for and nothing here does.
//
// Each action is a struct of its own read through accessors, rather than a reason held directly
// by the variant. A variant holding its reason has nowhere to put a second piece of context,
// and adding one means changing the variant's shape, which breaks every match written against
// it. The struct costs nothing today and keeps that change cheap.

use crate::io::key::stream::{Entry, KeysLost, Obstruction};
use crate::operation::sync::walk::{Pairing, SideState};

// What should happen to one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Decision {
    Transfer(Transfer),
    Skip(Skip),
    Delete(Delete),
}

impl Decision {
    pub(crate) fn transfer(reason: TransferReason) -> Self {
        Self::Transfer(Transfer { reason })
    }

    // The two sides matched, so the key stays as it is.
    pub(crate) fn skip_unchanged() -> Self {
        Self::Skip(Skip {
            cause: Cause::Unchanged,
        })
    }

    // The destination already holds this key and the mode refuses to write over one.
    pub(crate) fn skip_destination_exists() -> Self {
        Self::Skip(Skip {
            cause: Cause::DestinationExists,
        })
    }

    // A mode asked to be called again and nothing here does that.
    pub(crate) fn skip_deferred() -> Self {
        Self::Skip(Skip {
            cause: Cause::Deferred,
        })
    }

    // A skip for a key one side is in the way of, carrying what was in the way.
    pub(crate) fn skip_obstructed(obstruction: Obstruction) -> Self {
        Self::Skip(Skip {
            cause: Cause::Obstructed(obstruction),
        })
    }

    // A skip for a key a side could not account for, carrying how much that side lost.
    pub(crate) fn skip_unknown(lost: KeysLost) -> Self {
        Self::Skip(Skip {
            cause: Cause::Unknown(lost),
        })
    }

    pub(crate) fn delete() -> Self {
        Self::Delete(Delete {})
    }
}

// Sending this key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Transfer {
    reason: TransferReason,
}

impl Transfer {
    pub(crate) fn reason(&self) -> TransferReason {
        self.reason
    }
}

// Leaving this key alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Skip {
    cause: Cause,
}

// A reason together with whatever that reason carries.
//
// Private, and the only way a skip is built, so a reason needing a detail cannot be stated without
// one. What a caller reads is the flat reason beside the accessors below: a reporter grouping keys
// by why they were skipped should not have to unwrap a payload to do it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cause {
    Unchanged,
    DestinationExists,
    Deferred,
    Unknown(KeysLost),
    Obstructed(Obstruction),
}

impl Skip {
    pub(crate) fn reason(&self) -> SkipReason {
        match self.cause {
            Cause::Unchanged => SkipReason::Unchanged,
            Cause::DestinationExists => SkipReason::DestinationExists,
            Cause::Deferred => SkipReason::Deferred,
            Cause::Unknown(_) => SkipReason::Unknown,
            Cause::Obstructed(_) => SkipReason::Obstructed,
        }
    }

    // How much the side lost, for a skip that came from a side losing track.
    pub(crate) fn keys_lost(&self) -> Option<KeysLost> {
        match self.cause {
            Cause::Unknown(lost) => Some(lost),
            // Spelled out, so a cause added later has to say whether it lost keys.
            Cause::Unchanged
            | Cause::DestinationExists
            | Cause::Deferred
            | Cause::Obstructed(_) => None,
        }
    }

    // What was in the way, for a skip that came from one side being so.
    pub(crate) fn obstruction(&self) -> Option<Obstruction> {
        match self.cause {
            Cause::Obstructed(obstruction) => Some(obstruction),
            Cause::Unchanged | Cause::DestinationExists | Cause::Deferred | Cause::Unknown(_) => {
                None
            }
        }
    }
}

// Removing this key from the destination.
//
// Empty today, and a struct because a second way to reach a delete is already planned: an
// opt-in that removes destination entries the filters excluded. That will want to say which
// happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Delete {}

// Why a key is being sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransferReason {
    // The destination has nothing at this key.
    Missing,
    SizeDiffers,
    TimeDiffers,
    // A side could not describe what it holds, so no comparison can show the two match.
    Undescribable,
}

// Why a key is being left alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipReason {
    Unchanged,
    // A side could not account for this key, so its position says nothing about it. How much it
    // lost is on the skip itself.
    Unknown,
    // One side is in the way: the name holds nothing a transfer could read, or what it holds
    // cannot be read where it sits. Which one is on the skip itself. The name being occupied is
    // what keeps the matching key on the other side from being deleted.
    Obstructed,
    // The destination already holds this key, and the mode deciding refuses to write over one.
    // Separate from `Unchanged` because the two sides were never compared.
    DestinationExists,
    // A mode asked to be called again. Every mode shipped here answers immediately, so a run
    // that reaches this has a mode promising more than the plan can carry out.
    Deferred,
}

// A comparison's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    Decided(Decision),
    // Room for a comparison that reads bytes or makes a request before it can answer, which is
    // what comparing checksums or object properties needs. Nothing produces it yet; the case is
    // here because a comparison a caller wrote has to be able to say it from the start.
    Deferred(Deferred),
}

impl Verdict {
    pub(crate) fn decided(decision: Decision) -> Self {
        Self::Decided(decision)
    }
}

// Ask this comparison again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Deferred {}

// A side with nothing in the way of a transfer, and both fields a comparison reads.
//
// Reaching a mode in this shape is what keeps `Option` out of a comparison. Both fields are
// optional on the way in, and `None` is less than every `Some`, so a source with no time
// compares as older than a destination that has one and the key is skipped every run.
//
// Both are required together, which narrows a mode needing only one of them: a size-only
// comparison re-sends an entry whose size was readable and whose time was not. Accepted, because
// a listing always reports both and the alternative hands `Option` back to every mode to get
// wrong. A mode needing one field is free to read the entry for it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Described<'a, T> {
    size: u64,
    last_modified_secs: i64,
    entry: &'a Entry<T>,
}

impl<'a, T> Described<'a, T> {
    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn last_modified_secs(&self) -> i64 {
        self.last_modified_secs
    }

    // The entry itself, for a comparison reading something beyond size and time: storage class,
    // content type, cache-control or user metadata all live on the item the walker produced.
    pub(crate) fn entry(&self) -> &'a Entry<T> {
        self.entry
    }

    // A side in the shape a mode compares, or nothing when a field went unread.
    fn of(entry: &'a Entry<T>) -> Option<Self> {
        Some(Self {
            size: entry.meta.size?,
            last_modified_secs: entry.meta.last_modified_secs?,
            entry,
        })
    }
}

// A policy deciding what happens to one key.
//
// The side types are parameters so a mode covers the directions it can serve: uploading pairs a
// local entry against an object, downloading the reverse, copying two objects. A mode whose
// answer turns on direction writes one impl per pair and the compiler carries which is which; a
// mode that ignores direction writes one blanket impl.
pub(crate) trait Compare<S, D> {
    // The one case a mode decides. Both sides hold something, both can be moved, and all four
    // fields are values that were actually read.
    fn compare_described(&self, source: Described<'_, S>, destination: Described<'_, D>)
        -> Verdict;

    // What to answer where nothing is in the way and a field went unread.
    //
    // Sending it is right for a mode reading both fields, since one of them is missing and
    // nothing can show the two sides match. The entries come along because a mode reading only
    // one field may still have what it needs: a size comparison is unaffected by a timestamp
    // nobody could read. A mode refusing to write over a key the destination holds answers
    // without reading either.
    fn compare_undescribed(&self, source: &Entry<S>, destination: &Entry<D>) -> Verdict {
        let _ = (source, destination);
        Verdict::decided(Decision::transfer(TransferReason::Undescribable))
    }

    // Every case a mode has no say over.
    //
    // Each has one correct answer for all modes, and three of them fail quietly when a mode
    // gets them wrong: a side that lost track read as empty deletes a key nobody looked at, a
    // missing field fed to a comparison skips a key that was never sent, and a socket read as a
    // file asks for bytes that never arrive. Answering them here means a mode has to override
    // this method to get them wrong.
    fn compare(&self, pairing: &Pairing<S, D>) -> Verdict {
        let decision = match (pairing.source(), pairing.destination()) {
            // A side that could not account for this key leaves nothing to compare, and its
            // position settles nothing either. Where both lost track, the coarser answer is the
            // one reported: naming the finer loss would tell a consumer the keys around it are
            // accounted for, and on the side that lost a whole range they are not.
            (SideState::Unknown(source), SideState::Unknown(destination)) => {
                Decision::skip_unknown(coarser(*source, *destination))
            }
            (SideState::Unknown(lost), _) | (_, SideState::Unknown(lost)) => {
                Decision::skip_unknown(*lost)
            }
            (SideState::Present(source), SideState::Present(destination)) => {
                // What can stop a side depends on the role it plays here. The source is read, so
                // anything stopping a read stops the transfer. The destination is written over, so
                // only a block on writing counts, and that means a name with nothing behind it:
                // replacing a device is not what anyone asked for, and writing to a pipe nobody
                // reads never returns.
                let in_the_way = source.meta.obstruction.or_else(|| {
                    destination
                        .meta
                        .obstruction
                        .filter(Obstruction::blocks_overwrite)
                });
                if let Some(obstruction) = in_the_way {
                    Decision::skip_obstructed(obstruction)
                } else {
                    match (Described::of(source), Described::of(destination)) {
                        (Some(source), Some(destination)) => {
                            return self.compare_described(source, destination)
                        }
                        // A field nobody read cannot show a match. What to do about that is the
                        // mode's to say, and every mode but `NoOverwrite` answers by sending the
                        // key.
                        _ => return self.compare_undescribed(source, destination),
                    }
                }
            }
            (SideState::Present(source), SideState::Absent) => match source.meta.obstruction {
                Some(obstruction) => Decision::skip_obstructed(obstruction),
                None => Decision::transfer(TransferReason::Missing),
            },
            // Nothing is read and nothing is written over here, so nothing about either side can
            // be in the way. A socket or pipe the source has no counterpart for is a key the
            // destination holds alone, and removing it is what delete mode is for.
            (SideState::Absent, SideState::Present(_)) => Decision::delete(),
            (SideState::Absent, SideState::Absent) => {
                unreachable!("the join pairs a key at least one side produced")
            }
        };
        Verdict::decided(decision)
    }
}

// The coarser of two losses.
//
// A consumer acts on one lost key by holding that key back and trusting the rest. It cannot do
// that for a range, so a pair where either side lost a range is a range.
fn coarser(source: KeysLost, destination: KeysLost) -> KeysLost {
    match (source, destination) {
        (KeysLost::UnknownRange, _) | (_, KeysLost::UnknownRange) => KeysLost::UnknownRange,
        (KeysLost::OneKey, KeysLost::OneKey) => KeysLost::OneKey,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use crate::io::key::stream::EntryMeta;

    use super::*;

    // A stand-in for whatever a walker produced. What stops a transfer travels beside it, so
    // this carries nothing.
    #[derive(Debug, Clone, Copy)]
    struct Side;

    // A mode recording whether it was consulted, so a test can show the default answered
    // without asking.
    struct Recording {
        consulted: Cell<bool>,
    }

    impl Recording {
        fn new() -> Self {
            Self {
                consulted: Cell::new(false),
            }
        }
    }

    impl Compare<Side, Side> for Recording {
        fn compare_described(&self, _: Described<'_, Side>, _: Described<'_, Side>) -> Verdict {
            self.consulted.set(true);
            Verdict::decided(Decision::skip_unchanged())
        }
    }

    fn entry(
        size: Option<u64>,
        time: Option<i64>,
        obstruction: Option<Obstruction>,
    ) -> Entry<Side> {
        Entry {
            key: "a.txt".to_string(),
            meta: EntryMeta {
                size,
                last_modified_secs: time,
                obstruction,
            },
            source: Side,
        }
    }

    fn file() -> Entry<Side> {
        entry(Some(1), Some(0), None)
    }

    // A socket: described, and still holding nothing a transfer could send.
    fn socket() -> Entry<Side> {
        entry(Some(0), Some(0), Some(Obstruction::NothingToRead))
    }

    fn pair(source: SideState<Side>, destination: SideState<Side>) -> Pairing<Side, Side> {
        Pairing::new("a.txt".to_string(), source, destination)
    }

    // The default answer, beside whether the mode was asked for it.
    fn decide(source: SideState<Side>, destination: SideState<Side>) -> (Verdict, bool) {
        let mode = Recording::new();
        let verdict = mode.compare(&pair(source, destination));
        (verdict, mode.consulted.get())
    }

    fn unchanged() -> Verdict {
        Verdict::decided(Decision::skip_unchanged())
    }

    fn obstructed(obstruction: Obstruction) -> Verdict {
        Verdict::decided(Decision::skip_obstructed(obstruction))
    }

    fn sent(reason: TransferReason) -> Verdict {
        Verdict::decided(Decision::transfer(reason))
    }

    #[test]
    fn a_source_that_could_not_account_for_the_key_gets_no_action() {
        // Reading this as empty is what deletes a key nobody managed to look at.
        let (verdict, consulted) = decide(
            SideState::Unknown(KeysLost::OneKey),
            SideState::Present(file()),
        );
        assert_eq!(
            verdict,
            Verdict::decided(Decision::skip_unknown(KeysLost::OneKey))
        );
        assert!(
            !consulted,
            "the mode is never asked about a key nobody read"
        );
    }

    #[test]
    fn a_skip_from_a_lost_key_carries_how_much_was_lost() {
        // A consumer holding one key back acts differently from one that cannot trust position
        // any more, so the amount has to survive the decision.
        let (verdict, _) = decide(
            SideState::Unknown(KeysLost::UnknownRange),
            SideState::Present(file()),
        );
        let Verdict::Decided(Decision::Skip(skip)) = verdict else {
            panic!("expected a skip, got {verdict:?}")
        };
        assert_eq!(skip.reason(), SkipReason::Unknown);
        assert_eq!(skip.keys_lost(), Some(KeysLost::UnknownRange));
    }

    #[test]
    fn a_skip_for_any_other_reason_carries_no_loss() {
        let (verdict, _) = decide(SideState::Present(socket()), SideState::Present(file()));
        let Verdict::Decided(Decision::Skip(skip)) = verdict else {
            panic!("expected a skip, got {verdict:?}")
        };
        assert_eq!(skip.reason(), SkipReason::Obstructed);
        assert_eq!(skip.obstruction(), Some(Obstruction::NothingToRead));
        assert_eq!(skip.keys_lost(), None);
    }

    #[test]
    fn a_destination_that_could_not_account_for_the_key_gets_no_action() {
        assert_eq!(
            decide(
                SideState::Present(file()),
                SideState::Unknown(KeysLost::UnknownRange),
            ),
            (
                Verdict::decided(Decision::skip_unknown(KeysLost::UnknownRange)),
                false
            )
        );
    }

    #[test]
    fn a_key_the_destination_lacks_is_transferred() {
        assert_eq!(
            decide(SideState::Present(file()), SideState::Absent),
            (sent(TransferReason::Missing), false)
        );
    }

    #[test]
    fn a_key_the_source_lacks_is_a_delete() {
        // Whether the run carries it out is the delete setting's business, read where the plan
        // is executed. The comparison says what the two states mean.
        assert_eq!(
            decide(SideState::Absent, SideState::Present(file())),
            (Verdict::decided(Decision::delete()), false)
        );
    }

    #[test]
    fn a_destination_only_key_holding_no_bytes_is_still_deleted() {
        // Nothing is being written here, so whether bytes could move decides nothing. A local
        // socket or pipe the source has no counterpart for is a key the destination holds alone,
        // and leaving it would mean delete mode never cleans one up.
        assert_eq!(
            decide(SideState::Absent, SideState::Present(socket())),
            (Verdict::decided(Decision::delete()), false)
        );
    }

    #[test]
    fn two_sides_that_both_lost_track_report_the_coarser_loss() {
        // One key can be held back on its own; a range cannot. Reporting the narrower of the two
        // would have a consumer trust a position neither side can vouch for.
        let (verdict, _) = decide(
            SideState::Unknown(KeysLost::OneKey),
            SideState::Unknown(KeysLost::UnknownRange),
        );
        let Verdict::Decided(Decision::Skip(skip)) = verdict else {
            panic!("expected a skip, got {verdict:?}")
        };
        assert_eq!(skip.keys_lost(), Some(KeysLost::UnknownRange));
    }

    #[test]
    fn the_coarser_loss_is_reported_whichever_side_lost_the_range() {
        // The mirror of the case above. Reading only one side would downgrade a source that lost a
        // whole subtree to the one key the destination lost, and a consumer acting on one name
        // trusts every other position on both sides.
        let (verdict, _) = decide(
            SideState::Unknown(KeysLost::UnknownRange),
            SideState::Unknown(KeysLost::OneKey),
        );
        let Verdict::Decided(Decision::Skip(skip)) = verdict else {
            panic!("expected a skip, got {verdict:?}")
        };
        assert_eq!(skip.keys_lost(), Some(KeysLost::UnknownRange));
    }

    #[test]
    fn two_sides_that_each_lost_one_key_report_one_key() {
        let (verdict, _) = decide(
            SideState::Unknown(KeysLost::OneKey),
            SideState::Unknown(KeysLost::OneKey),
        );
        let Verdict::Decided(Decision::Skip(skip)) = verdict else {
            panic!("expected a skip, got {verdict:?}")
        };
        assert_eq!(skip.keys_lost(), Some(KeysLost::OneKey));
    }

    #[test]
    fn a_source_with_nothing_to_read_is_skipped() {
        assert_eq!(
            decide(SideState::Present(socket()), SideState::Present(file())),
            (obstructed(Obstruction::NothingToRead), false)
        );
    }

    #[test]
    fn a_source_with_nothing_to_read_is_skipped_where_the_destination_is_empty() {
        // The arm answering `Missing` is the one this has to be kept away from: it would ask
        // for a socket's bytes, and reading one may never finish.
        assert_eq!(
            decide(SideState::Present(socket()), SideState::Absent),
            (obstructed(Obstruction::NothingToRead), false)
        );
    }

    #[test]
    fn a_destination_with_nothing_to_read_is_skipped() {
        // Writing here would replace the device or pipe holding the name.
        assert_eq!(
            decide(SideState::Present(file()), SideState::Present(socket())),
            (obstructed(Obstruction::NothingToRead), false)
        );
    }

    #[test]
    fn an_archived_source_is_skipped() {
        assert_eq!(
            decide(
                SideState::Present(entry(Some(1), Some(0), Some(Obstruction::Archived))),
                SideState::Present(file()),
            ),
            (obstructed(Obstruction::Archived), false),
            "its bytes cannot be read, so there is nothing to send"
        );
    }

    #[test]
    fn an_archived_source_is_skipped_even_where_nothing_is_there_to_overwrite() {
        // The destination holding nothing is what makes this its own case. Deciding it by asking
        // whether the cause stops an overwrite reads as sound — there is nothing to overwrite —
        // and an archive does not stop one, so the pair would fall through to a transfer and ask
        // for bytes that cannot be read. What matters here is the source: it cannot be read at
        // all, whatever the destination holds.
        assert_eq!(
            decide(
                SideState::Present(entry(Some(1), Some(0), Some(Obstruction::Archived))),
                SideState::Absent,
            ),
            (obstructed(Obstruction::Archived), false),
            "an archived object is unreadable whether or not the destination has the key"
        );
    }

    #[test]
    fn an_archived_destination_is_compared_and_written_over() {
        // Writing over an object never reads what is already there. Answering both roles from one
        // question would refuse every upload to a key holding an archived object, which is the
        // case that pins the two apart — the source test above uses the same cause and skips.
        let (verdict, consulted) = decide(
            SideState::Present(file()),
            SideState::Present(entry(Some(9), Some(0), Some(Obstruction::Archived))),
        );
        assert!(
            consulted,
            "the mode decides this pair, since nothing is in the way: {verdict:?}"
        );
    }

    #[test]
    fn a_destination_holding_nothing_readable_is_still_not_written_over() {
        // The other half of the same distinction: a pipe or device does stop an overwrite, so this
        // must keep skipping while the archived case above stops doing so.
        assert_eq!(
            decide(
                SideState::Present(file()),
                SideState::Present(entry(Some(0), Some(0), Some(Obstruction::NothingToRead))),
            ),
            (obstructed(Obstruction::NothingToRead), false)
        );
    }

    #[test]
    fn what_is_in_the_way_is_answered_before_what_could_not_be_described() {
        // A socket whose fields also went unread. Asking about the fields first would answer
        // "send it", which asks for bytes that never arrive — so the order of these two checks is
        // the thing being pinned, and every other obstructed pair is described on purpose.
        assert_eq!(
            decide(
                SideState::Present(entry(None, None, Some(Obstruction::NothingToRead))),
                SideState::Present(file()),
            ),
            (obstructed(Obstruction::NothingToRead), false)
        );
    }

    #[test]
    fn a_source_whose_size_went_unread_is_transferred() {
        assert_eq!(
            decide(
                SideState::Present(entry(None, Some(0), None)),
                SideState::Present(file()),
            ),
            (sent(TransferReason::Undescribable), false),
            "the mode is never handed a field nobody read"
        );
    }

    #[test]
    fn a_source_whose_time_went_unread_is_transferred() {
        // Handing this to a mode is the trap: `None` is below every `Some`, so a comparison
        // asking whether the destination is older answers no and the key is never sent.
        assert_eq!(
            decide(
                SideState::Present(entry(Some(1), None, None)),
                SideState::Present(file()),
            ),
            (sent(TransferReason::Undescribable), false)
        );
    }

    #[test]
    fn a_destination_whose_time_went_unread_is_transferred() {
        assert_eq!(
            decide(
                SideState::Present(file()),
                SideState::Present(entry(Some(1), None, None)),
            ),
            (sent(TransferReason::Undescribable), false)
        );
    }

    #[test]
    fn two_described_sides_reach_the_mode() {
        let (verdict, consulted) = decide(SideState::Present(file()), SideState::Present(file()));
        assert!(consulted, "the one case a mode decides reaches it");
        assert_eq!(verdict, unchanged());
    }

    #[test]
    fn a_mode_reads_the_values_the_sides_held() {
        struct Echo;
        impl Compare<Side, Side> for Echo {
            fn compare_described(
                &self,
                source: Described<'_, Side>,
                destination: Described<'_, Side>,
            ) -> Verdict {
                assert_eq!(source.entry().key, "a.txt", "the entry comes along");
                if source.size() == destination.size() {
                    Verdict::decided(Decision::skip_unchanged())
                } else {
                    Verdict::decided(Decision::transfer(TransferReason::SizeDiffers))
                }
            }
        }
        let pairing = pair(
            SideState::Present(entry(Some(10), Some(0), None)),
            SideState::Present(entry(Some(20), Some(0), None)),
        );
        assert_eq!(
            Echo.compare(&pairing),
            sent(TransferReason::SizeDiffers),
            "both sizes reach the mode as the walk read them"
        );
    }

    #[test]
    fn the_same_states_always_decide_the_same_way() {
        // Deciding reads one key and keeps nothing, so a repeated call repeats the answer. This
        // pins that no state accumulates behind the trait, which is what a plan a caller can
        // reproduce depends on.
        let mode = Recording::new();
        let first = mode.compare(&pair(SideState::Present(file()), SideState::Absent));
        let second = mode.compare(&pair(SideState::Present(file()), SideState::Absent));
        assert_eq!(first, second);
    }

    #[test]
    fn a_mode_can_answer_for_every_direction_at_once() {
        // A mode whose answer ignores which side is local writes one blanket impl, so offering
        // a custom comparison does not force one rule to be written three times.
        struct SizeOnly;
        impl<S, D> Compare<S, D> for SizeOnly {
            fn compare_described(
                &self,
                source: Described<'_, S>,
                destination: Described<'_, D>,
            ) -> Verdict {
                if source.size() == destination.size() {
                    Verdict::decided(Decision::skip_unchanged())
                } else {
                    Verdict::decided(Decision::transfer(TransferReason::SizeDiffers))
                }
            }
        }
        let pairing = pair(SideState::Present(file()), SideState::Present(file()));
        assert_eq!(SizeOnly.compare(&pairing), unchanged());
    }
}
