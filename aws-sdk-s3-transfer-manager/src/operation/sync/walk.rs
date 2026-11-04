/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// Pairing a local tree against an S3 prefix, one key at a time.
//
// Both sides arrive in key order, which is what lets absence be worked out from position:
// once a side has produced a key sorting after the one being decided, it can never
// produce that one. Nothing else has to be remembered, which is why the merge holds one
// entry per side and no history.
//
// The walk owns both streams because the order it depends on is not what either walker
// does by default, and because one rule set has to reach both sides — a key excluded on
// one side but still listed on the other reads as a key that was deleted.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::io::key::stream::{
    key_under_root, AliasedName, Entry, KeyStream, KeysLost, Obstruction, StreamError,
};
use crate::io::walk::WalkErrorKind;

use links::{name_of, Links, ALIASED_NAMES_HELD};
pub(crate) use links::{names_cover, LinkScan};
pub(crate) use walker::LocalDestination;
#[cfg(test)]
pub(crate) use walker::{LocalAndBucket, Walker};

// What one side holds at the key the merge has reached.
#[derive(Debug, Clone)]
pub(crate) enum SideState<T> {
    // The side produced an entry for this key.
    Present(Entry<T>),
    // The side went past this key without producing it, so nothing is there.
    Absent,
    // The side could not account for this key. How much it lost travels with the state,
    // because a consumer reading absence from position needs to know the position is no
    // longer something to read from.
    //
    // Say the local walk cannot open `photos/2019/` and carries on with `photos/2020/`. The
    // keys under `photos/2019/` never arrive, and position alone reads that as nothing being
    // there, so delete mode would remove `photos/2019/x.jpg` from the bucket while the local
    // file sits there unread. Those keys arrive here.
    Unknown(KeysLost),
}

impl<T> SideState<T> {
    // The entry this side holds, if it holds one.
    pub(crate) fn entry(&self) -> Option<&Entry<T>> {
        match self {
            SideState::Present(entry) => Some(entry),
            SideState::Absent | SideState::Unknown(_) => None,
        }
    }

    // Whether this side failed to account for the key.
    pub(crate) fn is_unknown(&self) -> bool {
        matches!(self, SideState::Unknown(_))
    }
}

// One key, and what each side holds at it.
#[derive(Debug)]
pub(crate) struct Pairing<S, D> {
    key: String,
    source: SideState<S>,
    destination: SideState<D>,
}

impl<S, D> Pairing<S, D> {
    // Built by the merge in every other case. A test for whoever consumes a pairing needs to
    // state one directly, without a pair of streams behind it.
    #[cfg(test)]
    pub(crate) fn new(key: String, source: SideState<S>, destination: SideState<D>) -> Self {
        Self {
            key,
            source,
            destination,
        }
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn source(&self) -> &SideState<S> {
        &self.source
    }

    pub(crate) fn destination(&self) -> &SideState<D> {
        &self.destination
    }
}

// One side's next entry, held until the key it carries has been paired.
#[derive(Debug)]
enum Head<T> {
    // Nothing read from this side yet.
    Unread,
    // An entry waiting for the other side to reach its key.
    Entry(Entry<T>),
    // The side produced everything it had.
    Finished,
}

impl<T> Head<T> {
    fn entry(&self) -> Option<&Entry<T>> {
        match self {
            Head::Entry(entry) => Some(entry),
            Head::Unread | Head::Finished => None,
        }
    }
}

// One side of the merge: its stream, the key it is holding, and what it could not account for.
//
// Both sides keep identical bookkeeping, so it lives here once rather than as a matched pair of
// fields on the merge. Recording what a failure cost is the same work whichever side reported it,
// so it is written once here and the two sides cannot drift apart.
struct Side<K: KeyStream> {
    stream: K,
    head: Head<K::Source>,
    // What a survivable failure cost, held until the merge is past the keys it hid.
    //
    // Anything the other side holds inside that stretch is a key this one could not account for.
    // Reading such a key as absent is what allows it to be deleted, which is the whole reason the
    // answer is carried here.
    gap: Stretches,
    // Keys this side named as lost, held until the merge reaches each one.
    //
    // A failure costing one key arrives before that key sorts, so the answer cannot be given
    // where it is heard. Reading a directory already collects every child before any is handed
    // over, and the errors from that read are queued with them, so these keys are a subset of
    // what the walk holds anyway.
    //
    // Ordered by key and not by arrival, because a walk reports the failures from one directory
    // in whatever order the filesystem listed it. Two unreadable children arriving as `z` then
    // `a` would leave `a` unmatched against a queue drained from the front, and an unmatched key
    // reads as absent — a delete for a file sitting on disk whose loss was reported.
    lost: BTreeSet<String>,
    // Set when this side lost one key and could not say which.
    //
    // A listing names the key it dropped. A walk names a path, and the failure arrives at the
    // position of the directory holding it while the key sorts later, somewhere inside — so the
    // one key at risk cannot be picked out here. So every later key on this side is answered
    // unknown, which is the only answer that cannot delete an object whose file was merely
    // unreadable.
    lost_unnamed: bool,
    // The last key this side produced, which the next one has to sort after.
    //
    // The merge reads absence from position, so a side going backwards is read as absence and then
    // as presence: one key handed both a delete and a transfer, by a plan calling itself whole. A
    // debug assertion on the one root known to list unordered covers development and compiles out,
    // where a listing whose collation differs or a derivation that changes produces this in a
    // release build.
    last: Option<String>,
    // The root this side's paths are taken under, when it walks a filesystem.
    //
    // A walk names the file it could not read by absolute path, and turning that into a key
    // needs the root it sits under. The stream that reported the failure was handed a walker
    // which never says what its root is, so the answer comes from here — which is what makes one
    // lost key nameable here, where a layer down has no root to name it against.
    root: Option<PathBuf>,
    // `take` keeps here the key of the entry it handed over last. The string is empty before the
    // first. `take` clones that key anyway to release what the merge passed, so keeping it adds no
    // copy. The later of the two sides' keys shows how far the merge has paired.
    taken: String,
    // Whether this side is where entries land. A link that reaches nothing there leaves its own
    // name free, because a download can replace the link.
    destination: bool,
}

impl<K: KeyStream> Side<K> {
    fn new(stream: K) -> Self {
        Self {
            stream,
            head: Head::Unread,
            last: None,
            gap: Stretches::default(),
            lost: BTreeSet::new(),
            lost_unnamed: false,
            root: None,
            taken: String::new(),
            destination: false,
        }
    }

    // Read one entry if the head is empty, recording what any failure cost.
    //
    // The failure comes back so the merge can report it. Nothing is recorded for a failure that
    // ended the side, because no later key arrives from it for the merge to answer at.
    async fn fill(&mut self) -> Option<StreamError> {
        if !matches!(self.head, Head::Unread) {
            return None;
        }
        match self.stream.next_entry().await {
            Some(Ok(entry)) => {
                // Checked before the entry is taken rather than when it is handed over, because a
                // key read as absent here is one the other side may already have been told about.
                if let Some(last) = &self.last {
                    if entry.key.as_str() <= last.as_str() {
                        let err = StreamError::OutOfOrder {
                            key: entry.key.clone(),
                            after: last.clone(),
                        };
                        // The entry is dropped. Its position is what made it answerable, and the
                        // cost recorded below answers every later key on this side as unknown, so
                        // nothing is deleted or overwritten on the strength of where it sat.
                        self.gap.add(Stretch { under: None });
                        return Some(err);
                    }
                }
                self.last = Some(entry.key.clone());
                self.head = Head::Entry(entry);
                None
            }
            // The stream says whether that failure ended it. Asking is what the enumeration
            // layer offers for this; classifying the error here would put a second opinion
            // beside the walk's own, and the two could drift apart.
            Some(Err(err)) => {
                if !self.stream.is_done() {
                    match cost_of(&err, self.root.as_deref(), self.destination) {
                        Cost::Key(key) => {
                            self.lost.insert(key);
                        }
                        Cost::UnnamedKey => self.lost_unnamed = true,
                        Cost::Stretch(stretch) => self.gap.add(stretch),
                        Cost::Below(name) => {
                            self.gap.below.insert(name);
                        }
                        Cost::Nothing => {}
                    }
                }
                Some(err)
            }
            None => {
                self.head = Head::Finished;
                None
            }
        }
    }

    fn is_done(&self) -> bool {
        self.stream.is_done()
    }

    fn is_finished(&self) -> bool {
        matches!(self.head, Head::Finished)
    }

    // Return whether the head entry carries `obstruction`.
    fn head_is(&self, obstruction: Obstruction) -> bool {
        self.head
            .entry()
            .is_some_and(|entry| entry.meta.obstruction == Some(obstruction))
    }

    // Take the entry at the head, releasing every stretch the merge has now passed.
    //
    // A key inside a stretch, or one sorting before it, releases nothing: the failure was heard
    // early and what it cost is still ahead.
    fn take(&mut self) -> Entry<K::Source> {
        if let Some(entry) = self.head.entry() {
            let key = entry.key.clone();
            self.gap.release_passed(&key);
            // `missing` is the other path, for a side answering about a key it did not produce, and
            // it prunes both of these too. Either path alone leaves the other's keys growing the set
            // for the length of the run instead of keeping it inside what the directories currently
            // open are holding.
            self.release_before(&key);
            self.taken = key;
        }
        match std::mem::replace(&mut self.head, Head::Unread) {
            Head::Entry(entry) => entry,
            _ => unreachable!("taken only with an entry at the head"),
        }
    }

    // Take the head entry, and hold back every key under its name when the entry is a link the walk
    // did not resolve. The walk never looked behind such a link, so this side cannot account for
    // any key under the link's name. A key there then reads as unknown on this side and not as
    // absent. On an upload that stops a delete, and on a download it stops a write through the link.
    fn take_holding(&mut self) -> Entry<K::Source> {
        let entry = self.take();
        if matches!(
            entry.meta.obstruction,
            Some(Obstruction::UnfollowedLink | Obstruction::ExcludedLink)
        ) {
            self.gap.add(Stretch {
                under: Some(entry.key.clone()),
            });
        }
        entry
    }

    // What this side says about a key it did not produce. Absent while it is accounting for
    // itself, unknown while a failure has left it unable to, and unknown for a key it named as
    // lost.
    fn missing(&mut self, key: &str) -> SideState<K::Source> {
        self.release_before(key);
        self.gap.release_passed(key);
        // The key leaves the set whatever gets reported, or it stays held until something sorts past
        // it and the side keeps answering unknown for a key it has already answered for.
        let named = self.lost.remove(key);
        // A range outranks one key, and both can be true at once: a directory that stopped part way
        // through and a child of it that could not be stat'd cost this side both at the same
        // position. Reporting the key would tell a consumer the keys around this one are accounted
        // for, and on a side that lost a range they are not — so the coarser answer is the one that
        // goes out, the same way the comparison takes the coarser of its two sides.
        if self.gap.covers(key) || self.lost_unnamed {
            return SideState::Unknown(KeysLost::UnknownRange);
        }
        if named {
            return SideState::Unknown(KeysLost::OneKey);
        }
        SideState::Absent
    }

    // Drop every held key sorting before this one.
    //
    // The merge runs in key order, so a key still held below this one was passed without either
    // side reaching it, and nothing will ever ask for a decision about it.
    fn release_before(&mut self, key: &str) {
        self.lost = self.lost.split_off(key);
    }
}

// A stretch of keys one failure hid, bounded by a prefix taken from the failure's own path.
//
// The bound matters because a failure does not always arrive where the keys it hid would sort. A
// directory nobody could open is reported where that directory sorts, but a symlink loop is noticed
// while reading the parent, so it arrives ahead of every sibling — including siblings sorting
// earlier than the subtree it cost. Releasing the stretch at the side's next key would let such a
// sibling stand in for keys nobody enumerated.
//
// A stretch always costs an unknown range. A failure costing one key is held as that key, or as
// the side's whole account where the key could not be worked out, so neither reaches here.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Stretch {
    // `None` where no prefix could be worked out, which holds the rest of the side: a stretch
    // whose extent is unknown could cover any later key.
    under: Option<String>,
}

// Every stretch a side still owes an answer for.
//
// A side can be holding more than one at a time, and they do not arrive in the order their keys sort: a
// symlink loop noticed while the root is read comes before a failure descending into a directory
// whose name sorts earlier. Keeping one at a time forgets whichever arrived first, and the keys it
// covered go back to reading absent.
#[derive(Debug, Clone, Default)]
struct Stretches {
    // The names bounded stretches sit under, in key order.
    //
    // A set rather than a list because every one of the three things asked of this is asked once per
    // key or once per failure, and a scan makes each of them cost what the whole run has held. With N
    // unreadable directories ahead of M keys, a list costs N² to fill and N·M to consult.
    under: BTreeSet<String>,
    // Names whose keys below are held, while the name itself stays free. A link that reaches
    // nothing, or that loops back, leaves nothing at its own name, but it may stand for a directory.
    below: BTreeSet<String>,
    // A stretch bounding nothing covers every later key and is never passed, so it needs no name and
    // no ordering — only to be remembered.
    everything: bool,
}

impl Stretches {
    fn add(&mut self, stretch: Stretch) {
        match stretch.under {
            None => self.everything = true,
            Some(name) => {
                self.under.insert(name);
            }
        }
    }

    // Whether any stretch still owes an answer for this key.
    fn covers(&self, key: &str) -> bool {
        if self.everything {
            return true;
        }
        // A bounded stretch covers this key when its name is the key or an ancestor of it. So the
        // names worth looking for are the key and its ancestors — a handful — rather than every name
        // being held.
        if self.under.contains(key) {
            return true;
        }
        key.bytes().enumerate().any(|(at, byte)| {
            byte == b'/' && (self.under.contains(&key[..at]) || self.below.contains(&key[..at]))
        })
    }

    // Drop every stretch the merge has passed.
    fn release_passed(&mut self, key: &str) {
        release_passed_in(&mut self.under, key);
        release_passed_in(&mut self.below, key);
    }
}

// Drop every name in `names` the merge has passed at `key`.
fn release_passed_in(names: &mut BTreeSet<String>, key: &str) {
    if names.is_empty() {
        return;
    }
    // Everything sorting before the key is behind the merge, with one exception: a name the key
    // extends without entering its subtree — `link` against `link.txt`, where `.` sorts below `/` —
    // is neither covered nor passed, because the whole subtree is still ahead. Those are put back,
    // and there are at most as many as the key is long.
    let at_or_after = names.split_off(key);
    let held: Vec<String> = key
        .bytes()
        .enumerate()
        .filter(|(_, byte)| *byte <= b'/')
        .map(|(at, _)| &key[..at])
        .filter(|prefix| names.contains(*prefix))
        .map(str::to_string)
        .collect();
    *names = at_or_after;
    names.extend(held);
}

impl Stretch {
    // Whether this key is one the failure hid.
    fn covers(&self, key: &str) -> bool {
        match &self.under {
            None => true,
            Some(name) => at_or_under(key, name),
        }
    }

    // Whether the merge reaching this key has passed everything the failure hid.
    //
    // Not simply the negation of `covers`. The covered set runs from the name itself to the end of
    // `name/`, so a key extending the name with a byte below `/` — `link.txt` against `link`, where
    // `.` is 0x2E and `/` is 0x2F — is neither covered nor past it: the whole subtree is still
    // ahead. Releasing there would read every key the failure hid as absent.
    fn passed_by(&self, key: &str) -> bool {
        match &self.under {
            None => false,
            Some(name) => match key.strip_prefix(name.as_str()) {
                Some(rest) => rest.as_bytes().first().is_some_and(|b| *b > b'/'),
                None => key > name.as_str(),
            },
        }
    }
}

// Whether a key is the named one or sits beneath it.
//
// The name counts as hidden along with its subtree, because the walk reports a range for a name
// whose type it could not read — it may have been a file. Matching the subtree alone would leave
// that one name reading absent, and delete mode removes what reads absent.
//
// The boundary check is what keeps a neighbour out: under `link`, the key `link/inner` is covered
// and `linkfoo` is not.
fn at_or_under(key: &str, name: &str) -> bool {
    key.starts_with(name) && (key.len() == name.len() || key.as_bytes()[name.len()] == b'/')
}

// What a link scan found once it read the whole destination.
#[derive(Debug)]
pub(crate) enum LinkScanResult {
    // Every name the tree's followed links reach, at most `ALIASED_NAMES_HELD` of them.
    Names(Vec<AliasedName>),
    // The tree holds more names than the bound, so the run holds every local delete.
    TooMany,
}

// Two key-ordered streams merged into one pairing per key.
// `Progress` tells callers whether the merge can infer absence from key position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Progress {
    // The merge has not finished. It may produce more pairings.
    Pairing,
    // Both streams finished. Every produced key has a pairing.
    Accounted,
    // A terminal failure stopped the merge. Later keys cannot read absence from position.
    Stopped,
}

pub(crate) struct Walk<S: KeyStream, D: KeyStream> {
    src: Side<S>,
    dst: Side<D>,
    // Set when a key went unaccounted for, so a caller can tell a whole plan from a partial
    // one. A run that lost keys and reports a clean plan is one whose caller cannot know it
    // acted on less than it was asked about.
    incomplete: bool,
    // Set when a side reported a failure that ends it.
    //
    // A transfer needs a source, and a delete needs the source established absent, so a side
    // that has stopped supplies neither. It also answers `None` from here, which position alone
    // reads as "nothing is there" — so carrying on would read every key the other side still
    // holds as absent, and delete mode would remove all of them.
    ended_by_failure: bool,
    // The link scan this walk's run starts, until the run takes it.
    link_scan: Option<LinkScan>,
    links: Links,
}

impl<S: KeyStream, D: KeyStream> Walk<S, D> {
    pub(crate) fn new(source: S, destination: D) -> Self {
        let mut dst = Side::new(destination);
        dst.destination = true;
        Self {
            src: Side::new(source),
            dst,
            incomplete: false,
            ended_by_failure: false,
            link_scan: None,
            links: Links::NoScan,
        }
    }

    // Give the run its link scan. The run reads the scan beside the merge and hands the result back
    // through `accept_link_scan`.
    pub(crate) fn take_link_scan(&mut self) -> Option<LinkScan> {
        self.link_scan.take()
    }

    // Take what the link scan found. Hold its names on the source side, and return every name a
    // waiting local delete must not touch, sorted. `None` means every waiting delete stays.
    pub(crate) fn accept_link_scan(&mut self, result: LinkScanResult) -> Option<Vec<String>> {
        let found = match std::mem::replace(&mut self.links, Links::NoScan) {
            Links::Scanning { found } => found,
            _ => Vec::new(),
        };
        let LinkScanResult::Names(scanned) = result else {
            self.src.gap.add(Stretch { under: None });
            self.links = Links::Scanned { names: Vec::new() };
            return None;
        };
        let mut names: Vec<String> = Vec::with_capacity(scanned.len() + found.len());
        for name in scanned {
            names.push(name_of(&name).to_string());
            self.add_alias_hold(name);
        }
        names.extend(found.iter().map(|name| name_of(name).to_string()));
        names.sort_unstable();
        names.dedup();
        self.links = Links::Scanned {
            names: names.clone(),
        };
        Some(names)
    }

    // The next key either side holds, or the failure that interrupted one of them.
    // `None` once both sides have finished.
    //
    // One call reads at most one entry from each side: a head is filled only when the
    // key it held has been paired, so the side sorting ahead waits where it is.
    pub(crate) async fn next(
        &mut self,
    ) -> Option<Result<Pairing<S::Source, D::Source>, StreamError>> {
        if self.ended_by_failure {
            return None;
        }
        loop {
            // A failure that lost no key, such as a file that vanished, leaves the plan whole.
            if let Some(err) = self.src.fill().await {
                if err.keys_lost() != KeysLost::Nothing {
                    self.incomplete = true;
                }
                if self.src.is_done() {
                    self.ended_by_failure = true;
                }
                return Some(Err(err));
            }
            let failure = self.dst.fill().await;
            // A followed destination link makes two keys name one file. The source holds back the
            // deletes under every name the link aliases, including when the read also failed.
            for name in self.dst.stream.take_aliased_names() {
                self.hold_aliased(name);
            }
            // A link in the destination that reaches nothing is no failure. It holds the keys
            // below its name, and a download can still land at the name itself.
            if failure.as_ref().is_some_and(reaches_nothing) {
                continue;
            }
            if let Some(err) = failure {
                if err.keys_lost() != KeysLost::Nothing {
                    self.incomplete = true;
                }
                if self.dst.is_done() {
                    self.ended_by_failure = true;
                }
                return Some(Err(err));
            }
            // A key one side reached while the other has run out is absent there. A side sorting
            // later says the same thing: reaching a key past this one settles that this one never
            // arrives.
            let order = match (&self.src.head, &self.dst.head) {
                (Head::Finished, Head::Finished) => return None,
                (Head::Entry(_), Head::Finished) => Ordering::Less,
                (Head::Finished, Head::Entry(_)) => Ordering::Greater,
                (Head::Entry(src), Head::Entry(dst)) => src.key.cmp(&dst.key),
                (Head::Unread, _) | (_, Head::Unread) => {
                    unreachable!("both heads are filled before they are compared")
                }
            };
            // The filter excluded this link's own name, so the name makes no pairing. The merge
            // skips past it only at the merge position. `take` releases every hold below the key,
            // and the other side's head can still sort there and need one. The link still hides the
            // keys under its name, so its side holds those keys back.
            if order.is_le() && self.src.head_is(Obstruction::ExcludedLink) {
                self.src.take_holding();
                continue;
            }
            if order.is_ge() && self.dst.head_is(Obstruction::ExcludedLink) {
                self.dst.take_holding();
                continue;
            }
            return Some(Ok(match order {
                Ordering::Less => self.take_source_only(),
                Ordering::Greater => self.take_destination_only(),
                Ordering::Equal => self.take_both(),
            }));
        }
    }

    // The roots the two sides walk, for the sides that walk a filesystem.
    #[must_use]
    pub(crate) fn with_roots(
        mut self,
        source: Option<PathBuf>,
        destination: Option<PathBuf>,
    ) -> Self {
        self.src.root = source;
        self.dst.root = destination;
        self
    }

    // Whether every key both sides hold was accounted for.
    //
    // False once anything went unread, whether the merge carried on past it or stopped. What a
    // caller does with that belongs to whoever reports a run.
    pub(crate) fn is_plan_complete(&self) -> bool {
        !self.incomplete
    }

    // Report whether the merge can continue pairing, accounted for every key, or stopped.
    pub(crate) fn progress(&self) -> Progress {
        if self.ended_by_failure {
            Progress::Stopped
        } else if self.src.is_finished() && self.dst.is_finished() {
            Progress::Accounted
        } else {
            Progress::Pairing
        }
    }

    // A key one side could not account for leaves a hole in the plan, whether a failure or an
    // unfollowed link hid it.
    fn note_unknown<T>(&mut self, side: &SideState<T>) {
        if matches!(side, SideState::Unknown(_)) {
            self.incomplete = true;
        }
    }

    // Hold back the deletes under a name a followed destination link aliases.
    //
    // The merge may already have reached the name. A delete under it then went out before the walk
    // found the link, unless the run still holds its deletes for the link scan. While the scan
    // reads, the name joins the ones the waiting deletes are checked against. Once the scan ended, a
    // reached name the scan never saw marks the plan incomplete: someone created the link during the
    // run. A run without a scan marks every reached name.
    fn hold_aliased(&mut self, name: AliasedName) {
        // No name sorts at or before the empty key, so nothing reads as reached before a pairing.
        let paired = std::cmp::max(self.src.taken.as_str(), self.dst.taken.as_str());
        let reached = match &name {
            // The merge reaches a file when it pairs that key or a later one.
            AliasedName::File(name) => name.as_str() <= paired,
            // The merge reaches a directory when it enters the subtree or passes it.
            AliasedName::Directory(name) => {
                let stretch = Stretch {
                    under: Some(name.clone()),
                };
                stretch.covers(paired) || stretch.passed_by(paired)
            }
        };
        match &mut self.links {
            Links::NoScan => self.incomplete |= reached,
            Links::Scanning { found } => {
                if found.len() < ALIASED_NAMES_HELD {
                    found.push(name.clone());
                } else {
                    // The waiting deletes can no longer be checked against every name, so the
                    // source holds back every delete.
                    self.src.gap.add(Stretch { under: None });
                }
            }
            Links::Scanned { names } => {
                let seen = names
                    .binary_search_by(|n| n.as_str().cmp(name_of(&name)))
                    .is_ok();
                self.incomplete |= reached && !seen;
            }
        }
        self.add_alias_hold(name);
    }

    // Hold back the deletes under one aliased name. Past the bound on held names, the source holds
    // back every later delete.
    fn add_alias_hold(&mut self, name: AliasedName) {
        let (AliasedName::File(name) | AliasedName::Directory(name)) = name;
        if self.src.gap.under.len() < ALIASED_NAMES_HELD {
            self.src.gap.add(Stretch { under: Some(name) });
        } else {
            self.src.gap.add(Stretch { under: None });
        }
    }

    fn take_source_only(&mut self) -> Pairing<S::Source, D::Source> {
        let entry = self.src.take_holding();
        let destination = self.dst.missing(&entry.key);
        self.note_unknown(&destination);
        Pairing {
            key: entry.key.clone(),
            source: SideState::Present(entry),
            destination,
        }
    }

    fn take_destination_only(&mut self) -> Pairing<S::Source, D::Source> {
        let entry = self.dst.take_holding();
        let source = self.src.missing(&entry.key);
        self.note_unknown(&source);
        Pairing {
            key: entry.key.clone(),
            source,
            destination: SideState::Present(entry),
        }
    }

    fn take_both(&mut self) -> Pairing<S::Source, D::Source> {
        // Neither side is missing here, so neither has anything to answer for.
        let src = self.src.take_holding();
        let dst = self.dst.take_holding();
        Pairing {
            key: src.key.clone(),
            source: SideState::Present(src),
            destination: SideState::Present(dst),
        }
    }
}

// What a failure the side survived costs the merge.
//
// Two of these look alike from `keys_lost()` alone, which reports one key for both. The difference
// is whether a key exists for the merge to answer for: a file nobody could read has one, and naming it is
// a matter of working out which, where a name no key can carry has none at all.
#[derive(Debug)]
enum Cost {
    // One key, and the failure named it. The side holds that key until the merge reaches it,
    // and answers unknown there.
    Key(String),
    // One key, and which one could not be worked out. Every later key on that side has to stay
    // open, since any of them could be the one that went unread.
    UnnamedKey,
    // Every key the failure hid, under the prefix it names where one could be worked out.
    Stretch(Stretch),
    // Every key below this name, and not the name itself.
    Below(String),
    // Nothing for the merge to answer for. The name produced no key on this side, so no pairing will
    // ever ask about it, and holding anything back would hold back a key chosen at random.
    Nothing,
}

// A listing says which key it dropped. A walk says which path it could not read, and a path is not
// a key: turning one into the other needs the walk root, which lives a layer up from the stream
// that reported it.
fn cost_of(err: &StreamError, root: Option<&Path>, destination: bool) -> Cost {
    let key_of = |walk: &crate::io::walk::WalkError| {
        root.and_then(|root| walk.path().and_then(|p| key_under_root(root, p)))
    };
    match err {
        // A loop reaches a directory, so its own name counts as a directory, and only the keys
        // below it are unknown. In the destination, a link that reaches nothing leaves its
        // name free the same way, because a download can replace the link. In the source, that
        // link has nothing to send at its name, so the name stays held too.
        StreamError::Walk(walk)
            if walk.kind() == WalkErrorKind::SymlinkCycle
                || (destination && walk.kind() == WalkErrorKind::BrokenSymlink) =>
        {
            match key_of(walk) {
                Some(name) => Cost::Below(name),
                None => Cost::Stretch(Stretch { under: None }),
            }
        }
        // A listing names the key it dropped, already taken relative to the root.
        StreamError::MalformedListing { key: Some(key), .. } => Cost::Key(key.clone()),
        // A name no key can carry takes no part in the comparison, so there is no key here to
        // answer for. What it cost a run shows up as the plan coming out incomplete.
        StreamError::UnkeyableName(_) => Cost::Nothing,
        // A file that went away costs the comparison nothing to know, so there is no position to
        // hold open. Placed ahead of the arms below because the tail they fall to is the coarsest
        // answer there is, and this is the finest.
        StreamError::Walk(walk) if walk.kind() == WalkErrorKind::Vanished => {
            tracing::debug!(path = ?walk.path(), "a file went away before it could be read");
            Cost::Nothing
        }
        // A walk names a path, which is a key only once the root it sits under is known. Three
        // things can stop that: no root was supplied, the error carries no path, or the root and
        // the walk disagree about the path's form — a root left uncanonicalized against a walk
        // that resolved it, say. Each one drops back to holding the side's whole account open,
        // which is safe and much coarser, so it says so.
        StreamError::Walk(walk) if err.keys_lost() == KeysLost::OneKey => match key_of(walk) {
            Some(key) => Cost::Key(key),
            None => {
                tracing::warn!(
                    path = ?walk.path(),
                    root = ?root,
                    "could not name the lost key, so this side's account stays open for the run"
                );
                Cost::UnnamedKey
            }
        },
        _ if err.keys_lost() == KeysLost::OneKey => Cost::UnnamedKey,
        // A stretch, bounded by the name it cost where the failure named a path and the root is
        // known. The bound is the name itself, which `covers` reads as that name and everything
        // beneath it. Without a bound the rest of the side is answered unknown, since a failure
        // reported before the keys it hid says nothing about where they stop.
        _ => Cost::Stretch(Stretch {
            under: match err {
                StreamError::Walk(walk) => key_of(walk),
                _ => None,
            },
        }),
    }
}

// Whether a failure is a link that reaches nothing: a dangling link or an endless chain. In a
// download's destination that is no failure.
fn reaches_nothing(err: &StreamError) -> bool {
    matches!(err, StreamError::Walk(walk) if walk.kind() == WalkErrorKind::BrokenSymlink)
}

mod links;
#[cfg(test)]
mod tests;
mod walker;
