/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// This module names the keys that each link in a download's destination reaches. The walk
// holds back the deletes under those keys.

use std::path::{Path, PathBuf};

use crate::io::key::stream::{key_under_root, AliasedName};
use crate::io::walk::{FollowedLink, FsWalk, SortOrder};

use super::walker::Walker;

// Followed destination links keep at most this many names held at once, and the link scan records
// at most this many names for the whole tree. Past the bound, the source holds back every later
// delete. The value matches how many transfers may wait before the merge pauses, so the held names
// cost about what that queue does.
pub(crate) const ALIASED_NAMES_HELD: usize = 1024;

// Where a run stands with its link scan. The destination walk follows the same links the scan
// does, so the walk names only the links the scan missed.
#[derive(Debug)]
pub(super) enum Links {
    // The run has no link scan, so it deletes nothing through a link, and the walk drops the links
    // it follows.
    NoScan,
    // The scan is still reading, and every local delete waits for it. The walk keeps each link it
    // follows, and names the ones the scan missed once the scan ends.
    Scanning {
        namer: LinkNamer,
        followed: Vec<FollowedLink>,
    },
    // The scan ended. `seen` holds the paths of the links it followed, and `names` the names those
    // links reach, both sorted. A link the walk follows now and `seen` lacks was created after the
    // scan read its directory.
    Scanned {
        namer: LinkNamer,
        seen: Vec<PathBuf>,
        names: Vec<String>,
    },
}

// Whether a link stands among a key's parent directories below the root. A read, a write, or
// a delete through that link would act on the link's target. The key's own name stays out of the
// check, because delete mode removes a link as a link and a download's rename replaces one.
//
// The check goes through the filesystem's own name lookup, so on a disk that ignores case, asking
// about `photos` finds the link `Photos`. An absent component holds no link below it, and an
// unreadable one fails the action itself, so neither counts.
pub(crate) fn link_on_path(root: &Path, key: &str) -> bool {
    let Some((parents, _own_name)) = key.rsplit_once('/') else {
        return false;
    };
    let mut at = root.to_path_buf();
    for part in parents.split('/') {
        at.push(part);
        match std::fs::symlink_metadata(&at) {
            Ok(meta) if meta.file_type().is_symlink() => return true,
            Ok(_) => {}
            Err(_) => return false,
        }
    }
    false
}

// The key an aliased name stands for, of either kind.
pub(super) fn name_of(name: &AliasedName) -> &str {
    let (AliasedName::File(name) | AliasedName::Directory(name)) = name;
    name
}

pub(super) fn into_name(name: AliasedName) -> String {
    let (AliasedName::File(name) | AliasedName::Directory(name)) = name;
    name
}

// The names of the directories above `key`, from the top down. `a/b/c` yields `a`, then `a/b`.
pub(crate) fn key_parents(key: &str) -> impl Iterator<Item = &str> {
    key.bytes()
        .enumerate()
        .filter(|(_, byte)| *byte == b'/')
        .map(move |(at, _)| &key[..at])
}

// Whether `names`, sorted, holds `key` or a directory above it.
pub(crate) fn names_cover(names: &[String], key: &str) -> bool {
    let has = |name: &str| names.binary_search_by(|n| n.as_str().cmp(name)).is_ok();
    has(key) || key_parents(key).any(has)
}

// Whether `sorted` holds `item`.
pub(super) fn contains<T: Ord + ?Sized, U: std::borrow::Borrow<T>>(sorted: &[U], item: &T) -> bool {
    sorted.binary_search_by(|x| x.borrow().cmp(item)).is_ok()
}

// Turns the links a walk followed into the names they reach. The destination walk and the link
// scan share it, so both name a link the same way.
#[derive(Debug)]
pub(super) struct LinkNamer {
    // The walk's paths sit under this root, as the caller spelled it.
    root: PathBuf,
    // A link's target takes its key under this root, with every link on the root resolved. `name`
    // resolves the root when the walk first follows a link, so a tree without links never pays for
    // it.
    canonical_root: Option<PathBuf>,
}

impl LinkNamer {
    pub(super) fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            canonical_root: None,
        }
    }

    // Append the names each of `links` reaches to `out`.
    pub(super) fn name(&mut self, links: &[FollowedLink], out: &mut Vec<AliasedName>) {
        if links.is_empty() {
            return;
        }
        if self.canonical_root.is_none() {
            self.canonical_root = std::fs::canonicalize(&self.root).ok();
        }
        // A target and a later link on a chain have resolved paths, so they take their keys under
        // the resolved root.
        let resolved = |path: &Path| {
            self.canonical_root
                .as_deref()
                .and_then(|root| key_under_root(root, path))
        };
        let target = |link: &Path| std::fs::canonicalize(link).ok().and_then(|t| resolved(&t));
        for link in links {
            match link {
                // Every key through the link names a file of the target. Deleting such a key removes
                // that file, so the merge holds the link's own name.
                FollowedLink::Directory(path) => {
                    let own = key_under_root(&self.root, path);
                    out.extend(own.map(AliasedName::Directory));
                    out.extend(target(path).map(AliasedName::Directory));
                }
                // Removing a file link unlinks the link and leaves its target, so the link's own name
                // stays deletable. Removing a later link on the chain leaves this one dangling.
                FollowedLink::File(path) => {
                    let chain = later_links(path)
                        .into_iter()
                        .filter_map(|hop| resolved(&hop));
                    out.extend(chain.map(AliasedName::File));
                    out.extend(target(path).map(AliasedName::File));
                }
            }
        }
    }
}

// A second walk of a download's destination that records only the links it follows. The run
// reads it beside the merge, so the first download does not wait for it. Every local delete waits
// until it ends, because a link anywhere in the tree can reach the file a delete removes.
#[derive(Debug)]
pub(crate) struct LinkScan {
    walk: FsWalk,
    namer: LinkNamer,
    pub(super) names: Vec<AliasedName>,
    // The path of each link the scan followed, so the destination walk can tell which links the
    // scan already named.
    pub(super) seen: Vec<PathBuf>,
    // Set once the tree holds more names than the bound. The scan stops there, because one more
    // name changes nothing: the run holds every local delete either way.
    pub(super) too_many: bool,
    done: bool,
}

impl LinkScan {
    pub(super) fn new(walker: &Walker, root: &Path) -> Self {
        Self {
            walk: walker.fs_walk(root.to_path_buf(), SortOrder::Native, true),
            namer: LinkNamer::new(root),
            names: Vec::new(),
            seen: Vec::new(),
            too_many: false,
            done: false,
        }
    }

    // Whether the scan has read the whole tree, or given up past the bound.
    pub(crate) fn is_done(&self) -> bool {
        self.done
    }

    // Read up to `budget` directories. Return true once the scan has read the whole tree. A
    // directory the scan cannot read costs it nothing: the destination walk cannot read that
    // directory either, so no key reaches a file through a link inside it.
    pub(crate) async fn advance(&mut self, budget: usize) -> bool {
        if self.done {
            return true;
        }
        self.done = self.walk.read_dirs(budget);
        let links = self.walk.take_followed();
        self.seen
            .extend(links.iter().map(|link| link.path().to_path_buf()));
        self.namer.name(&links, &mut self.names);
        // Many links can reach one target, so the scan counts each name once before it judges the
        // bound.
        if self.names.len() > ALIASED_NAMES_HELD {
            self.names
                .sort_unstable_by(|a, b| name_of(a).cmp(name_of(b)));
            self.names.dedup_by(|a, b| name_of(a) == name_of(b));
            if self.names.len() > ALIASED_NAMES_HELD {
                self.too_many = true;
                self.done = true;
                self.names = Vec::new();
                self.seen = Vec::new();
            }
        }
        self.done
    }
}

// Return the links a chain passes through after its first. Each path joins a resolved parent
// directory and the link's own name, because resolving the whole path would name the file at the
// end of the chain.
//
// The walk resolved this chain already, and one path passes through at most 40 links on Linux and
// 32 on macOS. The bound stops a chain that became a loop since.
fn later_links(link: &Path) -> Vec<PathBuf> {
    const MOST_LINKS: usize = 40;
    let mut later = Vec::new();
    let mut at = link.to_path_buf();
    while later.len() < MOST_LINKS {
        let Ok(target) = std::fs::read_link(&at) else {
            break;
        };
        let next = at.parent().unwrap_or(Path::new("")).join(target);
        let (Some(dir), Some(name)) = (next.parent(), next.file_name()) else {
            break;
        };
        let is_link = std::fs::symlink_metadata(dir.join(name))
            .is_ok_and(|meta| meta.file_type().is_symlink());
        if !is_link {
            break;
        }
        let Ok(dir) = std::fs::canonicalize(dir) else {
            break;
        };
        at = dir.join(name);
        later.push(at.clone());
    }
    later
}
