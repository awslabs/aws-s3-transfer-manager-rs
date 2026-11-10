/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// This module names the keys that each link in a download's destination reaches. The walk
// holds back the deletes under those keys.

use std::path::{Path, PathBuf};

// Followed destination links keep at most this many names held at once. Past the bound, the source
// holds back every later delete. The value matches how many transfers may wait before the merge
// pauses, so the held names cost about what that queue does.
pub(super) const ALIASED_NAMES_HELD: usize = 1024;

// Return the links a chain passes through after its first. Each path joins a resolved parent
// directory and the link's own name, because resolving the whole path would name the file at the
// end of the chain.
//
// The walk resolved this chain already, and one path passes through at most 40 links on Linux and
// 32 on macOS. The bound stops a chain that became a loop since.
pub(super) fn later_links(link: &Path) -> Vec<PathBuf> {
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
