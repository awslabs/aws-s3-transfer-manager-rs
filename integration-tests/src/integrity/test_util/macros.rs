/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The case DSL: `download_cases!` and `algorithm_cases!` declare integrity test cases as tables.
//!
//! Each case is a [`Case`]: one object, one way of downloading it, an optional fault or hold, one
//! sink, one expected outcome, and the targets it runs on. A table's nesting becomes each case's
//! module path, which holds `static CASE` and one test function per target that runs it
//! ([`Case::run`]). So a test's name says what its case is.
//!
//! # `download_cases!`
//!
//! Downloads of one kind of object, under several download settings and faults:
//!
//! ```text
//! download_cases! {
//!     <group> {
//!         object: |<alg>| <Object>;
//!         algorithms: [<algorithm>, ...];
//!         via: <mechanism>;
//!         targets: [<target>, ...];
//!         download <setup> {
//!             options: <Download>;
//!             [hold: first_range;]
//!             [ignore: "<reason>";]
//!             expect <outcome> {
//!                 [<sink>, ...],
//!                 <action>(<position>): [<sink>, ...],
//!                 ...
//!             }
//!             ...
//!         }
//!         ...
//!     }
//! }
//! ```
//!
//! | Element                 | Required    | Meaning                                              |
//! | ----------------------- | ----------- | ---------------------------------------------------- |
//! | `<group>`               | one or more | One kind of object. Its name is a path segment.      |
//! | `object:`               | yes         | The [`Object`] to upload, built for each algorithm.  |
//! | `algorithms:`           | yes         | The upload algorithms. Each gets its own module.     |
//! | `via:`                  | yes         | How the bytes are checked: a [`Mechanism`], pinned.  |
//! | `targets:`              | yes         | Where cases run. Faulted and held ones: mocks only.  |
//! | `download <setup>`      | one or more | One way of downloading. Its name is a path segment.  |
//! | `options:`              | yes         | A [`Download`]: `Download::new()` and its methods.   |
//! | `hold:`                 | no          | `first_range`: the first range ends last ([`Hold`]). |
//! | `ignore:`               | no          | Every case in the setup is `#[ignore]`d with it.     |
//! | `expect <outcome>`      | one or more | The outcome of the block's cases: an [`Expect`].     |
//! | `[<sink>, ...]`         | no          | One case per sink, with no fault. Comes first.       |
//! | `<action>(<position>):` | no          | One case per sink, with that fault.                  |
//!
//! An outcome appears at most once per setup, and an `expect` block with no entries fails to
//! compile. Test path:
//!
//! ```text
//! <group>::<algorithm>::<setup>::<outcome>[::<action>::<position>]::<sink>::<target>
//! ```
//!
//! # `algorithm_cases!`
//!
//! Upload algorithms crossed with object shapes. Each algorithm line gives, for each of its
//! algorithms, how the object is validated and two outcomes on the stream sink: with no fault,
//! and with the object's last byte corrupted (`corrupt(tail)`).
//!
//! ```text
//! algorithm_cases! {
//!     <shape> {
//!         object: |<alg>| <Object>;
//!         [options: <Download>;]
//!         [tier: extended;]
//!         targets: [<target>, ...];
//!         [<algorithm>, ...] via <mechanism>
//!             => intact: <outcome>, corrupt: <outcome> [, ignore: "<reason>"];
//!         ...
//!     }
//! }
//! ```
//!
//! | Element        | Required    | Meaning                                                 |
//! | -------------- | ----------- | ------------------------------------------------------- |
//! | `<shape>`      | one or more | One object shape. Its name is a path segment.           |
//! | `object:`      | yes         | As in `download_cases!`. `\|_\|` ignores the algorithm. |
//! | `options:`     | no          | The shape's [`Download`]. Default `Download::new()`.    |
//! | `tier:`        | no          | `extended`: E2E runs the shape on Linux only.           |
//! | `targets:`     | yes         | Where cases run. Corrupt ones: mocks only.              |
//! | algorithm line | one or more | Per algorithm: `via`, then each outcome.                |
//!
//! Test path:
//!
//! ```text
//! <shape>[::extended]::<algorithm>::<outcome>[::corrupt::tail]::stream::<target>
//! ```
//!
//! # Faults
//!
//! A fault is an action at a position, written `<action>(<position>)`. The mock applies it to
//! whichever GET's range, from a `Range` header or a `partNumber`, contains the byte at that
//! position, so a case doesn't depend on the order of requests. A response carries at most one
//! fault or hold.
//!
//! | Action          | What the mock does to the GET containing the byte                |
//! | --------------- | ---------------------------------------------------------------- |
//! | `corrupt`       | Flips the byte, in every such GET. Checksums unchanged.          |
//! | `truncate`      | Cuts the first such GET short. Its retry is intact.              |
//! | `corrupt_retry` | Cuts the first such GET short, then flips the byte in its retry. |
//!
//! | Position | Byte                        | Range                                         |
//! | -------- | --------------------------- | --------------------------------------------- |
//! | `first`  | 0                           | The first, which discovery fetches            |
//! | `later`  | `B`, the download part size | The second, which the request planner fetches |
//! | `tail`   | `L - 1`, the last byte      | The final range                               |
//!
//! `B` is [`Case::range_size`] and `L` the object's length. A `corrupt_retry` case shows that a
//! retried range is checked against the retry's bytes.
//!
//! # Words
//!
//! | Kind           | Words                                                          | Checked by |
//! | -------------- | -------------------------------------------------------------- | ---------- |
//! | Algorithm      | `crc32` `crc32c` `crc64nvme` `sha1` `sha256` `none`            | constant   |
//! | Mechanism      | `whole_response` `per_part` `full_object` `none`               | constant   |
//! | Outcome        | `validated` `integrity_error` `not_validated` `undetected`     | constant   |
//! | Reason         | `Unavailable` `Disabled` `PartialCoverage` `CompositeChecksum` | constant   |
//! | Fault action   | `corrupt` `truncate` `corrupt_retry`                           | macro rule |
//! | Fault position | `first` `later` `tail`                                         | constant   |
//! | Hold           | `first_range`                                                  | constant   |
//! | Sink           | `stream` `path` `file`                                         | constant   |
//! | Target         | `mock_gp` `mock_express` `real_gp` `real_express`              | macro rule |
//! | Tier           | `extended`                                                     | macro rule |
//!
//! `validated` and `integrity_error` take the object's algorithm unless given one, as in
//! `validated(crc64nvme)`; `not_validated` and `undetected` take a reason. A misspelled word that
//! resolves to a constant in [`vocab`](super::vocab) fails to compile with `cannot find value`
//! naming it; one matched by a macro rule, and a misspelled field name, fail with `no rules
//! expected the token`.
//!
//! # Generated items
//!
//! A case with a fault or a hold gets test functions only for its mock targets, and the `real_*`
//! functions are compiled only under `--cfg e2e_test`; [`Case::emitted_targets`] applies the same
//! rule to a case value. Every module level also has a `cases()` function that appends its cases
//! to a [`Cases`], which `required_coverage` walks.
//!
//! # Selecting cases
//!
//! The path spells out each case, so libtest's substring filter selects a slice of the tables.
//! Two filters select the union of their matches; a longer path prefix narrows.
//!
//! ```text
//! ::crc32::                           every CRC32 case
//! ::corrupt_retry::                   every case with that fault action
//! ::mock_express                      every case on the mock's directory bucket
//! download_tests::full_object_mpu::   one group
//! ```
//!
//! # Implementation
//!
//! The macros whose names start with `__` are steps of the two public macros. Each level passes
//! the settings that apply below it as one token tree, the context:
//! `(<mechanism>, (<object>), (<download>), [<ignore>?], <extended>, [<targets>], <hold>)`.
//!
//! [`Case`]: super::vocab::Case
//! [`Case::run`]: super::vocab::Case::run
//! [`Case::range_size`]: super::vocab::Case::range_size
//! [`Case::emitted_targets`]: super::vocab::Case::emitted_targets
//! [`Cases`]: super::vocab::Cases
//! [`Object`]: crate::test_util::object::Object
//! [`Mechanism`]: super::vocab::Mechanism
//! [`Download`]: super::vocab::Download
//! [`Hold`]: super::vocab::Hold
//! [`Expect`]: super::vocab::Expect

/// Outcome blocks: one `mod <outcome>` each, and `cases()`.
#[macro_export]
macro_rules! __outcomes {
    ($ctx:tt; $( $outcome:ident $( ($($args:tt)*) )? { $($body:tt)* } )+) => {
        $(
            pub(crate) mod $outcome {
                $crate::__outcome_body!($ctx, ($outcome [$($($args)*)?]); $($body)*);
            }
        )+
        pub(crate) fn cases(v: &mut $crate::integrity::test_util::vocab::Cases) {
            $( $outcome::cases(v); )+
        }
    };
}

/// One outcome block's body: an optional bare sink list, then `<action>(<position>): [<sinks>]`
/// entries. The entries are sorted into one list per action, so each action becomes one module
/// holding a module per position: `corrupt::later::path`.
#[macro_export]
macro_rules! __outcome_body {
    (@munch $ctx:tt, $exp:tt, $bare:tt, [$($c:tt)*], $t:tt, $r:tt;
        corrupt($at:ident): [$($s:ident),+] $(, $($rest:tt)*)?) => {
        $crate::__outcome_body!(
            @munch $ctx, $exp, $bare, [$($c)* ($at [$($s),+])], $t, $r; $($($rest)*)?
        );
    };
    (@munch $ctx:tt, $exp:tt, $bare:tt, $c:tt, [$($t:tt)*], $r:tt;
        truncate($at:ident): [$($s:ident),+] $(, $($rest:tt)*)?) => {
        $crate::__outcome_body!(
            @munch $ctx, $exp, $bare, $c, [$($t)* ($at [$($s),+])], $r; $($($rest)*)?
        );
    };
    (@munch $ctx:tt, $exp:tt, $bare:tt, $c:tt, $t:tt, [$($r:tt)*];
        corrupt_retry($at:ident): [$($s:ident),+] $(, $($rest:tt)*)?) => {
        $crate::__outcome_body!(
            @munch $ctx, $exp, $bare, $c, $t, [$($r)* ($at [$($s),+])]; $($($rest)*)?
        );
    };
    (@munch $ctx:tt, $exp:tt, [$($bare:ident),*], $c:tt, $t:tt, $r:tt;) => {
        $( $crate::__case!($ctx, $exp, no_fault, (no_fault), $bare); )*
        $crate::__action!(corrupt, $ctx, $exp, $c);
        $crate::__action!(truncate, $ctx, $exp, $t);
        $crate::__action!(corrupt_retry, $ctx, $exp, $r);
        pub(crate) fn cases(v: &mut $crate::integrity::test_util::vocab::Cases) {
            $( $bare::cases(v); )*
            $crate::__action_cases!(corrupt, $c, v);
            $crate::__action_cases!(truncate, $t, v);
            $crate::__action_cases!(corrupt_retry, $r, v);
        }
    };
    ($ctx:tt, $exp:tt;) => {
        compile_error!("an `expect` block needs at least one entry");
    };
    ($ctx:tt, $exp:tt; [$($bare:ident),+] $(, $($rest:tt)*)?) => {
        $crate::__outcome_body!(@munch $ctx, $exp, [$($bare),+], [], [], []; $($($rest)*)?);
    };
    ($ctx:tt, $exp:tt; $($rest:tt)*) => {
        $crate::__outcome_body!(@munch $ctx, $exp, [], [], [], []; $($rest)*);
    };
}

/// One action's entries: `mod <action> { mod <position> { <a case per sink> } }`.
#[macro_export]
macro_rules! __action {
    ($action:ident, $ctx:tt, $exp:tt, []) => {};
    ($action:ident, $ctx:tt, $exp:tt, [$( ($at:ident [$($s:ident),+]) )+]) => {
        pub(crate) mod $action {
            $(
                pub(crate) mod $at {
                    $( $crate::__case!($ctx, $exp, faulted, ($action $at), $s); )+
                    pub(crate) fn cases(v: &mut $crate::integrity::test_util::vocab::Cases) {
                        $( $s::cases(v); )+
                    }
                }
            )+
            pub(crate) fn cases(v: &mut $crate::integrity::test_util::vocab::Cases) {
                $( $at::cases(v); )+
            }
        }
    };
}

/// Collects an action's cases, if the block has any entries for it.
#[macro_export]
macro_rules! __action_cases {
    ($action:ident, [], $v:ident) => {};
    ($action:ident, [$($x:tt)+], $v:ident) => {
        $action::cases($v);
    };
}

/// A case's fault: `None`, or the action at the position.
#[macro_export]
macro_rules! __fault {
    (no_fault) => {
        None
    };
    ($action:ident $at:ident) => {
        fault::$action(at::$at)
    };
}

/// One case: `mod <sink> { static CASE; a test function per target; cases() }`.
#[macro_export]
macro_rules! __case {
    (
        ($mech:ident, ($obj:expr), ($dl:expr), $ign:tt, $ext:tt, [$($t:ident),+], $hold:ident),
        ($o:ident [$($args:tt)*]), $kind:ident, $fault:tt, $sink:ident
    ) => {
        pub(crate) mod $sink {
            #[allow(unused_imports)]
            use $crate::integrity::test_util::vocab::{*, alg::*};
            pub(crate) static CASE: Case = Case {
                path: module_path!(),
                mechanism: mechanism::$mech,
                object: $obj,
                download: $dl,
                hold: hold::$hold,
                fault: $crate::__fault!$fault,
                sink: sink::$sink,
                expect: $crate::__expect!($o [$($args)*] ($obj)),
                ignore: $crate::__opt!($ign),
                extended: $ext,
                targets: &[$($crate::__target!($t)),+],
            };
            $( $crate::__test_fn!($kind, $hold, $ign, $t); )+
            pub(crate) fn cases(v: &mut Cases) {
                v.push(&CASE);
            }
        }
    };
}

/// `validated` and `integrity_error` without an argument take the object's algorithm.
#[macro_export]
macro_rules! __expect {
    ($o:ident [] ($obj:expr)) => {
        expect::$o(($obj).alg)
    };
    ($o:ident [$($args:tt)+] $obj:tt) => {
        expect::$o($($args)+)
    };
}

/// A test function per target. A real target gets one only for a case with no fault and no
/// hold, and only under `--cfg e2e_test`.
#[macro_export]
macro_rules! __test_fn {
    ($f:ident, $h:ident, $ign:tt, mock_gp) => {
        $crate::__test_fn!(@emit $ign, mock_gp, MockGp);
    };
    ($f:ident, $h:ident, $ign:tt, mock_express) => {
        $crate::__test_fn!(@emit $ign, mock_express, MockExpress);
    };
    (no_fault, no_hold, $ign:tt, real_gp) => {
        #[cfg(e2e_test)]
        $crate::__test_fn!(@emit $ign, real_gp, RealGp);
    };
    (no_fault, no_hold, $ign:tt, real_express) => {
        #[cfg(e2e_test)]
        $crate::__test_fn!(@emit $ign, real_express, RealExpress);
    };
    ($f:ident, $h:ident, $ign:tt, real_gp) => {};
    ($f:ident, $h:ident, $ign:tt, real_express) => {};
    (@emit [], $name:ident, $t:ident) => {
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            CASE.run(CaseTarget::$t).await
        }
    };
    (@emit [$ign:literal], $name:ident, $t:ident) => {
        #[tokio::test(flavor = "multi_thread")]
        #[ignore = $ign]
        async fn $name() {
            CASE.run(CaseTarget::$t).await
        }
    };
}

/// A target word's `CaseTarget`.
#[macro_export]
macro_rules! __target {
    (mock_gp) => {
        CaseTarget::MockGp
    };
    (mock_express) => {
        CaseTarget::MockExpress
    };
    (real_gp) => {
        CaseTarget::RealGp
    };
    (real_express) => {
        CaseTarget::RealExpress
    };
}

/// `[<literal>]` as `Some(<literal>)`, and `[]` as `None`.
#[macro_export]
macro_rules! __opt {
    ([$s:literal]) => {
        Some($s)
    };
    ([]) => {
        None
    };
}

/// Declares download cases: one kind of object per group, downloaded under each setup. See the
/// [module documentation](self) for the syntax and the generated items.
#[macro_export]
macro_rules! download_cases {
    ($( $group:ident {
        object: |$a:tt| $obj:expr;
        algorithms: [$($alg:ident),+];
        via: $mech:ident;
        targets: [$($t:ident),+];
        download $($setups:tt)+
    } )+) => {
        $(
            pub(crate) mod $group {
                $crate::download_cases!(
                    @algs ($a, $obj), [$($alg),+], $mech, [$($t),+]; download $($setups)+
                );
            }
        )+
        pub(crate) fn cases(v: &mut $crate::integrity::test_util::vocab::Cases) {
            $( $group::cases(v); )+
        }
    };
    // One module per algorithm, by recursion, so the setups are repeated without nesting.
    (@algs $obj:tt, [$($alg:ident),+], $mech:ident, $targets:tt; $($setups:tt)+) => {
        $crate::download_cases!(@alg_mods $obj, [$($alg),+], $mech, $targets; $($setups)+);
        pub(crate) fn cases(v: &mut $crate::integrity::test_util::vocab::Cases) {
            $( $alg::cases(v); )+
        }
    };
    (@alg_mods $obj:tt, [$alg:ident $(, $rest:ident)*], $mech:ident, $targets:tt;
        $($setups:tt)+) => {
        pub(crate) mod $alg {
            $crate::download_cases!(@setups $alg, $obj, $mech, $targets; $($setups)+);
        }
        $crate::download_cases!(@alg_mods $obj, [$($rest),*], $mech, $targets; $($setups)+);
    };
    (@alg_mods $obj:tt, [], $mech:ident, $targets:tt; $($setups:tt)+) => {};
    (@setups $alg:ident, ($a:tt, $obj:expr), $mech:ident, $targets:tt;
        $( download $setup:ident {
            options: $dl:expr;
            $(hold: $hold:ident;)?
            $(ignore: $ign:literal;)?
            $( expect $outcome:ident $( ($($args:tt)*) )? { $($body:tt)* } )+
        } )+) => {
        $(
            pub(crate) mod $setup {
                $crate::download_cases!(
                    @hold [$($hold)?]
                    ($mech, ({ let $a = alg::$alg; $obj }), ($dl), [$($ign)?], false, $targets);
                    $( $outcome($($($args)*)?) { $($body)* } )+
                );
            }
        )+
        pub(crate) fn cases(v: &mut $crate::integrity::test_util::vocab::Cases) {
            $( $setup::cases(v); )+
        }
    };
    (@hold [] ($($ctx:tt)*); $($blocks:tt)+) => {
        $crate::__outcomes!(($($ctx)*, no_hold); $($blocks)+);
    };
    (@hold [$h:ident] ($($ctx:tt)*); $($blocks:tt)+) => {
        $crate::__outcomes!(($($ctx)*, $h); $($blocks)+);
    };
}

/// Declares algorithm cases: upload algorithms crossed with object shapes, each intact and with
/// its last byte corrupted. See the [module documentation](self) for the syntax and the generated
/// items.
#[macro_export]
macro_rules! algorithm_cases {
    ($( $shape:ident {
        object: |$a:tt| $obj:expr;
        $(options: $dl:expr;)?
        $(tier: $tier:ident;)?
        targets: [$($t:ident),+];
        $(
            [$($alg:ident),+] via $mech:ident
                => intact: $io:ident $( ($($ia:tt)*) )?,
                   corrupt: $co:ident $( ($($ca:tt)*) )?
                   $(, ignore: $ign:literal)?;
        )+
    } )+) => {
        $(
            pub(crate) mod $shape {
                $crate::algorithm_cases!(
                    @tier [$($tier)?] ($a, $obj), [$($dl)?], [$($t),+];
                    $( [$($alg),+] $mech ($io [$($($ia)*)?]) ($co [$($($ca)*)?]) [$($ign)?]; )+
                );
            }
        )+
        pub(crate) fn cases(v: &mut $crate::integrity::test_util::vocab::Cases) {
            $( $shape::cases(v); )+
        }
    };
    // `tier: extended` nests the shape's tests in an `extended` module, which CI skips by path.
    (@tier [extended] $($rest:tt)+) => {
        pub(crate) mod extended {
            $crate::algorithm_cases!(@lines true, $($rest)+);
        }
        pub(crate) fn cases(v: &mut $crate::integrity::test_util::vocab::Cases) {
            extended::cases(v);
        }
    };
    (@tier [] $($rest:tt)+) => {
        $crate::algorithm_cases!(@lines false, $($rest)+);
    };
    (@lines $ext:tt, $obj:tt, [], $($rest:tt)+) => {
        $crate::algorithm_cases!(
            @lines $ext, $obj, [$crate::integrity::test_util::vocab::Download::new()], $($rest)+
        );
    };
    (@lines $ext:tt, $obj:tt, [$dl:expr], $targets:tt;
        $( [$($alg:ident),+] $mech:ident $i:tt $c:tt $ign:tt; )+) => {
        $( $(
            $crate::algorithm_cases!(
                @alg $alg, $obj, ($mech, ($dl), $ign, $ext, $targets), $i, $c
            );
        )+ )+
        pub(crate) fn cases(v: &mut $crate::integrity::test_util::vocab::Cases) {
            $( $( $alg::cases(v); )+ )+
        }
    };
    (@alg $alg:ident, ($a:tt, $obj:expr), ($mech:ident, $dl:tt, $ign:tt, $ext:tt, $targets:tt),
        ($io:ident [$($ia:tt)*]), ($co:ident [$($ca:tt)*])) => {
        pub(crate) mod $alg {
            $crate::__outcomes!(
                ($mech, ({ let $a = alg::$alg; $obj }), $dl, $ign, $ext, $targets, no_hold);
                $io($($ia)*) { [stream] }
                $co($($ca)*) { corrupt(tail): [stream] }
            );
        }
    };
}

/// The macros' self-test: one small invocation of each macro, and tests asserting exactly what
/// they generate.
///
/// The invocations cover a fault-free and a faulted entry, a held setup, two algorithms, a mock
/// and a real target, an argument-less and an argument-taking outcome, and the `extended` tier.
/// Their cases are ignored with a reason that says they are not meant to run.
#[cfg(test)]
mod self_test {
    use crate::integrity::test_util::vocab::*;

    const IGNORE: Option<&str> = Some("macro self-test: declares cases, not meant to run");

    mod d {
        crate::download_cases! {
            full {
                object: |alg| Object::mpu(FullObject, alg);
                algorithms: [crc64nvme, crc32];
                via: full_object;
                targets: [mock_gp, real_gp];
                download default {
                    options: Download::new();
                    ignore: "macro self-test: declares cases, not meant to run";
                    expect validated { [stream], truncate(first): [stream] }
                    expect integrity_error { corrupt(later): [file] }
                }
                download held {
                    options: Download::new().part_size(8 * MIB);
                    hold: first_range;
                    ignore: "macro self-test: declares cases, not meant to run";
                    expect validated { [stream] }
                }
            }
        }
    }
    mod a {
        crate::algorithm_cases! {
            split {
                object: |alg| Object::single_put(alg).len(12 * MIB);
                tier: extended;
                targets: [mock_gp];
                [sha1] via none
                    => intact: not_validated(Unavailable),
                       corrupt: undetected(Unavailable),
                       ignore: "macro self-test: declares cases, not meant to run";
            }
        }
    }

    /// The prefix of every path the invocations generate.
    const PREFIX: &str = "integration_tests::integrity::test_util::macros::self_test::";

    /// The invocations generate exactly these cases, in this order: each path follows the
    /// nesting, an argument-less outcome takes the object's algorithm, each fault and hold word
    /// becomes its value, and each case's emitted targets drop the real target when the case is
    /// faulted or held.
    #[test]
    fn macros_generate_paths_values_and_targets() {
        let mut v = Vec::new();
        d::cases(&mut v);
        a::cases(&mut v);
        let got: Vec<_> = v
            .iter()
            .map(|c| {
                (
                    c.path.trim_start_matches(PREFIX),
                    c.fault,
                    c.hold,
                    c.expect,
                    c.emitted_targets().collect::<Vec<_>>(),
                )
            })
            .collect();
        use CaseTarget::*;
        let held = Some(Hold::FirstRange);
        assert_eq!(
            got,
            vec![
                (
                    "d::full::crc64nvme::default::validated::stream",
                    None,
                    None,
                    Expect::Validated(Crc64Nvme),
                    vec![MockGp, RealGp]
                ),
                (
                    "d::full::crc64nvme::default::validated::truncate::first::stream",
                    fault::truncate(At::First),
                    None,
                    Expect::Validated(Crc64Nvme),
                    vec![MockGp]
                ),
                (
                    "d::full::crc64nvme::default::integrity_error::corrupt::later::file",
                    fault::corrupt(At::Later),
                    None,
                    Expect::IntegrityError(Crc64Nvme),
                    vec![MockGp]
                ),
                (
                    "d::full::crc64nvme::held::validated::stream",
                    None,
                    held,
                    Expect::Validated(Crc64Nvme),
                    vec![MockGp]
                ),
                (
                    "d::full::crc32::default::validated::stream",
                    None,
                    None,
                    Expect::Validated(Crc32),
                    vec![MockGp, RealGp]
                ),
                (
                    "d::full::crc32::default::validated::truncate::first::stream",
                    fault::truncate(At::First),
                    None,
                    Expect::Validated(Crc32),
                    vec![MockGp]
                ),
                (
                    "d::full::crc32::default::integrity_error::corrupt::later::file",
                    fault::corrupt(At::Later),
                    None,
                    Expect::IntegrityError(Crc32),
                    vec![MockGp]
                ),
                (
                    "d::full::crc32::held::validated::stream",
                    None,
                    held,
                    Expect::Validated(Crc32),
                    vec![MockGp]
                ),
                (
                    "a::split::extended::sha1::not_validated::stream",
                    None,
                    None,
                    Expect::NotValidated(Unavailable),
                    vec![MockGp]
                ),
                (
                    "a::split::extended::sha1::undetected::corrupt::tail::stream",
                    fault::corrupt(At::Tail),
                    None,
                    Expect::Undetected(Unavailable),
                    vec![MockGp]
                ),
            ]
        );
        assert!(v.iter().all(|c| c.ignore == IGNORE));
    }

    /// The test functions the invocations generate are exactly those
    /// [`Case::emitted_targets`] names, as the test binary lists them: so the macros' emission
    /// rule and the case value's rule agree, and no faulted or held case gets a real-target test.
    #[test]
    fn macros_emit_a_test_per_emitted_target() {
        let mut v = Vec::new();
        d::cases(&mut v);
        a::cases(&mut v);
        let mut expected: Vec<String> = v
            .iter()
            .flat_map(|c| {
                c.emitted_targets()
                    .filter(|t| t.is_mock() || cfg!(e2e_test))
                    .map(move |t| format!("{}::{}", c.path, target_word(t)))
            })
            .map(|path| path.trim_start_matches("integration_tests::").to_owned())
            .collect();
        expected.sort();

        let list = std::process::Command::new(std::env::current_exe().expect("test binary"))
            // Every generated test is ignored, so `--ignored` lists them and nothing else.
            .args(["--list", "--format", "terse", "--ignored", "self_test::"])
            .output()
            .expect("list the test binary's tests");
        let mut listed: Vec<String> = String::from_utf8(list.stdout)
            .expect("utf-8 test list")
            .lines()
            .filter_map(|line| line.strip_suffix(": test"))
            .filter(|name| name.starts_with("integrity::test_util::macros::self_test::"))
            .map(str::to_owned)
            .collect();
        listed.sort();
        // Ten mock tests, and the two fault-free, unheld cases' `real_gp` tests under e2e_test.
        assert_eq!(expected.len(), if cfg!(e2e_test) { 12 } else { 10 });
        assert_eq!(listed, expected);
    }

    /// A target's word, which names its test function.
    fn target_word(target: CaseTarget) -> &'static str {
        match target {
            CaseTarget::MockGp => "mock_gp",
            CaseTarget::MockExpress => "mock_express",
            CaseTarget::RealGp => "real_gp",
            CaseTarget::RealExpress => "real_express",
        }
    }
}
