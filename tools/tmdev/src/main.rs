/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Developer commands for the Transfer Manager repository.

mod diagnostics;
mod release;

use clap::{CommandFactory, Parser, Subcommand};
use std::process::ExitCode;

/// Parsed developer command and process-wide diagnostic verbosity.
#[derive(Parser)]
#[command(about = "Transfer Manager developer commands")]
struct Cli {
    /// Include command, registry, and source-verification diagnostics.
    #[arg(short, long, global = true)]
    verbose: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

/// Developer operations selected by the top-level parser.
#[derive(Subcommand)]
enum Command {
    /// Prepare, audit, check, tag, or publish workspace releases.
    Release(release::Args),
}

/// Runs one command and reports its complete error chain on failure.
fn main() -> ExitCode {
    let cli = Cli::parse();
    diagnostics::init(cli.verbose);
    let result = match cli.command {
        Some(Command::Release(args)) => release::run(args),
        None => Cli::command().print_help().map_err(Into::into),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            log::error!("{error:#}");
            ExitCode::FAILURE
        }
    }
}
