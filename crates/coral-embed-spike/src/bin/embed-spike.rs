//! SPIKE (coral#2867): the spike alone, for runtime comparisons.

#![allow(clippy::print_stderr, reason = "spike")]

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match coral_embed_spike::run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error:?}");
            ExitCode::FAILURE
        }
    }
}
