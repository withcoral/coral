#![allow(
    clippy::print_stdout,
    reason = "CLI branding intentionally writes directly to stdout"
)]

use dialoguer::console::style;

const CORAL_LOGO_ASCII: [&str; 17] = [
    "              ###",
    "         ##   ###",
    "        .##   .##",
    "         ##   ##   ###",
    "   .      ######   ##",
    "   ### ##  .#.##  ##   ##",
    "   ##. ###  ##    ##  ###",
    "   ##  .##  ##  ########.",
    "    #####   #######.",
    "###    #    # .##  .",
    "######.###  ####   ##  ###",
    "  ########  ##    ##  ###",
    "        ###. #  ########",
    "          ##  #######",
    "          ######",
    "           .##",
    "            ##",
];

const CORAL_WORDMARK_ASCII: [&str; 6] = [
    " ██████╗ ██████╗ ██████╗  █████╗ ██╗",
    "██╔════╝██╔═══██╗██╔══██╗██╔══██╗██║",
    "██║     ██║   ██║██████╔╝███████║██║",
    "██║     ██║   ██║██╔══██╗██╔══██║██║",
    "╚██████╗╚██████╔╝██║  ██║██║  ██║███████╗",
    " ╚═════╝ ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝",
];

pub(crate) fn print_welcome_header() {
    let wordmark_offset = 5;
    let logo_width = CORAL_LOGO_ASCII.iter().map(|s| s.len()).max().unwrap_or(0);

    for (row, logo_line) in CORAL_LOGO_ASCII.iter().enumerate() {
        let padded_logo = format!("{logo_line:<logo_width$}");
        if row >= wordmark_offset && row < wordmark_offset + CORAL_WORDMARK_ASCII.len() {
            let mark_idx = row - wordmark_offset;
            let wordmark_line = CORAL_WORDMARK_ASCII
                .get(mark_idx)
                .expect("wordmark index is bounded by display range");
            println!(
                "{}  {}",
                style(&padded_logo).color256(209),
                style(wordmark_line).bold(),
            );
        } else {
            println!("{}", style(&padded_logo).color256(209));
        }
    }

    println!();
    println!("Coral gives you one SQL interface to query APIs, files, and live data sources.");
    println!("The more sources you connect, the more powerful it gets.");
    println!("Data never leaves your environment.");
}
