#![allow(
    clippy::print_stdout,
    reason = "CLI branding intentionally writes directly to stdout"
)]

use dialoguer::console::style;

use crate::host::CliHost;

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

pub(crate) fn print_welcome_header(host: &mut dyn CliHost) -> Result<(), anyhow::Error> {
    let wordmark_offset = 5;
    let logo_width = CORAL_LOGO_ASCII.iter().map(|s| s.len()).max().unwrap_or(0);

    for (row, logo_line) in CORAL_LOGO_ASCII.iter().enumerate() {
        let padded_logo = format!("{logo_line:<logo_width$}");
        if row >= wordmark_offset && row < wordmark_offset + CORAL_WORDMARK_ASCII.len() {
            let mark_idx = row - wordmark_offset;
            host.println(&format!(
                "{}  {}",
                style(&padded_logo).color256(209),
                style(CORAL_WORDMARK_ASCII[mark_idx]).bold(),
            ))?;
        } else {
            host.println(&format!("{}", style(&padded_logo).color256(209)))?;
        }
    }

    host.println("")?;
    host.println("Coral gives you one SQL interface to query APIs, files, and live data sources.")?;
    host.println("The more sources you connect, the more powerful it gets.")?;
    host.println("Data never leaves your environment.")?;
    Ok(())
}
