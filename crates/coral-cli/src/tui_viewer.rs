use std::io;
use arrow::record_batch::RecordBatch;
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::Alignment,
    style::{Color, Style},
    widgets::{Block, Borders, Paragraph},
    Terminal,
};
use unicode_width::UnicodeWidthStr;
use coral_client::format_batches_table;

pub(crate) fn run_tui_viewer(batches: &[RecordBatch]) -> Result<(), anyhow::Error> {
    // Generate the formatted table string
    let table_string = format_batches_table(batches)?;
    let lines: Vec<&str> = table_string.lines().collect();
    let max_y = u16::try_from(lines.len().saturating_sub(1)).unwrap_or(u16::MAX);
    let max_x = u16::try_from(lines.iter().map(|l| l.width()).max().unwrap_or(0)).unwrap_or(u16::MAX);

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    
    // Ensure terminal state is safely restored on all exit paths
    scopeguard::defer! {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut scroll_y: u16 = 0;
    let mut scroll_x: u16 = 0;

    loop {
        terminal.draw(|f| {
            let area = f.area();
            
            // Adjust max scroll bounds based on terminal size
            let visible_y = area.height.saturating_sub(2); // Subtract borders
            let visible_x = area.width.saturating_sub(2);
            
            let clamped_y = scroll_y.min(max_y.saturating_sub(visible_y));
            let clamped_x = scroll_x.min(max_x.saturating_sub(visible_x));

            let paragraph = Paragraph::new(table_string.as_str())
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(" Coral SQL Results (q to quit, arrows to scroll) ")
                        .border_style(Style::default().fg(Color::Cyan)),
                )
                .alignment(Alignment::Left)
                .scroll((clamped_y, clamped_x));

            f.render_widget(paragraph, area);
            
            // Sync back the clamped values
            scroll_y = clamped_y;
            scroll_x = clamped_x;
        })?;

        if let Event::Key(key) = event::read()? {
            if key.kind == KeyEventKind::Press {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Down | KeyCode::Char('j') => {
                        scroll_y = scroll_y.saturating_add(1);
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        scroll_y = scroll_y.saturating_sub(1);
                    }
                    KeyCode::Right | KeyCode::Char('l') => {
                        scroll_x = scroll_x.saturating_add(3);
                    }
                    KeyCode::Left | KeyCode::Char('h') => {
                        scroll_x = scroll_x.saturating_sub(3);
                    }
                    KeyCode::PageDown => {
                        let visible = terminal.size().map(|s| s.height).unwrap_or(10).saturating_sub(2);
                        scroll_y = scroll_y.saturating_add(visible);
                    }
                    KeyCode::PageUp => {
                        let visible = terminal.size().map(|s| s.height).unwrap_or(10).saturating_sub(2);
                        scroll_y = scroll_y.saturating_sub(visible);
                    }
                    KeyCode::Home => {
                        scroll_y = 0;
                        scroll_x = 0;
                    }
                    KeyCode::End => {
                        scroll_y = max_y;
                    }
                    _ => {}
                }
            }
        }
    }

    // Teardown is handled automatically by scopeguard::defer

    Ok(())
}
