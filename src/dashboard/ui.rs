use std::io;
use std::sync::Arc;
use std::time::Duration;

use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use parking_lot::RwLock;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Sparkline, Table},
    Frame, Terminal,
};
use rust_decimal::Decimal;

use super::state::{DashboardState, OrderStatus};

pub type SharedDashboard = Arc<RwLock<DashboardState>>;

const ORANGE: Color = Color::Rgb(255, 165, 0);
const GREEN: Color = Color::Rgb(0, 255, 100);
const RED: Color = Color::Rgb(255, 60, 60);
const CYAN: Color = Color::Rgb(0, 200, 255);
const DIM: Color = Color::Rgb(120, 120, 120);
const BG: Color = Color::Rgb(15, 15, 25);
const PANEL_BG: Color = Color::Rgb(20, 22, 35);

fn pnl_color(val: Decimal) -> Color {
    if val > Decimal::ZERO {
        GREEN
    } else if val < Decimal::ZERO {
        RED
    } else {
        Color::White
    }
}

fn pnl_color_f64(val: f64) -> Color {
    if val > 0.0 {
        GREEN
    } else if val < 0.0 {
        RED
    } else {
        Color::White
    }
}

fn format_pnl(val: Decimal) -> String {
    if val >= Decimal::ZERO {
        format!("+${:.2}", val)
    } else {
        format!("-${:.2}", val.abs())
    }
}

/// Run the TUI event loop. Blocks until user presses 'q' or Ctrl+C.
/// `shutdown_flag` is set to true to signal the orchestrator to stop.
pub fn run_dashboard(
    dashboard: SharedDashboard,
    shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
) -> io::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    loop {
        if shutdown_flag.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }

        terminal.draw(|f| {
            let state = dashboard.read().clone();
            render_dashboard(f, &state);
        })?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Char('Q') => {
                        shutdown_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                        break;
                    }
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        shutdown_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

fn render_dashboard(f: &mut Frame, state: &DashboardState) {
    let size = f.area();
    f.render_widget(Block::default().style(Style::default().bg(BG)), size);

    // Main layout: [status_bar | body | bottom_stats]
    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // status bar
            Constraint::Min(10),   // body
            Constraint::Length(3), // bottom stats
        ])
        .split(size);

    render_status_bar(f, main_chunks[0], state);
    render_body(f, main_chunks[1], state);
    render_bottom_stats(f, main_chunks[2], state);
}

fn render_status_bar(f: &mut Frame, area: Rect, state: &DashboardState) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(70), Constraint::Percentage(30)])
        .split(area);

    // Left: BTC price + PNL + stats
    let btc_change = if state.btc_open > 0.0 {
        ((state.btc_price - state.btc_open) / state.btc_open) * 100.0
    } else {
        0.0
    };
    let btc_arrow = if btc_change >= 0.0 { "▲" } else { "▼" };

    let line = Line::from(vec![
        Span::styled(
            " BTC ",
            Style::default().fg(ORANGE).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("${:.2} ", state.btc_price),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("{btc_arrow}{:.2}% ", btc_change.abs()),
            Style::default().fg(pnl_color_f64(btc_change)),
        ),
        Span::styled(" │ ", Style::default().fg(DIM)),
        Span::styled("PNL ", Style::default().fg(DIM)),
        Span::styled(
            format_pnl(state.total_pnl),
            Style::default()
                .fg(pnl_color(state.total_pnl))
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" │ ", Style::default().fg(DIM)),
        Span::styled("Today ", Style::default().fg(DIM)),
        Span::styled(
            format_pnl(state.today_pnl),
            Style::default().fg(pnl_color(state.today_pnl)),
        ),
        Span::styled(" │ ", Style::default().fg(DIM)),
        Span::styled(
            format!("WR {:.0}% ", state.win_rate * 100.0),
            Style::default().fg(CYAN),
        ),
        Span::styled(" │ ", Style::default().fg(DIM)),
        Span::styled(
            format!("Periods {} ", state.total_periods),
            Style::default().fg(Color::White),
        ),
        Span::styled(" │ ", Style::default().fg(DIM)),
        Span::styled(
            format!("Open {} ", state.open_positions),
            Style::default().fg(ORANGE),
        ),
    ]);

    let status = Paragraph::new(line).block(
        Block::default()
            .borders(Borders::BOTTOM)
            .border_style(Style::default().fg(DIM))
            .style(Style::default().bg(PANEL_BG)),
    );
    f.render_widget(status, chunks[0]);

    // Right: countdown + market window
    let countdown = state.countdown_str();
    let market_label = if state.active_market_question.is_empty() {
        "No active market".to_string()
    } else {
        // Extract just the time range from the question
        let q = &state.active_market_question;
        if let Some(dash_idx) = q.find(" - ") {
            q[dash_idx + 3..].to_string()
        } else {
            q.clone()
        }
    };

    let right_line = Line::from(vec![
        Span::styled("Next ", Style::default().fg(DIM)),
        Span::styled(
            &countdown,
            Style::default().fg(ORANGE).add_modifier(Modifier::BOLD),
        ),
        Span::styled(" │ ", Style::default().fg(DIM)),
        Span::styled(market_label, Style::default().fg(CYAN)),
        Span::raw(" "),
    ]);

    let right = Paragraph::new(right_line)
        .alignment(ratatui::layout::Alignment::Right)
        .block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(DIM))
                .style(Style::default().bg(PANEL_BG)),
        );
    f.render_widget(right, chunks[1]);
}

fn render_body(f: &mut Frame, area: Rect, state: &DashboardState) {
    // Body: [left_panel (70%) | right_sidebar (30%)]
    let body_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(70), Constraint::Percentage(30)])
        .split(area);

    render_left_panel(f, body_chunks[0], state);
    render_right_sidebar(f, body_chunks[1], state);
}

fn render_left_panel(f: &mut Frame, area: Rect, state: &DashboardState) {
    // Left: [charts_row | order_feed | pipeline]
    let left_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(10), // charts row
            Constraint::Min(6),     // order feed
            Constraint::Length(5),  // pipeline
        ])
        .split(area);

    render_charts_row(f, left_chunks[0], state);
    render_order_feed(f, left_chunks[1], state);
    render_pipeline(f, left_chunks[2], state);
}

fn render_charts_row(f: &mut Frame, area: Rect, state: &DashboardState) {
    // Charts: [PNL chart | BTC price | Equity curve]
    let chart_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(34),
            Constraint::Percentage(33),
            Constraint::Percentage(33),
        ])
        .split(area);

    // Cumulative PNL sparkline
    let pnl_data: Vec<u64> = normalize_sparkline_data(&state.pnl_history);
    let pnl_spark = Sparkline::default()
        .block(
            Block::default()
                .title(Line::from(vec![
                    Span::styled(
                        " Cumulative PNL ",
                        Style::default().fg(GREEN).add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        format_pnl(state.total_pnl),
                        Style::default().fg(pnl_color(state.total_pnl)),
                    ),
                ]))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(DIM))
                .style(Style::default().bg(PANEL_BG)),
        )
        .data(&pnl_data)
        .style(Style::default().fg(GREEN));
    f.render_widget(pnl_spark, chart_chunks[0]);

    // BTC Price sparkline
    let btc_data: Vec<u64> = normalize_sparkline_data(&state.btc_price_history);
    let btc_change_pct = if state.btc_open > 0.0 {
        ((state.btc_price - state.btc_open) / state.btc_open) * 100.0
    } else {
        0.0
    };
    let btc_spark = Sparkline::default()
        .block(
            Block::default()
                .title(Line::from(vec![
                    Span::styled(
                        " BTC/USD ",
                        Style::default().fg(ORANGE).add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        format!("${:.2}", state.btc_price),
                        Style::default().fg(Color::White),
                    ),
                ]))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(DIM))
                .style(Style::default().bg(PANEL_BG)),
        )
        .data(&btc_data)
        .style(Style::default().fg(if btc_change_pct >= 0.0 { CYAN } else { RED }));
    f.render_widget(btc_spark, chart_chunks[1]);

    // Equity curve sparkline
    let eq_data: Vec<u64> = normalize_sparkline_data(&state.equity_history);
    let eq_spark = Sparkline::default()
        .block(
            Block::default()
                .title(Span::styled(
                    " Equity Curve ",
                    Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
                ))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(DIM))
                .style(Style::default().bg(PANEL_BG)),
        )
        .data(&eq_data)
        .style(Style::default().fg(CYAN));
    f.render_widget(eq_spark, chart_chunks[2]);
}

fn render_order_feed(f: &mut Frame, area: Rect, state: &DashboardState) {
    let header = Row::new(vec![
        Cell::from("Time").style(Style::default().fg(DIM)),
        Cell::from("Market").style(Style::default().fg(DIM)),
        Cell::from("Side").style(Style::default().fg(DIM)),
        Cell::from("Outcome").style(Style::default().fg(DIM)),
        Cell::from("Price").style(Style::default().fg(DIM)),
        Cell::from("Size").style(Style::default().fg(DIM)),
        Cell::from("Status").style(Style::default().fg(DIM)),
    ]);

    let rows: Vec<Row> = state
        .order_feed
        .iter()
        .rev()
        .take(area.height.saturating_sub(4) as usize)
        .map(|o| {
            let status_color = match o.status {
                OrderStatus::Filled => GREEN,
                OrderStatus::Pending => ORANGE,
                OrderStatus::Cancelled => DIM,
                OrderStatus::Rejected => RED,
            };
            let side_color = if o.side == "BUY" { GREEN } else { RED };
            let outcome_color = if o.outcome == "UP" || o.outcome == "YES" {
                GREEN
            } else {
                RED
            };

            Row::new(vec![
                Cell::from(o.time.format("%H:%M:%S").to_string()).style(Style::default().fg(DIM)),
                Cell::from(o.market.clone()).style(Style::default().fg(Color::White)),
                Cell::from(o.side.clone()).style(Style::default().fg(side_color)),
                Cell::from(o.outcome.clone()).style(Style::default().fg(outcome_color)),
                Cell::from(format!("{:.2}", o.price)).style(Style::default().fg(Color::White)),
                Cell::from(format!("{}", o.size)).style(Style::default().fg(Color::White)),
                Cell::from(o.status.to_string()).style(Style::default().fg(status_color)),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(9),
        Constraint::Min(12),
        Constraint::Length(5),
        Constraint::Length(6),
        Constraint::Length(7),
        Constraint::Length(5),
        Constraint::Length(10),
    ];

    let table = Table::new(rows, widths)
        .header(header.bottom_margin(0))
        .block(
            Block::default()
                .title(Span::styled(
                    " Order Feed ",
                    Style::default().fg(ORANGE).add_modifier(Modifier::BOLD),
                ))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(DIM))
                .style(Style::default().bg(PANEL_BG)),
        );
    f.render_widget(table, area);
}

fn render_pipeline(f: &mut Frame, area: Rect, state: &DashboardState) {
    let p = &state.pipeline;

    let step = |name: &str, ok: bool| -> Span {
        if ok {
            Span::styled(
                format!(" {name} "),
                Style::default().fg(Color::Black).bg(GREEN),
            )
        } else {
            Span::styled(
                format!(" {name} "),
                Style::default().fg(Color::White).bg(Color::Rgb(60, 60, 60)),
            )
        }
    };
    let arrow = Span::styled(" → ", Style::default().fg(DIM));

    let pipeline_line = Line::from(vec![
        Span::raw("  "),
        step("CEX Feed", p.cex_feed_ok),
        arrow.clone(),
        step("PM Odds", p.pm_odds_ok),
        arrow.clone(),
        step(&format!("Edge {:.1}%", p.last_edge * 100.0), p.edge_found),
        arrow.clone(),
        step(&format!("Kelly {:.2}", p.last_kelly), p.kelly_ok),
        arrow.clone(),
        step("EXEC", p.exec_ok),
    ]);

    let fv_line = Line::from(vec![
        Span::raw("  "),
        Span::styled("FV↑ ", Style::default().fg(GREEN)),
        Span::styled(
            format!("{:.4}", state.fv_up),
            Style::default().fg(Color::White),
        ),
        Span::styled("  FV↓ ", Style::default().fg(RED)),
        Span::styled(
            format!("{:.4}", state.fv_down),
            Style::default().fg(Color::White),
        ),
        Span::styled("  σ ", Style::default().fg(CYAN)),
        Span::styled(
            format!("{:.8}", state.sigma),
            Style::default().fg(Color::White),
        ),
        Span::styled("  Bid↑ ", Style::default().fg(GREEN)),
        Span::styled(
            format!("{:.2}", state.bid_yes),
            Style::default().fg(Color::White),
        ),
        Span::styled("  Bid↓ ", Style::default().fg(RED)),
        Span::styled(
            format!("{:.2}", state.bid_no),
            Style::default().fg(Color::White),
        ),
        Span::styled("  Σ ", Style::default().fg(ORANGE)),
        Span::styled(
            format!("{:.2}", state.combined_bid),
            Style::default().fg(Color::White),
        ),
    ]);

    let pipeline_widget = Paragraph::new(vec![pipeline_line, fv_line]).block(
        Block::default()
            .title(Span::styled(
                " Execution Pipeline ",
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            ))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(DIM))
            .style(Style::default().bg(PANEL_BG)),
    );
    f.render_widget(pipeline_widget, area);
}

fn render_right_sidebar(f: &mut Frame, area: Rect, state: &DashboardState) {
    // Right sidebar: [signal_flow | positions]
    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8), // signal flow / market info
            Constraint::Min(6),    // positions
        ])
        .split(area);

    render_signal_flow(f, right_chunks[0], state);
    render_positions(f, right_chunks[1], state);
}

fn render_signal_flow(f: &mut Frame, area: Rect, state: &DashboardState) {
    let btc_move = if state.btc_open > 0.0 {
        state.btc_price - state.btc_open
    } else {
        0.0
    };
    let direction = if btc_move > 0.0 {
        "UP"
    } else if btc_move < 0.0 {
        "DOWN"
    } else {
        "FLAT"
    };
    let dir_color = if btc_move > 0.0 {
        GREEN
    } else if btc_move < 0.0 {
        RED
    } else {
        DIM
    };

    let lines = vec![
        Line::from(vec![
            Span::styled("  Direction: ", Style::default().fg(DIM)),
            Span::styled(
                direction,
                Style::default().fg(dir_color).add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("  BTC Move:  ", Style::default().fg(DIM)),
            Span::styled(
                format!("{:+.2}", btc_move),
                Style::default().fg(pnl_color_f64(btc_move)),
            ),
        ]),
        Line::from(vec![
            Span::styled("  Open:      ", Style::default().fg(DIM)),
            Span::styled(
                format!("${:.2}", state.btc_open),
                Style::default().fg(Color::White),
            ),
        ]),
        Line::from(vec![
            Span::styled("  Vol/s:     ", Style::default().fg(DIM)),
            Span::styled(
                format!("{:.8}", state.vol_per_sec),
                Style::default().fg(CYAN),
            ),
        ]),
        Line::from(vec![
            Span::styled("  Markets:   ", Style::default().fg(DIM)),
            Span::styled(
                format!("{}/{}", state.active_market_count, state.markets_discovered),
                Style::default().fg(Color::White),
            ),
        ]),
    ];

    let widget = Paragraph::new(lines).block(
        Block::default()
            .title(Span::styled(
                " Signal Flow ",
                Style::default().fg(ORANGE).add_modifier(Modifier::BOLD),
            ))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(DIM))
            .style(Style::default().bg(PANEL_BG)),
    );
    f.render_widget(widget, area);
}

fn render_positions(f: &mut Frame, area: Rect, state: &DashboardState) {
    let rows: Vec<Row> = state
        .positions
        .iter()
        .map(|p| {
            let outcome_color = if p.outcome == "UP" || p.outcome == "YES" {
                GREEN
            } else {
                RED
            };
            let pnl_c = pnl_color(p.pnl);
            let status = if p.resolved {
                p.winner.as_deref().unwrap_or("—")
            } else {
                "OPEN"
            };
            let status_color = if p.resolved { CYAN } else { ORANGE };

            Row::new(vec![
                Cell::from(p.outcome.clone()).style(
                    Style::default()
                        .fg(outcome_color)
                        .add_modifier(Modifier::BOLD),
                ),
                Cell::from(format!("{:.2}", p.entry_price))
                    .style(Style::default().fg(Color::White)),
                Cell::from(format!("{}", p.size)).style(Style::default().fg(Color::White)),
                Cell::from(format_pnl(p.pnl)).style(Style::default().fg(pnl_c)),
                Cell::from(status.to_string()).style(Style::default().fg(status_color)),
            ])
        })
        .collect();

    let header = Row::new(vec![
        Cell::from("Side").style(Style::default().fg(DIM)),
        Cell::from("Entry").style(Style::default().fg(DIM)),
        Cell::from("Size").style(Style::default().fg(DIM)),
        Cell::from("PNL").style(Style::default().fg(DIM)),
        Cell::from("Status").style(Style::default().fg(DIM)),
    ]);

    let widths = [
        Constraint::Length(5),
        Constraint::Length(6),
        Constraint::Length(5),
        Constraint::Length(8),
        Constraint::Length(6),
    ];

    let table = Table::new(rows, widths).header(header).block(
        Block::default()
            .title(Span::styled(
                " Positions ",
                Style::default().fg(GREEN).add_modifier(Modifier::BOLD),
            ))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(DIM))
            .style(Style::default().bg(PANEL_BG)),
    );
    f.render_widget(table, area);
}

fn render_bottom_stats(f: &mut Frame, area: Rect, state: &DashboardState) {
    let line = Line::from(vec![
        Span::styled("  Avg/Trade ", Style::default().fg(DIM)),
        Span::styled(
            format_pnl(state.avg_per_trade),
            Style::default().fg(pnl_color(state.avg_per_trade)),
        ),
        Span::styled("  │  Sharpe ", Style::default().fg(DIM)),
        Span::styled(format!("{:.2}", state.sharpe), Style::default().fg(CYAN)),
        Span::styled("  │  Max DD ", Style::default().fg(DIM)),
        Span::styled(
            format!("${:.2}", state.max_drawdown),
            Style::default().fg(RED),
        ),
        Span::styled("  │  Open Pos ", Style::default().fg(DIM)),
        Span::styled(
            format!("{}", state.open_positions),
            Style::default().fg(ORANGE),
        ),
        Span::styled("  │  Kelly F* ", Style::default().fg(DIM)),
        Span::styled(
            format!("{:.3}", state.kelly_fraction),
            Style::default().fg(CYAN),
        ),
        Span::styled("  │  DD Limit ", Style::default().fg(DIM)),
        Span::styled(format!("${:.2}", state.dd_limit), Style::default().fg(RED)),
        Span::styled("  │  W/L ", Style::default().fg(DIM)),
        Span::styled(
            format!("{}/{}", state.wins, state.losses),
            Style::default().fg(Color::White),
        ),
        Span::styled("  │  Uptime ", Style::default().fg(DIM)),
        Span::styled(format_uptime(state.uptime_secs), Style::default().fg(DIM)),
    ]);

    let bar = Paragraph::new(line).block(
        Block::default()
            .borders(Borders::TOP)
            .border_style(Style::default().fg(DIM))
            .style(Style::default().bg(PANEL_BG)),
    );
    f.render_widget(bar, area);
}

/// Normalize floating point data to u64 values for Sparkline (0-100 range).
fn normalize_sparkline_data(data: &std::collections::VecDeque<f64>) -> Vec<u64> {
    if data.is_empty() {
        return vec![0];
    }
    let min = data.iter().copied().fold(f64::INFINITY, f64::min);
    let max = data.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let range = max - min;
    if range < 1e-12 {
        return data.iter().map(|_| 50).collect();
    }
    data.iter()
        .map(|&v| ((v - min) / range * 100.0) as u64)
        .collect()
}

fn format_uptime(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}
