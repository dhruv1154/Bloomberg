use ratatui::{
    backend::Backend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Sparkline, Table},
    Frame,
};

use crate::state::{get_market_status, MarketStatus, SecurityState, TerminalState};

pub fn render<B: Backend>(f: &mut Frame, state: &TerminalState) {
    // 1. Establish the main structural constraints
    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Top Panel: Command Route Entry Bar
            Constraint::Min(12),    // Center split: Graph (Left) & News Feed (Right)
            Constraint::Length(8),  // Bottom Matrix tables grouping regions
            Constraint::Length(3),  // Absolute Footer: latency numbers and thread allocations
        ])
        .split(f.size());

    // DRAW TOP PANEL (COMMAND ROUTE ENTRY BAR)
    let cmd_block = Block::default()
        .borders(Borders::ALL)
        .title(" GLOBAL COMMAND TERMINAL ROUTE ")
        .border_style(Style::default().fg(Color::Yellow))
        .bg(Color::Rgb(10, 15, 20));

    let cmd_span = if state.command_mode {
        Line::from(vec![
            Span::styled(" :CMD> ", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::styled(&state.command_buffer, Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
            Span::styled(" █ ", Style::default().fg(Color::Yellow).add_modifier(Modifier::SLOW_BLINK)),
            Span::styled(" (Press ESC to cancel console typing, ENTER to commit command query)", Style::default().fg(Color::DarkGray)),
        ])
    } else {
        let active_filter_str = if state.filter_query.is_empty() {
            "Press ':' to enter dynamic command console... (e.g. :filter TCS or :clear)".to_string()
        } else {
            format!("ACTIVE TERM FILTER: '{}' (Press ':' and type ':clear' to reset)", state.filter_query)
        };
        Line::from(vec![
            Span::styled(" :CMD> ", Style::default().fg(Color::DarkGray)),
            Span::styled(active_filter_str, Style::default().fg(Color::Gray).add_modifier(Modifier::ITALIC)),
        ])
    };
    f.render_widget(Paragraph::new(cmd_span).block(cmd_block), main_chunks[0]);

    // SPLIT CENTER PANEL
    let center_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(50), // Left Side: Active Ticker Visual details and plot trend
            Constraint::Percentage(50), // Right Side: Global news wire feeds
        ])
        .split(main_chunks[1]);

    // CENTER LEFT: Active Ticker metrics + price histogram sparkline
    let left_sub_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(7), // Details
            Constraint::Min(5),    // Sparkline Plot Trend
        ])
        .split(center_chunks[0]);

    let selected_sec = state.selected_security();
    let change_color = if selected_sec.change >= 0.0 { Color::Green } else { Color::Red };
    let indicator = if selected_sec.change >= 0.0 { "▲" } else { "▼" };
    let active_status = get_market_status(&selected_sec.exchange, state.current_time);

    let left_para_text = vec![
        Line::from(vec![
            Span::styled(" ACTIVE TICKER:    ", Style::default().fg(Color::Yellow)),
            Span::styled(format!("{} ", selected_sec.symbol), Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
            Span::raw("("),
            Span::styled(&selected_sec.name, Style::default().fg(Color::Gray)),
            Span::raw(")"),
        ]),
        Line::from(vec![
            Span::styled(" LAST PRICE:      ", Style::default().fg(Color::Yellow)),
            Span::styled(
                format!("{}{:.2}", selected_sec.currency.symbol(), selected_sec.price),
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD)
            ),
        ]),
        Line::from(vec![
            Span::styled(" CHG / %CHG:       ", Style::default().fg(Color::Yellow)),
            Span::styled(
                format!("{} {:+.2} ({:+.2}%)", indicator, selected_sec.change, selected_sec.pct_change),
                Style::default().fg(change_color).add_modifier(Modifier::BOLD)
            ),
        ]),
        Line::from(vec![
            Span::styled(" EXCHANGE SESSION: ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{} [", selected_sec.exchange)),
            match active_status {
                MarketStatus::Open => Span::styled("OPEN", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
                MarketStatus::Closed => Span::styled("CLOSED", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
                MarketStatus::PreMarket => Span::styled("PRE-MARKET", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
            },
            Span::raw("]"),
        ]),
        Line::from(vec![
            Span::styled(" RUNNING VOLUME:   ", Style::default().fg(Color::Yellow)),
            Span::raw(format_volume(selected_sec.volume)),
        ]),
    ];

    f.render_widget(
        Paragraph::new(left_para_text).block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" {} DOCK STATS ", selected_sec.symbol))
                .border_style(Style::default().fg(Color::Yellow))
        ),
        left_sub_chunks[0],
    );

    // Sparkline trend rendering
    let min_val = selected_sec.history.iter().copied().fold(f64::INFINITY, f64::min);
    let max_val = selected_sec.history.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let range = if max_val - min_val > 0.0 { max_val - min_val } else { 1.0 };
    let normalized_sparks: Vec<u64> = selected_sec.history.iter()
        .map(|v| (((v - min_val) / range) * 8.0) as u64)
        .collect();

    let chart_block = Block::default()
        .borders(Borders::ALL)
        .title(" PRICE TICK HISTOGRAM TREND (ZERO-ALLOC COORDS) ")
        .border_style(Style::default().fg(Color::Yellow));

    let sparkline = Sparkline::default()
        .block(chart_block)
        .style(Style::default().fg(change_color))
        .data(&normalized_sparks);

    f.render_widget(sparkline, left_sub_chunks[1]);

    // CENTER RIGHT: News wire feed box
    let mut news_lines = Vec::new();
    for news in state.news_articles.iter().rev().take(5) {
        let clock_span = Span::styled(format!("[{}] ", news.timestamp), Style::default().fg(Color::Cyan));
        let src_span = Span::styled(format!("{}: ", news.source), Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD));
        let urgency_color = match news.urgency.as_str() {
            "HIGH" => Color::Red,
            "MED" => Color::LightYellow,
            _ => Color::Gray,
        };
        let head_span = Span::styled(&news.headline, Style::default().fg(urgency_color));
        news_lines.push(Line::from(vec![clock_span, src_span, head_span]));
    }

    let news_box = Paragraph::new(news_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" DIRECT QUANT WIRE (REAL-TIME RSS/REUTERS STREAM) ")
                .border_style(Style::default().fg(Color::Yellow))
        );
    f.render_widget(news_box, center_chunks[1]);

    // BOTTOM PANEL: Multi-exchange overview grids grouped by region
    let bottom_regions = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(33), // Indian DESK (NSE/BSE)
            Constraint::Percentage(33), // Chinese DESK (SSE)
            Constraint::Percentage(34), // US DESK (NYSE/NASDAQ)
        ])
        .split(main_chunks[2]);

    let global_selected_symbol = &state.selected_security().symbol;

    // Render Indian DESK NSE/BSE column
    let in_table = make_region_table(
        " INDIA DESK (NSE/BSE) ",
        Color::Yellow,
        &state.securities,
        "NSE",
        global_selected_symbol,
        state.current_time,
    );
    f.render_widget(in_table, bottom_regions[0]);

    // Render Chinese DESK SSE column
    let cn_table = make_region_table(
        " CHINA DESK (SSE) ",
        Color::Cyan,
        &state.securities,
        "SSE",
        global_selected_symbol,
        state.current_time,
    );
    f.render_widget(cn_table, bottom_regions[1]);

    // Render US DESK NYSE/NASDAQ column
    let us_table = make_region_table(
        " US DESK (NYSE/NASDAQ) ",
        Color::DarkGray,
        &state.securities,
        "US",
        global_selected_symbol,
        state.current_time,
    );
    f.render_widget(us_table, bottom_regions[2]);

    // ABSOLUTE FOOTER PANEL
    let footer_text = vec![
        Line::from(vec![
            Span::styled(" LATENCY: ", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::styled(format!("P50: {:.2}us", state.latency_p50_us), Style::default().fg(Color::Green)),
            Span::raw(" | "),
            Span::styled(format!("P99: {:.2}us", state.latency_p99_us), Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
            Span::raw(" | "),
            Span::styled("AFFINITY: ", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::styled("THREAD PINNED REGISTRY CORE #1", Style::default().fg(Color::Cyan)),
            Span::raw(" | "),
            Span::styled("[Esc] Mode Release | [:] Connect Console | [T] Tick Sim | [SPACE] Step Time", Style::default().fg(Color::DarkGray)),
        ])
    ];

    f.render_widget(
        Paragraph::new(footer_text)
            .block(Block::default().borders(Borders::ALL).border_style(Style::default().fg(Color::DarkGray)))
            .alignment(ratatui::layout::Alignment::Center),
        main_chunks[3]
    );
}

fn make_region_table<'a>(
    title: &'a str,
    border_color: Color,
    securities: &'a [SecurityState],
    exchange_filter: &'a str,
    global_selected_symbol: &str,
    current_time: chrono::DateTime<chrono::FixedOffset>,
) -> Table<'a> {
    let mut rows = Vec::new();
    for sec in securities {
        // Resolve exchange filter match
        let matches_filter = if exchange_filter == "US" {
            sec.exchange == "NYSE" || sec.exchange == "NASDAQ"
        } else {
            sec.exchange == exchange_filter
        };

        if !matches_filter {
            continue;
        }

        let is_selected = sec.symbol == global_selected_symbol;
        let style = if is_selected {
            Style::default().fg(Color::Yellow).bg(Color::Rgb(20, 28, 38)).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Gray)
        };

        let indicator = if is_selected { "▶ " } else { "  " };
        let opt_status = get_market_status(&sec.exchange, current_time);
        let status_span = match opt_status {
            MarketStatus::Open => Span::styled("OPEN", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            MarketStatus::Closed => Span::styled("CLSD", Style::default().fg(Color::Red)),
            MarketStatus::PreMarket => Span::styled("PRE", Style::default().fg(Color::Cyan)),
        };

        let change_style = if sec.change >= 0.0 {
            Style::default().fg(Color::Green)
        } else {
            Style::default().fg(Color::Red)
        };

        rows.push(Row::new(vec![
            Cell::from(format!("{}{}", indicator, sec.symbol)),
            Cell::from(format!("{}{:.2}", sec.currency.symbol(), sec.price)),
            Cell::from(Span::styled(format!("{:+.2}%", sec.pct_change), change_style)),
            Cell::from(status_span),
        ]).style(style));
    }

    Table::new(
        rows,
        [
            Constraint::Percentage(28),
            Constraint::Percentage(28),
            Constraint::Percentage(26),
            Constraint::Percentage(18),
        ]
    )
    .header(
        Row::new(vec!["SYMBOL", "LAST PRICE", "CHG%", "STATE"])
            .style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(title)
            .border_style(Style::default().fg(border_color))
    )
}

fn format_volume(vol: u64) -> String {
    if vol >= 1_000_000_000 {
        format!("{:.2}B", vol as f64 / 1_000_000_000.0)
    } else if vol >= 1_000_000 {
        format!("{:.2}M", vol as f64 / 1_000_000.0)
    } else if vol >= 1_000 {
        format!("{:.2}K", vol as f64 / 1_000.0)
    } else {
        vol.to_string()
    }
}
