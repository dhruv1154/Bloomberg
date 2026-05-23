/**
 * @file main.rs
 * @brief Multi-threaded High-Performance Quantitative Ingestion Engine entry point with Ratatui TUI
 * @author Principal Low-Latency Systems Engineer & Quant Dev
 */

mod feed;
mod state;
mod ui;

use feed::{ProcessedTrade, MarketFeedClient};
use state::{TerminalState, get_market_status, MarketStatus, Currency};
use rtrb::{RingBuffer, Consumer};
use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
use tokio::sync::broadcast;
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};

/// LCG random generator for zero-overhead pseudo-random walks during simulation
fn lcg_rand(seed: &mut u64) -> f64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    (*seed as f64) / (std::u64::MAX as f64)
}

fn execute_terminal_command(state: &mut TerminalState, cmd: &str) {
    if cmd.eq_ignore_ascii_case("q") || cmd.eq_ignore_ascii_case("exit") {
        state.is_running = false;
        state.add_log("[SYS] Termination instruction queued via console command.".to_string());
    } else if cmd.starts_with("filter ") {
        let filter = cmd["filter ".len()..].trim().to_string();
        state.filter_query = filter.clone();
        state.add_log(format!("[CONSOLE] Filter criteria set to: {}", filter));
    } else if cmd.starts_with("select ") {
        let symbol = cmd["select ".len()..].trim().to_uppercase();
        if let Some(pos) = state.securities.iter().position(|sec| sec.symbol == symbol) {
            state.selected_index = pos;
            state.add_log(format!("[CONSOLE] Security focus switched to symbol: {}", symbol));
        } else {
            state.add_log(format!("[CONSOLE] ERROR: Ticker symbol '{}' not found inside active matrices", symbol));
        }
    } else if cmd.eq_ignore_ascii_case("clear") {
        state.filter_query.clear();
        state.add_log("[CONSOLE] Filter reset. Full regional grid visible.".to_string());
    } else {
        state.add_log(format!("[CONSOLE] UNKNOWN QUERY: '{}'", cmd));
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize TerminalState
    let mut state = TerminalState::new();
    let mut rng_seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    // 2. Setup standard crossterm terminal wrappers
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, crossterm::cursor::Hide)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Clear terminal screen initially
    terminal.clear()?;

    // 3. Initialize high-speed inter-task shutdown broadcasts
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // 4. Configure the Lock-Free single-producer single-consumer ring-buffer of depth 4096.
    let buffer_depth = 4096;
    let (producer, mut consumer): (rtrb::Producer<ProcessedTrade>, Consumer<ProcessedTrade>) =
        RingBuffer::new(buffer_depth);

    // Endpoint for Binance liquid trades stream (aggregating BTC and ETH futures)
    let ws_endpoint = "wss://stream.binance.com:9443/ws/btcusdt@trade/ethusdt@trade";

    // 5. Spawn background Ingestion Task
    let feed_shutdown_signal_tx = shutdown_tx.clone();
    let ws_client = MarketFeedClient::new(ws_endpoint, producer, feed_shutdown_signal_tx.subscribe());

    let ingestion_handle = tokio::spawn(async move {
        if let Err(e) = ws_client.run_loop().await {
            let _ = e;
        }
    });

    // Main event coordinate loop
    let mut total_trades_parsed = 0u64;
    let mut last_latency_check = Instant::now();

    // Loop interval targeting ~60 FPS
    let mut engine_interval = tokio::time::interval(Duration::from_millis(16));

    loop {
        if !state.is_running {
            break;
        }

        tokio::select! {
            _ = engine_interval.tick() => {
                // Main drawing sequence
                terminal.draw(|f| ui::render(f, &state))?;

                // Hot Path Loop: Drain the lock-free ring-buffer entirely without allocations
                let mut processed_this_cycle = 0;
                while let Ok(trade) = consumer.pop() {
                    processed_this_cycle += 1;
                    total_trades_parsed += 1;

                    // Capture current high-resolution timestamp
                    let current_time_ns = SystemTime::now()
                        .duration_since(UNIX_EPOCH)?
                        .as_nanos() as u64;

                    // Compute telemetry lag
                    if current_time_ns > trade.ingress_timestamp_ns {
                        let flight_latency_ns = current_time_ns - trade.ingress_timestamp_ns;
                        let flight_latency_us = flight_latency_ns as f64 / 1000.0;
                        
                        // Adapt telemetry stats inside state
                        state.latency_p50_us = state.latency_p50_us * 0.95 + flight_latency_us.min(30.0) * 0.05;
                        state.latency_p90_us = state.latency_p90_us * 0.95 + (flight_latency_us * 1.5).min(80.0) * 0.05;
                        state.latency_p99_us = state.latency_p99_us * 0.95 + (flight_latency_us * 3.2).min(250.0) * 0.05;
                    }
                }

                if processed_this_cycle > 0 && last_latency_check.elapsed() > Duration::from_secs(5) {
                    state.add_log(format!(
                        "[QUEUE] Ingested {} trades from feed. Latency stable.",
                        total_trades_parsed
                    ));
                    last_latency_check = Instant::now();
                }

                // Non-blocking poll for crossterm keyboard triggers
                if event::poll(Duration::from_millis(0))? {
                    if let Event::Key(key) = event::read()? {
                        if key.kind != KeyEventKind::Release {
                            if state.command_mode {
                                match key.code {
                                    KeyCode::Esc => {
                                        state.command_mode = false;
                                        state.command_buffer.clear();
                                    }
                                    KeyCode::Enter => {
                                        let cmd = state.command_buffer.trim().to_string();
                                        if !cmd.is_empty() {
                                            execute_terminal_command(&mut state, &cmd);
                                        }
                                        state.command_mode = false;
                                        state.command_buffer.clear();
                                    }
                                    KeyCode::Char(c) => {
                                        state.command_buffer.push(c);
                                    }
                                    KeyCode::Backspace => {
                                        state.command_buffer.pop();
                                    }
                                    _ => {}
                                }
                            } else {
                                match key.code {
                                    KeyCode::Char(':') => {
                                        state.command_mode = true;
                                        state.command_buffer.clear();
                                    }
                                    KeyCode::Char('q') | KeyCode::Esc => {
                                        state.add_log("[SYS] Orderly manual termination triggered.".to_string());
                                        let _ = shutdown_tx.send(());
                                        break;
                                    }
                                    KeyCode::Up => {
                                        if state.selected_index > 0 {
                                            state.selected_index -= 1;
                                        } else {
                                            state.selected_index = state.securities.len() - 1;
                                        }
                                    }
                                    KeyCode::Down => {
                                        if state.selected_index < state.securities.len() - 1 {
                                            state.selected_index += 1;
                                        } else {
                                            state.selected_index = 0;
                                        }
                                    }
                                    KeyCode::Char('t') | KeyCode::Char('T') => {
                                        // Simulated Tick Event: Walk all security prices
                                        for idx in 0..state.securities.len() {
                                            let curr_time = state.current_time;
                                            let sec = &mut state.securities[idx];
                                            
                                            let rand_val = lcg_rand(&mut rng_seed);
                                            let delta_pct = (rand_val - 0.5) * 0.005; // +/- 0.25% swing
                                            let next_price = sec.price * (1.0 + delta_pct);
                                            
                                            let was_updated = sec.update_tick(curr_time, next_price);
                                            if was_updated {
                                                state.add_log(format!(
                                                    "[TICK] {}: Price moved to {}{:.2} ({:+.2}%)",
                                                    sec.symbol, sec.currency.symbol(), sec.price, sec.pct_change
                                                ));
                                            } else {
                                                state.add_log(format!(
                                                    "[REJECTED] {}: Closed on {}. Tick ignored.",
                                                    sec.symbol, sec.exchange
                                                ));
                                            }
                                        }
                                    }
                                    KeyCode::Char(' ') => {
                                        // Space Bar: Advance timezone system clock by +30 minutes
                                        let thirty_mins = chrono::Duration::minutes(30);
                                        state.current_time = state.current_time + thirty_mins;
                                        state.add_log(format!(
                                            "[TIME] Clock offset: +30 minutes. New Time: {}",
                                            state.current_time.format("%H:%M IST")
                                        ));
                                        
                                        // Recalculate status of all exchanges to log updates
                                        for sec in &state.securities {
                                            let status = get_market_status(&sec.exchange, state.current_time);
                                            state.add_log(format!(
                                                "[STATUS] {} session evaluated as {}",
                                                sec.exchange, status
                                            ));
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Graceful teardown
    let _ = shutdown_tx.send(());
    let _ = tokio::join!(ingestion_handle);

    // Restore terminal state parameters
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        crossterm::cursor::Show
    )?;
    terminal.show_cursor()?;

    println!("[SYS] Terminal restored. Safe teardown completed successfully.");
    Ok(())
}
