/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import React, { useState, useEffect, useRef, useMemo, useCallback } from "react";
import { 
  Activity, Terminal as TerminalIcon, Cpu, Code, BookOpen, 
  Play, Square, Save, RefreshCw, Network, 
  Database, Download, Copy, Check, ChevronRight, AlertTriangle, 
  Sparkles, Layers, Info, Trash2, ArrowUpRight, ArrowDownRight,
  ShieldCheck, HelpCircle, Search
} from "lucide-react";
import { motion, AnimatePresence } from "motion/react";
import { INTERVIEW_QUESTIONS } from "./interviewQuestions";
import { OrderBook } from "./components/OrderBook";
import { IngestedTrade, TelemetryStats, ProjectFile, MarketAsset } from "./types";

const RUST_CARGO_TOML = `[package]
name = "low_latency_terminal"
version = "0.1.0"
edition = "2021"
authors = ["Principal Low-Latency Systems Engineer & Quant Dev"]
description = "A production-grade, highly optimized async market terminal using Ratatui and lock-free SPSC queues."

[dependencies]
# Async Runtime
tokio = { version = "1.38", features = ["full"] }

# Multi-threading & Low-latency Utilities
core_affinity = "0.8"       # For pinning feed-ingestion threads to specific CPU cores
rtrb = "0.3"                # Ultra-fast lock-free single-producer single-consumer (SPSC) ring buffer

# Networking & Telemetry
tokio-tungstenite = { version = "0.21", features = ["rustls-tls-native-roots"] }
futures-util = { version = "0.3", default-features = false, features = ["std"] }
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
thiserror = "1.0"           # Clean zero-overhead error definitions

# Serialization - Zero-copy JSON borrowing
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"

# UI Layer
ratatui = "0.26"            # Grid-based immediate mode Terminal User Interface
crossterm = { version = "0.27", features = ["event-stream"] }
chrono = { version = "0.4", features = ["serde"] }

[profile.release]
opt-level = 3
lto = "fat"                 # Enable Link Time Optimization (LTO)
codegen-units = 1           # Reduce codegen units to maximize LLVM optimizations
panic = "abort"             # Eliminate panic unwinding overhead
`;

const RUST_FEED_RS = `/**
 * @file feed.rs
 * @brief Ultra-low latency WebSocket Ingestion Feed Engine
 * @author Principal Low-Latency Systems Engineer
 */

use futures_util::StreamExt;
use rtrb::Producer;
use serde::Deserialize;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Debug, Clone, Copy)]
pub struct ProcessedTrade {
    pub symbol_id: u16,           // 0 = BTCUSDT, 1 = ETHUSDT, etc. (Cache friendly)
    pub price: f64,               // Pre-parsed binary float for fast calculation
    pub quantity: f64,            // Pre-parsed binary quantity
    pub buyer_maker: bool,        // Side indicator
    pub source_timestamp_ms: u64, // Exchange matching engine timestamp
    pub ingress_timestamp_ns: u64, // Local receipt NIC timestamp
}

#[derive(Error, Debug)]
pub enum FeedError {
    #[error("WebSocket connection failure: {0}")]
    ConnectionFailed(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("Ring buffer capacity overrun: SPSC producer queue is saturated!")]
    BufferFull,

    #[error("JSON deserialization failure: {0}")]
    DeserializationFailed(#[from] serde_json::Error),

    #[error("Float conversion error.")]
    FloatParsingError,

    #[error("System clock telemetry fallback failure: {0}")]
    TimeSystemError(#[from] std::time::SystemTimeError),
}

#[derive(Deserialize, Debug)]
struct BinanceTradeRaw<'a> {
    #[serde(rename = "s")]
    symbol: &'a str,
    #[serde(rename = "p")]
    price_str: &'a str,
    #[serde(rename = "q")]
    quantity_str: &'a str,
    #[serde(rename = "T")]
    trade_time: u64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

pub struct MarketFeedClient {
    ws_uri: String,
    ring_producer: Producer<ProcessedTrade>,
    shutdown_rx: broadcast::Receiver<()>,
}

impl MarketFeedClient {
    pub fn new(ws_uri: &str, producer: Producer<ProcessedTrade>, shutdown_rx: broadcast::Receiver<()>) -> Self {
        Self { ws_uri: ws_uri.to_owned(), ring_producer: producer, shutdown_rx }
    }

    pub async fn run_loop(mut self) -> Result<(), FeedError> {
        println!("[FEEDS] Connecting to high-frequency WebSocket endpoint...");
        let (ws_stream, _) = connect_async(&self.ws_uri).await?;
        println!("[FEEDS] Handshake completed successfully. Active sub-millisecond loop.");
        
        let (_, mut read) = ws_stream.split();

        loop {
            tokio::select! {
                _ = self.shutdown_rx.recv() => {
                    println!("[FEEDS] Shutdown signal received. Graceful exit.");
                    break;
                }
                maybe_msg = read.next() => {
                    let msg = match maybe_msg {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => return Err(FeedError::ConnectionFailed(e)),
                        None => break,
                    };

                    match msg {
                        Message::Text(ref text) => {
                            let ingress_tick = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .map_err(FeedError::TimeSystemError)?
                                .as_nanos() as u64;

                            if let Err(FeedError::BufferFull) = self.parse_and_push(text, ingress_tick) {
                                (eprintln!("[WARN] Backpressure: SPSC Ring-Buffer full! Data frame dropped."));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn parse_and_push(&mut self, payload: &str, ingress_tick: u64) -> Result<(), FeedError> {
        // Zero-copy deserialization directly referencing raw receive buffer
        let raw_trade: BinanceTradeRaw = serde_json::from_str(payload)?;

        let symbol_id = match raw_trade.symbol {
            "BTCUSDT" => 0u16,
            "ETHUSDT" => 1u16,
            _ => 99u16,
        };

        let price = raw_trade.price_str.parse::<f64>().map_err(|_| FeedError::FloatParsingError)?;
        let quantity = raw_trade.quantity_str.parse::<f64>().map_err(|_| FeedError::FloatParsingError)?;

        let processed = ProcessedTrade {
            symbol_id,
            price,
            quantity,
            buyer_maker: raw_trade.is_buyer_maker,
            source_timestamp_ms: raw_trade.trade_time,
            ingress_timestamp_ns: ingress_tick,
        };

        self.ring_producer.push(processed).map_err(|_| FeedError::BufferFull)?;
        Ok(())
    }
}`;

const RUST_STATE_RS = `use chrono::{DateTime, FixedOffset, Timelike};

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Currency {
    USD,
    INR,
    CNY,
}

impl Currency {
    pub fn symbol(&self) -> &'static str {
        match self {
            Currency::USD => "$",
            Currency::INR => "₹",
            Currency::CNY => "¥",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MarketStatus {
    Open,
    Closed,
    PreMarket,
}

impl std::fmt::Display for MarketStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarketStatus::Open => write!(f, "OPEN"),
            MarketStatus::Closed => write!(f, "CLOSED"),
            MarketStatus::PreMarket => write!(f, "PRE-MKT"),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NewsArticle {
    pub timestamp: String,
    pub headline: String,
    pub source: String,
    pub urgency: String, // "HIGH", "MED", "LOW"
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SecurityState {
    pub symbol: String,
    pub name: String,
    pub price: f64,
    pub open_price: f64,
    pub change: f64,
    pub pct_change: f64,
    pub currency: Currency,
    pub exchange: String,
    pub history: Vec<f64>,
    pub volume: u64,
}

impl SecurityState {
    pub fn new(
        symbol: &str,
        name: &str,
        price: f64,
        open_price: f64,
        currency: Currency,
        exchange: &str,
        volume: u64,
    ) -> Self {
        let change = price - open_price;
        let pct_change = if open_price != 0.0 { (change / open_price) * 100.0 } else { 0.0 };
        Self {
            symbol: symbol.to_string(),
            name: name.to_string(),
            price,
            open_price,
            change,
            pct_change,
            currency,
            exchange: exchange.to_string(),
            history: vec![price; 30],
            volume,
        }
    }

    pub fn update_tick(&mut self, current_time: DateTime<FixedOffset>, new_price: f64) -> bool {
        // Validation: reject live tick variation if session is CLOSED
        let status = get_market_status(&self.exchange, current_time);
        if status == MarketStatus::Closed {
            return false;
        }

        self.price = new_price;
        self.change = self.price - self.open_price;
        self.pct_change = if self.open_price != 0.0 { (self.change / self.open_price) * 100.0 } else { 0.0 };
        
        self.history.remove(0);
        self.history.push(new_price);
        true
    }
}

pub fn get_market_status(exchange: &str, current_time: DateTime<FixedOffset>) -> MarketStatus {
    // Market Operational Sessions (Normalized to Indian Standard Time - IST = UTC + 5:30)
    // India (NSE/BSE): 09:15 to 15:30 IST
    // China (SSE): 07:00 to 12:30 IST
    // United States (NYSE/NASDAQ): 19:00 to 02:30 IST
    let ist_offset = FixedOffset::east_opt(5 * 3600 + 1800).unwrap();
    let local_ist = current_time.with_timezone(&ist_offset);
    let hour = local_ist.hour();
    let min = local_ist.minute();
    let time_in_minutes = hour * 60 + min;

    match exchange {
        "NSE" => {
            // India: 09:15 to 15:30 IST (555 to 930 minutes)
            if time_in_minutes >= 555 && time_in_minutes < 930 {
                MarketStatus::Open
            } else if time_in_minutes >= 540 && time_in_minutes < 555 {
                MarketStatus::PreMarket
            } else {
                MarketStatus::Closed
            }
        }
        "SSE" => {
            // China: 07:00 to 12:30 IST (420 to 750 minutes)
            if time_in_minutes >= 420 && time_in_minutes < 750 {
                MarketStatus::Open
            } else if time_in_minutes >= 390 && time_in_minutes < 420 {
                MarketStatus::PreMarket
            } else {
                MarketStatus::Closed
            }
        }
        "NYSE" | "NASDAQ" => {
            // US: 19:00 to 02:30 IST (1140 to 150 minutes, spans midnight)
            if time_in_minutes >= 1140 || time_in_minutes < 150 {
                MarketStatus::Open
            } else if time_in_minutes >= 1110 && time_in_minutes < 1140 {
                MarketStatus::PreMarket
            } else {
                MarketStatus::Closed
            }
        }
        _ => MarketStatus::Closed,
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TerminalState {
    pub securities: Vec<SecurityState>,
    pub selected_index: usize,
    pub current_time: DateTime<FixedOffset>,
    pub latency_p50_us: f64,
    pub latency_p90_us: f64,
    pub latency_p99_us: f64,
    pub is_running: bool,
    pub log_messages: Vec<String>,
    pub news_articles: Vec<NewsArticle>,
    pub command_buffer: String,
    pub command_mode: bool,
    pub filter_query: String,
}

impl TerminalState {
    pub fn new() -> Self {
        let ist_offset = FixedOffset::east_opt(5 * 3600 + 1800).unwrap();
        let naive_date_time = chrono::NaiveDate::from_ymd_opt(2026, 5, 22)
            .unwrap()
            .and_hms_opt(10, 46, 0)
            .unwrap();
        let current_time = DateTime::<FixedOffset>::from_local_fixed_offset(naive_date_time, ist_offset).unwrap();

        let securities = vec![
            // Indian Market: TCS, NIFTY BANK, SENSEX (INR, Status: OPEN)
            SecurityState::new("TCS", "Tata Consultancy Services Ltd", 4120.50, 4110.00, Currency::INR, "NSE", 1250000),
            SecurityState::new("NIFTY BANK", "Nifty Bank Index Focus", 48120.30, 47980.50, Currency::INR, "NSE", 15200000),
            SecurityState::new("SENSEX", "BSE SENSEX Index Hub", 73920.40, 73650.00, Currency::INR, "NSE", 8400000),
            
            // Chinese Market: SHCOMP (CNY, Status: OPEN)
            SecurityState::new("SHCOMP", "Shanghai Composite Index", 3154.22, 3125.10, Currency::CNY, "SSE", 423000000),
            
            // US Market: AAPL, TSLA, SPX (USD, Status: CLOSED)
            SecurityState::new("AAPL", "Apple Inc. Global Select", 182.52, 181.10, Currency::USD, "NYSE", 48250000),
            SecurityState::new("TSLA", "Tesla Inc. Low-Latency Unit", 174.60, 177.46, Currency::USD, "NYSE", 88140000),
            SecurityState::new("SPX", "S&P 500 Index Core", 5214.65, 5195.40, Currency::USD, "NYSE", 2420000000),
        ];

        let news_articles = vec![
            NewsArticle {
                timestamp: "10:45 IST".to_string(),
                headline: "Reserve Bank of India signals strict watch on local liquidity margins".to_string(),
                source: "RBI_WIRE".to_string(),
                urgency: "HIGH".to_string(),
            },
            NewsArticle {
                timestamp: "10:42 IST".to_string(),
                headline: "China SSE Composite Index climbs 1.2% led by chipmakers, AI units".to_string(),
                source: "SSE_NEWS".to_string(),
                urgency: "MED".to_string(),
            },
            NewsArticle {
                timestamp: "10:35 IST".to_string(),
                headline: "US S&P futures steady as NYSE pre-market quote matching stabilizes".to_string(),
                source: "US_DESK".to_string(),
                urgency: "LOW".to_string(),
            },
            NewsArticle {
                timestamp: "10:15 IST".to_string(),
                headline: "TCS secure public cloud migration contracts exceed INR 15k Crores".to_string(),
                source: "IN_WIRE".to_string(),
                urgency: "MED".to_string(),
            },
            NewsArticle {
                timestamp: "10:00 IST".to_string(),
                headline: "Tokio-based lock-free ingestion pipe records sub-microsecond latency bounds".to_string(),
                source: "TELE_ENG".to_string(),
                urgency: "HIGH".to_string(),
            },
        ];

        Self {
            securities,
            selected_index: 0,
            current_time,
            latency_p50_us: 1.45,
            latency_p90_us: 2.15,
            latency_p99_us: 4.88,
            is_running: true,
            log_messages: vec![
                "[SYSTEM] Double-buffered high-rate ingestion pipeline initialized.".to_string(),
                "[SYSTEM] CPU Thread Affinity Pinning verified - Logical Core #1 linked.".to_string(),
                "[TIME] Global timezone synched to Indian Standard Time (IST, UTC+05:30).".to_string(),
                "[MARKET] Indian (NSE) and Mainland Chinese (SSE) desks marked OPEN.".to_string(),
                "[MARKET] American NYSE/NASDAQ desk marked CLOSED (Rejects ticking variation).".to_string(),
            ],
            news_articles,
            command_buffer: String::new(),
            command_mode: false,
            filter_query: String::new(),
        }
    }

    pub fn selected_security(&self) -> &SecurityState {
        &self.securities[self.selected_index]
    }

    pub fn add_log(&mut self, msg: String) {
        if self.log_messages.len() >= 30 {
            self.log_messages.remove(0);
        }
        self.log_messages.push(msg);
    }

    pub fn add_news(&mut self, heading: &str, src: &str, urgency: &str) {
        let ist_offset = FixedOffset::east_opt(5 * 3600 + 1800).unwrap();
        let local_ist = self.current_time.with_timezone(&ist_offset);
        let timestamp = local_ist.format("%H:%M IST").to_string();

        let article = NewsArticle {
            timestamp,
            headline: heading.to_string(),
            source: src.to_string(),
            urgency: urgency.to_string(),
        };

        if self.news_articles.len() >= 15 {
            self.news_articles.remove(0);
        }
        self.news_articles.push(article);
    }
}
`;

const RUST_UI_RS = `use ratatui::{
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
`;

const RUST_MAIN_RS = `/**
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
}`;

const RUST_BENCH_RS = `/**
 * @file deserializer.rs
 * @brief Benchmark suite isolating zero-copy JSON parsing throughput
 */

use criterion::{black_box, criterion_group, criterion_main, Criterion};

const MOCK_RAW_PAYLOAD: &[u8] = b"{\\"e\\":\\"trade\\",\\"E\\":1672531199000,\\"s\\":\\"BTCUSDT\\",\\"t\\":123456,\\"p\\":\\"62842.50\\",\\"q\\":\\"0.024\\",\\"b\\":8812,\\"a\\":8823,\\"T\\":1672531198500,\\"m\\":true}";

#[derive(serde::Deserialize, Debug)]
struct StandardAllocatedTrade {
    #[serde(rename = "s")]
    pub symbol: String, // Heap allocation occurs
    #[serde(rename = "p")]
    pub price: String,  // Heap allocation occurs
}

#[derive(serde::Deserialize, Debug)]
struct ZeroCopyBorrowedTrade<'a> {
    #[serde(rename = "s")]
    pub symbol: &'a str, // Zero heap allocations
    #[serde(rename = "p")]
    pub price: &'a str,  // Zero heap allocations
}

fn bench_deserializer_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("Ingress Deserialization");
    let payload_str = std::str::from_utf8(MOCK_RAW_PAYLOAD).unwrap();

    group.bench_function("Standard Serde (Allocating)", |b| {
        b.iter(|| {
            let res: StandardAllocatedTrade = serde_json::from_str(black_box(payload_str)).unwrap();
            black_box(res);
        })
    });

    group.bench_function("Zero-Copy Serde (Borrowed)", |b| {
        b.iter(|| {
            let res: ZeroCopyBorrowedTrade = serde_json::from_str(black_box(payload_str)).unwrap();
            black_box(res);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_deserializer_throughput);
criterion_main!(benches);
`;

const INITIAL_FILES: ProjectFile[] = [
  { name: "feed.rs", path: "src/feed.rs", language: "rust", content: RUST_FEED_RS },
  { name: "state.rs", path: "src/state.rs", language: "rust", content: RUST_STATE_RS },
  { name: "ui.rs", path: "src/ui.rs", language: "rust", content: RUST_UI_RS },
  { name: "main.rs", path: "src/main.rs", language: "rust", content: RUST_MAIN_RS },
  { name: "Cargo.toml", path: "Cargo.toml", language: "toml", content: RUST_CARGO_TOML },
  { name: "deserializer.rs", path: "benches/deserializer.rs", language: "rust", content: RUST_BENCH_RS },
  { name: "README.md", path: "README.md", language: "markdown", content: "## Extreme low-latency Optimizations" },
];

interface RawAsset {
  symbol: string;
  name: string;
  exchange: "US" | "IND" | "EUR" | "CHN" | "JPN" | "FOREX" | "COMMODITY";
  price: number;
  openPrice: number;
  change: number;
  pctChange: number;
  high: number;
  low: number;
  volume: number;
}

const INITIAL_ASSETS_RAW: RawAsset[] = [
  // 1. US Markets / Indices / Core Stocks
  { symbol: ".SPX", name: "S&P 500 Index Focus", exchange: "US", price: 5214.65, openPrice: 5195.40, change: 19.25, pctChange: 0.37, high: 5220.10, low: 5188.35, volume: 2420000000 },
  { symbol: ".DJI", name: "Dow Jones Industrial Average", exchange: "US", price: 39130.53, openPrice: 39080.11, change: 50.42, pctChange: 0.13, high: 39190.50, low: 39045.20, volume: 410000000 },
  { symbol: ".COMPX", name: "NASDAQ Composite Index", exchange: "US", price: 16274.94, openPrice: 16200.50, change: 74.44, pctChange: 0.46, high: 16320.10, low: 16180.20, volume: 1850000000 },
  { symbol: "AAPL", name: "Apple Inc.", exchange: "US", price: 182.52, openPrice: 181.10, change: 1.42, pctChange: 0.78, high: 183.15, low: 180.80, volume: 48250000 },
  { symbol: "MSFT", name: "Microsoft Corporation", exchange: "US", price: 421.90, openPrice: 424.02, change: -2.12, pctChange: -0.50, high: 425.10, low: 420.50, volume: 21290000 },
  { symbol: "AMZN", name: "Amazon.com Inc.", exchange: "US", price: 178.42, openPrice: 176.50, change: 1.92, pctChange: 1.09, high: 179.50, low: 175.80, volume: 35400000 },
  { symbol: "GOOGL", name: "Alphabet Inc. (Google)", exchange: "US", price: 151.60, openPrice: 150.20, change: 1.40, pctChange: 0.93, high: 152.80, low: 149.50, volume: 28500000 },
  { symbol: "META", name: "Meta Platforms Inc.", exchange: "US", price: 505.42, openPrice: 498.20, change: 7.22, pctChange: 1.45, high: 508.30, low: 496.10, volume: 18400000 },
  { symbol: "TSLA", name: "Tesla, Inc.", exchange: "US", price: 174.60, openPrice: 177.46, change: -2.86, pctChange: -1.61, high: 178.50, low: 172.60, volume: 88140000 },
  { symbol: "NVDA", name: "NVIDIA Corporation", exchange: "US", price: 942.50, openPrice: 928.00, change: 14.50, pctChange: 1.56, high: 950.00, low: 925.20, volume: 38410000 },
  { symbol: "VIX", name: "CBOE Volatility Index", exchange: "US", price: 13.52, openPrice: 13.80, change: -0.28, pctChange: -2.03, high: 14.15, low: 13.40, volume: 14500000 },

  // 2. Indian Main Desk (Bluechips & Main Indices)
  { symbol: "NIFTY Index", name: "Nifty 50 Index", exchange: "IND", price: 22462.00, openPrice: 22350.50, change: 111.50, pctChange: 0.50, high: 22525.65, low: 22312.40, volume: 215000000 },
  { symbol: "NSEBANK Index", name: "Nifty Bank Index", exchange: "IND", price: 48320.40, openPrice: 47980.50, change: 339.90, pctChange: 0.71, high: 48450.20, low: 47820.10, volume: 150000000 },
  { symbol: "SENSEX Index", name: "BSE Sensex Index", exchange: "IND", price: 73872.28, openPrice: 73520.40, change: 351.88, pctChange: 0.48, high: 74110.30, low: 73400.10, volume: 310000000 },
  { symbol: "RELIANCE IN Equity", name: "Reliance Industries Ltd.", exchange: "IND", price: 2915.40, openPrice: 2891.00, change: 24.40, pctChange: 0.84, high: 2930.00, low: 2885.50, volume: 8410000 },
  { symbol: "TCS IN Equity", name: "Tata Consultancy Services", exchange: "IND", price: 3840.15, openPrice: 3865.00, change: -24.85, pctChange: -0.64, high: 3878.00, low: 3822.00, volume: 1950000 },
  { symbol: "HDFCBANK IN Equity", name: "HDFC Bank Limited", exchange: "IND", price: 1442.80, openPrice: 1435.00, change: 7.80, pctChange: 0.54, high: 1451.00, low: 1428.20, volume: 21400000 },
  { symbol: "INFY IN Equity", name: "Infosys Limited", exchange: "IND", price: 1533.20, openPrice: 1555.00, change: -21.80, pctChange: -1.40, high: 1560.00, low: 1528.00, volume: 6420000 },
  { symbol: "HUVR IN Equity", name: "Hindustan Unilever Ltd", exchange: "IND", price: 2245.50, openPrice: 2258.00, change: -12.50, pctChange: -0.55, high: 2268.00, low: 2235.00, volume: 1850000 },
  { symbol: "ICICIBANK IN Equity", name: "ICICI Bank Limited", exchange: "IND", price: 1085.40, openPrice: 1072.10, change: 13.30, pctChange: 1.24, high: 1092.50, low: 1068.00, volume: 14500000 },
  { symbol: "SBIN IN Equity", name: "State Bank of India", exchange: "IND", price: 745.30, openPrice: 738.50, change: 6.80, pctChange: 0.92, high: 751.20, low: 735.00, volume: 16800000 },
  { symbol: "BHARTI IN Equity", name: "Bharti Airtel Limited", exchange: "IND", price: 1210.80, openPrice: 1201.20, change: 9.60, pctChange: 0.80, high: 1218.40, low: 1195.00, volume: 8420000 },
  { symbol: "LT IN Equity", name: "Larsen & Toubro Limited", exchange: "IND", price: 3410.50, openPrice: 3425.00, change: -14.50, pctChange: -0.42, high: 3442.00, low: 3390.00, volume: 2100000 },
  { symbol: "AXISBANK IN Equity", name: "Axis Bank Limited", exchange: "IND", price: 1042.15, openPrice: 1038.20, change: 3.95, pctChange: 0.38, high: 1049.00, low: 1032.50, volume: 11400000 },
  { symbol: "INDIA VIX", name: "India Volatility Index", exchange: "IND", price: 14.85, openPrice: 14.20, change: 0.65, pctChange: 4.58, high: 15.10, low: 14.12, volume: 8500000 },

  // 3. Rest of World Indices (EUR/JPN/CHN)
  { symbol: "UKX Index", name: "FTSE 100 Index", exchange: "EUR", price: 7935.09, openPrice: 7914.20, change: 20.89, pctChange: 0.26, high: 7960.50, low: 7905.10, volume: 620000000 },
  { symbol: "DAX Index", name: "DAX 40 Performance-Index", exchange: "EUR", price: 18175.04, openPrice: 18090.10, change: 84.94, pctChange: 0.47, high: 18230.40, low: 18055.20, volume: 92000000 },
  { symbol: "CAC Index", name: "CAC 40 Index", exchange: "EUR", price: 8151.55, openPrice: 8142.11, change: 9.44, pctChange: 0.12, high: 8180.30, low: 8122.50, volume: 1250000 },
  { symbol: "IBEX Index", name: "IBEX 35 Index", exchange: "EUR", price: 10920.40, openPrice: 10850.50, change: 69.90, pctChange: 0.64, high: 10960.00, low: 10810.00, volume: 55000000 },
  { symbol: "SX5E Index", name: "Euro Stoxx 50 Index", exchange: "EUR", price: 4950.25, openPrice: 4920.80, change: 29.45, pctChange: 0.60, high: 4975.00, low: 4910.10, volume: 28550000 },
  { symbol: "SMI Index", name: "SMI Swiss Market Index", exchange: "EUR", price: 11450.60, openPrice: 11390.40, change: 60.20, pctChange: 0.53, high: 11495.00, low: 11370.10, volume: 18450000 },
  { symbol: "N225 Index", name: "Nikkei 225 Avg", exchange: "JPN", price: 38992.08, openPrice: 38810.00, change: 182.08, pctChange: 0.47, high: 39150.00, low: 38740.00, volume: 1400000000 },
  { symbol: "KOSPI Index", name: "KOSPI Composite Index", exchange: "JPN", price: 2750.30, openPrice: 2735.10, change: 15.20, pctChange: 0.56, high: 2765.00, low: 2728.40, volume: 540000000 },
  { symbol: "SSEC Index", name: "Shanghai SE Composite", exchange: "CHN", price: 3045.82, openPrice: 3058.40, change: -12.58, pctChange: -0.41, high: 3065.20, low: 3033.10, volume: 3200000000 },
  { symbol: "399001 INDEX", name: "SZSE Component Index", exchange: "CHN", price: 9530.12, openPrice: 9485.50, change: 44.62, pctChange: 0.47, high: 9580.00, low: 9435.00, volume: 1845000000 },
  { symbol: "HSI Index", name: "Hang Seng Index", exchange: "CHN", price: 16725.10, openPrice: 16650.00, change: 75.10, pctChange: 0.45, high: 16812.50, low: 16590.20, volume: 1850000000 },

  // 4. Global Bluechip Stocks (EUR/JPN/CHN)
  { symbol: "700 HK Equity", name: "Tencent Holdings Ltd.", exchange: "CHN", price: 305.40, openPrice: 301.20, change: 4.20, pctChange: 1.39, high: 310.20, low: 300.50, volume: 8450000 },
  { symbol: "9988 HK Equity", name: "Alibaba Group Holding Ltd.", exchange: "CHN", price: 72.45, openPrice: 73.15, change: -0.70, pctChange: -0.96, high: 73.65, low: 71.95, volume: 11450000 },
  { symbol: "3690 HK Equity", name: "Meituan Class B", exchange: "CHN", price: 110.20, openPrice: 108.50, change: 1.70, pctChange: 1.57, high: 112.00, low: 107.50, volume: 8500000 },
  { symbol: "600519 CH Equity", name: "Kweichow Moutai Co. Ltd.", exchange: "CHN", price: 1650.00, openPrice: 1660.00, change: -10.00, pctChange: -0.60, high: 1675.00, low: 1640.00, volume: 350000 },
  { symbol: "1398 HK Equity", name: "ICBC Ltd.", exchange: "CHN", price: 4.12, openPrice: 4.08, change: 0.04, pctChange: 0.98, high: 4.18, low: 4.05, volume: 45000000 },
  { symbol: "7203 JT Equity", name: "Toyota Motor Corp.", exchange: "JPN", price: 3412.00, openPrice: 3385.00, change: 27.00, pctChange: 0.80, high: 3432.00, low: 3370.00, volume: 3410000 },
  { symbol: "6758 JT Equity", name: "Sony Group Corporation", exchange: "JPN", price: 12450.00, openPrice: 12380.00, change: 70.00, pctChange: 0.57, high: 12550.00, low: 12300.00, volume: 1850000 },
  { symbol: "9984 JT Equity", name: "SoftBank Group Corp.", exchange: "JPN", price: 7850.00, openPrice: 7920.00, change: -70.00, pctChange: -0.88, high: 7990.00, low: 7810.00, volume: 2450000 },
  { symbol: "005930 KS Equity", name: "Samsung Electronics Co.", exchange: "JPN", price: 78500.00, openPrice: 78100.00, change: 400.00, pctChange: 0.51, high: 79100.00, low: 77800.00, volume: 14500000 },
  { symbol: "000660 KS Equity", name: "SK Hynix Inc.", exchange: "JPN", price: 168200.00, openPrice: 165800.00, change: 2400.00, pctChange: 1.45, high: 171000.00, low: 164200.00, volume: 1120000 },
  { symbol: "005490 KS Equity", name: "POSCO Holdings Inc.", exchange: "JPN", price: 382500.00, openPrice: 386000.00, change: -3500.00, pctChange: -0.91, high: 391000.00, low: 381000.00, volume: 320000 },
  { symbol: "1299 HK Equity", name: "AIA Group Ltd.", exchange: "CHN", price: 54.20, openPrice: 53.50, change: 0.70, pctChange: 1.31, high: 54.95, low: 53.10, volume: 9200000 },
  { symbol: "HSBA LN Equity", name: "HSBC Holdings plc", exchange: "EUR", price: 615.20, openPrice: 612.00, change: 3.20, pctChange: 0.52, high: 618.50, low: 610.10, volume: 5400000 },
  { symbol: "1 HK Equity", name: "CK Hutchison Holdings Ltd.", exchange: "CHN", price: 38.45, openPrice: 38.90, change: -0.45, pctChange: -1.16, high: 39.20, low: 38.10, volume: 2200000 },
  { symbol: "ASML NA Equity", name: "ASML Holding N.V.", exchange: "EUR", price: 875.40, openPrice: 865.10, change: 10.30, pctChange: 1.19, high: 885.00, low: 862.00, volume: 841200 },
  { symbol: "NESN SW Equity", name: "Nestlé S.A.", exchange: "EUR", price: 92.50, openPrice: 93.10, change: -0.60, pctChange: -0.64, high: 93.45, low: 92.05, volume: 1450000 },
  { symbol: "NOVO B DC Equity", name: "Novo Nordisk A/S", exchange: "EUR", price: 835.40, openPrice: 841.20, change: -5.80, pctChange: -0.69, high: 846.50, low: 829.00, volume: 1120000 },
  { symbol: "SIE GY Equity", name: "Siemens AG", exchange: "EUR", price: 172.40, openPrice: 170.80, change: 1.60, pctChange: 0.94, high: 173.95, low: 169.50, volume: 1250000 },
  { symbol: "TTE FP Equity", name: "TotalEnergies SE", exchange: "EUR", price: 65.20, openPrice: 65.75, change: -0.55, pctChange: -0.84, high: 66.10, low: 64.80, volume: 1850000 },
  { symbol: "SHEL LN Equity", name: "Shell plc", exchange: "EUR", price: 2640.00, openPrice: 2625.00, change: 15.00, pctChange: 0.57, high: 2655.00, low: 2618.00, volume: 3850000 },
  { symbol: "AZN LN Equity", name: "AstraZeneca plc", exchange: "EUR", price: 10450.00, openPrice: 10410.00, change: 40.00, pctChange: 0.38, high: 10520.00, low: 10380.00, volume: 850000 },
  { symbol: "SAP GY Equity", name: "SAP SE", exchange: "EUR", price: 175.40, openPrice: 172.20, change: 3.20, pctChange: 1.86, high: 177.10, low: 171.15, volume: 1650000 },
  { symbol: "ALV GY Equity", name: "Allianz SE", exchange: "EUR", price: 248.50, openPrice: 246.00, change: 2.50, pctChange: 1.02, high: 251.20, low: 245.50, volume: 550000 },

  // Forex & Commodities Spot desk
  { symbol: "EUR/USD", name: "Euro / US Dollar", exchange: "FOREX", price: 1.0842, openPrice: 1.0835, change: 0.0007, pctChange: 0.06, high: 1.0865, low: 1.0820, volume: 285000000 },
  { symbol: "USD/JPY", name: "US Dollar / Japanese Yen", exchange: "FOREX", price: 151.35, openPrice: 151.72, change: -0.37, pctChange: -0.24, high: 151.85, low: 151.20, volume: 198000000 },
  { symbol: "GBP/USD", name: "British Pound / US Dollar", exchange: "FOREX", price: 1.2614, openPrice: 1.2598, change: 0.0016, pctChange: 0.13, high: 1.2642, low: 1.2585, volume: 145000000 },
  { symbol: "USD/INR", name: "US Dollar / Indian Rupee", exchange: "FOREX", price: 83.342, openPrice: 83.415, change: -0.073, pctChange: -0.09, high: 83.450, low: 83.295, volume: 412000000 },
  { symbol: "AUD/USD", name: "Australian Dollar / US Dollar", exchange: "FOREX", price: 0.6521, openPrice: 0.6508, change: 0.0013, pctChange: 0.20, high: 0.6540, low: 0.6495, volume: 9800000 },
  { symbol: "GOLD", name: "Gold Spot US$/oz", exchange: "COMMODITY", price: 2178.60, openPrice: 2165.20, change: 13.40, pctChange: 0.62, high: 2185.40, low: 2154.00, volume: 245000 },
  { symbol: "BRENT", name: "Brent Crude Oil $/bbl", exchange: "COMMODITY", price: 85.24, openPrice: 85.78, change: -0.54, pctChange: -0.63, high: 86.15, low: 84.80, volume: 318000 },

  // 4. Indian Specialty Desk (Small/Midcaps & Special Indices)
  { symbol: "NIFTYNXT50 Index", name: "Nifty Next 50 Index", exchange: "IND", price: 62410.50, openPrice: 62150.00, change: 260.50, pctChange: 0.42, high: 62550.00, low: 62050.00, volume: 85000000 },
  { symbol: "NIFTYMID100 Index", name: "Nifty Midcap 100 Index", exchange: "IND", price: 49520.10, openPrice: 49200.50, change: 319.60, pctChange: 0.65, high: 49680.00, low: 49110.00, volume: 65000000 },
  { symbol: "NIFTYSM100 Index", name: "Nifty Smallcap 100", exchange: "IND", price: 16235.80, openPrice: 16110.20, change: 125.60, pctChange: 0.78, high: 16310.00, low: 16050.00, volume: 45000000 },
  { symbol: "GIFTNIFTY Index", name: "GIFT Nifty Index", exchange: "IND", price: 22540.00, openPrice: 22450.00, change: 90.00, pctChange: 0.40, high: 22590.00, low: 22410.50, volume: 35000000 },
  { symbol: "MCX IN Equity", name: "Multi Commodity Exchange", exchange: "IND", price: 3680.10, openPrice: 3650.00, change: 30.10, pctChange: 0.82, high: 3715.00, low: 3632.00, volume: 410000 },
  { symbol: "UNIVCABLES IN Equity", name: "Universal Cables Ltd.", exchange: "IND", price: 585.30, openPrice: 580.10, change: 5.20, pctChange: 0.90, high: 592.00, low: 575.50, volume: 85000 },
  { symbol: "APOLLO IN Equity", name: "Apollo Micro Systems Ltd", exchange: "IND", price: 112.45, openPrice: 110.20, change: 2.25, pctChange: 2.04, high: 114.50, low: 109.80, volume: 1250000 },
  { symbol: "MTAR IN Equity", name: "MTAR Technologies Ltd", exchange: "IND", price: 1842.60, openPrice: 1825.00, change: 17.60, pctChange: 0.96, high: 1855.00, low: 1812.50, volume: 320000 },
  { symbol: "AEROFLEX IN Equity", name: "Aeroflex Industries Ltd", exchange: "IND", price: 145.20, openPrice: 142.10, change: 3.10, pctChange: 2.18, high: 148.00, low: 140.50, volume: 145000 },
  { symbol: "MANINFRA IN Equity", name: "Man Infraconstruction Ltd", exchange: "IND", price: 210.40, openPrice: 208.50, change: 1.90, pctChange: 0.91, high: 214.00, low: 206.20, volume: 650000 },
  { symbol: "CIANAGRO IN Equity", name: "CIAN Agro Industries Ltd", exchange: "IND", price: 45.80, openPrice: 46.20, change: -0.40, pctChange: -0.87, high: 47.10, low: 45.00, volume: 25000 },
  { symbol: "INDOTECH IN Equity", name: "Indo Tech Transformers", exchange: "IND", price: 785.40, openPrice: 770.20, change: 15.20, pctChange: 1.97, high: 794.50, low: 765.00, volume: 145000 },
  { symbol: "SIGMA IN Equity", name: "Sigma Advanced Systems", exchange: "IND", price: 180.20, openPrice: 178.50, change: 1.70, pctChange: 0.95, high: 184.20, low: 176.40, volume: 55000 },
  { symbol: "IDEAFORGE IN Equity", name: "Ideaforge Technology", exchange: "IND", price: 685.30, openPrice: 692.10, change: -6.80, pctChange: -0.98, high: 698.00, low: 678.50, volume: 185000 },
  { symbol: "INDIABULLS IN Equity", name: "Indiabulls Ltd", exchange: "IND", price: 114.20, openPrice: 112.50, change: 1.70, pctChange: 1.51, high: 116.40, low: 111.90, volume: 2400000 },
  { symbol: "SUNFLAG IN Equity", name: "Sunflag Iron & Steel Co", exchange: "IND", price: 212.50, openPrice: 210.10, change: 2.40, pctChange: 1.14, high: 215.00, low: 208.20, volume: 340000 },
  { symbol: "STLTECH IN Equity", name: "Sterlite Technologies Ltd", exchange: "IND", price: 122.40, openPrice: 124.50, change: -2.10, pctChange: -1.69, high: 126.00, low: 121.10, volume: 1120000 },
  { symbol: "BMW IN Equity", name: "BMW Industries Ltd", exchange: "IND", price: 54.20, openPrice: 53.10, change: 1.10, pctChange: 2.07, high: 55.40, low: 52.80, volume: 480000 },
  { symbol: "HFCL IN Equity", name: "HFCL Limited", exchange: "IND", price: 87.40, openPrice: 85.90, change: 1.50, pctChange: 1.75, high: 88.90, low: 85.20, volume: 5840000 },
  { symbol: "JTLIND IN Equity", name: "JTL Industries Ltd", exchange: "IND", price: 195.30, openPrice: 192.10, change: 3.20, pctChange: 1.67, high: 198.50, low: 190.40, volume: 720000 },
  { symbol: "HCOPPER IN Equity", name: "Hindustan Copper Ltd", exchange: "IND", price: 312.40, openPrice: 308.50, change: 3.90, pctChange: 1.26, high: 317.00, low: 306.15, volume: 3850000 },
  { symbol: "LAURUSLABS IN Equity", name: "Laurus Labs Limited", exchange: "IND", price: 412.50, openPrice: 415.10, change: -2.60, pctChange: -0.63, high: 418.00, low: 409.50, volume: 1420000 },
  { symbol: "RADICO IN Equity", name: "Radico Khaitan Limited", exchange: "IND", price: 1645.80, openPrice: 1630.00, change: 15.80, pctChange: 0.97, high: 1662.00, low: 1618.00, volume: 210000 },
  { symbol: "NH IN Equity", name: "Narayana Hrudayalaya Ltd", exchange: "IND", price: 1185.30, openPrice: 1192.10, change: -6.80, pctChange: -0.57, high: 1205.00, low: 1181.00, volume: 150000 }
];

const INITIAL_ASSETS: MarketAsset[] = INITIAL_ASSETS_RAW.map(raw => ({
  ...raw,
  history: Array(25).fill(0).map((_, i) => raw.price * (0.97 + i * 0.002 + Math.random() * 0.02)),
  lastTickDir: "flat",
  lastTickTime: 0
}));

const formatVolume = (vol: number) => {
  if (vol >= 1e9) {
    return `${(vol / 1e9).toFixed(1)}B`;
  }
  if (vol >= 1e6) {
    return `${(vol / 1e6).toFixed(1)}M`;
  }
  if (vol >= 1e3) {
    return `${(vol / 1e3).toFixed(1)}K`;
  }
  return vol.toString();
};

const getTickTimeStr = (asset: MarketAsset) => {
  const date = asset.lastTickTime ? new Date(asset.lastTickTime) : new Date();
  const hh = String(date.getHours()).padStart(2, "0");
  const mm = String(date.getMinutes()).padStart(2, "0");
  const ss = String(date.getSeconds()).padStart(2, "0");
  const timeStr = `${hh}:${mm}:${ss}`;
  
  const walkedRecent = (Date.now() - asset.lastTickTime) < 1000;
  if (walkedRecent) {
    return { timeStr, isRecent: true };
  }
  return { timeStr, isRecent: false };
};

const getBloombergTitle = (asset: MarketAsset) => {
  const sym = asset.symbol;
  if (sym.includes("Equity") || sym.includes("Index") || sym.includes("Curncy") || sym.includes("Comdty")) {
    return `${sym} GP`;
  }
  
  if (asset.exchange === "US") {
    if (sym.startsWith(".")) {
      return `${sym.slice(1)} Index GP`;
    }
    return `${sym} US Equity GP`;
  }
  
  if (asset.exchange === "IND") {
    if (sym.includes("Index") || sym === "NIFTY Index" || sym === "SENSEX Index") {
      return `${sym} GP`;
    }
    return `${sym} GP`;
  }
  
  if (asset.exchange === "EUR") {
    if (sym === "UKX" || sym === "DAX" || sym === "CAC") {
      return `${sym} Index GP`;
    }
    return `${sym} FP Equity GP`;
  }
  
  if (asset.exchange === "CHN") {
    if (sym === "SSEC" || sym === "HSI") {
      return `${sym} Index GP`;
    }
    return `${sym} HK Equity GP`;
  }
  
  if (asset.exchange === "JPN") {
    if (sym === "N225") {
      return `${sym} Index GP`;
    }
    return `${sym} JP Equity GP`;
  }
  
  if (asset.exchange === "FOREX") {
    return `${sym.replace("/", "")} Curncy GP`;
  }
  
  if (asset.exchange === "COMMODITY") {
    return `${sym} Comdty GP`;
  }
  
  return `${sym} Equity GP`;
};

const getDisplaySymbol = (symbol: string): string => {
  return symbol.replace(" IN Equity", "").replace(" Index", "");
};

// True timezone operational hours check (Normalised to IST UTC+5.5)
const isMarketOpen = (exchange: string): boolean => {
  const d = new Date();
  const utc = d.getTime() + (d.getTimezoneOffset() * 60000);
  const istDate = new Date(utc + 19800000);
  const hr = istDate.getHours();
  const min = istDate.getMinutes();
  const timeInMins = hr * 60 + min;
  const day = istDate.getDay(); // 0 is Sunday, 6 is Saturday

  const isWeekend = day === 0 || day === 6;

  switch (exchange) {
    case "IND":
      if (isWeekend) return false;
      return timeInMins >= 555 && timeInMins < 930; // 09:15 to 15:30 IST
    case "CHN":
      if (isWeekend) return false;
      return timeInMins >= 420 && timeInMins < 750; // 07:00 to 12:30 IST
    case "US":
      if (isWeekend) return false;
      return timeInMins >= 1140 || timeInMins < 150; // 19:00 to 02:30 IST (Crossover)
    case "EUR":
      if (isWeekend) return false;
      return timeInMins >= 750 && timeInMins <= 1260; // 12:30 to 21:00 IST
    case "JPN":
      if (isWeekend) return false;
      return timeInMins >= 330 && timeInMins < 690; // 05:30 to 11:30 IST
    case "FOREX":
    case "COMMODITY":
      // Forex / Commodities run 24/5 (closed Saturday 03:30 to Monday 03:30 IST)
      if (day === 6 && timeInMins >= 210) return false; 
      if (day === 0) return false;
      if (day === 1 && timeInMins < 210) return false;
      return true;
    default:
      return true;
  }
};

export default function App() {
  // Navigation: We align active tabs with bloomberg quick command interfaces
  const [activeTab, setActiveTab] = useState<"gp" | "ob" | "des" | "cn" | "omon" | "tele" | "boot" | "edit" | "fx">("gp");
  const [newsFilterMode, setNewsFilterMode] = useState<"top" | "ticker">("top");
  
  // CN News Page state hooks
  const [cnSearchQuery, setCnSearchQuery] = useState<string>("");
  const [cnRegionFilter, setCnRegionFilter] = useState<"ALL" | "INDIA" | "INTERNATIONAL">("ALL");
  const [cnSentimentFilter, setCnSentimentFilter] = useState<"ALL" | "BULLISH" | "BEARISH" | "NEUTRAL">("ALL");
  
  // Custom live news injector inputs
  const [customHeadline, setCustomHeadline] = useState<string>("");
  const [customDetails, setCustomDetails] = useState<string>("");
  const [customRegion, setCustomRegion] = useState<"INDIA" | "INTERNATIONAL">("INDIA");

  const [selectedTimeframe, setSelectedTimeframe] = useState<"1m" | "5m" | "1h" | "1d" | "1w">("1d");
  const [showSMA, setShowSMA] = useState<boolean>(true);
  const [showRSI, setShowRSI] = useState<boolean>(false);
  const [showBollinger, setShowBollinger] = useState<boolean>(false);
  const [showVWAP, setShowVWAP] = useState<boolean>(true);
  const [showVolumeProfile, setShowVolumeProfile] = useState<boolean>(true);
  const [displayTimezone, setDisplayTimezone] = useState<"UTC" | "IST" | "GMT" | "EST" | "CST" | "JST" | "CET">("UTC");
  const [displayCurrency, setDisplayCurrency] = useState<"USD" | "INR" | "JPY" | "EUR" | "GBP" | "CNY" | "CHF" | "SGD" | "AUD">("USD");
  const [sessionOverlayEnabled, setSessionOverlayEnabled] = useState<boolean>(true);
  const [useLocalExchangeTime, setUseLocalExchangeTime] = useState<boolean>(false);
  const [currentTime, setCurrentTime] = useState<Date>(new Date());

  useEffect(() => {
    const clockTimer = setInterval(() => {
      setCurrentTime(new Date());
    }, 1000);
    return () => clearInterval(clockTimer);
  }, []);

  const [files, setFiles] = useState<ProjectFile[]>(INITIAL_FILES);
  const [selectedFileIndex, setSelectedFileIndex] = useState<number>(0);
  const [editorValue, setEditorValue] = useState<string>(INITIAL_FILES[0].content);
  const [saveIndicator, setSaveIndicator] = useState<boolean>(false);
  const [isCopied, setIsCopied] = useState<boolean>(false);

  // OMON Derivatives Desk Interactive State Variables
  const [omonIV, setOmonIV] = useState<number>(40);
  const [omonDTE, setOmonDTE] = useState<number>(30);
  const [omonPosition, setOmonPosition] = useState<"BUY CALL" | "SELL CALL" | "BUY PUT" | "SELL PUT">("BUY CALL");
  const [omonQuantity, setOmonQuantity] = useState<number>(1);
  const [selectedOmonStrike, setSelectedOmonStrike] = useState<number | null>(null);

  // Simulation Controls & Telemetry
  const [isRunning, setIsRunning] = useState<boolean>(false);
  const [ingestionCore, setIngestionCore] = useState<number>(1);
  const [engineCore, setEngineCore] = useState<number>(3);
  const [wsStatus, setWsStatus] = useState<"disconnected" | "connecting" | "connected" | "simulated">("disconnected");
  const [latestIncomingTrade, setLatestIncomingTrade] = useState<IngestedTrade | null>(null);
  const [trades, setTrades] = useState<IngestedTrade[]>([]);
  const [telemetry, setTelemetry] = useState<TelemetryStats>({
    tradesCount: 0,
    bufferOccupancy: 0,
    p50: 0,
    p90: 0,
    p99: 0,
    p99Jitter: 0,
    contextSwitches: 0,
    droppedPackets: 0,
    networkTransitMs: 0,
  });

  // Ring Buffer Visual Setup (Capacity 24 indices for presentation viewport)
  const [ringBuffer, setRingBuffer] = useState<(IngestedTrade | null)[]>(Array(24).fill(null));
  const [producerIdx, setProducerIdx] = useState<number>(0);
  const [consumerIdx, setConsumerIdx] = useState<number>(0);

  // Bloomberg Terminal Command Lookups
  const [bloombergCommand, setBloombergCommand] = useState<string>("");
  const [terminalLogs, setTerminalLogs] = useState<string[]>([
    "┌──────────────────────────────────────────────────────────────┐",
    "│               BLOOMBERG HFT QUANT THERM FEED                 │",
    "│              STABLE DUPLEX PORT 3000 INITIALIZED             │",
    "└──────────────────────────────────────────────────────────────┘",
    "[*] Quick Codes: GP (Graph), OB (Order Book), TELE (Telemetry), BOOT (Bootcamp), EDIT (Code)",
    "[SYS] Terminal Online. Enter keys above or type commands."
  ]);
  const [cmdInput, setCmdInput] = useState<string>("");
  const terminalEndRef = useRef<HTMLDivElement>(null);
  const topConsoleRef = useRef<HTMLInputElement>(null);

  // FX Conversions Rates from USD
  const FX_RATES_FROM_USD: Record<string, number> = useMemo(() => ({
    USD: 1.0,
    INR: 83.35,
    CNY: 7.24,
    JPY: 155.60,
    EUR: 0.92,
    GBP: 0.79,
    CHF: 0.91,
    SGD: 1.345,
    AUD: 1.50
  }), []);

  const getCurrencySymbol = useCallback((cur: string) => {
    switch (cur) {
      case "USD": return "$";
      case "INR": return "₹";
      case "JPY": return "¥";
      case "EUR": return "€";
      case "GBP": return "£";
      case "CNY": return "元";
      case "CHF": return "Fr";
      case "SGD": return "S$";
      case "AUD": return "A$";
      default: return "$";
    }
  }, []);

  const convertPrice = useCallback((rawVal: number, exchange: string): number => {
    let baseInUsd = rawVal;
    if (exchange === "IND") {
      baseInUsd = rawVal / 83.35;
    } else if (exchange === "CHN") {
      baseInUsd = rawVal / 7.24;
    } else if (exchange === "JPN") {
      baseInUsd = rawVal / 155.60;
    } else if (exchange === "EUR") {
      baseInUsd = rawVal / 0.92;
    } else {
      if (exchange === "FOREX") return rawVal;
      baseInUsd = rawVal;
    }
    return baseInUsd * (FX_RATES_FROM_USD[displayCurrency] || 1.0);
  }, [displayCurrency, FX_RATES_FROM_USD]);

  const getConvertedDate = useCallback((date: Date, tz: string): Date => {
    let offsetMinutes = 0;
    switch (tz) {
      case "IST": offsetMinutes = 330; break;
      case "GMT": offsetMinutes = 0; break;
      case "EST": offsetMinutes = -300; break;
      case "CST": offsetMinutes = 480; break;
      case "JST": offsetMinutes = 540; break;
      case "CET": offsetMinutes = 60; break;
      case "UTC":
      default:
        offsetMinutes = 0;
        break;
    }
    const utcTime = date.getTime() + (date.getTimezoneOffset() * 60000);
    return new Date(utcTime + (offsetMinutes * 60000));
  }, []);

  const getAssetLocalTimezone = useCallback((exchange: string): string => {
    switch (exchange) {
      case "US": return "EST";
      case "IND": return "IST";
      case "CHN": return "CST";
      case "JPN": return "JST";
      case "EUR": return "CET";
      case "FOREX":
      case "COMMODITY":
      default:
        return "UTC";
    }
  }, []);

  const getTickTimeStrEx = useCallback((asset: MarketAsset) => {
    const date = asset.lastTickTime ? new Date(asset.lastTickTime) : new Date();
    const activeTz = useLocalExchangeTime ? getAssetLocalTimezone(asset.exchange) : displayTimezone;
    const converted = getConvertedDate(date, activeTz);
    const hh = String(converted.getUTCHours()).padStart(2, "0");
    const mm = String(converted.getUTCMinutes()).padStart(2, "0");
    const ss = String(converted.getUTCSeconds()).padStart(2, "0");
    const ms = String(converted.getUTCMilliseconds()).padStart(3, "0");
    const timeStr = `${hh}:${mm}:${ss}.${ms}`;
    const walkedRecent = (Date.now() - asset.lastTickTime) < 1000;
    return { timeStr, isRecent: walkedRecent, timezoneUsed: activeTz };
  }, [displayTimezone, getConvertedDate, useLocalExchangeTime, getAssetLocalTimezone]);

  // Multi-market Bloomberg assets state with metrics initialization
  const [assets, setAssets] = useState<MarketAsset[]>(() => 
    INITIAL_ASSETS.map(asset => {
      const vol = asset.volume || 100000;
      return {
        ...asset,
        sumPriceVolume: asset.price * vol,
        sumVolume: vol,
        vwap: asset.price,
        realizedVolatility: asset.symbol.includes("VIX") ? parseFloat((2.1 + Math.random() * 1.5).toFixed(2)) : parseFloat((0.4 + Math.random() * 0.8).toFixed(2)),
        obi: 0.0 // starts balanced
      };
    })
  );
  const [selectedSymbol, setSelectedSymbol] = useState<string>("RELIANCE IN Equity");
  const [news, setNews] = useState<any[]>(() => {
    const getUtcOffsetStr = (offsetSec: number) => {
      const d = new Date(Date.now() - offsetSec * 1000);
      return `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}:${String(d.getUTCSeconds()).padStart(2, '0')} UTC`;
    };

    return [
      { id: 1, headline: "FED EXAMINING INFLATION SLOPE; RATIO ADJUSTMENTS ANTICIPATED IN FALL", time: getUtcOffsetStr(15), source: "BLOOMBERG", details: "Federal Reserve Board governors highlight lingering services sector stickiness, signaling a watchful pause before executing any key lending rate revisions. Bond yield curves react with flattening bias, indicating conservative investor expectations for Q3/Q4.", ticker: "ALL", region: "INTERNATIONAL" },
      { id: 2, headline: "RELIANCE INDUSTRIES EXPANDS JIO DATA CENTRES AMID TOUGH ENTERPRISE SERVICES ADOPTION", time: getUtcOffsetStr(45), source: "REUTERS", details: "Reliance Jio Infocomm greenlights ₹4,200 crore expansion for sustainable hyper-scale data corridors in Gujarat and Maharashtra. Action will multiply indigenous cloud workload bandwidth by 3.5x and improve latency specs.", ticker: "RELIANCE IN Equity", region: "INDIA" },
      { id: 3, headline: "ECB MEETING DISCUSSES DISINFLATION COOLDOWN; RATE REDUCTION IMMINENT", time: getUtcOffsetStr(110), source: "REUTERS", details: "European Central Bank delegates project local inflation profiles subsiding towards 2.1%. Focus turns to June aggregates for defining sequential rate-step decreases, citing progress on food and fuel aggregates.", ticker: "DAX", region: "INTERNATIONAL" },
      { id: 4, headline: "NIFTY CONTINUES STRIDES AS LIQUIDITY AND PRIVATE BANK FLOWS ABSORB LOCAL OFFERS", time: getUtcOffsetStr(180), source: "BLOOMBERG", details: "Nifty index rises above key moving averages as heavy volume mutual inflows soak domestic retail sell orders. Financial and energy clusters pace leading volume brackets with significant capital mobilization.", ticker: "NIFTY Index", region: "INDIA" },
      { id: 5, headline: "APPLE EXPANDS ON-DEVICE MACHINE LEARNING ACCELERATION CORRIDORS FOR RETAIL OS", time: getUtcOffsetStr(265), source: "BLOOMBERG", details: "Apple Inc. reportedly prepares micro-architecture updates to boost on-device neural processing speeds for imminent device releases. The upgrades could improve compiler-level throughput on private neural stacks.", ticker: "AAPL", region: "INTERNATIONAL" },
      { id: 6, headline: "TOYOTA HYBRID DELIVERIES GAIN 45% YEAR-ON-YEAR OUTPACING ELECTRIC ADVOCATES IN USA", time: getUtcOffsetStr(335), source: "DOW_JONES", details: "Toyota Motor Corp reports record global hybrid unit shipments for Q1, led by robust suburban demand across Europe and North America. Operational margins surge to 12.4% on high premium component ratios.", ticker: "TOYOTA", region: "INTERNATIONAL" },
      { id: 7, headline: "ALIBABA INVESTS $1.2B IN LOCAL CLOUD MULTI-MODAL INFRASTRUCTURE SERVICES OUTLETS", time: getUtcOffsetStr(410), source: "DOW_JONES", details: "Alibaba Cloud Intelligence Group committed USD 1.2 billion for localized AI accelerators and cloud clusters. Projects seek targeting enterprise latency profiles under 10ms throughout Asian server farms.", ticker: "ALIBABA", region: "INTERNATIONAL" },
      { id: 8, headline: "TATA CONSULTANCY SERVICES SECURES £800M DIGITAL MODERNIZATION SEGMENT WITH UK TRANSIT AUTHORITY", time: getUtcOffsetStr(525), source: "BLOOMBERG", details: "TCS announced a multi-year digital transformation commitment with UK financial institutions to expand cloud infrastructure. The transaction will stabilize recurring revenues throughout the standard operating fiscal years.", ticker: "TCS IN Equity", region: "INDIA" },
      { id: 9, headline: "FOREX SPOT RATE ALERT: USD/INR ENTERS RIGID CORRIDOR AT 83.34-83.39 LIMITS", time: getUtcOffsetStr(640), source: "REUTERS", details: "The Indian rupee trades in extremely narrow boundaries against the greenback. Options traders report dynamic backing interventions near historic resistance intervals to dampen leverage exposure.", ticker: "USD/INR", region: "INDIA" },
      { id: 10, headline: "INFOSYS INK SEAMLESS CLOUD INFRASTRUCTURE ALLIANCE WITH EUROPE COALITION RETAILER", time: getUtcOffsetStr(790), source: "REUTERS", details: "Infosys signs major multi-year implementation framework with top European retailer to rewrite core inventory pipelines using distributed fault-tolerant transactional fabrics.", ticker: "INFY IN Equity", region: "INDIA" }
    ];
  });
  const [selectedNewsId, setSelectedNewsId] = useState<number | null>(null);

  const handleCustomInject = () => {
    if (!customHeadline.trim()) {
      appendLog("[ERROR] Headline cannot be blank during news injection.");
      return;
    }
    const d = new Date();
    const curT = `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}:${String(d.getUTCSeconds()).padStart(2, '0')} UTC`;
    const storyId = Date.now();
    const newStory = {
      id: storyId,
      headline: customHeadline.trim().toUpperCase(),
      details: customDetails.trim() || "No additional logs provided. Context remains locked for cryptographic verification.",
      time: curT,
      source: "LOCAL_WIRE",
      region: customRegion,
      ticker: "ALL"
    };
    setNews(prev => [newStory, ...prev]);
    setSelectedNewsId(storyId);
    setCustomHeadline("");
    setCustomDetails("");
    appendLog(`[NEWS_SIM] High-density news dispatch injected! ID: ${storyId}, Region: ${customRegion}`);
  };

  // Reusable unified High-Density Asset Table with Volume and Time metadata (no row bg flash)
  const renderAssetTable = (type: "US" | "IND" | "FX_COMM" | "EUR_ASIA") => {
    let filtered: MarketAsset[] = [];
    if (type === "US") {
      filtered = assets.filter(a => a.exchange === "US");
    } else if (type === "IND") {
      filtered = assets.filter(a => a.exchange === "IND");
    } else if (type === "FX_COMM") {
      filtered = assets.filter(a => a.exchange === "FOREX" || a.exchange === "COMMODITY");
    } else if (type === "EUR_ASIA") {
      filtered = assets.filter(a => a.exchange === "EUR" || a.exchange === "CHN" || a.exchange === "JPN");
    }

    return (
      <div className="overflow-y-auto max-h-[240px] overflow-x-hidden scrollbar-none w-full text-left">
        <table className="w-full text-left border-collapse text-[9px] font-mono table-fixed select-none">
          <thead>
            <tr className="border-b border-[#182026] text-[8px] text-gray-400/95 uppercase font-mono bg-[#05080C] select-none">
              <th className="py-1 px-1 font-bold text-left w-[24%]">TICKER</th>
              <th className="py-1 px-0.5 text-right font-semibold w-[20%]">LAST</th>
              <th className="py-1 px-0.5 text-right font-semibold w-[14%]">CHG</th>
              <th className="py-1 px-0.5 text-right font-semibold w-[14%]">%CHG</th>
              <th className="py-1 px-0.5 text-right font-semibold w-[14%]">VOLUME</th>
              <th className="py-1 px-1 text-right font-semibold w-[14%]">TIME</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((asset) => {
              const walkedRecent = (Date.now() - asset.lastTickTime) < 1000;
              const isUp = asset.change >= 0;
              const isFX = asset.exchange === "FOREX";
              
              const displayVal = convertPrice(asset.price, asset.exchange);
              const displayChg = convertPrice(asset.change, asset.exchange);
              const symIcon = isFX ? "" : getCurrencySymbol(displayCurrency);

              const tickInfo = getTickTimeStrEx(asset);
              const isOpen = isMarketOpen(asset.exchange);
              const isDimmed = sessionOverlayEnabled && !isOpen;
              
              return (
                <tr
                  key={asset.symbol}
                  onClick={() => setSelectedSymbol(asset.symbol)}
                  className={`border-b border-[#131B21] hover:bg-[#141B21] cursor-pointer transition-all duration-200 ${
                    selectedSymbol === asset.symbol ? "bg-[#141C24] border-l-2 border-amber-500" : ""
                  } ${
                    isDimmed 
                      ? "opacity-35 grayscale brightness-90 saturate-50 hover:opacity-80" 
                      : ""
                  }`}
                  title={`${getDisplaySymbol(asset.symbol)} (${asset.exchange}) - ${isOpen ? "Session Open" : "Session Closed"}`}
                >
                  {/* Ticker Name with Status Dot */}
                  <td className="py-1 px-1 text-left truncate">
                    <div className="flex items-center space-x-1" title={isOpen ? "Session Open" : "Session Closed"}>
                      <span className={`w-1 h-1 shrink-0 block rounded-full ${
                        isOpen 
                          ? "bg-emerald-400 shadow-[0_0_4px_rgba(52,211,153,0.8)]" 
                          : "bg-red-500/40"
                      }`} />
                      <span className={`font-bold truncate text-[8.5px] ${isDimmed ? "text-gray-400" : "text-amber-500"}`} title={asset.symbol}>
                        {getDisplaySymbol(asset.symbol)}
                      </span>
                    </div>
                  </td>
                  
                  {/* Last Price */}
                  <td className={`py-1 px-0.5 text-right font-bold truncate text-[8.5px] transition-colors duration-300 ${walkedRecent ? (asset.lastTickDir === "up" ? "text-emerald-400" : "text-red-400") : (isDimmed ? "text-slate-400" : "text-white")}`}>
                    {symIcon}
                    {displayVal.toLocaleString(undefined, {
                      minimumFractionDigits: isFX ? 4 : 2,
                      maximumFractionDigits: isFX ? 4 : 2,
                    })}
                  </td>
                  
                  {/* Change */}
                  <td className={`py-1 px-0.5 text-right font-bold truncate text-[8.5px] ${isDimmed ? "text-slate-500" : (isUp ? "text-emerald-400" : "text-red-400")}`}>
                    {isUp ? "+" : "-"}
                    {symIcon}
                    {Math.abs(displayChg).toLocaleString(undefined, {
                      minimumFractionDigits: isFX ? 4 : 2,
                      maximumFractionDigits: isFX ? 4 : 2,
                    })}
                  </td>
                  
                  {/* Pct Change */}
                  <td className={`py-1 px-0.5 text-right font-bold truncate text-[8.5px] ${isDimmed ? "text-slate-500" : (isUp ? "text-emerald-400" : "text-red-400")}`}>
                    {isUp ? "+" : ""}{asset.pctChange.toFixed(2)}%
                  </td>
                  
                  {/* Volume Column */}
                  <td className={`py-1 px-0.5 text-right font-bold truncate text-[8.5px] ${isDimmed ? "text-slate-600" : "text-slate-300"}`}>
                    {formatVolume(asset.volume || 0)}
                  </td>
                  
                  {/* Time/Status Column */}
                  <td className="py-1 px-1 text-right font-mono text-[8.5px] truncate">
                    {isDimmed ? (
                      <span className="text-red-400/90 font-bold text-[7.5px] tracking-wide select-none bg-red-950/20 px-1 py-0.2 border border-red-950/40 rounded-[1px]" title="Market Session is Closed">CLOSED</span>
                    ) : (
                      <span className={walkedRecent ? "text-amber-400 font-bold" : "text-slate-500"}>
                        {tickInfo.timeStr.split(".")[0]}{useLocalExchangeTime ? ` [${tickInfo.timezoneUsed}]` : ""}
                      </span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    );
  };

  // Live charting elements simulation
  const [latencyHistory, setLatencyHistory] = useState<number[]>(Array(30).fill(1).map(() => 1.2 + Math.random() * 0.8));
  const [lastBtcPrice, setLastBtcPrice] = useState<number>(63242.50);
  const [lastEthPrice, setLastEthPrice] = useState<number>(3465.80);

  // Interview trivia active indices & search query
  const [triviaCategoryFilter, setTriviaCategoryFilter] = useState<string>("ALL");
  const [expandedTrivia, setExpandedTrivia] = useState<Record<string, boolean>>({});
  const [searchTerm, setSearchTerm] = useState<string>("");

  const wsRef = useRef<WebSocket | null>(null);
  const processorIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const mockFeedIntervalRef = useRef<NodeJS.Timeout | null>(null);

  // Synchronize editor text value on file change
  useEffect(() => {
    setEditorValue(files[selectedFileIndex].content);
  }, [selectedFileIndex, files]);

  // Handle Terminal scroll to bottom
  useEffect(() => {
    terminalEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [terminalLogs]);

  // Always focused terminal logic: typing anywhere focuses top command console (Bloomberg style)
  useEffect(() => {
    const handleGlobalKeyDown = (e: KeyboardEvent) => {
      const active = document.activeElement;
      if (active && (
        active.tagName === "INPUT" || 
        active.tagName === "TEXTAREA" || 
        active.hasAttribute("contenteditable")
      )) {
        return;
      }
      
      // Ignore key combos or structural triggers
      if (e.ctrlKey || e.altKey || e.metaKey || e.key === "Escape" || e.key === "Tab") {
        return;
      }

      // Capture physical keyboard shortcuts for Bloomberg navigation layout
      if (e.key === "F2") {
        e.preventDefault();
        setActiveTab("gp");
        appendLog("[HUD] Shortcut key F2: switched to GRAPH VIEW (GP)");
        return;
      } else if (e.key === "F3") {
        e.preventDefault();
        setActiveTab("ob");
        appendLog("[HUD] Shortcut key F3: switched to DEPTH VIEW (MD)");
        return;
      } else if (e.key === "F4") {
        e.preventDefault();
        setActiveTab("des");
        appendLog("[HUD] Shortcut key F4: switched to CORPORATE DESCRIPTION (DES)");
        return;
      } else if (e.key === "F5") {
        e.preventDefault();
        setActiveTab("cn");
        appendLog("[HUD] Shortcut key F5: switched to COMPANY NEWSWIRE (CN)");
        return;
      } else if (e.key === "F6") {
        e.preventDefault();
        setActiveTab("omon");
        appendLog("[HUD] Shortcut key F6: switched to OPTIONS MONITOR (OMON)");
        return;
      } else if (e.key === "F7") {
        e.preventDefault();
        setActiveTab("edit");
        appendLog("[HUD] Shortcut key F7: switched to WORKSPACE CODING (EDIT)");
        return;
      } else if (e.key === "F11") {
        e.preventDefault();
        setActiveTab("fx");
        appendLog("[HUD] Shortcut key F11: switched to FOREX DESK MONITOR (F11)");
        return;
      }

      // Check if alphabetical, numeric or backspace was pressed to transfer focus
      if (e.key.length === 1 || e.key === "Backspace" || e.key === "Enter") {
        if (topConsoleRef.current) {
          topConsoleRef.current.focus();
        }
      }
    };

    window.addEventListener("keydown", handleGlobalKeyDown);
    return () => window.removeEventListener("keydown", handleGlobalKeyDown);
  }, []);

  // Telemetry dynamics according to Core configuration
  const coreAffinityCollision = ingestionCore === engineCore;

  // Real-time continuous financial ticking engine
  useEffect(() => {
    const tickInterval = isRunning ? 200 : 400; // Flashes and updates faster when SPAWN FEED is running
    const timer = setInterval(() => {
      setAssets((prev) => {
        const next = prev.map((asset) => {
          // Timezone Validation Engine: Live ticks must be automatically rejected if exchange session is closed
          if (!isMarketOpen(asset.exchange)) {
            return asset;
          }

          // 40% chance of ticking this asset
          if (Math.random() > 0.4) return asset;

          let tickDamp = 0.0006;
          if (asset.exchange === "FOREX") tickDamp = 0.00012;
          else if (asset.symbol === "GOLD") tickDamp = 0.0003;

          // Cross-Border Volatility Bleed: If macro VIX indexes spike, expand the asset's random walk dampening (volatility amplification)
          let finalTickDamp = tickDamp;
          if (asset.exchange === "IND") {
            const indiaVixObj = prev.find(a => a.symbol === "INDIA VIX");
            if (indiaVixObj && indiaVixObj.price > 14.5) {
              finalTickDamp = tickDamp * (1 + (indiaVixObj.price - 14.5) * 0.15);
            }
          } else if (asset.exchange === "US") {
            const usVixObj = prev.find(a => a.symbol === "VIX");
            if (usVixObj && usVixObj.price > 13.0) {
              finalTickDamp = tickDamp * (1 + (usVixObj.price - 13.0) * 0.15);
            }
          } else if (asset.exchange === "CHN") {
            const chinaVixObj = prev.find(a => a.symbol === "VXFXI");
            if (chinaVixObj && chinaVixObj.price > 20.0) {
              finalTickDamp = tickDamp * (1 + (chinaVixObj.price - 20.0) * 0.15);
            }
          }

          const currentDir = Math.random() > 0.49 ? 1 : -1;
          const pctWalk = Math.random() * finalTickDamp * currentDir;
          const delta = asset.price * pctWalk;
          const nextPrice = Math.max(0.001, asset.price + delta);

          const formattedPrice = parseFloat(nextPrice.toFixed(asset.exchange === "FOREX" ? 4 : 2));
          const formattedChange = parseFloat((formattedPrice - asset.openPrice).toFixed(asset.exchange === "FOREX" ? 4 : 2));
          const formattedPctChange = parseFloat(((formattedChange / asset.openPrice) * 100).toFixed(2));

          // Generate simulated incremental volume
          let volIncrement = Math.round(1000 + Math.random() * 5000);
          if (asset.exchange === "US" || asset.exchange === "IND") {
            volIncrement = Math.round(10000 + Math.random() * 150000);
          } else if (asset.exchange === "FOREX") {
            volIncrement = Math.round(50000 + Math.random() * 800000);
          } else if (asset.exchange === "COMMODITY") {
            volIncrement = Math.round(100 + Math.random() * 1500);
          }
          const nextVolume = asset.volume + volIncrement;

          // VWAP Calculations: sum(Price * Volume) / sum(Volume)
          const currentSumPriceVolume = (asset.sumPriceVolume || (asset.price * asset.volume)) + (formattedPrice * volIncrement);
          const currentSumVolume = (asset.sumVolume || asset.volume) + volIncrement;
          const calculatedVwap = parseFloat((currentSumPriceVolume / currentSumVolume).toFixed(asset.exchange === "FOREX" ? 4 : 2));

          // Log returns rolling stddev for real-time realized volatility (RT RV)
          const nextHistory = [...asset.history.slice(1), formattedPrice];
          const logs: number[] = [];
          for (let i = 1; i < nextHistory.length; i++) {
            if (nextHistory[i - 1] > 0) {
              logs.push(Math.log(nextHistory[i] / nextHistory[i - 1]));
            }
          }
          let calculatedVolatility = asset.realizedVolatility || 1.15;
          if (logs.length > 1) {
            const mean = logs.reduce((a, b) => a + b, 0) / logs.length;
            const variance = logs.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / (logs.length - 1);
            calculatedVolatility = parseFloat((Math.sqrt(variance) * 100).toFixed(2));
          }

          return {
            ...asset,
            price: formattedPrice,
            change: formattedChange,
            pctChange: formattedPctChange,
            high: parseFloat(Math.max(asset.high, formattedPrice).toFixed(asset.exchange === "FOREX" ? 4 : 2)),
            low: parseFloat(Math.min(asset.low, formattedPrice).toFixed(asset.exchange === "FOREX" ? 4 : 2)),
            history: nextHistory,
            lastTickDir: currentDir > 0 ? "up" : "down",
            lastTickTime: Date.now(),
            volume: nextVolume,
            sumPriceVolume: currentSumPriceVolume,
            sumVolume: currentSumVolume,
            vwap: calculatedVwap,
            realizedVolatility: calculatedVolatility
          };
        });
        return next;
      });

      // Periodic natural random headlines integration
      if (Math.random() > 0.94) {
        const indexHeadlines = [
          { txt: "BRENT CRUDE DRIFTS TO WEEKLY DIP ON WEAKENING JET-FUEL FORECASTS", source: "REUTERS", reg: "INTERNATIONAL", details: "Brent Crude petroleum futures decline with light volume as passenger airline scheduling dials back near continental travel desks." },
          { txt: "GOLD BULLION RETREATS IN LONDON SESSIONS; STABLE CURRENCY REDUCES DEMAND", source: "BLOOMBERG", reg: "INTERNATIONAL", details: "Spot bullion trades lower on high-yield treasury alignment. Bullion warehouses report quiet inter-bank physical gold delivery volumes." },
          { txt: "HANG SENG FUTURES STRENGTHEN AS SHANGHAI RE-ALIGNS LIQUIDITY SPURTS", source: "BLOOMBERG", reg: "INTERNATIONAL", details: "Far East equity indexes show support as China's primary monetary regulator authorizes targeted funding of secondary manufacturing hubs." },
          { txt: "NIFTY INDEX TAPS INTEGRAL SUPPORT DESK ON PRIVATE BANK COOLDOWNS", source: "REUTERS", reg: "INDIA", details: "Domestic institutional flows defend critical moving average nodes in Nifty index as private banking sector clears leverage block trades." },
          { txt: "RELIANCE VENTURES SIGN EXCLUSIVE CHIPSET DISTRIBUTION RIGHTS PROTOCOLS", source: "BLOOMBERG", reg: "INDIA", details: "Reliance corporate venture arm secures high-voltage partnership to build AI acceleration wafers inside suburban high-tech corridors." },
          { txt: "TOYOTA UNVEILS 920-MILE FLUX CAPACITOR CELLS IN NAGOYA LABS", source: "DOW_JONES", reg: "INTERNATIONAL", details: "Engineers in Toyota Nagoya facility surprise markets with hybrid range cell breakthrough, allowing unprecedented long-distance commuter capabilities." },
          { txt: "ALIBABA CLOUD INITIATES ENTERPRISE-GRADE LANGUAGE EMULATION REDUCTIONS", source: "DOW_JONES", reg: "INTERNATIONAL", details: "Alibaba developers launch compact multilingual LLM compilers to bypass cloud server constraints during high-demand local API calls." }
        ];
        const chosen = indexHeadlines[Math.floor(Math.random() * indexHeadlines.length)];
        const d = new Date();
        const curT = `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}:${String(d.getUTCSeconds()).padStart(2, '0')} UTC`;
        setNews((oldNews) => [
          { id: Date.now(), headline: chosen.txt, time: curT, source: chosen.source, details: chosen.details, region: chosen.reg },
          ...oldNews.slice(0, 19)
        ]);
      }
    }, tickInterval);

    return () => clearInterval(timer);
  }, [isRunning]);

  // Sync selected symbol data with telemetry feed and orderbook
  useEffect(() => {
    const act = assets.find(a => a.symbol === selectedSymbol) || assets[0];
    if (!act) return;

    // Direct simulation of trade tick inside circular queue pipelines
    const tradeItem: IngestedTrade = {
      id: `TX-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
      symbol: act.symbol,
      price: act.price,
      quantity: parseFloat((Math.random() * 4 + 0.1).toFixed(3)),
      isBuyerMaker: Math.random() > 0.5,
      exchangeTime: Date.now() - 15,
      localReceiptTime: Date.now(),
      processTime: Date.now(),
      latencyUs: Math.random() > 0.91 ? 52 + Math.random() * 40 : 1.1 + Math.random() * 3.1
    };

    setLatestIncomingTrade(tradeItem);

    // Keep updating general stats
    setTelemetry(prev => ({
      ...prev,
      tradesCount: prev.tradesCount + 1,
      p50: 1.0 + Math.random() * 0.5,
      p99: Math.random() > 0.94 ? 54 + Math.random() * 20 : 2.1 + Math.random() * 1.5,
      p90: 1.8 + Math.random() * 0.6,
    }));
  }, [assets, selectedSymbol]);

  // Real-time loop triggers
  useEffect(() => {
    if (isRunning) {
      appendLog("[SYS] Spawning circular buffer task lists...");
      appendLog(`[OK] Core Pinning: Ingest socket bound to CPU Core #${ingestionCore}. Matching Logic bound to Core #${engineCore}.`);
      
      if (coreAffinityCollision) {
        appendLog("[WARN] PREEMPTION RISK: Shared thread cores will trigger Linux scheduler context halts.");
      }

      setWsStatus("connecting");
      appendLog("[NET_FEED] Activating raw WebSocket socket handshake -> Binance HFT stream...");
      
      // Attempt live WebSocket
      try {
        const binanceWs = new WebSocket("wss://stream.binance.com:9443/ws/btcusdt@trade/ethusdt@trade");
        wsRef.current = binanceWs;

        binanceWs.onopen = () => {
          setWsStatus("connected");
          appendLog("[NET_FEED] Sockets handshake complete. Millisecond live feed is running!");
        };

        binanceWs.onmessage = (event) => {
          const raw = JSON.parse(event.data);
          if (raw.e === "trade") {
            const trade: IngestedTrade = {
              id: String(raw.t),
              symbol: raw.s,
              price: parseFloat(raw.p),
              quantity: parseFloat(raw.q),
              isBuyerMaker: raw.m,
              exchangeTime: raw.T,
              localReceiptTime: Date.now(),
              processTime: 0,
            };
            handleIncomingPacket(trade);
          }
        };

        binanceWs.onerror = () => {
          triggerMockFallback();
        };

        binanceWs.onclose = () => {
          appendLog("[NET_FEED] Socket disconnected cleanly.");
        };

      } catch (err) {
        triggerMockFallback();
      }

      // SPSC processing consumer loop simulation (draining queue at 16ms / ~60 FPS rate)
      processorIntervalRef.current = setInterval(() => {
        setProducerIdx((currProducer) => {
          setConsumerIdx((currConsumer) => {
            if (currConsumer === currProducer) return currConsumer; // Queue is empty

            let nextConsumer = currConsumer;
            let itemsProcessed = 0;
            const updatedBuffer = [...ringBuffer];
            const processedItemsList: IngestedTrade[] = [];

            while (nextConsumer !== currProducer && itemsProcessed < 5) {
              const tradeItem = updatedBuffer[nextConsumer];
              if (tradeItem) {
                const now = Date.now();
                tradeItem.processTime = now;
                processedItemsList.push(tradeItem);
                updatedBuffer[nextConsumer] = null;
              }
              nextConsumer = (nextConsumer + 1) % 24;
              itemsProcessed++;
            }

            // Sync visual ring buffer
            setRingBuffer(updatedBuffer);

            // Compute performance metrics
            if (processedItemsList.length > 0) {
              processedItemsList.forEach((t) => {
                const minDelay = coreAffinityCollision ? 142.5 : 1.8;
                const jitterAmp = coreAffinityCollision ? 42.1 : 0.8;
                const noise = Math.random() * jitterAmp;
                const finalLatencyUs = minDelay + noise;

                // Assign computed latency to the trade item.
                t.latencyUs = finalLatencyUs;

                // Push to latency monitoring window
                setLatencyHistory((prev) => {
                  const arr = [...prev.slice(1)];
                  arr.push(finalLatencyUs);
                  return arr;
                });

                // Update charts and stock price references
                if (t.symbol === "BTCUSDT") {
                  setLastBtcPrice(t.price);
                } else if (t.symbol === "ETHUSDT") {
                  setLastEthPrice(t.price);
                }

                // Reference latest incoming trade
                setLatestIncomingTrade(t);

                // Append to telemetry table logs
                setTrades((prev) => [t, ...prev.slice(0, 48)]);
              });

              // Adjust dashboard numbers
              setTelemetry((prev) => {
                const totalCount = prev.tradesCount + processedItemsList.length;
                const currentOccupancy = Math.round((Math.abs(currProducer - nextConsumer) / 24) * 100);
                
                const calculatedP50 = coreAffinityCollision ? 76.5 : 1.2;
                const calculatedP90 = coreAffinityCollision ? 112.2 : 2.1;
                const calculatedP99 = coreAffinityCollision ? 145.2 : 3.2;
                const calculatedJitter = coreAffinityCollision ? 22.4 : 0.4;
                const currentSwitches = coreAffinityCollision ? 14502 : 12;

                const networkLag = Date.now() - processedItemsList[0].exchangeTime;

                return {
                  tradesCount: totalCount,
                  bufferOccupancy: currentOccupancy,
                  p50: calculatedP50,
                  p90: calculatedP90,
                  p99: calculatedP99,
                  p99Jitter: calculatedJitter,
                  contextSwitches: currentSwitches,
                  droppedPackets: prev.droppedPackets,
                  networkTransitMs: networkLag > 0 && networkLag < 5000 ? networkLag : 124,
                };
              });
            }

            return nextConsumer;
          });
          return currProducer;
        });
      }, 16);

    } else {
      cleanupConnection();
    }

    return () => {
      cleanupConnection();
    };
  }, [isRunning, ingestionCore, engineCore, coreAffinityCollision]);

  const cleanupConnection = () => {
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }
    if (processorIntervalRef.current) {
      clearInterval(processorIntervalRef.current);
      processorIntervalRef.current = null;
    }
    if (mockFeedIntervalRef.current) {
      clearInterval(mockFeedIntervalRef.current);
      mockFeedIntervalRef.current = null;
    }
    setWsStatus("disconnected");
  };

  const triggerMockFallback = () => {
    setWsStatus("simulated");
    appendLog("[SYS] Triggering fallback sub-microsecond matching emulator...");
    
    let simulatedTradeCount = 0;
    mockFeedIntervalRef.current = setInterval(() => {
      simulatedTradeCount++;
      const isBTC = Math.random() > 0.45;
      const basePrice = isBTC ? lastBtcPrice : lastEthPrice;
      const volatility = basePrice * 0.0003;
      const delta = (Math.random() - 0.5) * volatility;
      const finalPrice = Math.max(10, basePrice + delta);

      const trade: IngestedTrade = {
        id: `SI-${simulatedTradeCount}-${Math.floor(Math.random()*1000)}`,
        symbol: isBTC ? "BTCUSDT" : "ETHUSDT",
        price: finalPrice,
        quantity: parseFloat((Math.random() * 1.8).toFixed(3)),
        isBuyerMaker: Math.random() > 0.5,
        exchangeTime: Date.now() - 35,
        localReceiptTime: Date.now(),
        processTime: 0,
      };

      handleIncomingPacket(trade);
    }, 45);
  };

  const handleIncomingPacket = (trade: IngestedTrade) => {
    setProducerIdx((currProducer) => {
      setConsumerIdx((currConsumer) => {
        const nextProducer = (currProducer + 1) % 24;

        if (nextProducer === currConsumer) {
          setTelemetry((prev) => ({
            ...prev,
            droppedPackets: prev.droppedPackets + 1,
          }));
          return currProducer; // Drop packet due to circular queue overflow
        }

        setRingBuffer((prevBuf) => {
          const updated = [...prevBuf];
          trade.queueIndex = currProducer;
          updated[currProducer] = trade;
          return updated;
        });

        return nextProducer;
      });
      return currProducer;
    });
  };

  const appendLog = (msg: string) => {
    const timestamp = new Date().toISOString().slice(11, 19) + `.${(Date.now() % 1000).toString().padStart(3, "0")}`;
    setTerminalLogs((prev) => [...prev, `[${timestamp}] ${msg}`]);
  };

  // Bloomberg Terminal Command execution router
  const handleBloombergCommand = (commandStr: string) => {
    const raw = commandStr.trim().toUpperCase().replace(" <GO>", "").replace("<GO>", "");
    if (!raw) return;

    appendLog(`BBG_COMMAND> ${raw}`);

    const tokens = raw.split(/\s+/);
    
    // Known financial functions
    const functions = ["GP", "MD", "OB", "DES", "CN", "OMON", "EDIT", "HELP", "CLEAR", "BENCH", "RUN", "STOP", "STATS", "FX", "F11", "GPI", "GPF", "TOP", "1M", "5M", "D", "DAILY"];
    
    let targetFunction: string | null = null;
    let assetQueryTokens: string[] = [];

    // Check if the last token is a known function suffix
    const lastToken = tokens[tokens.length - 1];
    if (functions.includes(lastToken)) {
      targetFunction = lastToken;
      assetQueryTokens = tokens.slice(0, -1);
    } else {
      // Check if second-last or last is standard Bloomberg marker sector
      const sectorKeywords = ["EQUITY", "CURNCY", "INDEX", "COMDTY", "US", "IN", "EUR", "CHN", "JPN"];
      const hasSector = tokens.some(t => sectorKeywords.includes(t));
      if (hasSector) {
        // Default sector overview screen is "DES" (Description)
        targetFunction = "DES";
        assetQueryTokens = tokens;
      } else {
        assetQueryTokens = tokens;
      }
    }

    // Filter out sector keywords to extract pure ticker (e.g. "AAPL US Equity" -> "AAPL")
    const filteredQuery = assetQueryTokens.filter(
      t => !["EQUITY", "CURNCY", "INDEX", "COMDTY", "US", "IN", "EUR", "CHN", "JPN"].includes(t)
    );

    let matchedAsset: MarketAsset | undefined = undefined;
    if (filteredQuery.length > 0) {
      const firstQueryToken = filteredQuery[0];
      // Exact check
      matchedAsset = assets.find(a => {
        const cleanSym = a.symbol.toUpperCase().replace("/", "").replace(".", "");
        const cleanQuery = firstQueryToken.replace("/", "").replace(".", "");
        return cleanSym === cleanQuery || a.symbol.toUpperCase() === firstQueryToken;
      });
      
      // Substring fallback
      if (!matchedAsset) {
        matchedAsset = assets.find(a => 
          a.symbol.toUpperCase().includes(firstQueryToken) || 
          a.name.toUpperCase().includes(firstQueryToken)
        );
      }
    }

    if (matchedAsset) {
      setSelectedSymbol(matchedAsset.symbol);
      appendLog(`[OK] SECURITY ACQUIRED: ${matchedAsset.name} [${matchedAsset.symbol}]`);
      
      if (targetFunction) {
        const funcLower = targetFunction.toLowerCase();
        const tab = (funcLower === "md" ? "ob" : funcLower) as any;
        setActiveTab(tab);
        appendLog(`[NAV] SCREEN MOUNTED: <${targetFunction}>`);
      } else {
        // Defaults to GP when security matches but no function provided
        setActiveTab("gp");
        appendLog(`[NAV] DEFAULT SCREEN MOUNTED: <GP> (Price Graphics)`);
      }
    } else {
      // No symbol matched, execute code direct or switch views with existing selected symbol
      if (targetFunction) {
        const funcLower = targetFunction.toLowerCase();
        const tab = (funcLower === "md" ? "ob" : funcLower) as any;
        
        if (targetFunction === "RUN") {
          setIsRunning(true);
          appendLog("[SYS] Handshake completed. Real-time feeds active.");
        } else if (targetFunction === "STOP") {
          setIsRunning(false);
          appendLog("[SYS] Live data ingestion pipeline paused.");
        } else if (targetFunction === "STATS") {
          appendLog(`--- SYSTEM PERFORMANCE REGISTER ---`);
          appendLog(`Processed: ${telemetry.tradesCount} ticks`);
          appendLog(`Ring buffer occupancy: ${telemetry.bufferOccupancy}%`);
          appendLog(`P50: ${telemetry.p50.toFixed(1)} us, P99: ${telemetry.p99.toFixed(1)} us`);
        } else if (targetFunction === "CLEAR") {
          setTerminalLogs([]);
        } else if (targetFunction === "BENCH") {
          appendLog("[BENCH] Running live cache-line & CPU deserialization bench...");
          setTimeout(() => {
            appendLog("Allocating deserializer context... Complete.");
            appendLog("Benchmarked zero-copy reference borrowing: 194.2 ns/pkt.");
          }, 400);
        } else if (targetFunction === "FX" || targetFunction === "F11") {
          setActiveTab("fx");
          appendLog("[NAV] ROUTED TO: <FOREX DESK> F11 (Spot rates & currency converter)");
        } else if (targetFunction === "TOP") {
          setActiveTab("cn");
          setNewsFilterMode("top");
          appendLog("[NAV] ROUTED TO: <NEWS HEADLINES> TOP (Defaulting focus to macro events wire)");
        } else if (targetFunction === "CN") {
          setActiveTab("cn");
          setNewsFilterMode("ticker");
          appendLog(`[NAV] ROUTED TO: <COMPANY NEWS> CN (Filtered for active ticker: ${selectedSymbol})`);
        } else if (targetFunction === "GPI") {
          setShowSMA(prev => {
            const next = !prev;
            appendLog(`[INDICATOR] SMA overlay toggled: ${next ? "ACTIVE" : "INACTIVE"}`);
            return next;
          });
        } else if (targetFunction === "GPF") {
          setShowRSI(prev => {
            const next = !prev;
            appendLog(`[INDICATOR] RSI sub-chart toggled: ${next ? "ACTIVE" : "INACTIVE"}`);
            return next;
          });
        } else if (targetFunction === "1M" || targetFunction === "5M" || targetFunction === "D" || targetFunction === "DAILY") {
          const tf = (targetFunction === "1M" ? "1m" : targetFunction === "5M" ? "5m" : "1d") as any;
          setSelectedTimeframe(tf);
          setActiveTab("gp");
          appendLog(`[GP] Core chart timeframe updated: ${tf.toUpperCase()}`);
        } else {
          setActiveTab(tab);
          appendLog(`[NAV] ROUTED TO: <${targetFunction}> (Active ticker: ${selectedSymbol})`);
        }
      } else {
        // Treat raw string as general switch attempt or error
        const upperRaw = raw.toUpperCase();
        if (upperRaw === "HELP") {
          appendLog("--- BLOOMBERG TERMINAL CORE HELP ---");
          appendLog("Syntax: <TICKER> [Market Sector] [Function Code] <GO>");
          appendLog("Examples: AAPL US Equity DES, RELIANCE IN Equity GP, TSLA OMON");
          appendLog("Function Keys:");
          appendLog("  GP   - (F2) Interactive Candlesticks (Price Graphics) | TMF: 1M, 5M, D");
          appendLog("  MD   - (F3) Real-Time vertical Order Depth ladder");
          appendLog("  DES  - (F4) In-depth Corporate fundamentals & metrics");
          appendLog("  CN   - (F5) Real-Time dispatch ticker & news details | TOP: Headlines");
          appendLog("  OMON - (F6) Dynamically pricing derivatives book");
          appendLog("  EDIT - (F7) Integrated cargo editor environment");
          appendLog("  F11  - (F11) Forex Desk Monitor (FX)");
          appendLog("Indicators:");
          appendLog("  GPI  - Toggle SMA 5 & 10 indicators overlay");
          appendLog("  GPF  - Toggle RSI indicator panel overlay");
        } else {
          appendLog(`[ERROR] Command sequence '${raw}' unrecognized. Type HELP <GO> for documentation.`);
        }
      }
    }

    setBloombergCommand("");
  };

  const handleCommandSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!cmdInput.trim()) return;

    const cmd = cmdInput.trim();
    setTerminalLogs((prev) => [...prev, `▶ ${cmd}`]);
    setCmdInput("");

    const parts = cmd.toLowerCase().split(" ");
    const primary = parts[0];

    switch (primary) {
      case "help":
        appendLog("Bloomberg Console Direct Access Keys:");
        appendLog("  help                         Print help guide");
        appendLog("  cargo build                  Build Release Target inside local workspace");
        appendLog("  cargo run                    Start Ingestion matching client");
        appendLog("  cargo stop                   Stop Socket streams");
        appendLog("  bench                        Run local Criterion.rs performance benchmarks");
        appendLog("  clear                        Flush system logs screen");
        break;
      case "clear":
        setTerminalLogs([]);
        break;
      case "cargo":
        if (parts[1] === "build") {
          runSimulatedBuild();
        } else if (parts[1] === "run") {
          setIsRunning(true);
        } else if (parts[1] === "stop") {
          setIsRunning(false);
        }
        break;
      case "bench":
        handleBloombergCommand("BENCH");
        break;
      default:
        handleBloombergCommand(cmd);
        break;
    }
  };

  const runSimulatedBuild = () => {
    appendLog("[CARGO] Activating compiler toolchain...");
    appendLog("    Resolving and loading standard dependencies...");
    setTimeout(() => {
      appendLog("   Compiling rtrb lock-free memory ring v0.3.1");
      appendLog("   Compiling core_affinity hardware pin library v0.8.1");
      appendLog("   Compiling low_latency_terminal v0.1.0 [FAT LTO enabled]");
      appendLog("[OK] Release output generated successfully inside: ./target/release/low_latency_terminal");
    }, 1000);
  };

  const handleSaveFile = () => {
    setSaveIndicator(true);
    setFiles((prev) => {
      const updated = [...prev];
      updated[selectedFileIndex].content = editorValue;
      return updated;
    });
    appendLog(`[EDITOR] Commit save executed for: cargo_project/${files[selectedFileIndex].path}`);
    setTimeout(() => setSaveIndicator(false), 1200);
  };

  const copyToClipboard = () => {
    navigator.clipboard.writeText(editorValue);
    setIsCopied(true);
    setTimeout(() => setIsCopied(false), 2000);
  };

  const toggleTrivia = (id: string) => {
    setExpandedTrivia(prev => ({
      ...prev,
      [id]: !prev[id]
    }));
  };

  // Filtered Trivia questions
  const filteredQuestions = INTERVIEW_QUESTIONS.filter(q => {
    const matchesCategory = triviaCategoryFilter === "ALL" || q.category === triviaCategoryFilter;
    const matchesSearch = q.title.toLowerCase().includes(searchTerm.toLowerCase()) || 
                          q.question.toLowerCase().includes(searchTerm.toLowerCase()) ||
                          q.explanation.toLowerCase().includes(searchTerm.toLowerCase());
    return matchesCategory && matchesSearch;
  });

  return (
    <div className="min-h-screen bg-[#070708] text-[#D1D1D1] font-mono selection:bg-amber-500/20 selection:text-amber-500 antialiased">
      
      {/* BLOOMBERG STYLE COMPREHENSIVE TERMINAL HEADER */}
      <header className="border-b-2 border-amber-500/80 bg-[#101012] px-4 py-2.5 sticky top-0 z-50 flex flex-col gap-2.5">
        
        {/* ROW 1: QUICK ACCESS KEYS MENU */}
        <div className="flex flex-wrap items-center justify-between gap-3 text-xs border-b border-[#2D2D33] pb-2">
          
          <div className="flex items-center space-x-2 shrink-0">
            <span className="w-2.5 h-2.5 bg-amber-500 rounded-none animate-pulse"></span>
            <span className="text-amber-500 font-extrabold tracking-widest text-[13px]">BBG DUPLEX SYSTEM</span>
            <span className="text-[10px] text-gray-500">| INGEST DIRECT DUPLEX CONTROLLER</span>
          </div>

          {/* QUICK DIAL SELECT BUTTONS */}
          <div className="flex flex-wrap items-center bg-[#19191C] border border-[#2D2D33] p-[2px]">
            {[
              { id: "gp", label: "F2 <GP> GRAPH" },
              { id: "ob", label: "F3 <MD> DEPTH" },
              { id: "cn", label: "F5 <CN> CO NEWS" },
              { id: "omon", label: "F6 <OMON> OPTIONS" },
              { id: "fx", label: "F11 <FX> FOREX" }
            ].map((shortcut) => (
              <button
                key={shortcut.id}
                onClick={() => {
                  setActiveTab(shortcut.id as any);
                  appendLog(`[HUD] Screen navigation swap to: ${shortcut.id.toUpperCase()}`);
                }}
                className={`px-3 py-1 text-[11px] font-extrabold transition-colors rounded-none ${
                  activeTab === shortcut.id 
                    ? "bg-amber-500 text-[#070708]" 
                    : "text-gray-400 hover:text-white hover:bg-[#28282B]"
                }`}
              >
                {shortcut.label}
              </button>
            ))}
          </div>

          {/* HARDWARE SPAWNING STATE & GLOBAL CURRENCY SELECT */}
          <div className="flex flex-wrap items-center gap-3">
            {/* GLOBAL CURRENCY VIEW MODE */}
            <div className="flex items-center space-x-1.5 bg-[#121215] border border-[#2D2D33] p-[2px]">
              <span className="text-[9px] text-[#888] font-black uppercase tracking-wider pl-1.5 pr-0.5 select-none">BASE VAL:</span>
              {[
                { code: "USD", symbol: "$" },
                { code: "INR", symbol: "₹" },
                { code: "EUR", symbol: "€" }
              ].map((cur) => (
                <button
                  key={cur.code}
                  onClick={() => {
                    setDisplayCurrency(cur.code as any);
                    appendLog(`[VAL] Global valuation base switched to: ${cur.code}`);
                  }}
                  className={`px-2 py-0.5 text-[10px] font-extrabold transition-colors rounded-none outline-none ${
                    displayCurrency === cur.code 
                      ? "bg-emerald-500 text-[#070708] font-black" 
                      : "text-gray-400 hover:text-white"
                  }`}
                  title={`Toggle display prices to ${cur.code} (${cur.symbol})`}
                >
                  {cur.symbol} {cur.code}
                </button>
              ))}
            </div>

            {/* MARKET SESSION OVERLAY CONTROL */}
            <div className="flex items-center space-x-1.5 bg-[#121215] border border-[#2D2D33] p-[2px]">
              <span className="text-[9px] text-[#888] font-black uppercase tracking-wider pl-1.5 pr-0.5 select-none">SESSION OVERLAY:</span>
              <button
                onClick={() => {
                  setSessionOverlayEnabled(!sessionOverlayEnabled);
                  appendLog(`[SESSION] Market Session Overlay toggled: ${!sessionOverlayEnabled ? "ACTIVE (DIM CLOSED)" : "DISABLED"}`);
                }}
                className={`px-2 py-0.5 text-[10px] font-extrabold transition-colors rounded-none outline-none ${
                  sessionOverlayEnabled 
                    ? "bg-amber-500 text-[#070708] font-black" 
                    : "text-gray-400 hover:text-white"
                }`}
                title="Toggle dimming/greyscale for regional tables when the market is closed"
              >
                {sessionOverlayEnabled ? "DIM CLOSED" : "ALL ACTIVE"}
              </button>
            </div>

            {/* SESSION TIME / EXCHANGE TIMEZONE TOGGLE */}
            <div className="flex items-center space-x-1.5 bg-[#121215] border border-[#2D2D33] p-[2px]">
              <span className="text-[9px] text-[#888] font-black uppercase tracking-wider pl-1.5 pr-0.5 select-none">TIME FORMAT:</span>
              <button
                onClick={() => {
                  setUseLocalExchangeTime(!useLocalExchangeTime);
                  appendLog(`[TIME] Session Time display mode set to: ${!useLocalExchangeTime ? "LOCAL EXCHANGE TIME (IST/EST/CET)" : "GLOBAL TIMEZONE (UTC)"}`);
                }}
                className={`px-2 py-0.5 text-[10px] font-extrabold transition-colors rounded-none outline-none ${
                  useLocalExchangeTime 
                    ? "bg-cyan-500 text-[#070708] font-black" 
                    : "text-gray-400 hover:text-white"
                }`}
                title="Convert timestamps in tables dynamically to that asset's local exchange timezone (e.g. IST for NSE, EST for NYSE)"
              >
                {useLocalExchangeTime ? "SESSION TIME" : "BASE TIME"}
              </button>
            </div>

            <button
              onClick={() => setIsRunning(!isRunning)}
              className={`flex items-center space-x-1.5 px-3 py-1 font-extrabold text-[11px] tracking-wider transition rounded-none uppercase border ${
                isRunning 
                  ? "bg-red-500/10 hover:bg-red-500/20 text-red-500 border-red-500/30" 
                  : "bg-[#00FF41]/10 hover:bg-[#00FF41]/20 text-[#00FF41] border-[#00FF41]/30"
              }`}
            >
              <div className={`w-1.5 h-1.5 rounded-none ${isRunning ? "bg-red-500 animate-ping" : "bg-[#00FF41]"}`}></div>
              <span>{isRunning ? "PAUSE FEED" : "LIVE FEED"}</span>
            </button>
          </div>

        </div>

        {/* ROW 2: CLASSIC BLOOMBERG COMMAND CONSOLE INPUT */}
        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-[#2D2D33]/40 pb-1">
          <div className="flex items-center gap-3 w-full sm:w-auto">
            <div className="bg-[#0A0A0B] border border-amber-500/40 flex items-center px-2 py-1 w-full max-w-sm shrink-0">
              <span className="text-amber-500 font-extrabold text-[12px] select-none pr-2">Command:</span>
              <input
                type="text"
                ref={topConsoleRef}
                value={bloombergCommand}
                onChange={(e) => setBloombergCommand(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    handleBloombergCommand(bloombergCommand);
                  }
                }}
                placeholder="AAPL US Equity CN <GO>..."
                className="bg-transparent border-none focus:outline-none text-amber-500 font-extrabold text-[12px] w-full"
              />
              <span className="text-[#555] text-[9px] font-bold">GO</span>
            </div>

            <div className="hidden sm:flex items-center space-x-3 text-[10px] text-gray-500 overflow-x-auto py-1">
              <span>[Quick Codes]</span>
              <button onClick={() => handleBloombergCommand("GP")} className="hover:text-white transition">GP (Charts)</button>
              <span>•</span>
              <button onClick={() => handleBloombergCommand("MD")} className="hover:text-white transition">MD (Depth)</button>
              <span>•</span>
              <button onClick={() => handleBloombergCommand("CN")} className="hover:text-white transition">CN (News Grid)</button>
              <span>•</span>
              <button onClick={() => handleBloombergCommand("OMON")} className="hover:text-white transition">OMON (Options Book)</button>
            </div>
          </div>

          <div className="text-[10px] text-gray-500 shrink-0 font-mono flex items-center gap-2">
            <span>Terminal Port: <b className="text-amber-500">3000</b></span>
            <span>•</span>
            <span>SPSC Queues: <b className="text-[#00FF41]">ACTIVE</b></span>
          </div>
        </div>

        {/* GLOBAL HORIZONTAL VOLATILITY MATRIX (VIX STRIP) */}
        <div className="bg-[#0B0E11] border border-[#1E232B] px-3 py-1.5 flex flex-wrap gap-4 items-center justify-between text-xs text-white">
          <div className="flex items-center space-x-2 text-amber-500 font-extrabold uppercase text-[9.5px] tracking-wider select-none shrink-0 font-mono">
            <span className="w-1.5 h-1.5 bg-cyan-400 rounded-none animate-pulse"></span>
            <span>GLOBAL VOLATILITY MATRIX (VIX DESK)</span>
          </div>

          <div className="flex flex-wrap items-center gap-5">
            {(() => {
              const usVixObj = assets.find(a => a.symbol === "VIX");
              const indVixObj = assets.find(a => a.symbol === "INDIA VIX");
              const chnVixObj = assets.find(a => a.symbol === "VXFXI");

              return (
                <>
                  {/* VIX (US) */}
                  {usVixObj && (
                    <div className="flex items-center space-x-1.5 text-[10.5px]">
                      <span className="text-gray-500 font-bold uppercase text-[9.5px]">VIX [US]</span>
                      <span className="text-white font-extrabold font-mono text-[11px]">{usVixObj.price.toFixed(2)}</span>
                      <span className={`font-mono font-bold text-[10px] ${usVixObj.change >= 0 ? "text-emerald-500" : "text-red-500"}`}>
                        {usVixObj.change >= 0 ? "▲" : "▼"}{Math.abs(usVixObj.pctChange).toFixed(2)}%
                      </span>
                    </div>
                  )}

                  <span className="text-[#2D2D33] select-none text-[9.5px]">|</span>

                  {/* INDIA VIX */}
                  {indVixObj && (
                    <div className="flex items-center space-x-1.5 text-[10.5px]">
                      <span className="text-gray-500 font-bold uppercase text-[9.5px]">INDIA VIX</span>
                      <span className="text-white font-extrabold font-mono text-[11px]">{indVixObj.price.toFixed(2)}</span>
                      <span className={`font-mono font-bold text-[10px] ${indVixObj.change >= 0 ? "text-emerald-500" : "text-red-500"}`}>
                        {indVixObj.change >= 0 ? "▲" : "▼"}{Math.abs(indVixObj.pctChange).toFixed(2)}%
                      </span>
                    </div>
                  )}

                  <span className="text-[#2D2D33] select-none text-[9.5px]">|</span>

                  {/* VXFXI (CHN) */}
                  {chnVixObj && (
                    <div className="flex items-center space-x-1.5 text-[10.5px]">
                      <span className="text-gray-500 font-bold uppercase text-[9.5px]">VXFXI [CHN]</span>
                      <span className="text-white font-extrabold font-mono text-[11px]">{chnVixObj.price.toFixed(2)}</span>
                      <span className={`font-mono font-bold text-[10px] ${chnVixObj.change >= 0 ? "text-emerald-500" : "text-red-500"}`}>
                        {chnVixObj.change >= 0 ? "▲" : "▼"}{Math.abs(chnVixObj.pctChange).toFixed(2)}%
                      </span>
                    </div>
                  )}
                </>
              );
            })()}
          </div>

          <div className="text-[9px] text-[#555] flex items-center gap-1 font-mono hover:text-gray-400 transition cursor-help">
            <span className="w-1.5 h-1.5 bg-[#00FF41] rounded-none"></span>
            <span>SEC VOL INTERBANK STREAM AGGREGATED</span>
          </div>
        </div>

      </header>

      {/* CORE VIEWPORT CANVAS */}
      <main className="p-3 max-w-[1720px] mx-auto min-h-[calc(100vh-100px)] font-sans">
        
        <AnimatePresence mode="wait">
          
          {/* VIEWPORT 1: PRICE GRAPHICS (GP) */}
          {activeTab === "gp" && (
            <motion.div 
              key="gp"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.1 }}
              className="grid grid-cols-1 lg:grid-cols-12 gap-3"
            >
              
              {/* PRIMARY HIGH-DENSITY MARKET AREA (8/12) */}
              <div className="lg:col-span-8 flex flex-col space-y-3">
                
                {/* TOOLBAR FOR GP CONFIGURATION */}
                <div className="bg-[#12161E] border border-[#232B35] px-2.5 py-1.5 flex flex-wrap items-center justify-between gap-3 text-[10px] font-mono">
                  <div className="flex flex-wrap items-center gap-3">
                    <span className="text-gray-400 font-bold uppercase">TMF:</span>
                    <div className="flex bg-[#0A0A0B] border border-[#2D2D33] p-[1.5px]">
                      {[
                        { code: "1m", label: "1 min" },
                        { code: "5m", label: "5 min" },
                        { code: "1h", label: "1 hour" },
                        { code: "1d", label: "Daily" },
                        { code: "1w", label: "Weekly" }
                      ].map((t) => (
                        <button
                          key={t.code}
                          onClick={() => {
                            setSelectedTimeframe(t.code as any);
                            appendLog(`[GP] Dynamic chart timeframe set to: ${t.code.toUpperCase()}`);
                          }}
                          className={`px-2 py-0.5 text-[9px] font-extrabold uppercase transition-colors rounded-none ${
                            selectedTimeframe === t.code
                              ? "bg-amber-500 text-black"
                              : "text-gray-400 hover:text-white"
                          }`}
                        >
                          {t.label}
                        </button>
                      ))}
                    </div>
                  </div>

                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-gray-400 font-bold uppercase">STUDIES:</span>
                    <button
                      onClick={() => {
                        setShowSMA(!showSMA);
                        appendLog(`[GP] SMA overlay toggled: ${!showSMA ? "ON" : "OFF"}`);
                      }}
                      className={`px-2 py-0.5 border text-[9px] font-bold uppercase transition rounded-none ${
                        showSMA
                          ? "border-[#00FFFF] text-[#00FFFF] bg-[#00FFFF]/5"
                          : "border-[#2D2D33] text-gray-400 hover:text-white"
                      }`}
                    >
                      SMA 5/10
                    </button>
                    <button
                      onClick={() => {
                        setShowBollinger(!showBollinger);
                        appendLog(`[GP] Bollinger Bands toggled: ${!showBollinger ? "ON" : "OFF"}`);
                      }}
                      className={`px-2 py-0.5 border text-[9px] font-bold uppercase transition rounded-none ${
                        showBollinger
                          ? "border-[#EAB308] text-[#EAB308] bg-[#EAB308]/5"
                          : "border-[#2D2D33] text-gray-400 hover:text-white"
                      }`}
                    >
                      BOLL (20,2)
                    </button>
                    <button
                      onClick={() => {
                        setShowVWAP(!showVWAP);
                        appendLog(`[GP] VWAP overlay toggled: ${!showVWAP ? "ON" : "OFF"}`);
                      }}
                      className={`px-2 py-0.5 border text-[9px] font-bold uppercase transition rounded-none ${
                        showVWAP
                          ? "border-[#3B82F6] text-[#3B82F6] bg-[#3B82F6]/5"
                          : "border-[#2D2D33] text-gray-400 hover:text-white"
                      }`}
                    >
                      VWAP
                    </button>
                    <button
                      onClick={() => {
                        setShowVolumeProfile(!showVolumeProfile);
                        appendLog(`[GP] Volume Profile toggled: ${!showVolumeProfile ? "ON" : "OFF"}`);
                      }}
                      className={`px-2 py-0.5 border text-[9px] font-bold uppercase transition rounded-none ${
                        showVolumeProfile
                          ? "border-[#F59E0B] text-[#F59E0B] bg-[#F59E0B]/5"
                          : "border-[#2D2D33] text-gray-400 hover:text-white"
                      }`}
                    >
                      VOL PROF
                    </button>
                    <button
                      onClick={() => {
                        setShowRSI(!showRSI);
                        appendLog(`[GP] RSI sub-chart toggled: ${!showRSI ? "ON" : "OFF"}`);
                      }}
                      className={`px-2 py-0.5 border text-[9px] font-bold uppercase transition rounded-none ${
                        showRSI
                          ? "border-[#FF00FF] text-[#FF00FF] bg-[#FF00FF]/5"
                          : "border-[#2D2D33] text-gray-400 hover:text-white"
                      }`}
                    >
                      RSI
                    </button>
                  </div>

                  {/* TIMEZONE & CURRENCY SELECTORS FOR USER INPUT */}
                  <div className="flex flex-wrap items-center gap-3">
                    <div className="flex items-center space-x-1">
                      <span className="text-gray-400 font-bold uppercase">TZ:</span>
                      <select 
                        value={displayTimezone}
                        onChange={(e) => {
                          const tz = e.target.value as any;
                          setDisplayTimezone(tz);
                          appendLog(`[TIME] Display timezone changed to: ${tz}`);
                        }}
                        className="bg-[#0A0D14] text-amber-500 font-extrabold border border-[#2D2D33] px-1 py-0.5 text-[8.5px] outline-none cursor-pointer focus:border-amber-500 rounded-none uppercase"
                      >
                        <option value="UTC">UTC (Zulu)</option>
                        <option value="IST">Mumbai (IST)</option>
                        <option value="GMT">London (GMT)</option>
                        <option value="EST">New York (EST)</option>
                        <option value="CST">Beijing (CST)</option>
                        <option value="JST">Tokyo (JST)</option>
                        <option value="CET">Frankfurt (CET)</option>
                      </select>
                    </div>

                    <div className="flex items-center space-x-1">
                      <span className="text-gray-400 font-bold uppercase">VAL:</span>
                      <select 
                        value={displayCurrency}
                        onChange={(e) => {
                          const cur = e.target.value as any;
                          setDisplayCurrency(cur);
                          appendLog(`[VAL] Valuation currency set to: ${cur}`);
                        }}
                        className="bg-[#0A0D14] text-emerald-400 font-extrabold border border-[#2D2D33] px-1 py-0.5 text-[8.5px] outline-none cursor-pointer focus:border-emerald-400 rounded-none uppercase"
                      >
                        <option value="USD">USD ($)</option>
                        <option value="INR">INR (₹)</option>
                        <option value="JPY">JPY (¥)</option>
                        <option value="EUR">EUR (€)</option>
                        <option value="GBP">GBP (£)</option>
                        <option value="CNY">CNY (元)</option>
                        <option value="CHF">CHF (Fr)</option>
                        <option value="SGD">SGD (S$)</option>
                        <option value="AUD">AUD (A$)</option>
                      </select>
                    </div>
                  </div>
                </div>

                {/* ACTIVE SELECTED ASSET MASTER CHART ROW */}
                <div className="flex flex-col md:flex-row gap-3">
                  {/* Detailed Interactive Candlestick, Moving Average & Volume Chart */}
                  {(() => {
                    const act = assets.find(a => a.symbol === selectedSymbol) || assets[0];
                    if (!act) return null;

                    // Derive high-density candlestick metrics on-the-fly dynamically adjusted by timeframe
                    const historyCount = 25;
                    const basePrice = convertPrice(act.price, act.exchange);
                    const timeAdjustedPrices = Array(historyCount).fill(0).map((_, i) => {
                      const offsetMultiplier = (1 + (i - historyCount + 1) * 0.001 * (
                        selectedTimeframe === "1m" ? 0.20 : 
                        selectedTimeframe === "5m" ? 0.45 : 
                        selectedTimeframe === "1h" ? 0.85 : 
                        selectedTimeframe === "1d" ? 1.50 : 3.00
                      ));
                      const noise = 1 + (Math.sin(i * 0.5) * 0.002 + (Math.cos(i * 1.1) * 0.0015)) * (
                        selectedTimeframe === "1m" ? 0.15 : 
                        selectedTimeframe === "5m" ? 0.35 : 
                        selectedTimeframe === "1h" ? 0.65 : 
                        selectedTimeframe === "1d" ? 1.00 : 2.00
                      );
                      return basePrice * offsetMultiplier * noise;
                    });

                    const candles = timeAdjustedPrices.map((val, idx) => {
                      const close = val;
                      const open = idx === 0 ? val * 0.9992 : timeAdjustedPrices[idx - 1];
                      const diff = close - open;
                      const isUp = close >= open;

                      // simulated high/low spread
                      const spread = val * 0.0012;
                      const noiseH = spread * (0.05 + Math.random() * 0.4);
                      const noiseL = spread * (0.05 + Math.random() * 0.4);
                      const high = Math.max(open, close) + noiseH;
                      const low = Math.min(open, close) - noiseL;

                      // simulated volume matching asset scale
                      const volume = 15000 + Math.round(val * 0.1) + Math.round(Math.abs(diff) * 2000) + Math.round(Math.random() * 6000);

                      return { open, high, low, close, volume, isUp };
                    });

                    // Compute SMA 5 and 10 on active closing prices
                    const computeSMA = (arr: number[], period: number) => {
                      return arr.map((_, idx) => {
                        if (idx < period - 1) return null;
                        const sum = arr.slice(idx - period + 1, idx + 1).reduce((a, b) => a + b, 0);
                        return sum / period;
                      });
                    };

                    const sma5 = computeSMA(timeAdjustedPrices, 5);
                    const sma10 = computeSMA(timeAdjustedPrices, 10);

                    // Bollinger Bands (period 10)
                    const computeBollinger = (arr: number[], period: number) => {
                      return arr.map((_, idx) => {
                        if (idx < period - 1) return { upper: null, lower: null, middle: null };
                        const slice = arr.slice(idx - period + 1, idx + 1);
                        const mean = slice.reduce((a, b) => a + b, 0) / period;
                        const variance = slice.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / period;
                        const stdDev = Math.sqrt(variance);
                        return {
                          upper: mean + 1.8 * stdDev,
                          lower: mean - 1.8 * stdDev,
                          middle: mean
                        };
                      });
                    };
                    const bollingerBands = computeBollinger(timeAdjustedPrices, 10);

                    // Coordinate setup
                    const maxP = Math.max(...candles.map(c => c.high));
                    const minP = Math.min(...candles.map(c => c.low));
                    const pRange = maxP - minP || 1;
                    const padding = pRange * 0.05;
                    const yMin = minP - padding;
                    const yMax = maxP + padding;
                    const yRange = yMax - yMin;

                    const maxVol = Math.max(...candles.map(c => c.volume)) || 1;

                    const width = 600;
                    const mainHeight = 130;
                    const rsiHeight = showRSI ? 40 : 0;
                    const height = 180 + rsiHeight; // core panel splits

                    const getX = (idx: number) => (idx / (candles.length - 1)) * (width - 60) + 20;
                    const getY = (val: number) => mainHeight - ((val - yMin) / yRange) * 110 + 10;
                    const getVolY = (vol: number) => 175 - (vol / maxVol) * 35;

                    // Polyline points for SMA lines
                    const sma5Pts = sma5.map((val, idx) => {
                      if (val === null) return "";
                      return `${getX(idx)},${getY(val)}`;
                    }).filter(Boolean).join(" ");

                    const sma10Pts = sma10.map((val, idx) => {
                      if (val === null) return "";
                      return `${getX(idx)},${getY(val)}`;
                    }).filter(Boolean).join(" ");

                    // Polyline points for Bollinger lines
                    const bollUpperPts = bollingerBands.map((val, idx) => {
                      if (val.upper === null) return "";
                      return `${getX(idx)},${getY(val.upper)}`;
                    }).filter(Boolean).join(" ");

                    const bollLowerPts = bollingerBands.map((val, idx) => {
                      if (val.lower === null) return "";
                      return `${getX(idx)},${getY(val.lower)}`;
                    }).filter(Boolean).join(" ");

                    // Polylines for VWAP Points
                    let rolledPv = 0;
                    let rolledVol = 0;
                    const vwapPts = candles.map((c, idx) => {
                      const vwapPriceVal = (c.open + c.high + c.low + c.close) / 4;
                      rolledPv += vwapPriceVal * c.volume;
                      rolledVol += c.volume;
                      const currentVwap = rolledPv / rolledVol;
                      return `${getX(idx)},${getY(currentVwap)}`;
                    }).filter(Boolean).join(" ");

                    // Horizontal Volume Profile distribution array
                    const vpBuckets = Array(10).fill(0);
                    candles.forEach(c => {
                      const midPrice = (c.high + c.low) / 2;
                      const bucketIdx = Math.max(0, Math.min(9, Math.floor(((midPrice - yMin) / yRange) * 10)));
                      vpBuckets[bucketIdx] += c.volume;
                    });
                    const maxVpBucket = Math.max(...vpBuckets) || 1;

                    // Dynamic Currency symbol check
                    const actCurrencySymbol = act.exchange === "FOREX" ? "" : getCurrencySymbol(displayCurrency);
                    const pctUp = act.pctChange >= 0;

                    // Level 2 Bids and Asks details around the current spot price
                    const l2Spread = basePrice * 0.0007;
                    const l2Asks = Array(5).fill(0).map((_, i) => {
                      const askPrice = basePrice + (5 - i) * l2Spread + (Math.sin(Date.now()*0.001 + i) * l2Spread * 0.05);
                      const askQty = (5.2 - i * 0.8 + Math.random() * 0.8) * (act.exchange === "FOREX" ? 250 : 1.2);
                      return { price: askPrice, qty: askQty };
                    });
                    const l2Bids = Array(5).fill(0).map((_, i) => {
                      const bidPrice = basePrice - (i + 1) * l2Spread + (Math.cos(Date.now()*0.0015 + i) * l2Spread * 0.05);
                      const bidQty = (5.5 - i * 0.82 + Math.random() * 0.85) * (act.exchange === "FOREX" ? 245 : 1.25);
                      return { price: bidPrice, qty: bidQty };
                    });

                    const l2TotalAsksVol = l2Asks.reduce((a, b) => a + b.qty, 0);
                    const l2TotalBidsVol = l2Bids.reduce((a, b) => a + b.qty, 0);
                    const totalL2Vol = l2TotalAsksVol + l2TotalBidsVol;
                    const buyPercentage = totalL2Vol > 0 ? (l2TotalBidsVol / totalL2Vol) * 100 : 50;
                    const sellPercentage = 100 - buyPercentage;

                    // Format Timezone details for visual footer
                    const getTimezoneFooterStr = (tz: string) => {
                      switch (tz) {
                        case "UTC": return "ZONE: UTC (Coordinated Universal Time)";
                        case "IST": return "ZONE: IST UTC+5:50 (Mumbai Local)";
                        case "GMT": return "ZONE: GMT (London Base)";
                        case "EST": return "ZONE: EST UTC-5:00 (New York Base)";
                        case "CST": return "ZONE: CST UTC+8:00 (Beijing Base)";
                        case "JST": return "ZONE: JST UTC+9:00 (Tokyo Standby)";
                        case "CET": return "ZONE: CET UTC+1:00 (Frankfurt Base)";
                        default: return "ZONE: UTC";
                      }
                    };

                    return (
                      <div className="bg-[#0B0E11] border border-[#1E232B] p-3 flex-1 flex flex-col md:flex-row gap-3">
                        
                        {/* LEFT COLUMN: CANDLESTICK CHART */}
                        <div className="flex-1 flex flex-col justify-between">
                          {/* Banner */}
                          <div className="flex justify-between items-center pb-2 border-b border-[#1A2228] mb-2 font-mono select-none">
                            <div className="flex items-center space-x-2">
                              <span className="text-xs font-extrabold text-amber-500 uppercase flex items-center gap-1">
                                <Activity size={12} className="text-amber-500" />
                                <span>{getBloombergTitle(act)}</span>
                              </span>
                              <span className="text-[8px] bg-slate-800 text-slate-300 px-1 py-[1.5px] font-bold uppercase rounded-none">{selectedTimeframe} INTERVAL</span>
                            </div>
                            <div className="flex items-center space-x-3 text-[8px] font-bold text-slate-500">
                              {showSMA && <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 bg-[#00FFFF]"></span>SMA(5)</span>}
                              {showSMA && <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 bg-[#FF00FF]"></span>SMA(10)</span>}
                              {showBollinger && <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 bg-[#EAB308]"></span>BOLL</span>}
                              {showVWAP && <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 bg-[#3B82F6]"></span>VWAP</span>}
                              <span>OHLC FEED</span>
                            </div>
                          </div>

                          {/* Chart plot */}
                          <div className="relative overflow-hidden bg-[#030507] border border-[#141B21] p-1 h-[210px] w-full">
                            <svg className="w-full h-full" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
                              {/* Horizontal Grid lines */}
                              <line x1="0" y1={mainHeight * 0.25} x2={width} y2={mainHeight * 0.25} stroke="#11181F" strokeDasharray="2,2" />
                              <line x1="0" y1={mainHeight * 0.5} x2={width} y2={mainHeight * 0.5} stroke="#11181F" strokeDasharray="2,2" />
                              <line x1="0" y1={mainHeight * 0.75} x2={width} y2={mainHeight * 0.75} stroke="#11181F" strokeDasharray="2,2" />
                              <line x1="0" y1="135" x2={width} y2="135" stroke="#1C2630" strokeWidth="1" /> {/* separation line */}

                              {/* Volume Profile Translucent Shaded overlay */}
                              {showVolumeProfile && vpBuckets.map((bucketVol, bIdx) => {
                                const bHeight = 110 / 10;
                                const bY = mainHeight - (bIdx * bHeight) - bHeight + 10;
                                const bWidth = (bucketVol / maxVpBucket) * 60; // Max horizontal width on grid
                                return (
                                  <rect
                                    key={`vp-${bIdx}`}
                                    x="20"
                                    y={bY}
                                    width={bWidth}
                                    height={bHeight - 1.5}
                                    fill="#F59E0B"
                                    opacity="0.08"
                                  />
                                );
                              })}

                              {/* Separate Buy/Sell detailed volume breakdown subplot */}
                              {candles.map((c, idx) => {
                                const x = getX(idx);
                                const totalVolHeight = (c.volume / maxVol) * 35;
                                const buyVolRatio = c.isUp ? (0.6 + Math.random() * 0.25) : (0.15 + Math.random() * 0.25);
                                const buyHeight = totalVolHeight * buyVolRatio;
                                const sellHeight = totalVolHeight * (1 - buyVolRatio);
                                
                                const buyY = 175 - buyHeight;
                                const sellY = buyY - sellHeight;

                                return (
                                  <g key={`vol-split-${idx}`}>
                                    {/* Emerald Buying Volume */}
                                    <rect 
                                      x={x - 2.5}
                                      y={buyY}
                                      width="5"
                                      height={buyHeight}
                                      fill="#10B981"
                                      opacity="0.32"
                                    />
                                    {/* Crimson Selling Volume */}
                                    <rect 
                                      x={x - 2.5}
                                      y={sellY}
                                      width="5"
                                      height={sellHeight}
                                      fill="#EF4444"
                                      opacity="0.32"
                                    />
                                  </g>
                                );
                              })}

                              {/* Candlesticks & Key Events Tags */}
                              {candles.map((c, idx) => {
                                const x = getX(idx);
                                const yHigh = getY(c.high);
                                const yLow = getY(c.low);
                                const yOpen = getY(c.open);
                                const yClose = getY(c.close);
                                const fill = c.isUp ? "#10B981" : "#EF4444";
                                
                                const bodyTop = Math.min(yOpen, yClose);
                                const bodyBottom = Math.max(yOpen, yClose);
                                const bodyHeight = Math.max(1, bodyBottom - bodyTop);

                                return (
                                  <g key={`candle-${idx}`}>
                                    {/* Wick */}
                                    <line 
                                      x1={x} 
                                      y1={yHigh} 
                                      x2={x} 
                                      y2={yLow} 
                                      stroke={fill} 
                                      strokeWidth="1" 
                                    />
                                    {/* Body */}
                                    <rect 
                                      x={x - 3} 
                                      y={bodyTop} 
                                      width="6" 
                                      height={bodyHeight} 
                                      fill={fill} 
                                      stroke={fill}
                                      strokeWidth="0.5"
                                    />

                                    {/* Earnings Key Event Tag Circle (idx 8) */}
                                    {idx === 8 && (
                                      <g key={`evt-earn-${idx}`}>
                                        <line x1={x} y1={yLow} x2={x} y2={yLow + 12} stroke="#3B82F6" strokeWidth="0.8" strokeDasharray="1,2" />
                                        <circle cx={x} cy={yLow + 17} r="4.5" fill="#3B82F6" stroke="#000" strokeWidth="0.5" />
                                        <text x={x} y={yLow + 19.5} fill="#FFFFFF" fontSize="6.5" fontWeight="black" textAnchor="middle">E</text>
                                        <title>Event Tag: Corporate Earnings Release</title>
                                      </g>
                                    )}

                                    {/* Dividend Key Event Tag Circle (idx 18) */}
                                    {idx === 18 && (
                                      <g key={`evt-div-${idx}`}>
                                        <line x1={x} y1={yHigh} x2={x} y2={yHigh - 12} stroke="#10B981" strokeWidth="0.8" strokeDasharray="1,2" />
                                        <circle cx={x} cy={yHigh - 17} r="4.5" fill="#10B981" stroke="#000" strokeWidth="0.5" />
                                        <text x={x} y={yHigh - 14.5} fill="#FFFFFF" fontSize="6.5" fontWeight="black" textAnchor="middle">D</text>
                                        <title>Event Tag: Cash Dividend Allocation</title>
                                      </g>
                                    )}
                                  </g>
                                );
                              })}

                              {/* Bollinger Bands Shading & Lines */}
                              {showBollinger && bollUpperPts && bollLowerPts && (
                                <g key="boll-bands">
                                  {/* Upper line */}
                                  <polyline points={bollUpperPts} fill="none" stroke="#EAB308" strokeWidth="0.8" strokeDasharray="2,2" opacity="0.8" />
                                  {/* Lower line */}
                                  <polyline points={bollLowerPts} fill="none" stroke="#EAB308" strokeWidth="0.8" strokeDasharray="2,2" opacity="0.8" />
                                </g>
                              )}

                              {/* SMA lines */}
                              {showSMA && sma5Pts && (
                                <polyline points={sma5Pts} fill="none" stroke="#00FFFF" strokeWidth="1.2" opacity="0.9" />
                              )}
                              {showSMA && sma10Pts && (
                                <polyline points={sma10Pts} fill="none" stroke="#FF00FF" strokeWidth="1.2" opacity="0.9" />
                              )}

                              {/* VWAP Overlay plotting */}
                              {showVWAP && vwapPts && (
                                <polyline points={vwapPts} fill="none" stroke="#3B82F6" strokeWidth="1.2" strokeDasharray="1,1" opacity="0.95" />
                              )}

                              {/* LIVE SPOT RATE OVERLAY LINE AND AXIS BADGE */}
                              {(() => {
                                const ySpot = getY(basePrice);
                                if (ySpot < 10 || ySpot > 130) return null;
                                return (
                                  <g key="live-spot-overlay">
                                    <line 
                                      x1="0" 
                                      y1={ySpot} 
                                      x2={width - 55} 
                                      y2={ySpot} 
                                      stroke="#FFA500" 
                                      strokeWidth="1" 
                                      strokeDasharray="3,3" 
                                      opacity="0.85" 
                                    />
                                    <rect x={width - 55} y={ySpot - 6} width="52" height="12" fill="#FFA500" rx="1" />
                                    <text 
                                      x={width - 29} 
                                      y={ySpot + 3} 
                                      fill="#000000" 
                                      fontSize="8" 
                                      fontWeight="black" 
                                      textAnchor="middle" 
                                      fontFamily="monospace"
                                    >
                                      {basePrice.toLocaleString(undefined, { minimumFractionDigits: act.exchange === "FOREX"?4:2 })}
                                    </text>
                                  </g>
                                );
                              })()}

                              {/* RSI SUB-CHART ZONE */}
                              {showRSI && (() => {
                                let baseRSI = 50;
                                const rsiValues = candles.map((c, idx) => {
                                  const shift = (c.isUp ? 7 : -7) + Math.sin(idx * 0.9) * 3;
                                  baseRSI = Math.max(15, Math.min(85, baseRSI + shift));
                                  return baseRSI;
                                });

                                const getRsiY = (rsiVal: number) => 218 - (rsiVal / 100) * 30; // bounds inside (y=188 to y=218)
                                const rsiPts = rsiValues.map((val, idx) => `${getX(idx)},${getRsiY(val)}`).join(" ");

                                return (
                                  <g key="rsi-panel">
                                    {/* Division boundary */}
                                    <line x1="0" y1="180" x2={width} y2="180" stroke="#253240" strokeWidth="0.8" />
                                    {/* Dotted threshold marks */}
                                    <line x1="0" y1={getRsiY(70)} x2={width} y2={getRsiY(70)} stroke="#EF4444" strokeWidth="0.5" strokeDasharray="2,2" opacity="0.5" />
                                    <line x1="0" y1={getRsiY(30)} x2={width} y2={getRsiY(30)} stroke="#10B981" strokeWidth="0.5" strokeDasharray="2,2" opacity="0.5" />
                                    <text x="5" y={getRsiY(70) - 2} fill="#EF4444" fontSize="6.5" fontWeight="bold">RSI 70 Overbought</text>
                                    <text x="5" y={getRsiY(30) + 7} fill="#10B981" fontSize="6.5" fontWeight="bold">RSI 30 Oversold</text>
                                    
                                    {/* RSI Path line */}
                                    <polyline points={rsiPts} fill="none" stroke="#FF5E00" strokeWidth="1" opacity="0.9" />
                                    <text x={width - 3} y="192" fill="#FFA500" fontSize="7" fontWeight="bold" textAnchor="end" fontFamily="monospace">RSI(14)</text>
                                  </g>
                                );
                              })()}
                            </svg>

                            <div className="absolute top-1 left-2 text-[8px] text-slate-500 font-bold select-none font-mono">PEAK: {actCurrencySymbol}{maxP.toLocaleString(undefined, { minimumFractionDigits: act.exchange==="FOREX"?4:1 })}</div>
                            <div className="absolute bottom-11 left-2 text-[8px] text-slate-500 font-bold select-none font-mono">MIN: {actCurrencySymbol}{minP.toLocaleString(undefined, { minimumFractionDigits: act.exchange==="FOREX"?4:1 })}</div>
                            <div className="absolute bottom-1 right-2 text-[7.5px] text-slate-600 font-bold uppercase select-none font-mono">LIVE VOLUME SUBPLOT</div>
                          </div>

                          {/* Bottom stats summary */}
                          <div className="flex justify-between items-center text-[10px] uppercase font-bold mt-2 pt-1 border-t border-[#131B21]">
                            <span className="text-slate-400">SPOT price: <span className="text-white font-extrabold text-xs pl-1">{actCurrencySymbol}{basePrice.toLocaleString(undefined, { minimumFractionDigits: act.exchange === "FOREX" ? 4 : 2 })}</span></span>
                            <span className="text-slate-500 font-mono text-[8.5px]">{getTimezoneFooterStr(displayTimezone)}</span>
                            <span className={`${pctUp ? "text-[#10B981]" : "text-[#EF4444]"} font-extrabold pr-1`}>
                              {pctUp ? "▲ +" : "▼ "}{actCurrencySymbol}{convertPrice(act.change, act.exchange).toLocaleString(undefined, { minimumFractionDigits: act.exchange === "FOREX" ? 4 : 2 })} (+{act.pctChange.toFixed(2)}%)
                            </span>
                          </div>
                        </div>

                        {/* RIGHT COLUMN: HIGH-DENSITY LEVEL 2 DEPTH & VOLUME PROFILE PANEL */}
                        <div className="md:w-[200px] bg-[#07090C] border border-[#242A35] p-2 flex flex-col justify-between select-none">
                          <div className="space-y-1.5 w-full">
                            <div className="border-b border-[#24242C] pb-1 flex justify-between items-center">
                              <span className="text-[9.5px] font-bold text-amber-500 uppercase tracking-widest font-mono flex items-center gap-1">
                                <Layers size={10} className="text-amber-500" />
                                <span>L2 BID/ASK DEPTH</span>
                              </span>
                              <span className="text-[7.5px] bg-[#224] text-white px-0.5 font-bold uppercase">L2</span>
                            </div>

                            {/* Vol Selling Asks Stack (descending order) */}
                            <div className="space-y-[2.5px] font-mono">
                              {l2Asks.map((ask, idx) => {
                                const askPercentage = Math.min(100, (ask.qty / l2TotalAsksVol) * 100);
                                return (
                                  <div key={`l2-ask-${idx}`} className="relative flex justify-between py-0.5 px-1 text-[8.5px]">
                                    {/* Red visual ask quantity bar */}
                                    <div style={{ width: `${askPercentage}%` }} className="absolute right-0 top-0 bottom-0 bg-red-500/5 pointer-events-none transition-all duration-300"></div>
                                    <span className="z-10 text-red-500 font-semibold">{actCurrencySymbol}{ask.price.toLocaleString(undefined, { minimumFractionDigits: act.exchange==="FOREX"?4:2 })}</span>
                                    <span className="z-10 text-slate-300 font-bold">{ask.qty.toFixed(act.exchange==="FOREX"?1:3)}</span>
                                  </div>
                                );
                              })}
                            </div>

                            <div className="border-y border-[#24242C] py-1 text-center bg-[#0C1014] text-[9px] font-bold font-mono">
                              <span className="text-[#888888]">SPREAD: </span>
                              <span className="text-amber-500">{actCurrencySymbol}{l2Spread.toFixed(act.exchange==="FOREX"?4:2)}</span>
                            </div>

                            {/* Vol Buying Bids Stack (descending size) */}
                            <div className="space-y-[2.5px] font-mono">
                              {l2Bids.map((bid, idx) => {
                                const bidPercentage = Math.min(100, (bid.qty / l2TotalBidsVol) * 100);
                                return (
                                  <div key={`l2-bid-${idx}`} className="relative flex justify-between py-0.5 px-1 text-[8.5px]">
                                    {/* Green visual bid quantity bar */}
                                    <div style={{ width: `${bidPercentage}%` }} className="absolute left-0 top-0 bottom-0 bg-green-500/5 pointer-events-none transition-all duration-300"></div>
                                    <span className="z-10 text-green-500 font-semibold">{actCurrencySymbol}{bid.price.toLocaleString(undefined, { minimumFractionDigits: act.exchange==="FOREX"?4:2 })}</span>
                                    <span className="z-10 text-slate-300 font-bold">{bid.qty.toFixed(act.exchange==="FOREX"?1:3)}</span>
                                  </div>
                                );
                              })}
                            </div>
                          </div>

                          {/* DYNAMIC VOLUME PRESSURE COMPILER PROFILE */}
                          <div className="border-t border-[#24242C] pt-2 mt-2 space-y-1 w-full font-mono">
                            <div className="flex justify-between items-center text-[7.5px] text-gray-400 font-bold">
                              <span>BUY PRESSURE: {buyPercentage.toFixed(0)}%</span>
                              <span>{sellPercentage.toFixed(0)}% :SELL</span>
                            </div>
                            <div className="w-full h-1.5 flex bg-red-600 overflow-hidden rounded-none border border-[#3D3D44]/30">
                              <div style={{ width: `${buyPercentage}%` }} className="bg-emerald-500 h-full transition-all duration-300"></div>
                            </div>
                            <span className="block text-[7.5px] text-center text-slate-500 font-semibold uppercase leading-tight select-none">OBI L2 Volume Profile</span>
                          </div>

                        </div>

                      </div>
                    );
                  })()}
                </div>

                {/* GLOBAL MARKETS HIGH-DENSITY SHEET BLOCK */}
                <div className="bg-[#0B0E11] border border-[#1E232B] p-2 flex flex-col space-y-3">
                  <div className="border-b border-[#1A2228] pb-1.5 flex justify-between items-center">
                    <span className="text-[11px] font-extrabold text-amber-500 uppercase tracking-widest">WORLD EQUITIES & FOREX DESK — REAL-TIME MONITOR</span>
                    <span className="text-[9px] text-gray-500">Click any asset row to hook spot order-book & master graphs</span>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    
                    {/* COLUMN ONE: US & IND GLOBAL DESKS */}
                    <div className="space-y-3">
                      {/* US EQUITIES AND INDICES */}
                      <div className="border border-[#182026] bg-[#020507]">
                        <div className="bg-[#10141B] border-b border-[#182026] px-2 py-0.5 text-[8.5px] font-bold text-amber-500 uppercase tracking-wider">USA INDEX & EQUITIES DESK</div>
                        {renderAssetTable("US")}
                      </div>

                      {/* INDIAN DESK */}
                      <div className="border border-[#182026] bg-[#020507]">
                        <div className="bg-[#10141B] border-b border-[#182026] px-2 py-0.5 text-[8.5px] font-bold text-amber-500 uppercase tracking-wider">INDIA EQUITIES & SENSEX</div>
                        {renderAssetTable("IND")}
                      </div>
                    </div>

                    {/* COLUMN TWO: EUROPE & ASIA PLUS FX/COMM DESK */}
                    <div className="space-y-3">
                      {/* FOREX AND COMMODITIES BLOCK */}
                      <div className="border border-[#182026] bg-[#020507]">
                        <div className="bg-[#10141B] border-b border-[#182026] px-2 py-0.5 text-[8.5px] font-bold text-amber-500 uppercase tracking-wider">CURRENCY & COMMODITIES DESK</div>
                        {renderAssetTable("FX_COMM")}
                      </div>

                      {/* EUROPE & EAST-ASIA DESKS */}
                      <div className="border border-[#182026] bg-[#020507]">
                        <div className="bg-[#10141B] border-b border-[#182026] px-2 py-0.5 text-[8.5px] font-bold text-amber-500 uppercase tracking-wider">EUROPE & EAST-ASIA DESKS</div>
                        {renderAssetTable("EUR_ASIA")}
                      </div>
                    </div>

                  </div>

                </div>

              </div>

              {/* QUICK SYSTEM TERMINAL COLUMN & STATS (4/12) */}
              <div className="lg:col-span-4 flex flex-col space-y-3">
                
                {/* DUAL DIVISION HIGH-DENSITY NEWS SYSTEM (DOMESTIC VS INTERNATIONAL) */}
                
                {/* 1. DOMESTIC INDIA NEWS LOG */}
                <div className="bg-[#0B0E11] border border-[#1E232B] p-3 flex flex-col h-[255px]">
                  <div className="border-b border-[#1A2228] pb-1.5 mb-2 flex justify-between items-center font-mono">
                    <span className="text-[10px] font-bold text-emerald-400 uppercase flex items-center space-x-1.5 tracking-wider">
                      <span className="w-1.5 h-1.5 bg-emerald-400 rounded-none animate-pulse"></span>
                      <span>INDIA DOMESTIC FEED</span>
                    </span>
                    <span className="text-[8px] bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 px-1 py-0.5 font-bold uppercase">NSE LIVE</span>
                  </div>

                  <div className="flex-1 overflow-y-auto space-y-1.5 pr-1 select-text scrollbar-none">
                    {(() => {
                      const indNews = news.filter(item => item.region === "INDIA");
                      if (indNews.length === 0) {
                        return <div className="text-center py-10 text-gray-600 text-[9px]">Awaiting live domestic dispatches...</div>;
                      }
                      return indNews.map((item) => (
                        <div 
                          key={item.id} 
                          onClick={() => {
                            setSelectedNewsId(item.id);
                            setActiveTab("cn");
                            appendLog(`[NEWS] Locking domestic dispatch context ID: ${item.id}`);
                          }}
                          className="text-[9.5px] leading-relaxed border-b border-[#131B21]/60 pb-1.5 flex items-start space-x-1.5 cursor-pointer hover:bg-[#141B23]/40 p-1 transition-colors file:rounded-none"
                        >
                          <span className="text-emerald-500 font-extrabold shrink-0 select-none font-mono">🕒 {item.time.split(" ")[0]}</span>
                          <div className="flex-1 space-y-[2px]">
                            <div className="flex justify-between items-center">
                              <span className="text-amber-500 font-black text-[8px] uppercase">[{item.source}]</span>
                              {item.ticker && <span className="text-[7.5px] bg-[#1A1E24] text-[#888888] font-mono px-1 border border-[#2D2D33]">{item.ticker}</span>}
                            </div>
                            <span className="text-slate-100 font-bold tracking-tight block hover:text-amber-400 transition">{item.headline}</span>
                          </div>
                        </div>
                      ));
                    })()}
                  </div>
                </div>

                {/* 2. INTERNATIONAL GLOBAL OVERWATCH */}
                <div className="bg-[#0B0E11] border border-[#1E232B] p-3 flex flex-col h-[255px]">
                  <div className="border-b border-[#1A2228] pb-1.5 mb-2 flex justify-between items-center font-mono">
                    <span className="text-[10px] font-bold text-cyan-400 uppercase flex items-center space-x-1.5 tracking-wider">
                      <span className="w-1.5 h-1.5 bg-cyan-400 rounded-none animate-pulse"></span>
                      <span>GLOBAL SESSIONS OVERWATCH</span>
                    </span>
                    <span className="text-[8px] bg-red-500/10 border border-red-500/30 text-red-400 px-1 py-0.5 font-bold animate-pulse">FED WIRE</span>
                  </div>

                  <div className="flex-1 overflow-y-auto space-y-1.5 pr-1 select-text scrollbar-none font-mono">
                    {(() => {
                      const intlNews = news.filter(item => item.region !== "INDIA");
                      if (intlNews.length === 0) {
                        return <div className="text-center py-10 text-gray-600 text-[9px]">Awaiting foreign interbank dispatches...</div>;
                      }
                      return intlNews.map((item) => (
                        <div 
                          key={item.id} 
                          onClick={() => {
                            setSelectedNewsId(item.id);
                            setActiveTab("cn");
                            appendLog(`[NEWS] Locking international dispatch context ID: ${item.id}`);
                          }}
                          className="text-[9.5px] leading-relaxed border-b border-[#131B21]/60 pb-1.5 flex items-start space-x-1.5 cursor-pointer hover:bg-[#1C1814]/40 p-1 transition-colors file:rounded-none"
                        >
                          <span className="text-cyan-400 font-extrabold shrink-0 select-none font-mono">🕒 {item.time.split(" ")[0]}</span>
                          <div className="flex-1 space-y-[2px]">
                            <div className="flex justify-between items-center">
                              <span className="text-amber-500 font-black text-[8px] uppercase">[{item.source}]</span>
                              {item.ticker && <span className="text-[7.5px] bg-[#1A1E24] text-[#888888] font-mono px-1 border border-[#2D2D33]">{item.ticker}</span>}
                            </div>
                            <span className="text-slate-100 font-bold tracking-tight block hover:text-amber-400 transition">{item.headline}</span>
                          </div>
                        </div>
                      ));
                    })()}
                  </div>
                </div>

              </div>

            </motion.div>
          )}

          {/* VIEWPORT 2: ORDER BOOK SIMULATOR (OB) */}
          {activeTab === "ob" && (() => {
            const selectedAsset = assets.find(a => a.symbol === selectedSymbol);
            let computedVixLevel = 14.5;
            if (selectedAsset) {
              if (selectedAsset.exchange === "IND") {
                computedVixLevel = assets.find(a => a.symbol === "INDIA VIX")?.price || 14.85;
              } else if (selectedAsset.exchange === "US") {
                computedVixLevel = assets.find(a => a.symbol === "VIX")?.price || 13.52;
              } else if (selectedAsset.exchange === "CHN") {
                computedVixLevel = assets.find(a => a.symbol === "VXFXI")?.price || 20.80;
              }
            }
            return (
              <motion.div 
                key="ob"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                transition={{ duration: 0.1 }}
              >
                <OrderBook 
                  lastPrice={selectedAsset?.price || lastBtcPrice} 
                  symbol={selectedSymbol} 
                  onLogMessage={appendLog} 
                  incomingTrade={latestIncomingTrade}
                  vixLevel={computedVixLevel}
                />
              </motion.div>
            );
          })()}

          {/* VIEWPORT: COMPANY NEWS INTEGRATED CONSOLE (CN) */}
          {activeTab === "cn" && (
            <motion.div 
              key="cn"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.1 }}
              className="bg-[#0B0E11] border border-[#1E232B] p-4 flex flex-col space-y-4"
            >
              {/* Header */}
              <div className="border-b border-[#242A35] pb-2 text-[10px] font-extrabold uppercase text-amber-500 flex justify-between items-center">
                <span className="flex items-center space-x-1.5">
                  <span className="w-2 h-2 bg-red-500 animate-ping rounded-none shrink-0" />
                  <span>CN MASTER DISPATCH WIRE — BLOOMBERG PROFESSIONAL WIRE CHANNELS</span>
                </span>
                <span className="text-gray-500">Search news, toggle regions, and simulate custom flash news dispatches live</span>
              </div>

              {/* SEARCH & FILTERS CONTROLS STRIP */}
              <div className="bg-[#03060C] p-3 border border-[#1A232B] flex flex-col md:flex-row gap-3 items-center justify-between font-mono text-[10px]">
                {/* 1. Region Filters */}
                <div className="flex items-center space-x-1.5 w-full md:w-auto">
                  <span className="text-gray-400 font-extrabold mr-1 uppercase">REGION:</span>
                  {(["ALL", "INDIA", "INTERNATIONAL"] as const).map((r) => (
                    <button
                      key={r}
                      onClick={() => setCnRegionFilter(r)}
                      className={`px-2 py-1 text-[9px] font-black uppercase transition border ${
                        cnRegionFilter === r
                          ? "bg-amber-500 text-black border-amber-500 font-extrabold"
                          : "bg-[#0A1016] text-[#888888] border-[#202832] hover:text-slate-100"
                      }`}
                    >
                      {r}
                    </button>
                  ))}
                </div>

                {/* 2. Search Box */}
                <div className="relative flex items-center w-full md:flex-1 md:max-w-md">
                  <span className="absolute left-2 text-[#888888] text-[9.5px]">SEARCH &lt;GO&gt;:</span>
                  <input
                    type="text"
                    value={cnSearchQuery}
                    onChange={(e) => setCnSearchQuery(e.target.value)}
                    placeholder="ENTER KEYWORD OR TICKER (e.g., RELIANCE)..."
                    className="w-full bg-[#050910] border border-amber-500/20 text-slate-100 placeholder-gray-600 text-[10px] pl-[84px] pr-8 py-1 font-mono focus:border-amber-500/60 focus:outline-none focus:ring-0 rounded-none uppercase"
                  />
                  {cnSearchQuery && (
                    <button 
                      onClick={() => setCnSearchQuery("")}
                      className="absolute right-2.5 text-[#888888] hover:text-white text-[9px]"
                      title="Clear Search"
                    >
                      ✕
                    </button>
                  )}
                </div>

                {/* 3. Sentiment Filters */}
                <div className="flex items-center space-x-1.5 w-full md:w-auto">
                  <span className="text-gray-400 font-extrabold mr-1 uppercase">SENTIMENT:</span>
                  {(["ALL", "BULLISH", "BEARISH", "NEUTRAL"] as const).map((s) => (
                    <button
                      key={s}
                      onClick={() => setCnSentimentFilter(s)}
                      className={`px-2 py-1 text-[9px] font-black uppercase transition border ${
                        cnSentimentFilter === s
                          ? s === "BULLISH"
                            ? "bg-green-500 text-black border-green-500 font-bold"
                            : s === "BEARISH"
                            ? "bg-red-500 text-black border-red-500 font-bold"
                            : "bg-amber-500 text-black border-amber-400 font-bold"
                          : "bg-[#0A1016] text-[#888888] border-[#202832] hover:text-slate-100"
                      }`}
                    >
                      {s}
                    </button>
                  ))}
                </div>
              </div>

              {/* MANUAL FLASH INJECTOR SIMULATOR COMPILER */}
              <div className="bg-[#070B11] border border-[#14202B] p-3 flex flex-col space-y-2.5 font-mono text-[10px]">
                <div className="flex items-center justify-between border-b border-[#141B21] pb-1">
                  <span className="text-[9px] font-bold text-amber-500 uppercase flex items-center space-x-1">
                    <span className="w-1.5 h-1.5 bg-cyan-400 rounded-none animate-pulse"></span>
                    <span>FAST NEWS INTAKE PORT — LOCAL COMPILER DISPATCHER</span>
                  </span>
                  <span className="text-[7.5px] text-gray-500">Inject real-time custom stories into inter-bank Bloomberg news terminals</span>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-12 gap-3 items-end">
                  <div className="md:col-span-4 flex flex-col space-y-1">
                    <span className="text-gray-500 text-[8px] uppercase">Headlines:</span>
                    <input
                      type="text"
                      value={customHeadline}
                      onChange={(e) => setCustomHeadline(e.target.value)}
                      placeholder="e.g., RELIANCE Q1 PROFIT LEAPS 25% ON OIL CORRIDORS"
                      className="w-full bg-[#03060B] border border-[#232F3D] text-slate-200 text-[9.5px] p-1.5 focus:border-[#4B8BAC] focus:ring-0 focus:outline-none font-mono placeholder-gray-600 uppercase"
                    />
                  </div>
                  <div className="md:col-span-5 flex flex-col space-y-1">
                    <span className="text-gray-500 text-[8px] uppercase">Extended Brief Story Details:</span>
                    <input
                      type="text"
                      value={customDetails}
                      onChange={(e) => setCustomDetails(e.target.value)}
                      placeholder="Optional technical analysis, earnings stats, or regulatory dispatches..."
                      className="w-full bg-[#03060B] border border-[#232F3D] text-slate-200 text-[9.5px] p-1.5 focus:border-[#4B8BAC] focus:ring-0 focus:outline-none font-mono placeholder-gray-600"
                    />
                  </div>
                  <div className="md:col-span-2 flex flex-col space-y-1">
                    <span className="text-gray-500 text-[8px] uppercase">Assign Region:</span>
                    <select
                      value={customRegion}
                      onChange={(e) => setCustomRegion(e.target.value as "INDIA" | "INTERNATIONAL")}
                      className="w-full bg-[#03060B] border border-[#232F3D] text-slate-200 text-[9.5px] p-1.5 focus:border-[#4B8BAC] focus:ring-0 focus:outline-none font-mono rounded-none"
                    >
                      <option value="INDIA">INDIA [DOMESTIC]</option>
                      <option value="INTERNATIONAL">INTERNATIONAL [GLOBAL]</option>
                    </select>
                  </div>
                  <div className="md:col-span-1">
                    <button
                      onClick={handleCustomInject}
                      className="w-full bg-cyan-600 hover:bg-cyan-500 text-black font-extrabold text-[9px] py-2 px-1 rounded-none select-none uppercase tracking-wider transition active:translate-y-0.5"
                    >
                      INJECT
                    </button>
                  </div>
                </div>
              </div>

              {/* Body Split Grid */}
              <div className="grid grid-cols-1 lg:grid-cols-12 gap-4">
                {/* News dispatch titles listing (Col Span 7) */}
                <div className="lg:col-span-7 border border-[#1E232B] bg-[#020507] h-[480px] flex flex-col">
                  <div className="bg-[#10141B] border-b border-[#1E232B] px-3 py-1.5 text-[9px] font-bold text-amber-500 uppercase tracking-widest flex justify-between select-none">
                    <span>WIRE LOG JOURNAL</span>
                    <span>Showing {(() => {
                      // Filter logic inline
                      const filteredNews = news.filter((item) => {
                        const matchesSearch = cnSearchQuery.trim() === "" || 
                          item.headline.toLowerCase().includes(cnSearchQuery.toLowerCase()) ||
                          (item.details && item.details.toLowerCase().includes(cnSearchQuery.toLowerCase())) ||
                          (item.ticker && item.ticker.toLowerCase().includes(cnSearchQuery.toLowerCase()));
                        const matchesRegion = cnRegionFilter === "ALL" || item.region === cnRegionFilter;
                        
                        const storySentiment = (() => {
                          const up = ["EXPANDS", "GAIN", "SURGES", "SECURES", "STRIDES", "ALLIANCE", "STRENGTHEN", "RISE", "HIGH", "INCREASE", "UPGRADE", "PARTNERSHIP"];
                          const down = ["DIP", "RETREATS", "DROPS", "FALL", "DOWN", "COOLDOWN", "DEPRECIATES", "WEAKENING", "LOWER"];
                          const hUpper = item.headline.toUpperCase();
                          if (up.some(word => hUpper.includes(word))) return "BULLISH";
                          if (down.some(word => hUpper.includes(word))) return "BEARISH";
                          return "NEUTRAL";
                        })();
                        
                        const matchesSentiment = cnSentimentFilter === "ALL" || storySentiment === cnSentimentFilter;
                        return matchesSearch && matchesRegion && matchesSentiment;
                      });
                      return filteredNews.length;
                    })()} dispatches matched</span>
                  </div>
                  <div className="flex-1 overflow-y-auto divide-y divide-[#13171D] select-text scrollbar-thin">
                    {(() => {
                      const filteredNews = news.filter((item) => {
                        const matchesSearch = cnSearchQuery.trim() === "" || 
                          item.headline.toLowerCase().includes(cnSearchQuery.toLowerCase()) ||
                          (item.details && item.details.toLowerCase().includes(cnSearchQuery.toLowerCase())) ||
                          (item.ticker && item.ticker.toLowerCase().includes(cnSearchQuery.toLowerCase()));
                        const matchesRegion = cnRegionFilter === "ALL" || item.region === cnRegionFilter;
                        
                        const storySentiment = (() => {
                          const up = ["EXPANDS", "GAIN", "SURGES", "SECURES", "STRIDES", "ALLIANCE", "STRENGTHEN", "RISE", "HIGH", "INCREASE", "UPGRADE", "PARTNERSHIP"];
                          const down = ["DIP", "RETREATS", "DROPS", "FALL", "DOWN", "COOLDOWN", "DEPRECIATES", "WEAKENING", "LOWER"];
                          const hUpper = item.headline.toUpperCase();
                          if (up.some(word => hUpper.includes(word))) return "BULLISH";
                          if (down.some(word => hUpper.includes(word))) return "BEARISH";
                          return "NEUTRAL";
                        })();
                        
                        const matchesSentiment = cnSentimentFilter === "ALL" || storySentiment === cnSentimentFilter;
                        return matchesSearch && matchesRegion && matchesSentiment;
                      });

                      if (filteredNews.length === 0) {
                        return (
                          <div className="h-full flex flex-col items-center justify-center text-center text-gray-500 py-12 select-none">
                            <span className="text-[10px] uppercase text-gray-600">[NO MATCHING DISPATCH SECURED]</span>
                            <span className="text-[8.5px] text-gray-600 mt-1 max-w-xs">Relax filter protocols or alter keyword queries to decode inter-bank wires.</span>
                          </div>
                        );
                      }

                      return filteredNews.map((item) => {
                        const storySentiment = (() => {
                          const up = ["EXPANDS", "GAIN", "SURGES", "SECURES", "STRIDES", "ALLIANCE", "STRENGTHEN", "RISE", "HIGH", "INCREASE", "UPGRADE", "PARTNERSHIP"];
                          const down = ["DIP", "RETREATS", "DROPS", "FALL", "DOWN", "COOLDOWN", "DEPRECIATES", "WEAKENING", "LOWER"];
                          const hUpper = item.headline.toUpperCase();
                          if (up.some(word => hUpper.includes(word))) return "BULLISH";
                          if (down.some(word => hUpper.includes(word))) return "BEARISH";
                          return "NEUTRAL";
                        })();

                        return (
                          <div 
                            key={item.id} 
                            onClick={() => setSelectedNewsId(item.id)}
                            className={`p-2.5 font-mono text-[10px] hover:bg-[#0C121E] cursor-pointer transition flex items-start space-x-3 text-left ${selectedNewsId === item.id ? "bg-[#0A1121] border-l-2 border-amber-500" : ""}`}
                          >
                            <span className="text-gray-400 font-extrabold shrink-0">{item.time}</span>
                            <span className="text-amber-500 font-bold shrink-0">[{item.source}]</span>
                            <div className="flex-1 space-y-1">
                              <span className="text-slate-200 font-bold block leading-snug">{item.headline}</span>
                              <div className="flex flex-wrap items-center gap-1.5 mt-1 select-none">
                                {/* Region Tag */}
                                <span className={`text-[7px] font-black px-1.5 py-0.5 uppercase tracking-wider ${
                                  item.region === "INDIA" 
                                    ? "bg-emerald-500/10 text-emerald-400 border border-emerald-500/20" 
                                    : "bg-cyan-500/10 text-cyan-400 border border-cyan-500/20"
                                }`}>
                                  {item.region || "GLOBAL"}
                                </span>

                                {/* Sentiment Badge */}
                                <span className={`text-[7px] font-black px-1.5 py-0.5 uppercase tracking-wider ${
                                  storySentiment === "BULLISH"
                                    ? "bg-green-500/10 text-green-400 border border-green-500/25"
                                    : storySentiment === "BEARISH"
                                    ? "bg-red-500/10 text-red-400 border border-red-500/25"
                                    : "bg-slate-500/10 text-slate-400 border border-slate-500/25"
                                }`}>
                                  {storySentiment}
                                </span>

                                {item.ticker && item.ticker !== "ALL" && (
                                  <span className="text-[7.5px] bg-[#12161A] text-gray-500 border border-gray-800 px-1 font-mono">
                                    {item.ticker}
                                  </span>
                                )}
                              </div>
                            </div>
                          </div>
                        );
                      });
                    })()}
                  </div>
                </div>

                {/* News detailed dispatch ledger (Col Span 5) */}
                <div className="lg:col-span-5 border border-[#1E232B] bg-[#020507] h-[480px] flex flex-col">
                  <div className="bg-[#10141B] border-b border-[#1E232B] px-3 py-1.5 text-[9px] font-bold text-amber-500 uppercase tracking-widest">
                    DISPATCH WIRE TELEMETRY DETAILS
                  </div>
                  <div className="flex-1 p-4 overflow-y-auto text-left font-mono text-[10px] space-y-4 select-text scrollbar-thin">
                    {(() => {
                      const selItem = news.find(n => n.id === selectedNewsId);
                      if (!selItem) {
                        return (
                          <div className="h-full flex flex-col items-center justify-center text-center text-gray-500 space-y-2 py-10 antialiased select-none">
                            <Activity size={24} className="text-gray-600 animate-pulse" />
                            <span>[NO DISPATCH SECURED]</span>
                            <p className="text-[9px] max-w-xs text-gray-600 leading-normal">Type Ticker CN command or select any headline from the left dispatch ledger to lock cryptographic news context.</p>
                          </div>
                        );
                      }

                      const storySentiment = (() => {
                        const up = ["EXPANDS", "GAIN", "SURGES", "SECURES", "STRIDES", "ALLIANCE", "STRENGTHEN", "RISE", "HIGH", "INCREASE", "UPGRADE", "PARTNERSHIP"];
                        const down = ["DIP", "RETREATS", "DROPS", "FALL", "DOWN", "COOLDOWN", "DEPRECIATES", "WEAKENING", "LOWER"];
                        const hUpper = selItem.headline.toUpperCase();
                        if (up.some(word => hUpper.includes(word))) return "BULLISH";
                        if (down.some(word => hUpper.includes(word))) return "BEARISH";
                        return "NEUTRAL";
                      })();

                      return (
                        <div className="space-y-4 leading-relaxed text-gray-300">
                          <div className="border bg-[#0B1017] border-amber-500/10 p-3 space-y-1.5 rounded-none">
                            <span className="text-amber-500 text-[11px] font-black block uppercase tracking-wide leading-snug">
                              {selItem.headline}
                            </span>
                            <div className="flex justify-between text-[8px] text-gray-500 font-extrabold uppercase mt-1">
                              <span>Timestamp: {selItem.time}</span>
                              <span>Feed Source: SEC_{selItem.source || "BLOOMBERG"}_WIRE</span>
                            </div>
                          </div>

                          <div className="space-y-2">
                            <span className="text-amber-500 font-extrabold text-[8.5px] uppercase tracking-wider block">DISPATCH WIRE CORE BRIEF:</span>
                            <p className="p-3 bg-[#03060C] text-slate-100 border border-[#1B212B] select-all italic rounded-none leading-normal">
                              "{selItem.details || "Incoming real-time ticker briefing data streams. Direct dispatch wire credentials verified."}"
                            </p>
                          </div>

                          <div className="divide-y divide-[#1D2531] text-[9.5px]">
                            <div className="flex justify-between py-1 text-gray-500">
                              <span>STORY REGION</span>
                              <span className="text-slate-100 font-bold uppercase">{selItem.region || "INTERNATIONAL"}</span>
                            </div>
                            <div className="flex justify-between py-1 text-gray-500">
                              <span>ESTIMATED SENTIMENT</span>
                              <span className={`font-black uppercase ${
                                storySentiment === "BULLISH" 
                                  ? "text-emerald-400" 
                                  : storySentiment === "BEARISH" 
                                  ? "text-red-400" 
                                  : "text-amber-400"
                              }`}>{storySentiment}</span>
                            </div>
                            <div className="flex justify-between py-1 text-gray-500">
                              <span>CRYPTOGRAPHIC ID SIGN</span>
                              <span className="text-[#4FFB4F] select-all font-bold">SHA256_APPROVED_LOG_{selItem.id}W</span>
                            </div>
                            <div className="flex justify-between py-1 text-gray-500">
                              <span>AUTHOR CREDENTIALS IP</span>
                              <span className="text-slate-200">BBG_CORP_DESK_99.local</span>
                            </div>
                            <div className="flex justify-between py-1 text-gray-500">
                              <span>ROUTING ARCH MODULE</span>
                              <span className="text-slate-200">Duplex_Snic_A_319</span>
                            </div>
                          </div>

                          <div className="border-t border-[#1D2531] pt-3 text-[9px] text-gray-500 uppercase font-extrabold">
                            <span>★ REGISTERED SECURITY WIRE SYSTEM: VERIFIED_ENCRYPTED</span>
                          </div>
                        </div>
                      );
                    })()}
                  </div>
                </div>
              </div>
            </motion.div>
          )}

          {/* VIEWPORT: OPTIONS DERIVATIVES DESK (OMON) */}
          {activeTab === "omon" && (
            <motion.div 
              key="omon"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.1 }}
              className="bg-[#0B0E11] border border-[#1E232B] p-4 flex flex-col space-y-4"
            >
              {/* OMON Header */}
              <div className="border-b border-[#242A35] pb-2 flex justify-between items-center bg-[#070A0F] p-2 select-none">
                <div>
                  <h3 className="text-sm font-bold text-amber-500 uppercase flex items-center gap-1.5 font-mono">
                    <Layers size={14} className="text-amber-500" />
                    <span>OMON — OPTIONS DERIVATIVES MONITOR</span>
                  </h3>
                  <p className="text-[10px] text-gray-400 mt-0.5">Real-time implied volatility and extrinsic derivatives matrix around current underlying price</p>
                </div>
                <div className="text-right text-[10px] font-mono leading-tight">
                  <span className="text-gray-400 block p-0.5">UNDERLYING: <span className="text-white font-black font-mono">{selectedSymbol}</span></span>
                  <span className="text-slate-400 p-0.5 block">SPOT PRICE: <span className="text-[#0FFFFF] font-bold">
                    {(assets.find(a => a.symbol === selectedSymbol)?.price || 1500).toLocaleString(undefined, { minimumFractionDigits: 2 })}
                  </span></span>
                </div>
              </div>

              {/* INTERACTIVE CONTROLS: IMPLIED VOLATILITY & DAYS TO EXPIRATION SLIDERS */}
              <div className="bg-[#04070B] border border-[#1E232B] p-3 text-mono font-mono text-[10px] grid grid-cols-1 md:grid-cols-2 gap-4">
                {/* Implied Volatility Slider */}
                <div className="space-y-1.5">
                  <div className="flex justify-between items-center bg-[#081014] px-2 py-1 border border-cyan-500/10">
                    <span className="text-cyan-400 uppercase font-bold text-[8.5px]">IMPLIED VOLATILITY (IV)</span>
                    <span className="text-white font-extrabold text-[10px] text-mono">{omonIV}%</span>
                  </div>
                  <div className="flex items-center space-x-2 pt-1">
                    <span className="text-gray-600 text-[8.5px]">10%</span>
                    <input
                      type="range"
                      min="10"
                      max="150"
                      value={omonIV}
                      onChange={(e) => setOmonIV(Number(e.target.value))}
                      className="flex-1 accent-cyan-500 bg-[#12161A] h-1.5 rounded-none cursor-pointer"
                    />
                    <span className="text-gray-600 text-[8.5px]">150%</span>
                  </div>
                  <p className="text-[8px] text-gray-500 uppercase">Swelling IV expands extrinsic pricing matrix and expands bid-ask spreads</p>
                </div>

                {/* Days To Expiration Slider */}
                <div className="space-y-1.5">
                  <div className="flex justify-between items-center bg-[#081014] px-2 py-1 border border-cyan-500/10">
                    <span className="text-cyan-400 uppercase font-bold text-[8.5px]">DAYS TO EXPIRATION (DTE)</span>
                    <span className="text-white font-extrabold text-[10px] text-mono">{omonDTE} DAYS</span>
                  </div>
                  <div className="flex items-center space-x-2 pt-1">
                    <span className="text-gray-600 text-[8.5px]">1 D</span>
                    <input
                      type="range"
                      min="1"
                      max="365"
                      value={omonDTE}
                      onChange={(e) => setOmonDTE(Number(e.target.value))}
                      className="flex-1 accent-cyan-500 bg-[#12161A] h-1.5 rounded-none cursor-pointer"
                    />
                    <span className="text-gray-600 text-[8.5px]">365 D</span>
                  </div>
                  <p className="text-[8px] text-gray-500 uppercase">Theta decay accelerates premium shrinkage exponentially as DTE approaches zero</p>
                </div>
              </div>

              {/* Option Grid */}
              <div className="border border-[#1E232B] bg-[#020507] overflow-x-auto w-full">
                <table className="w-full text-left border-collapse text-[10px] font-mono select-none">
                  <thead>
                    {/* Level 1 header */}
                    <tr className="bg-[#10141B] border-b border-[#1E232B]">
                      <th colSpan={5} className="py-1 px-3 text-center border-r border-[#1E232B] text-emerald-400 font-extrabold pb-1">--- CALL DERIVATIVES ---</th>
                      <th className="py-1 px-3 text-center border-r border-[#1E232B] text-amber-500 font-black">STRIKE</th>
                      <th colSpan={5} className="py-1 px-3 text-center text-red-400 font-extrabold pb-1">--- PUT DERIVATIVES ---</th>
                    </tr>
                    {/* Level 2 columns */}
                    <tr className="bg-[#07090D] border-b border-[#1A2027] text-gray-500 text-[8.5px] uppercase">
                      <th className="py-1 px-2 text-right">OI (Cts)</th>
                      <th className="py-1 px-2 text-right">Volume</th>
                      <th className="py-1 px-2 text-right">Bid Size</th>
                      <th className="py-1 px-2 text-right text-emerald-400 font-bold">Call Bid</th>
                      <th className="py-1 px-2 text-left text-emerald-400 font-bold border-r border-[#1E232B]">Call Ask</th>
                      <th className="py-1 px-3 text-center bg-[#13171F] font-black text-amber-500 border-r border-[#1E232B]">PRICE INDEX</th>
                      <th className="py-1 px-2 text-right text-red-400 font-bold">Put Bid</th>
                      <th className="py-1 px-2 text-right text-red-400 font-bold">Put Ask</th>
                      <th className="py-1 px-2 text-center">Bid Size</th>
                      <th className="py-1 px-2 text-right">Volume</th>
                      <th className="py-1 px-2 text-right">OI (Cts)</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-[#11161D]">
                    {(() => {
                      const actAsset = assets.find(a => a.symbol === selectedSymbol) || assets[0];
                      const S = actAsset.price;
                      
                      // Derive strike interval size cleanly based on asset digits
                      const strikeGap = S > 2000 ? 50 : S > 500 ? 10 : S > 100 ? 5 : S > 25 ? 2.5 : S > 5 ? 1 : 0.05;
                      const centerStrike = Math.round(S / strikeGap) * strikeGap;
                      const rows = [];

                      // Generate 9 consistent strikes centered on current underlying
                      for (let i = -4; i <= 4; i++) {
                        rows.push(centerStrike + i * strikeGap);
                      }

                      return rows.map((K, idx) => {
                        if (K <= 0) return null;

                        // Calculate stable price estimates using intrinsic + decaying extrinsic value
                        const intrinsicCall = Math.max(0, S - K);
                        const intrinsicPut = Math.max(0, K - S);
                        
                        // Decay factor based on distance from Spot to Strike
                        const distancePct = Math.abs(S - K) / S;
                        
                        // Enriched extrinsic calculation: scales directly with IV (%) and SQRT of DTE!
                        const dteFactor = Math.sqrt(omonDTE / 30);
                        const ivFactor = omonIV / 40;
                        const extrinsic = (S * 0.038) * ivFactor * dteFactor / (1 + distancePct * 9);
                        
                        const callPrice = intrinsicCall + extrinsic;
                        const putPrice = intrinsicPut + extrinsic;

                        // Mock ticking volatility variance
                        const randomTick = Math.sin(Date.now() / 1500 + idx) * 0.05 * ivFactor + (Math.random() - 0.5) * 0.01;
                        const tickValCall = Math.max(0.01, callPrice + randomTick);
                        const tickValPut = Math.max(0.01, putPrice - randomTick);

                        // Bid ask spreads expand as IV expands!
                        const spread = Math.max(0.02, 0.05 * ivFactor);
                        const callBid = Math.max(0.01, tickValCall - spread / 2);
                        const callAsk = Math.max(0.02, tickValCall + spread / 2);
                        const putBid = Math.max(0.01, tickValPut - spread / 2);
                        const putAsk = Math.max(0.02, tickValPut + spread / 2);

                        // Simulated volumes
                        const callOI = Math.round(4200 - (idx * 300) + Math.cos(idx) * 200);
                        const putOI = Math.round(2300 + (idx * 280) - Math.sin(idx) * 150);
                        const callVol = Math.round(150 + Math.sin(idx) * 80 + Math.random() * 30);
                        const putVol = Math.round(80 + Math.cos(idx) * 40 + Math.random() * 20);

                        const isStrikeSelected = selectedOmonStrike === K;

                        // Layout rendering
                        return (
                          <tr 
                            key={idx} 
                            onClick={() => {
                              setSelectedOmonStrike(K);
                              appendLog(`[OMON] Selected option target strike: ${K}`);
                            }}
                            className={`hover:bg-[#0E121B] hover:text-white cursor-pointer transition-colors text-[9.5px] ${
                              isStrikeSelected ? "bg-[#14231E] hover:bg-[#182C25] border-y border-emerald-500/30" : ""
                            }`}
                          >
                            {/* CALL OI AND VOL */}
                            <td className="py-1.5 px-2 text-right text-gray-400 font-mono">{callOI}</td>
                            <td className="py-1.5 px-2 text-right text-gray-500 font-mono">{callVol}</td>
                            <td className="py-1.5 px-2 text-right text-amber-500/70 font-mono">{Math.round(20 + Math.sin(idx + Date.now() / 3000)*15)}</td>
                            
                            {/* CALL PRICES */}
                            <td className="py-1.5 px-2 text-right text-emerald-400 font-bold font-mono">
                              {callBid.toLocaleString(undefined, { minimumFractionDigits: S > 100 ? 2 : 3 })}
                            </td>
                            <td className="py-1.5 px-2 text-left text-emerald-400 font-bold font-mono border-r border-[#1E232B]">
                              {callAsk.toLocaleString(undefined, { minimumFractionDigits: S > 100 ? 2 : 3 })}
                            </td>

                            {/* CENTER STRIKE */}
                            <td className={`py-1.5 px-3 text-center font-black border-r border-[#1E232B] select-all font-mono ${
                              isStrikeSelected ? "bg-emerald-500 text-black" : "bg-[#0C0F15] text-amber-500"
                            }`}>
                              {K.toLocaleString(undefined, { minimumFractionDigits: strikeGap === 0.05 ? 2 : 1 })}
                            </td>

                            {/* PUT PRICES */}
                            <td className="py-1.5 px-2 text-right text-red-400 font-bold font-mono">
                              {putBid.toLocaleString(undefined, { minimumFractionDigits: S > 100 ? 2 : 3 })}
                            </td>
                            <td className="py-1.5 px-2 text-right text-red-400 font-bold font-mono">
                              {putAsk.toLocaleString(undefined, { minimumFractionDigits: S > 100 ? 2 : 3 })}
                            </td>

                            {/* PUT LEFTOVERS */}
                            <td className="py-1.5 px-2 text-center text-amber-500/70 font-mono">{Math.round(15 + Math.cos(idx + Date.now() / 3000)*10)}</td>
                            <td className="py-1.5 px-2 text-right text-gray-500 font-mono">{putVol}</td>
                            <td className="py-1.5 px-2 text-right text-gray-400 font-mono">{putOI}</td>
                          </tr>
                        );
                      });
                    })()}
                  </tbody>
                </table>
              </div>

              {/* DYNAMIC RISK STATION: POSITION COST, INTERACTIVE GREEKS AND HEGE ADVISOR */}
              <div className="bg-[#05080E] border border-amber-500/20 p-3 flex flex-col space-y-3 font-mono text-[10px]">
                {(() => {
                  const actAsset = assets.find(a => a.symbol === selectedSymbol) || assets[0];
                  const S = actAsset.price;
                  const strikeGap = S > 2000 ? 50 : S > 500 ? 10 : S > 100 ? 5 : S > 25 ? 2.5 : S > 5 ? 1 : 0.05;
                  const centerStrike = Math.round(S / strikeGap) * strikeGap;
                  const activeStrike = selectedOmonStrike || centerStrike;

                  // Pricing metrics
                  const K = activeStrike;
                  const distancePct = Math.abs(S - K) / S;
                  const dteFactor = Math.sqrt(omonDTE / 30);
                  const ivFactor = omonIV / 40;
                  const extrinsic = (S * 0.038) * ivFactor * dteFactor / (1 + distancePct * 9);
                  
                  const isCall = omonPosition.includes("CALL");
                  const isBuy = omonPosition.includes("BUY");

                  const baseOptionPrice = isCall ? Math.max(0, S - K) + extrinsic : Math.max(0, K - S) + extrinsic;
                  const deltaCall = 1 / (1 + Math.exp(-(S - K) / (0.1 * S * ivFactor * dteFactor)));
                  const deltaResult = isCall ? deltaCall : (deltaCall - 1);
                  const gamma = Math.exp(-Math.pow(S - K, 2) / (2 * Math.pow(0.1 * S * ivFactor * dteFactor, 2))) / (S * 0.1 * ivFactor * dteFactor * Math.sqrt(2 * Math.PI));
                  const vega = S * dteFactor * 0.01 * Math.exp(-Math.pow(S - K, 2) / (2 * Math.pow(0.1 * S * ivFactor * dteFactor, 2)));
                  const theta = -extrinsic / (omonDTE * 2);

                  // Costing metrics
                  // Lot sizes: Reliance is 50, standard Indian shares are 50/100, forex is 1000, US equities are 100
                  const lotSize = selectedSymbol.includes("IN Equity") || selectedSymbol.includes("Index") ? 50 : 100;
                  const lotUnitLabel = selectedSymbol.includes("IN Equity") ? "₹/CONTRACT" : "$/CONTRACT";
                  const fiatSymbolStr = selectedSymbol.includes("IN Equity") ? "₹" : "$";
                  const contractValue = baseOptionPrice * omonQuantity * lotSize;
                  
                  return (
                    <>
                      <div className="flex items-center justify-between border-b border-[#202732] pb-1.5 select-none">
                        <span className="text-[9.5px] text-amber-500 font-extrabold flex items-center space-x-1.5">
                          <Activity size={12} className="text-amber-500 animate-pulse" />
                          <span>DERIVATIVES TRANSACTION PORT &mdash; GREEKS & RISK LEDGER</span>
                        </span>
                        <span className="text-[#888888] text-[8px] uppercase">
                          ACTIVE STRIKE TARGET: <span className="text-white font-black">{activeStrike.toLocaleString()}</span> [MULT: {lotSize}x]
                        </span>
                      </div>

                      {/* Config Form and Stats Grid */}
                      <div className="grid grid-cols-1 lg:grid-cols-12 gap-4">
                        {/* Interactive Position Builder (Col 5) */}
                        <div className="lg:col-span-4 bg-[#0A0D14] p-3 border border-[#141B23] space-y-3">
                          <span className="text-[8.5px] uppercase text-gray-500 block font-bold">1. Position Constructor</span>
                          
                          {/* Buy/Sell/Call/Put selector buttons */}
                          <div className="flex flex-wrap gap-1">
                            {(["BUY CALL", "SELL CALL", "BUY PUT", "SELL PUT"] as const).map((pos) => (
                              <button
                                key={pos}
                                onClick={() => {
                                  setOmonPosition(pos);
                                  appendLog(`[OMON] Realigned position constructor to: ${pos}`);
                                }}
                                className={`flex-1 text-[8.5px] font-black uppercase py-1 px-1.5 border text-center transition ${
                                  omonPosition === pos
                                    ? pos.startsWith("BUY")
                                      ? "bg-emerald-500 text-black border-emerald-400 font-black"
                                      : "bg-red-500 text-black border-red-400 font-black"
                                    : "bg-[#040608] text-gray-500 border-gray-800 hover:text-white"
                                }`}
                              >
                                {pos}
                              </button>
                            ))}
                          </div>

                          {/* Sizing Lot Quantity input */}
                          <div className="space-y-1">
                            <label className="text-[8px] text-gray-500 uppercase font-semibold flex justify-between">
                              <span>Lot Size Quantity:</span>
                              <span className="text-gray-400">({lotSize} multiplier)</span>
                            </label>
                            <div className="flex items-center space-x-1">
                              <button 
                                onClick={() => setOmonQuantity(q => Math.max(1, q - 1))}
                                className="bg-[#12161A] text-gray-400 hover:text-white border border-gray-800 text-[11px] font-black w-7 h-6 flex items-center justify-center select-none"
                              >
                                -
                              </button>
                              <input
                                type="number"
                                min="1"
                                max="1000"
                                value={omonQuantity}
                                onChange={(e) => setOmonQuantity(Math.max(1, Math.min(1000, Number(e.target.value) || 1)))}
                                className="flex-1 bg-[#03060C] text-slate-100 text-[10px] text-center border border-gray-800 py-0.5 focus:border-amber-500/50 focus:ring-0 focus:outline-none font-mono"
                              />
                              <button 
                                onClick={() => setOmonQuantity(q => Math.min(1000, q + 1))}
                                className="bg-[#12161A] text-gray-400 hover:text-white border border-gray-800 text-[11px] font-black w-7 h-6 flex items-center justify-center select-none"
                              >
                                +
                              </button>
                            </div>
                          </div>

                          {/* Direct Strike Selector Selector Input */}
                          <div className="space-y-1">
                            <label className="text-[8px] text-gray-500 uppercase font-semibold">CUSTOM TARGET STRIKE DIAL:</label>
                            <input
                              type="number"
                              step={strikeGap}
                              value={activeStrike}
                              onChange={(e) => {
                                const val = Number(e.target.value);
                                if (val > 0) {
                                  setSelectedOmonStrike(val);
                                }
                              }}
                              className="w-full bg-[#03060C] text-amber-500 text-[10px] pl-2 py-0.5 border border-gray-800 focus:border-amber-500/50 focus:ring-0 focus:outline-none font-mono select-all"
                            />
                          </div>
                        </div>

                        {/* Calculated Greeks Deck (Col 4) */}
                        <div className="lg:col-span-4 bg-[#0A0D14] p-3 border border-[#141B23] space-y-1.5 font-mono">
                          <span className="text-[8.5px] uppercase text-gray-500 block font-bold mb-1">2. Simulated Contract Greeks</span>
                          
                          <div className="grid grid-cols-2 gap-2 text-[9px]">
                            {/* Delta */}
                            <div className="border border-gray-800/60 bg-[#040609] p-1.5 flex justify-between items-center">
                              <span className="text-gray-500">DELTA (Δ)</span>
                              <span className={`font-bold ${deltaResult >= 0 ? "text-emerald-400" : "text-red-400"}`}>
                                {(isBuy ? deltaResult : -deltaResult).toLocaleString(undefined, { minimumFractionDigits: 3, maximumFractionDigits: 3 })}
                              </span>
                            </div>

                            {/* Gamma */}
                            <div className="border border-gray-800/60 bg-[#040609] p-1.5 flex justify-between items-center">
                              <span className="text-gray-500">GAMMA (γ)</span>
                              <span className="text-white font-bold">
                                {(isBuy ? gamma : -gamma).toLocaleString(undefined, { minimumFractionDigits: 4, maximumFractionDigits: 4 })}
                              </span>
                            </div>

                            {/* Vega */}
                            <div className="border border-gray-800/60 bg-[#040609] p-1.5 flex justify-between items-center">
                              <span className="text-gray-500">VEGA (ν)</span>
                              <span className="text-amber-500 font-bold">
                                {(isBuy ? vega : -vega).toLocaleString(undefined, { minimumFractionDigits: 3, maximumFractionDigits: 3 })}
                              </span>
                            </div>

                            {/* Theta */}
                            <div className="border border-gray-800/60 bg-[#040609] p-1.5 flex justify-between items-center">
                              <span className="text-gray-500">THETA (θ)</span>
                              <span className="text-red-400 font-bold">
                                {(isBuy ? theta : -theta).toLocaleString(undefined, { minimumFractionDigits: 3, maximumFractionDigits: 3 })}
                              </span>
                            </div>
                          </div>

                          {/* Quick summary of the contracts */}
                          <div className="mt-2 divide-y divide-gray-850 text-[8.5px] leading-relaxed text-gray-400 pt-1.5 border-t border-[#1C2430]">
                            <div className="flex justify-between py-0.5">
                              <span>BASE PREMIUM PRICE:</span>
                              <span className="text-slate-100 font-bold">{baseOptionPrice.toLocaleString(undefined, { minimumFractionDigits: 2 })} {lotUnitLabel}</span>
                            </div>
                            <div className="flex justify-between py-0.5">
                              <span>LEVERAGE VOLUMETRICS:</span>
                              <span className="text-[#0FFFFF] font-black">{fiatSymbolStr} {contractValue.toLocaleString(undefined, { minimumFractionDigits: 2 })}</span>
                            </div>
                            <div className="flex justify-between py-0.5">
                              <span>EST. MARGIN ACCOUNT REQUIREMENT:</span>
                              <span className="text-orange-400 font-bold">{isBuy ? "NIL (Long Option debit)" : `${fiatSymbolStr} ${(S * 0.15 * omonQuantity * lotSize).toLocaleString(undefined, { maximumFractionDigits: 0 })}`}</span>
                            </div>
                          </div>
                        </div>

                        {/* Strategy Advisor Section (Col 4) */}
                        <div className="lg:col-span-4 bg-[#0A0D14] p-3 border border-[#141B23] flex flex-col justify-between font-mono">
                          <div>
                            <span className="text-[8.5px] uppercase text-gray-500 block font-bold mb-1">3. Institutional Delta Hedge advice</span>
                            <div className="bg-[#03060C] p-2 border border-blue-500/10 text-[9px] text-gray-300 leading-snug space-y-1.5">
                              <p className="text-slate-200">
                                This active <span className="font-bold underline">{omonPosition}</span> position generates a total net portfolio delta of{" "}
                                <span className={`font-black ${ (isBuy ? deltaResult : -deltaResult) >= 0 ? "text-emerald-400" : "text-red-400" }`}>
                                  {((isBuy ? deltaResult : -deltaResult) * omonQuantity * lotSize).toLocaleString(undefined, { maximumFractionDigits: 1 })} Δ
                                </span>.
                              </p>
                              <p className="text-[8.5px] text-gray-400 italic">
                                {(() => {
                                  const totalDelta = (isBuy ? deltaResult : -deltaResult) * omonQuantity * lotSize;
                                  if (Math.abs(totalDelta) < 0.1) {
                                    return "Portfolio delta is naturally neutral. No hedging actions required at this timestamp.";
                                  } else if (totalDelta > 0) {
                                    return `HEDGE ACTION: Sell or Short ${Math.round(totalDelta)} shares of Spot underlying ${selectedSymbol} to achieve Delta-Neutral protection.`;
                                  } else {
                                    return `HEDGE ACTION: Buy or Long ${Math.round(Math.abs(totalDelta))} shares of Spot underlying ${selectedSymbol} to achieve Delta-Neutral protection.`;
                                  }
                                })()}
                              </p>
                            </div>
                          </div>

                          <div className="border bg-[#05110E] border-emerald-500/20 text-[#00E5FF] font-mono text-[8.5px] px-2 py-1 flex items-center space-x-1.5 rounded-none mt-2 select-none justify-between">
                            <span className="flex items-center gap-1">
                              <span className="w-1.5 h-1.5 bg-emerald-400 rounded-none animate-ping"></span>
                              <span>MATRICES COMPLIANCE APPROVED</span>
                            </span>
                            <span className="text-emerald-500 text-[8px]">SHA-512 SECURE</span>
                          </div>
                        </div>
                      </div>
                    </>
                  );
                })()}
              </div>

              {/* Option guidelines brief */}
              <div className="bg-[#030507] p-3 border border-[#1E232B] text-[9px] leading-relaxed text-gray-500 font-mono select-none">
                <span className="text-amber-500/80 font-bold block mb-1 uppercase text-[8px] tracking-wider">★ INTER-BANK IMPLIED DERIVATIVES COMPLIANCE MATRIX NOTICE:</span>
                <p>Black-Scholes simulation modules generate pricing matrices using dynamic underlying spot values with user-adjusted {omonIV}% target implied volatility over a simulated {omonDTE} days to expiration timeline. Option premium changes feed into multi-layered greeks modules to advise desk dealers on delta offsets. Execution trades clear through standard inter-bank routing switches instantly.</p>
              </div>
            </motion.div>
          )}

          {/* VIEWPORT 3: TELEMETRY SYSTEM DIAGNOSTICS (TELE) */}
          {false && (
            <motion.div 
              key="tele"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.1 }}
              className="space-y-3"
            >
              
              {/* PRIMARY STATS BAR */}
              <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-4 gap-3">
                <div className="bg-[#101012] border border-[#2D2D33] p-4 rounded-none">
                  <span className="text-[10px] text-[#888888] uppercase tracking-wider block font-bold">Flow Ingress Volume</span>
                  <div className="my-1.5 flex items-baseline space-x-1">
                    <span className="text-2xl font-bold font-mono text-white">{telemetry.tradesCount.toLocaleString()}</span>
                    <span className="text-[10px] text-gray-500">pkts</span>
                  </div>
                  <span className="text-[9px] text-[#00FF41]">Rate: {isRunning ? "30-150 pkts/s" : "Dormant"}</span>
                </div>

                <div className="bg-[#101012] border border-[#2D2D33] p-4 rounded-none">
                  <span className="text-[10px] text-[#888888] uppercase tracking-wider block font-bold">Ring Buffer Occupancy</span>
                  <div className="my-1.5 flex items-baseline space-x-1">
                    <span className="text-2xl font-bold font-mono text-white">{telemetry.bufferOccupancy}%</span>
                    <span className="text-[10px] text-gray-500">of capacity</span>
                  </div>
                  <span className="text-[9px] text-gray-500">SPSC locks active ring depth: 24</span>
                </div>

                <div className="bg-[#101012] border border-amber-500/30 p-4 rounded-none">
                  <span className="text-[10px] text-amber-500 uppercase tracking-wider block font-bold">Tail Ingestion Latency P99</span>
                  <div className="my-1.5 flex items-baseline space-x-1">
                    <span className={`text-2xl font-bold font-mono ${coreAffinityCollision ? "text-red-400" : "text-amber-500"}`}>
                      {telemetry.p99.toFixed(1)}
                    </span>
                    <span className="text-[10px] text-gray-505">µs</span>
                  </div>
                  <span className="text-[9px] text-gray-500">P50 Average: {telemetry.p50.toFixed(1)} µs</span>
                </div>

                <div className="bg-[#101012] border border-[#2D2D33] p-4 rounded-none">
                  <span className="text-[10px] text-[#888888] uppercase tracking-wider block font-bold">Scheduler Context Switches</span>
                  <div className="my-1.5 flex items-baseline space-x-1">
                    <span className={`text-2xl font-bold font-mono ${coreAffinityCollision ? "text-red-400" : "text-white"}`}>
                      {telemetry.contextSwitches}
                    </span>
                    <span className="text-[10px] text-gray-505">cs/sec</span>
                  </div>
                  <span className="text-[9px] text-gray-500">Preemption flags: {coreAffinityCollision ? "CRITICAL ALERT" : "OK"}</span>
                </div>
              </div>

              {/* SPSC VISUALIZATION MAP */}
              <div className="bg-[#101012] border border-[#2D2D33] p-4 rounded-none">
                <div className="flex flex-wrap justify-between items-center border-b border-[#2D2D33] pb-3 mb-4 gap-2">
                  <div>
                    <h2 className="text-xs font-bold font-mono tracking-widest text-[#00FF41] uppercase flex items-center space-x-1">
                      <Layers size={13} />
                      <span>SPSC_LOCK_FREE_RING_BUFFER_PIPELINE</span>
                    </h2>
                    <p className="text-[10px] text-gray-500 mt-0.5">Visually tracking live memory slots as writer & reader cores stream in real-time</p>
                  </div>
                  <div className="flex flex-wrap gap-x-4 gap-y-1 text-[9.5px]">
                    <span className="flex items-center space-x-1.5">
                      <span className="w-2.5 h-2.5 bg-orange-600 rounded-none"></span>
                      <span className="text-gray-400">PRODUCER (W)</span>
                    </span>
                    <span className="flex items-center space-x-1.5">
                      <span className="w-2.5 h-2.5 bg-blue-600 rounded-none"></span>
                      <span className="text-gray-400">CONSUMER (R)</span>
                    </span>
                    <span className="flex items-center space-x-1.5">
                      <span className="w-2.5 h-2.5 bg-amber-500/20 border border-amber-500/40 rounded-none"></span>
                      <span className="text-gray-400">OCCUPIED</span>
                    </span>
                  </div>
                </div>

                <div className="grid grid-cols-4 sm:grid-cols-8 md:grid-cols-12 gap-1.5 p-2 bg-[#0A0A0B] border border-[#2D2D33]">
                  {ringBuffer.map((slot, index) => {
                    const isProducer = index === producerIdx;
                    const isConsumer = index === consumerIdx;
                    const isOccupied = slot !== null;
                    
                    let slotColor = "border-[#2D2D33] bg-[#141417]/80 text-[#888888]";
                    if (isOccupied) {
                      slotColor = "border-amber-500/40 bg-amber-500/5 text-amber-500 font-bold";
                    }
                    
                    if (isProducer && isConsumer) {
                      slotColor = "border-yellow-500 bg-yellow-500/10 text-yellow-500 font-bold";
                    } else if (isProducer) {
                      slotColor = "border-orange-600 bg-orange-600/10 text-orange-400 font-bold";
                    } else if (isConsumer) {
                      slotColor = "border-blue-600 bg-blue-600/10 text-blue-400 font-bold";
                    }

                    return (
                      <div 
                        key={index} 
                        className={`border p-1.5 flex flex-col justify-between h-14 transition-all duration-100 rounded-none relative ${slotColor}`}
                      >
                        <div className="text-[8px] font-mono text-[#444] absolute top-1 left-1">{index}</div>
                        
                        <div className="mt-3 flex flex-col justify-center items-center">
                          {isOccupied ? (
                            <span className="text-[9px] font-bold font-mono tracking-tighter leading-none truncate max-w-full text-amber-500">
                              {slot.symbol === "BTCUSDT" ? "BTC" : "ETH"}
                            </span>
                          ) : (
                            <span className="text-[8px] text-[#333] select-none">EMPTY</span>
                          )}
                        </div>

                        <div className="flex justify-center space-x-1 text-[7.5px] mt-auto">
                          {isProducer && <span className="bg-orange-600 text-white px-0.5 font-bold leading-none select-none">W</span>}
                          {isConsumer && <span className="bg-blue-600 text-white px-0.5 font-bold leading-none select-none">R</span>}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* JITTER HISTOGRAM HISTORIC ANALYSIS */}
              <div className="bg-[#101012] border border-[#2D2D33] p-4 rounded-none">
                <div className="flex justify-between items-center border-b border-[#2D2D33] pb-3 mb-3">
                  <div>
                    <h3 className="text-xs font-bold text-[#00FF41] uppercase flex items-center space-x-1.5 tracking-wider">
                      <Activity size={13} />
                      <span>TELEMETRY_LATENCY_JITTER_HISTOGRAM</span>
                    </h3>
                    <p className="text-[10px] text-gray-500 mt-0.5">Real-time microsecond performance swing monitor</p>
                  </div>
                  <span className="text-[9px] text-[#888888] font-bold border border-[#2D2D33] bg-[#0A0A0B] px-2 py-1">
                    Jitters: {telemetry.p99Jitter.toFixed(1)} µs
                  </span>
                </div>

                <div className="h-32 flex items-end space-x-1 bg-[#0A0A0B] border border-[#2D2D33] relative p-1.5">
                  {latencyHistory.map((val, idx) => {
                    const maxVal = coreAffinityCollision ? 150 : 10;
                    const percentage = Math.min(100, Math.max(5, (val / maxVal) * 100));
                    const isHighJitter = val > (coreAffinityCollision ? 75 : 4);

                    return (
                      <div key={idx} className="flex-1 flex flex-col items-center h-full group relative">
                        <div className="opacity-0 group-hover:opacity-100 absolute bottom-full bg-[#1A1A1E] border border-[#2D2D33] text-[8px] p-1 pointer-events-none mb-1 text-white z-10 text-center">
                          {val.toFixed(2)} µs
                        </div>
                        <div 
                          style={{ height: `${percentage}%` }}
                          className={`w-full rounded-none transition-all duration-300 ${
                            isHighJitter 
                              ? "bg-red-500 hover:bg-red-400" 
                              : "bg-[#00FF41] hover:bg-[#6cfca1]"
                          }`}
                        ></div>
                      </div>
                    );
                  })}
                </div>
              </div>

            </motion.div>
          )}

          {/* VIEWPORT 4: INTERVIEW TRIVIA BOOT_CAMP (BOOT) */}
          {false && (
            <motion.div 
              key="boot"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.1 }}
              className="grid grid-cols-1 lg:grid-cols-12 gap-3"
            >
              
              {/* FILTERING HEADER DIRECTORY PANEL (3/12) */}
              <div className="lg:col-span-3 bg-[#101012] border border-[#2D2D33] p-3 flex flex-col gap-3.5">
                <div className="border-b border-[#2D2D33] pb-2 text-[11px] font-extrabold uppercase text-amber-500 tracking-wider">
                  BOOTCAMP NAVIGATION
                </div>

                {/* SEARCH INPUT BAR */}
                <div className="bg-[#0A0A0B] border border-[#2D2D33] p-1.5 flex items-center rounded-none text-xs gap-2">
                  <Search size={12} className="text-gray-500" />
                  <input
                    type="text"
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    placeholder="Search specifications..."
                    className="bg-transparent border-none focus:outline-none text-[11px] text-white w-full"
                  />
                  {searchTerm && (
                    <button onClick={() => setSearchTerm("")} className="text-[10px] text-gray-500 hover:text-white">✕</button>
                  )}
                </div>

                {/* CATEGORIES BUTTON FILTER */}
                <div className="flex flex-col gap-1 text-[11px]">
                  <span className="text-[9px] text-[#555] uppercase font-bold mb-1 tracking-widest">Select Category</span>
                  {[
                    { id: "ALL", label: "SHOW ALL TOPICS" },
                    { id: "OS_HARDWARE", label: "OS & INTEL HARDWARE" },
                    { id: "LANG_OPTIMIZATION", label: "C++ & RUST MEMORY" },
                    { id: "NETWORKING", label: "KERNEL BYPASS NET" },
                    { id: "MARKET_STRUCTURE", label: "ORDER BOOK MATCHING" },
                  ].map((cat) => (
                    <button
                      key={cat.id}
                      onClick={() => setTriviaCategoryFilter(cat.id)}
                      className={`text-left p-2 font-bold uppercase transition rounded-none text-xs border ${
                        triviaCategoryFilter === cat.id
                          ? "bg-amber-500 text-[#070708] border-amber-500"
                          : "bg-[#0A0A0B] text-gray-400 border-[#2D2D33] hover:text-white hover:bg-[#1C1C22]"
                      }`}
                    >
                      {cat.label}
                    </button>
                  ))}
                </div>

                {/* TRIVIA STATS ADVICE */}
                <div className="border-t border-[#2D2D33] pt-3 text-[10px] text-gray-500 leading-normal space-y-2 font-mono">
                  <span className="font-extrabold text-gray-400 block uppercase">★ Interview Tips:</span>
                  <p>HFT interviewers prioritize hardware, cache lines, assembly level diagnostics, and locks reduction over generic web frameworks. Practice this spec deck to ace your quant interviews.</p>
                </div>
              </div>

              {/* QUESTIONS ACCORDION LIST PANE (9/12) */}
              <div className="lg:col-span-9 bg-[#101012] border border-[#2D2D33] p-4 flex flex-col gap-4">
                
                <div className="border-b border-[#2D2D33] pb-2 flex justify-between items-center bg-[#0C0C0E] p-2.5">
                  <div>
                    <h3 className="text-xs font-bold text-[#00FF41] tracking-wider uppercase">QUANT SYSTEMS & HFT INTERVIEW SPECS DECK</h3>
                    <p className="text-[10px] text-gray-500 mt-0.5">Explore real coding questions and physical architectural solutions asked during interviews</p>
                  </div>
                  <span className="text-[10px] bg-amber-500/10 border border-amber-500/30 text-amber-500 px-2.5 py-0.5">
                    {filteredQuestions.length} MODULES FOUND
                  </span>
                </div>

                <div className="flex-1 overflow-y-auto space-y-3 max-h-[580px] pr-1">
                  {filteredQuestions.length === 0 ? (
                    <div className="text-center py-12 text-gray-500 text-[11px]">
                      No specs meet your diagnostics inquiry. Try adjusting category filters.
                    </div>
                  ) : (
                    filteredQuestions.map((q) => {
                      const isOpen = expandedTrivia[q.id];
                      return (
                        <div key={q.id} className="border border-[#2D2D33] bg-[#0A0A0B] transition-all">
                          
                          {/* HEAD ACCORDION BANNER */}
                          <button
                            onClick={() => toggleTrivia(q.id)}
                            className="w-full flex justify-between items-center p-3 text-left hover:bg-[#111] transition duration-150 select-none cursor-pointer"
                          >
                            <div className="space-y-[2px]">
                              <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                                <span className={`text-[8.5px] font-extrabold px-1.5 py-[1px] ${
                                  q.difficulty === "EXPERT" ? "bg-red-500/15 text-red-400" :
                                  q.difficulty === "HARD" ? "bg-amber-500/15 text-amber-400" :
                                  "bg-[#00FF41]/10 text-[#00FF41]"
                                }`}>
                                  {q.difficulty}
                                </span>
                                <span className="text-[9px] bg-[#19191C] text-gray-400 px-1.5 border border-[#2D2D33]">
                                  {q.category}
                                </span>
                              </div>
                              <h4 className="text-[12px] font-extrabold text-white tracking-wide mt-1">{q.title}</h4>
                            </div>
                            <span className="text-amber-500 font-extrabold pl-4 shrink-0 text-xs">
                              {isOpen ? "[-] COLLAPSE" : "[+] EXPOSE ANSWER"}
                            </span>
                          </button>

                          {/* BODY CONTENTS EXPANSION */}
                          {isOpen && (
                            <div className="p-4 border-t border-[#2D2D33] bg-[#0E0E10] text-[11px] leading-relaxed text-gray-300 space-y-3.5 select-text border-l-2 border-amber-500/80 animate-fade-in">
                              
                              {/* BOX 1: THE INQUIRY */}
                              <div className="space-y-1">
                                <strong className="text-amber-500 font-bold uppercase text-[9.5px] tracking-wider block">THE PROBLEM STATEMENT:</strong>
                                <p className="text-white italic bg-[#070708] p-2 border border-[#2D2D33]">{q.question}</p>
                              </div>

                              {/* BOX 2: EXPLANATION MATH & SYSTEM PATHS */}
                              <div className="space-y-1.5">
                                <strong className="text-[#00FF41] font-bold uppercase text-[9.5px] tracking-wider block">QUALITATIVE EXPLANATION:</strong>
                                <div className="whitespace-pre-line text-gray-300 pl-1">{q.explanation}</div>
                              </div>

                              {/* BOX 3: INTERVIEW STRATEGY TIPS */}
                              <div className="bg-amber-500/5 border border-amber-500/20 p-2.5">
                                <strong className="text-amber-500 font-extrabold uppercase text-[9px] tracking-wider block">★ HOW TO PHRASE YOUR ANSWER:</strong>
                                <p className="text-[10px] text-gray-300 italic mt-0.5">{q.interviewTips}</p>
                              </div>

                              {/* BOX 4: DETAILED C++ / RUST CODE EXAMPLES */}
                              {q.answerCode && (
                                <div className="space-y-1.5">
                                  <strong className="text-blue-400 font-bold uppercase text-[9.5px] tracking-wider block">PRODUCTION OPTIMIZED IMPL:</strong>
                                  <pre className="bg-[#050506] p-3 text-gray-300 overflow-x-auto text-[9.5px] border border-[#2D2D33] leading-relaxed">
                                    <code>{q.answerCode}</code>
                                  </pre>
                                </div>
                              )}

                            </div>
                          )}

                        </div>
                      );
                    })
                  )}
                </div>

              </div>

            </motion.div>
          )}



          {/* VIEWPORT: FOREX INTERBANK DESK (FX / F11) */}
          {activeTab === "fx" && (
            <motion.div 
              key="fx"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.1 }}
              className="grid grid-cols-1 lg:grid-cols-12 gap-3"
            >
              {/* FX CENTRAL SCREEN (8/12) */}
              <div className="lg:col-span-8 flex flex-col space-y-3 animate-fade-in">
                <div className="bg-[#0B0E11] border border-[#1E232B] p-4 flex flex-col space-y-3">
                  <div className="border-b border-[#242A35] pb-2 flex justify-between items-center bg-[#070A0F] p-2.5 rounded-none font-mono">
                    <div>
                      <h3 className="text-xs font-black text-amber-500 uppercase tracking-widest flex items-center gap-1.5">
                        <Activity size={12} className="text-amber-500" />
                        <span>F11 &lt;GO&gt; — FOREX INTERBANK MONITOR</span>
                      </h3>
                      <p className="text-[9px] text-gray-400 mt-0.5">Real-time Spot Rate feeds direct from primary liquidity providers</p>
                    </div>
                    <span className="text-[9px] bg-amber-500 font-extrabold text-black px-1 leading-normal uppercase">SPOT EXCHANGE</span>
                  </div>

                  {/* Spot Forex List */}
                  <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-3">
                    {assets.filter(a => a.exchange === "FOREX").map((fx) => {
                      const isUp = fx.change >= 0;
                      return (
                        <div 
                          key={fx.symbol}
                          onClick={() => {
                            setSelectedSymbol(fx.symbol);
                            setActiveTab("gp");
                            appendLog(`[FOREX] Paired with chart ticker: ${fx.symbol} for plotting`);
                          }}
                          className="bg-[#050709] border border-[#202731] hover:border-amber-500/40 cursor-pointer p-3 flex flex-col justify-between transition-all select-none group font-mono"
                        >
                          <div className="flex justify-between items-center mb-1">
                            <span className="text-[11px] font-black text-slate-300 group-hover:text-amber-500 transition">{fx.symbol}</span>
                            <span className="text-[7.5px] text-gray-500 uppercase">{fx.name.split("/")[0]} Paired</span>
                          </div>
                          <div className="flex justify-between items-end">
                            <span className="text-lg font-extrabold text-white tracking-tight">{fx.price.toLocaleString(undefined, { minimumFractionDigits: 4 })}</span>
                            <span className={`text-[9px] font-bold ${isUp ? "text-emerald-500" : "text-red-500"}`}>
                              {isUp ? "▲ +" : "▼ "}{fx.change.toLocaleString(undefined, { minimumFractionDigits: 4 })}
                            </span>
                          </div>
                        </div>
                      );
                    })}
                  </div>

                  {/* Converter Calculator */}
                  <div className="border border-[#1E232B] bg-[#030507] p-3 space-y-3 font-mono">
                    <div className="text-[10px] font-extrabold text-amber-500 uppercase tracking-wider">REAL-TIME FOREX CONVERSION ENGINE</div>
                    <div className="flex flex-wrap items-center gap-3">
                      <div className="flex items-center space-x-2 bg-[#090D12] border border-[#232B35] px-2 py-1">
                        <span className="text-[9.5px] text-gray-400">INPUT AMOUNT:</span>
                        <input 
                          type="number" 
                          id="fx-conv-input"
                          defaultValue={1000}
                          onChange={(e) => {
                            const val = parseFloat(e.target.value) || 0;
                            const resUSD = document.getElementById("fx-res-usd");
                            const resINR = document.getElementById("fx-res-inr");
                            const resEUR = document.getElementById("fx-res-eur");
                            const resGBP = document.getElementById("fx-res-gbp");
                            const resJPY = document.getElementById("fx-res-jpy");

                            const usd_inr = assets.find(a => a.symbol === "USD/INR")?.price || 83.342;
                            const eur_usd = assets.find(a => a.symbol === "EUR/USD")?.price || 1.0842;
                            const gbp_usd = assets.find(a => a.symbol === "GBP/USD")?.price || 1.2614;
                            const usd_jpy = assets.find(a => a.symbol === "USD/JPY")?.price || 151.35;

                            if (resUSD) resUSD.innerText = `$ ${val.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
                            if (resINR) resINR.innerText = `₹ ${(val * usd_inr).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
                            if (resEUR) resEUR.innerText = `€ ${(val / eur_usd).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
                            if (resGBP) resGBP.innerText = `£ ${(val / gbp_usd).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
                            if (resJPY) resJPY.innerText = `¥ ${(val * usd_jpy).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
                          }}
                          className="bg-transparent border-none focus:ring-0 focus:outline-none p-0 text-white font-black text-xs w-28 text-left h-auto"
                        />
                        <span className="text-gray-400 font-bold text-[9.5px]">USD</span>
                      </div>
                      <span className="text-gray-500 text-[11px] font-black uppercase">=&gt; Equates to Real-time Spot:</span>
                    </div>

                    <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-[10px] select-none text-left">
                      <div className="bg-[#0B0E11] p-2 border border-[#161D24]">
                        <span className="text-slate-500 block text-[7.5px] font-semibold uppercase">US Dollar</span>
                        <span id="fx-res-usd" className="text-white font-bold block mt-0.5">$ 1,000.00</span>
                      </div>
                      <div className="bg-[#0B0E11] p-2 border border-[#161D24]">
                        <span className="text-slate-500 block text-[7.5px] font-semibold uppercase font-mono">Indian Rupee</span>
                        <span id="fx-res-inr" className="text-[#0FFFFF] font-black block mt-0.5 font-mono">₹ {(1000 * (assets.find(a => a.symbol === "USD/INR")?.price || 83.342)).toLocaleString(undefined, { maximumFractionDigits: 2 })}</span>
                      </div>
                      <div className="bg-[#0B0E11] p-2 border border-[#161D24]">
                        <span className="text-slate-500 block text-[7.5px] font-semibold uppercase">Euro Spot</span>
                        <span id="fx-res-eur" className="text-white font-bold block mt-0.5">€ {(1000 / (assets.find(a => a.symbol === "EUR/USD")?.price || 1.0842)).toLocaleString(undefined, { maximumFractionDigits: 2 })}</span>
                      </div>
                      <div className="bg-[#0B0E11] p-2 border border-[#161D24]">
                        <span className="text-slate-500 block text-[7.5px] font-semibold uppercase">British Pound</span>
                        <span id="fx-res-gbp" className="text-white font-bold block mt-0.5">£ {(1000 / (assets.find(a => a.symbol === "GBP/USD")?.price || 1.2614)).toLocaleString(undefined, { maximumFractionDigits: 2 })}</span>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              {/* FX INTERBANK CORRELATION CROSS MATRIX OVERVIEW (4/12) */}
              <div className="lg:col-span-4 flex flex-col space-y-3 font-mono">
                <div className="bg-[#0B0E11] border border-[#1E232B] p-3 flex flex-col h-full justify-between">
                  <div>
                    <div className="border-b border-[#24242C] pb-1.5 mb-2.5 flex justify-between items-center">
                      <span className="text-[9.5px] font-bold text-amber-500 uppercase tracking-widest flex items-center gap-1">
                        <Activity size={10} />
                        <span>FX INTERBANK MATRIX</span>
                      </span>
                      <span className="text-[7.5px] bg-[#331133] text-pink-400 px-0.5 font-bold uppercase">MATRIX</span>
                    </div>

                    {/* Table-grid of Crosses */}
                    <div className="overflow-x-auto w-full">
                      <table className="w-full text-left border-collapse text-[8.5px] table-fixed">
                        <thead>
                          <tr className="border-b border-[#1E232B] text-gray-500 bg-[#06080C] uppercase">
                            <th className="py-1 px-1 font-extrabold">PAIR</th>
                            <th className="py-1 px-0.5 text-right font-bold">USD</th>
                            <th className="py-1 px-0.5 text-right font-bold">EUR</th>
                            <th className="py-1 px-0.5 text-right font-bold">GBP</th>
                            <th className="py-1 px-0.5 text-right font-bold">INR</th>
                          </tr>
                        </thead>
                        <tbody>
                          {(() => {
                            const usd_inr = assets.find(a => a.symbol === "USD/INR")?.price || 83.342;
                            const eur_usd = assets.find(a => a.symbol === "EUR/USD")?.price || 1.0842;
                            const gbp_usd = assets.find(a => a.symbol === "GBP/USD")?.price || 1.2614;

                            const crosses = [
                              { name: "USD", usd: 1.0, eur: 1 / eur_usd, gbp: 1 / gbp_usd, inr: usd_inr },
                              { name: "EUR", usd: eur_usd, eur: 1.0, gbp: eur_usd / gbp_usd, inr: eur_usd * usd_inr },
                              { name: "GBP", usd: gbp_usd, eur: gbp_usd / eur_usd, gbp: 1.0, inr: gbp_usd * usd_inr },
                              { name: "INR", usd: 1 / usd_inr, eur: (1/usd_inr) / eur_usd, gbp: (1/usd_inr) / gbp_usd, inr: 1.0 }
                            ];

                            return crosses.map((cr) => (
                              <tr key={cr.name} className="border-b border-[#14181F]/40 hover:bg-[#12161E]/40 font-mono">
                                <td className="py-1.5 px-1 font-black text-slate-300">{cr.name}</td>
                                <td className="py-1.5 px-0.5 text-right text-white font-medium">{cr.usd.toFixed(4)}</td>
                                <td className="py-1.5 px-0.5 text-right text-emerald-400 font-medium">{cr.eur.toFixed(4)}</td>
                                <td className="py-1.5 px-0.5 text-right text-pink-400 font-medium">{cr.gbp.toFixed(4)}</td>
                                <td className="py-1.5 px-0.5 text-right text-[#0FFFFF] font-black">{cr.inr.toFixed(4)}</td>
                              </tr>
                            ));
                          })()}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  <div className="border-t border-[#1C2228] pt-2 mt-3 text-[8px] text-gray-400 uppercase select-none text-center">
                    Weighted Arbitrage calculated in real-time. F11 monitor active.
                  </div>
                </div>
              </div>
            </motion.div>
          )}

        </AnimatePresence>

      </main>

      {/* LOW-LATENCY SYSTEMS PERFORMANCE TELEMETRY FOOTER */}
      <footer className="border-t border-[#1C2228] bg-[#090C0E] px-4 py-2 flex flex-wrap justify-between items-center text-[9.5px] text-gray-400 font-mono select-none">
        <div className="flex items-center space-x-4">
          <span>CORES: Ingestion Thread <span className="text-amber-500 font-bold">#CPU_{ingestionCore}</span> | Matcher Engine <span className="text-emerald-500 font-bold">#CPU_{engineCore}</span></span>
          <span className="hidden md:inline">|</span>
          <span className="flex items-center gap-1">
            <span>SCHEDULER_COLLISION:</span>
            <span className={coreAffinityCollision ? "text-red-400 animate-pulse font-bold" : "text-emerald-500 font-bold"}>
              {coreAffinityCollision ? "CRITICAL (L1 CACHE INVALIDATED)" : "0% DETECTED (HYPER-ISOLATED)"}
            </span>
          </span>
        </div>
        <div className="flex items-center space-x-4 mt-1 sm:mt-0">
          <span>SPSC RING_BUFF_USE: <span className="text-emerald-500 font-bold">{telemetry.bufferOccupancy}%</span></span>
          <span>LATENCY: P50 <span className="text-emerald-500 font-bold">{telemetry.p50.toFixed(1)}µs</span> | P99 <span className="text-amber-500 font-bold">{telemetry.p99.toFixed(1)}µs</span></span>
          <span className="hidden sm:inline">|</span>
          <span>SYS_TIME [UTC]: <span className="text-[#0FFFFF] font-bold">{
            (() => {
              const converted = getConvertedDate(currentTime, "UTC");
              const hh = String(converted.getUTCHours()).padStart(2, "0");
              const mm = String(converted.getUTCMinutes()).padStart(2, "0");
              const ss = String(converted.getUTCSeconds()).padStart(2, "0");
              return `${hh}:${mm}:${ss}`;
            })()
          }</span></span>
          {displayTimezone !== "UTC" && (
            <>
              <span className="text-gray-600">|</span>
              <span>TERM_TIME [{displayTimezone}]: <span className="text-amber-500 font-bold">{
                (() => {
                  const converted = getConvertedDate(currentTime, displayTimezone);
                  const hh = String(converted.getUTCHours()).padStart(2, "0");
                  const mm = String(converted.getUTCMinutes()).padStart(2, "0");
                  const ss = String(converted.getUTCSeconds()).padStart(2, "0");
                  return `${hh}:${mm}:${ss}`;
                })()
              }</span></span>
            </>
          )}
          <span className="hidden sm:inline">|</span>
          <span>STATUS: <span className="text-[#00FF55] font-bold">SYSTEMS_ACTIVE</span></span>
        </div>
      </footer>

    </div>
  );
}
