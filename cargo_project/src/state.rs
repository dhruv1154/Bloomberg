use chrono::{DateTime, FixedOffset, Timelike};

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
