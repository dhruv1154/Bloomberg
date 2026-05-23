/**
 * Zero-copy low-latency market structures
 */

export interface IngestedTrade {
  id: string;
  symbol: string;
  price: number;
  quantity: number;
  isBuyerMaker: boolean;
  exchangeTime: number;
  localReceiptTime: number;
  processTime: number;
  queueIndex?: number;
  latencyUs?: number;
}

export interface TelemetryStats {
  tradesCount: number;
  bufferOccupancy: number; // circular queue fill %
  p50: number;             // average latency us
  p90: number;             // high-percentile latency us
  p99: number;             // extreme latency us
  p99_9?: number;          // ultra-tail latency
  p99Jitter: number;
  contextSwitches: number;
  droppedPackets: number;
  networkTransitMs: number;
}

export interface ProjectFile {
  name: string;
  path: string;
  language: string;
  content: string;
}

export interface OrderBookLevel {
  price: number;
  quantity: number;
  count: number;
  isMyOrder?: boolean;
}

export interface CustomOrder {
  id: string;
  price: number;
  quantity: number;
  remainingQuantity: number;
  side: "BUY" | "SELL";
  timestamp: number;
  status: "PENDING" | "FILLED" | "PARTIALLY_FILLED";
}

export interface TriviaQuestion {
  id: string;
  category: "OS_HARDWARE" | "LANG_OPTIMIZATION" | "NETWORKING" | "MARKET_STRUCTURE";
  title: string;
  difficulty: "EASY" | "MEDIUM" | "HARD" | "EXPERT";
  question: string;
  answerCode?: string;
  explanation: string;
  interviewTips: string;
}

export interface MarketAsset {
  symbol: string;
  name: string;
  exchange: "US" | "IND" | "EUR" | "CHN" | "JPN" | "FOREX" | "COMMODITY";
  price: number;
  openPrice: number;
  change: number;
  pctChange: number;
  high: number;
  low: number;
  history: number[];
  lastTickDir: "up" | "down" | "flat";
  lastTickTime: number;
  volume: number;
  obi?: number;                 // Order Book Imbalance
  vwap?: number;                // Volume-Weighted Average Price
  realizedVolatility?: number;  // Real-time Realized Volatility (%)
  sumPriceVolume?: number;      // Running sum of price * volume increment
  sumVolume?: number;           // Running sum of volume increments
}

