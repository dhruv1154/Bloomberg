import React, { useState, useEffect } from "react";
import { OrderBookLevel, CustomOrder, IngestedTrade } from "../types";
import { Plus, Trash2, CheckCircle2, TrendingUp, TrendingDown, Layers, HelpCircle, Sparkles } from "lucide-react";

interface OrderBookProps {
  lastPrice: number;
  symbol: string;
  onLogMessage: (msg: string) => void;
  incomingTrade: IngestedTrade | null;
  vixLevel?: number; // Real-time regional volatility/fear metric
}

export const OrderBook: React.FC<OrderBookProps> = ({ lastPrice, symbol, onLogMessage, incomingTrade, vixLevel = 14.5 }) => {
  const [bids, setBids] = useState<OrderBookLevel[]>([]);
  const [asks, setAsks] = useState<OrderBookLevel[]>([]);
  
  // Dynamic Currency Helper
  const getCurrencySign = () => {
    if (symbol.includes("IN") || symbol.includes("NIFTY") || symbol.includes("SENSEX") || symbol.includes("RELIANCE") || symbol.includes("TCS") || symbol.includes("HDFCBANK") || symbol.includes("INFY")) {
      return "₹";
    }
    if (symbol.includes("EUR") || symbol.includes("DAX")) {
      return "€";
    }
    if (symbol.includes("JPY") || symbol.includes("N225")) {
      return "¥";
    }
    if (symbol.includes("GBP")) {
      return "£";
    }
    return "$";
  };
  const cSign = getCurrencySign();

  const [userOrders, setUserOrders] = useState<CustomOrder[]>([]);
  const [showLatencyAlert, setShowLatencyAlert] = useState<boolean>(false);
  const [lastLatencyValue, setLastLatencyValue] = useState<number | null>(null);
  const [recentLatencies, setRecentLatencies] = useState<number[]>(() =>
    Array(20).fill(0).map(() => 1.0 + Math.random() * 1.5)
  );

  const [midPriceHistory, setMidPriceHistory] = useState<{ timestamp: number; price: number }[]>([]);
  const [showPriceAlert, setShowPriceAlert] = useState<boolean>(false);

  // Compute P99 latency of recent 20 trade latencies
  const getP99 = (arr: number[]) => {
    if (arr.length === 0) return 0;
    const sorted = [...arr].sort((a, b) => a - b);
    const index = Math.min(sorted.length - 1, Math.floor(sorted.length * 0.99));
    return sorted[index];
  };

  const currentP99 = getP99(recentLatencies);
  const isP99AboveThreshold = currentP99 > 50;
  
  // Quick parameters input
  const [inputPrice, setInputPrice] = useState<string>("");
  const [inputQty, setInputQty] = useState<string>("0.5");
  const [orderSide, setOrderSide] = useState<"BUY" | "SELL">("BUY");

  // High-fidelity active slippage calculator states
  const [slippageSize, setSlippageSize] = useState<string>("15");
  const [slippageSide, setSlippageSide] = useState<"BUY" | "SELL">("BUY");

  // Latency pulse automatic expiration timer
  useEffect(() => {
    if (showLatencyAlert) {
      const timer = setTimeout(() => {
        setShowLatencyAlert(false);
      }, 700);
      return () => clearTimeout(timer);
    }
  }, [showLatencyAlert]);

  // Price threshold alert automatic expiration timer
  useEffect(() => {
    if (showPriceAlert) {
      const timer = setTimeout(() => {
        setShowPriceAlert(false);
      }, 1500);
      return () => clearTimeout(timer);
    }
  }, [showPriceAlert]);

  const spread = asks.length > 0 && bids.length > 0 ? asks[0].price - bids[0].price : 0;
  const midPrice = asks.length > 0 && bids.length > 0 ? (bids[0].price + asks[0].price) / 2 : lastPrice;

  // Track the rolling 2-second mid-price window to check for >0.5% shifts
  useEffect(() => {
    if (!midPrice) return;
    const now = Date.now();
    setMidPriceHistory((prev) => {
      const cutoff = now - 2000;
      const filtered = prev.filter(item => item.timestamp >= cutoff);
      const nextHistory = [...filtered, { timestamp: now, price: midPrice }];

      if (nextHistory.length > 1) {
        const prices = nextHistory.map(item => item.price);
        const minP = Math.min(...prices);
        const maxP = Math.max(...prices);

        if (minP > 0) {
          const percentChange = (maxP - minP) / minP;
          if (percentChange >= 0.005) {
            setShowPriceAlert(true);
            onLogMessage(
              `[VOLATILITY] HIGH_VOLATILITY TRIGGERED! Mid-price shifted ${(percentChange * 100).toFixed(2)}% in the rolling 2-second window (Mid: ${cSign}${midPrice.toFixed(2)})`
            );
          }
        }
      }
      return nextHistory;
    });
  }, [midPrice]);

  // Generate initial book around central price
  useEffect(() => {
    generateInitialBook(lastPrice);
    onLogMessage(`[ENGINE] Order Book initialized around spot price ${cSign}${lastPrice.toFixed(2)}`);
  }, [symbol]);

  // Trigger warning logs when VIX level changes or spikes
  useEffect(() => {
    if (!vixLevel || vixLevel <= 14.5) return;
    onLogMessage(
      `[MACRO BLEED] Preemption active: Volatility index at ${vixLevel.toFixed(2)} - Widening spreads by +${((vixLevel - 14.5) * 12).toFixed(1)}% & depleting depth by -${((1 - Math.max(0.25, 1 - (vixLevel - 14.5) * 0.06)) * 100).toFixed(1)}%`
    );
  }, [vixLevel]);

  // Handle incoming ticks to update depth and check user orders
  useEffect(() => {
    if (!incomingTrade) return;

    if (incomingTrade.latencyUs !== undefined) {
      setLastLatencyValue(incomingTrade.latencyUs);
      if (incomingTrade.latencyUs > 50) {
        setShowLatencyAlert(true);
      }
      setRecentLatencies((prev) => {
        const next = [...prev];
        if (next.length >= 20) {
          next.shift();
        }
        next.push(incomingTrade.latencyUs!);
        return next;
      });
    }

    const currentPrice = incomingTrade.price;
    const currentQty = incomingTrade.quantity;

    // Check custom orders for crosses/fills
    setUserOrders((prevOrders) => {
      let updated = false;
      const next = prevOrders.map((order) => {
        if (order.status !== "PENDING") return order;

        let isCrossed = false;
        if (order.side === "BUY" && currentPrice <= order.price) {
          isCrossed = true;
        } else if (order.side === "SELL" && currentPrice >= order.price) {
          isCrossed = true;
        }

        if (isCrossed) {
          updated = true;
          const fillQty = Math.min(order.remainingQuantity, currentQty);
          const remaining = order.remainingQuantity - fillQty;
          const newStatus = remaining === 0 ? "FILLED" : "PARTIALLY_FILLED";
          
          onLogMessage(
            `[MATCH] MATCHED Order ID ${order.id}! ${order.side} ${fillQty.toFixed(3)} ${symbol} @ ${cSign}${order.price.toFixed(2)} [Status: ${newStatus}]`
          );

          return {
            ...order,
            remainingQuantity: remaining,
            status: newStatus as any,
          };
        }
        return order;
      });

      return updated ? next : prevOrders;
    });

    // Dynamically shift order book levels near currentPrice
    updateBookDepth(currentPrice, incomingTrade.isBuyerMaker);

  }, [incomingTrade]);

  // Set default price when lastPrice moves significantly
  useEffect(() => {
    if (lastPrice && !inputPrice) {
      setInputPrice(lastPrice.toFixed(2));
    }
  }, [lastPrice]);

  const generateInitialBook = (centerPrice: number) => {
    const isBTC = symbol.includes("BTC");
    const tickSize = isBTC ? 0.5 : 0.05;
    
    // VIX Bleed Scaling
    const vixScale = vixLevel && vixLevel > 14.5 ? (1 + (vixLevel - 14.5) * 0.12) : 1.0;
    const vixDepthScale = vixLevel && vixLevel > 14.5 ? Math.max(0.25, 1 - (vixLevel - 14.5) * 0.06) : 1.0;

    const bidLevels: OrderBookLevel[] = [];
    const askLevels: OrderBookLevel[] = [];

    for (let i = 1; i <= 8; i++) {
      // Bids (Buy orders below centerpiece) with widened offsets from VIX
      const bidPrice = centerPrice - (i * tickSize * vixScale) - (Math.random() * tickSize * 0.2);
      bidLevels.push({
        price: parseFloat(bidPrice.toFixed(2)),
        quantity: parseFloat(((2 + Math.random() * 8) * vixDepthScale).toFixed(3)),
        count: Math.max(1, Math.floor((1 + Math.random() * 4) * vixDepthScale)),
      });

      // Asks (Sell orders above centerpiece) with widened offsets from VIX
      const askPrice = centerPrice + (i * tickSize * vixScale) + (Math.random() * tickSize * 0.2);
      askLevels.push({
        price: parseFloat(askPrice.toFixed(2)),
        quantity: parseFloat(((2 + Math.random() * 8) * vixDepthScale).toFixed(3)),
        count: Math.max(1, Math.floor((1 + Math.random() * 4) * vixDepthScale)),
      });
    }

    setBids(bidLevels.sort((a, b) => b.price - a.price));
    setAsks(askLevels.sort((a, b) => a.price - b.price));
  };

  const updateBookDepth = (tradePrice: number, isMaker: boolean) => {
    // Add trade quantity to active level or create perturbation
    const tickSize = symbol.includes("BTC") ? 0.5 : 0.05;
    
    if (isMaker) {
      // Sell driven trade -> hits bids
      setBids((prevBids) => {
        const next = [...prevBids];
        const target = next.find(b => Math.abs(b.price - tradePrice) < tickSize);
        if (target) {
          target.quantity = Math.max(0.1, target.quantity - Math.random() * 0.5);
        } else if (next.length > 0) {
          next[0].quantity = Math.max(0.2, next[0].quantity + (Math.random() - 0.4) * 0.1);
        }
        return next;
      });
    } else {
      // Buy driven trade -> hits asks
      setAsks((prevAsks) => {
        const next = [...prevAsks];
        const target = next.find(a => Math.abs(a.price - tradePrice) < tickSize);
        if (target) {
          target.quantity = Math.max(0.1, target.quantity - Math.random() * 0.5);
        } else if (next.length > 0) {
          next[0].quantity = Math.max(0.2, next[0].quantity + (Math.random() - 0.4) * 0.1);
        }
        return next;
      });
    }
  };

  const handlePlaceOrder = (e: React.FormEvent) => {
    e.preventDefault();
    const price = parseFloat(inputPrice);
    const qty = parseFloat(inputQty);

    if (isNaN(price) || price <= 0 || isNaN(qty) || qty <= 0) {
      alert("Invalid order specs! Price and Quantity must be positive floats.");
      return;
    }

    const newOrder: CustomOrder = {
      id: `USR-${Math.floor(1000 + Math.random() * 9000)}`,
      price,
      quantity: qty,
      remainingQuantity: qty,
      side: orderSide,
      timestamp: Date.now(),
      status: "PENDING",
    };

    setUserOrders(prev => [newOrder, ...prev]);
    onLogMessage(
      `[ORDER] Successfully added LIMIT ${orderSide} instruction for ${qty} ${symbol} @ ${cSign}${price.toFixed(2)} into Memory Index.`
    );

    // Insert user order visually into the book levels
    if (orderSide === "BUY") {
      setBids(prev => {
        const match = prev.find(b => Math.abs(b.price - price) < 0.01);
        if (match) {
          return prev.map(b => b.price === match.price ? { ...b, quantity: b.quantity + qty, count: b.count + 1, isMyOrder: true } : b);
        } else {
          return [...prev, { price, quantity: qty, count: 1, isMyOrder: true }].sort((a, b) => b.price - a.price);
        }
      });
    } else {
      setAsks(prev => {
        const match = prev.find(a => Math.abs(a.price - price) < 0.01);
        if (match) {
          return prev.map(a => a.price === match.price ? { ...a, quantity: a.quantity + qty, count: a.count + 1, isMyOrder: true } : a);
        } else {
          return [...prev, { price, quantity: qty, count: 1, isMyOrder: true }].sort((a, b) => a.price - b.price);
        }
      });
    }
  };

  const handleClearOrder = (orderId: string) => {
    setUserOrders(prev => {
      const target = prev.find(o => o.id === orderId);
      if (target) {
        onLogMessage(`[CANCEL] Cancelled LIMIT ${target.side} Order ID ${orderId}`);
      }
      return prev.filter(o => o.id !== orderId);
    });
  };

  // Dynamic Slippage HFT Calculations
  const getSlippageStats = () => {
    const size = parseFloat(slippageSize) || 0;
    if (size <= 0) return { avgPrice: 0, slippage: 0, impactPct: 0, totalCost: 0, matchedQty: 0, hasLeftover: false, leftoverQty: 0 };

    let remaining = size;
    let totalCost = 0;
    let matchedQty = 0;
    const targets = slippageSide === "BUY" ? asks : bids; // buy matches on sell asks, sell matches on buy bids

    for (const lvl of targets) {
      if (remaining <= 0) break;
      const take = Math.min(lvl.quantity, remaining);
      totalCost += take * lvl.price;
      matchedQty += take;
      remaining -= take;
    }

    if (matchedQty === 0) return { avgPrice: 0, slippage: 0, impactPct: 0, totalCost: 0, matchedQty: 0, hasLeftover: false, leftoverQty: size };

    const avgPrice = totalCost / matchedQty;
    const bestPrice = targets.length > 0 ? targets[0].price : avgPrice;
    const slippage = Math.abs(avgPrice - bestPrice);
    const impactPct = bestPrice > 0 ? (slippage / bestPrice) * 100 : 0;

    return {
      avgPrice,
      slippage,
      impactPct,
      totalCost,
      matchedQty,
      hasLeftover: remaining > 0,
      leftoverQty: remaining
    };
  };

  const slippageStats = getSlippageStats();

  const triggerSpoofing = (side: "BUY" | "SELL") => {
    if (side === "BUY") {
      setBids(prev => {
        if (prev.length === 0) return prev;
        const tick = symbol.includes("BTC") ? 0.5 : 0.05;
        const spoofPrice = parseFloat((prev[0].price + tick * 0.1).toFixed(2));
        onLogMessage(`[SPOOF] SENSORY SPOOF ORDER INJECTED: BUY 65.000 ${symbol} @ ${cSign}${spoofPrice.toFixed(2)} - Pulling sellers lower!`);
        return [
          { price: spoofPrice, quantity: 65, count: 1, isMyOrder: false },
          ...prev
        ].sort((a,b) => b.price - a.price);
      });
    } else {
      setAsks(prev => {
        if (prev.length === 0) return prev;
        const tick = symbol.includes("BTC") ? 0.5 : 0.05;
        const spoofPrice = parseFloat((prev[0].price - tick * 0.1).toFixed(2));
        onLogMessage(`[SPOOF] SENSORY SPOOF ORDER INJECTED: SELL 65.000 ${symbol} @ ${cSign}${spoofPrice.toFixed(2)} - Forcing bidders to back off!`);
        return [
          { price: spoofPrice, quantity: 65, count: 1, isMyOrder: false },
          ...prev
        ].sort((a,b) => a.price - b.price);
      });
    }
  };

  const injectLiquidityBurst = () => {
    setBids(prev => {
      onLogMessage(`[LIQUIDITY] INTENTIONAL MARKET MAKER BURST: Expanding Bid queues by +300% volume depth!`);
      return prev.map(b => ({ ...b, quantity: parseFloat((b.quantity * 2.8).toFixed(3)) }));
    });
    setAsks(prev => {
      onLogMessage(`[LIQUIDITY] INTENTIONAL MARKET MAKER BURST: Expanding Ask queues by +300% volume depth!`);
      return prev.map(a => ({ ...a, quantity: parseFloat((a.quantity * 2.8).toFixed(3)) }));
    });
  };

  const executeSimulatedBlockTrade = () => {
    const actAsset = bids.length > 0 && asks.length > 0 ? (bids[0].price + asks[0].price) / 2 : lastPrice;
    onLogMessage(`[BLOCK TRADE] INSTITUTIONAL CROSS-SETTLEMENT: Direct trade printed on desk. Size: 1,500 ${symbol} @ Avg ${cSign}${actAsset.toFixed(2)}`);
  };

  // Total volume sizes of visible stack
  const totalBidVolume = bids.reduce((acc, curr) => acc + curr.quantity, 0);
  const totalAskVolume = asks.reduce((acc, curr) => acc + curr.quantity, 0);

  return (
    <div className="bg-[#141417] border border-[#2D2D33] p-4 flex flex-col gap-4 font-mono text-xs rounded-none">
      
      {/* SECTION HEADER */}
      <div className="border-b border-[#2D2D33] pb-3 flex flex-wrap justify-between items-center gap-2">
        <div>
          <h3 className="text-xs font-bold tracking-widest text-[#00FF41] uppercase flex items-center col-gap-2 space-x-1">
            <Layers size={13} />
            <span>PRICE-TIME_PRIORITY_ORDER_BOOK_ENGINE</span>
          </h3>
          <p className="text-[10px] text-[#888888] mt-0.5">Interact with a real-time matching loop. Build professional insights on fills, priority lines, & queue cancellation.</p>
        </div>
        <div id="spread-box" className="bg-[#0A0A0B] border border-[#2D2D33] px-2.5 py-1 flex items-center space-x-2 text-[10px]">
          <span className="text-[#888888]">MID_PRICE:</span>
          <span className="text-[#00FF41] font-bold">{cSign}{midPrice.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
          <span className="text-[#888888] pl-2 border-l border-[#2D2D33]">SPREAD:</span>
          <span className="text-amber-500 font-bold">{cSign}{spread.toFixed(2)}</span>
        </div>
      </div>

      {/* HORIZONTAL SPARKLINE CHART FOR TRADE LATENCIES */}
      <div className="bg-[#0A0A0B] border border-[#2D2D33] p-2 flex flex-col sm:flex-row sm:items-center justify-between gap-3 text-[10px] rounded-none">
        <div className="flex flex-wrap items-center gap-2 font-bold uppercase tracking-wider">
          <span className="text-[#888888]">QUEUE_TRANSACTIONS:</span>
          <span className={isP99AboveThreshold ? "text-amber-500 animate-pulse" : "text-[#00FF41]"}>
            P99 Latency: {currentP99.toFixed(1)} µs
          </span>
          <span className="text-[#555]">|</span>
          <span className="text-gray-400">Window: 20 Ticks</span>
        </div>

        <div className="flex items-center space-x-3">
          {/* Sparkline track Container */}
          <div className="flex items-end space-x-[2px] h-6 w-[180px] bg-[#141417]/40 px-1 py-0.5 border border-[#2D2D33]/40">
            {recentLatencies.map((val, idx) => {
              // High scale of values
              const maxVal = Math.max(...recentLatencies, 5);
              const minVal = Math.min(...recentLatencies, 0);
              const heightRange = maxVal - minVal || 1;
              const heightPercent = Math.min(100, Math.max(12, ((val - minVal) / heightRange) * 100));

              // Sparkline transitions from green to amber if current P99 is above 50us
              const barColorClass = isP99AboveThreshold
                ? "bg-amber-500"
                : "bg-[#00FF41]";

              return (
                <div
                  key={idx}
                  style={{ height: `${heightPercent}%` }}
                  className={`w-[7px] shrink-0 transition-all duration-300 ${barColorClass} relative group cursor-help`}
                >
                  <div className="absolute bottom-full left-1/2 -translate-x-1/2 bg-[#1C1C22] border border-[#3D3D44] text-[8.5px] px-1 py-0.5 text-white whitespace-nowrap opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity duration-150 mb-1 z-40 shadow-md">
                    {val.toFixed(1)} µs
                  </div>
                </div>
              );
            })}
          </div>

          {/* Quick status badge indicator */}
          <div className={`px-2 py-0.5 font-bold uppercase tracking-widest text-[9px] border ${
            isP99AboveThreshold
              ? "bg-amber-500/10 text-amber-500 border-amber-500/30 animate-pulse"
              : "bg-[#00FF41]/10 text-[#00FF41] border-[#00FF41]/20"
          }`}>
            {isP99AboveThreshold ? "DEGRADED" : "NOMINAL"}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-4">
        
        {/* LEFT COLUMN: ORDER DEPTH GRID (7/12) */}
        <div className="lg:col-span-7 flex flex-col space-y-2">
          
          {/* MICROSTRUCTURE LOGIC: ORDER BOOK IMBALANCE BAR (OBI) */}
          {(() => {
            const topBidQty = bids.length > 0 ? bids[0].quantity : 0;
            const topAskQty = asks.length > 0 ? asks[0].quantity : 0;
            const obi = (topBidQty + topAskQty) > 0 ? (topBidQty - topAskQty) / (topBidQty + topAskQty) : 0;

            return (
              <div className="bg-[#0A0A0B] border border-[#2D2D33] p-2 flex flex-col space-y-1.5 font-mono text-[9px] rounded-none select-none">
                <div className="flex justify-between items-center">
                  <span className="text-amber-500 font-bold uppercase tracking-wider">MICROSTRUCTURE IMBALANCE (OBI)</span>
                  <span className="text-gray-400 font-bold font-mono">OBI: <span className={obi >= 0 ? "text-[#00FF41]" : "text-red-500"}>{obi >= 0 ? "+" : ""}{obi.toFixed(3)}</span></span>
                </div>
                <div className="w-full h-2.5 bg-[#020507] border border-[#23252B] relative flex items-center">
                  {/* Center partition indicator */}
                  <span className="absolute left-1/2 top-0 bottom-0 w-[1px] bg-amber-500/25 z-20"></span>
                  {/* Pressure area fill */}
                  {obi >= 0 ? (
                    <div 
                      style={{ left: "50%", width: `${Math.min(50, obi * 50)}%` }}
                      className="absolute h-full bg-emerald-500/10 border-l border-emerald-400/25"
                    ></div>
                  ) : (
                    <div 
                      style={{ right: "50%", width: `${Math.min(50, Math.abs(obi) * 50)}%` }}
                      className="absolute h-full bg-red-500/10 border-r border-red-400/25"
                    ></div>
                  )}
                  {/* Sliding needle anchor */}
                  <div 
                    style={{ left: `${50 + (obi * 50)}%` }}
                    className="absolute w-1 h-3.5 -mt-[1px] bg-amber-500 border border-black z-30 transform -translate-x-1/2"
                  ></div>
                </div>
                <div className="flex justify-between text-[7px] text-gray-500 font-semibold uppercase">
                  <span>SELL-SIDE PRESSURE</span>
                  <span>BALANCED BOOK</span>
                  <span>BUY-SIDE PRESSURE</span>
                </div>
              </div>
            );
          })()}
          
          <div className={`grid grid-cols-2 gap-2 text-[9.5px] uppercase font-bold tracking-wider px-2 py-1 bg-[#0A0A0B] border transition-all duration-300 ${
            showPriceAlert
              ? "border-amber-500 text-amber-500"
              : showLatencyAlert 
                ? "border-red-500 text-red-500" 
                : "border-[#2D2D33] text-[#888888]"
          }`}>
            <span>SELL ASKS (ASK VOL: {totalAskVolume.toFixed(2)})</span>
            <div className="text-right flex items-center justify-end gap-1.5 font-bold">
              {showPriceAlert && (
                <span className="bg-amber-500/10 text-amber-500 border border-amber-500/30 px-1 py-0.5 text-[8.5px] animate-pulse whitespace-nowrap font-mono">
                  ⚠️ VOLATILITY SPIKE (0.5% MOVE)
                </span>
              )}
              {showLatencyAlert && !showPriceAlert && (
                <span className="bg-red-500/10 text-red-500 border border-red-500/30 px-1 py-0.5 text-[8.5px] animate-pulse whitespace-nowrap font-mono">
                  ⚠️ PREEMPTION SPIKE: {lastLatencyValue?.toFixed(1)} µs
                </span>
              )}
              <span>BUY BIDS (BID VOL: {totalBidVolume.toFixed(2)})</span>
            </div>
          </div>

          <div className={`grid grid-cols-1 sm:grid-cols-2 gap-3 h-[280px] transition-all duration-300 ${
            showPriceAlert 
              ? "border-amber-500 bg-amber-950/15 animate-pulse"
              : showLatencyAlert 
                ? "border-red-500/60 bg-red-950/5 animate-pulse" 
                : "border-transparent"
          } p-1 border`}>
            {/* SELL SIDE (ASKS) */}
            <div className="bg-[#101012] border border-[#2D2D33] overflow-y-auto p-1.5 flex flex-col justify-end">
              <div className="space-y-[2px] w-full">
                {asks.slice().reverse().map((ask, idx) => {
                  // Percentage width for volume bar
                  const percent = Math.min(100, (ask.quantity / totalAskVolume) * 100);
                  return (
                    <div 
                      key={idx} 
                      className={`group relative flex justify-between py-1 px-1.5 transition duration-150 ${
                        ask.isMyOrder ? "bg-[#FF3B30]/10 border-r-2 border-red-500" : "hover:bg-[#1C1C22]"
                      }`}
                    >
                      {/* Depth visual bar overlay (colored red for sell asks) */}
                      <div 
                        style={{ width: `${percent}%` }}
                        className="absolute right-0 top-0 bottom-0 bg-[#FF3B30]/5 pointer-events-none transition-all duration-300"
                      ></div>

                      <div className="z-10 flex items-center space-x-2 text-[10px]">
                        <span className="text-[#FF3B30] font-bold">{cSign}{ask.price.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                        {ask.isMyOrder && <span className="text-[7.5px] bg-[#FF3B30] text-white px-0.5 leading-none">MINE</span>}
                      </div>
                      <div className="z-10 text-right text-gray-400 font-bold text-[10px]">
                        <span>{ask.quantity.toFixed(3)}</span>
                        <span className="text-[8.5px] text-[#555] font-normal ml-1">({ask.count})</span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* BUY SIDE (BIDS) */}
            <div className="bg-[#101012] border border-[#2D2D33] overflow-y-auto p-1.5">
              <div className="space-y-[2px]">
                {bids.map((bid, idx) => {
                  const percent = Math.min(100, (bid.quantity / totalBidVolume) * 100);
                  return (
                    <div 
                      key={idx} 
                      className={`group relative flex justify-between py-1 px-1.5 transition duration-150 ${
                        bid.isMyOrder ? "bg-[#00FF41]/10 border-l-2 border-green-500" : "hover:bg-[#1C1C22]"
                      }`}
                    >
                      {/* Depth visual bar overlay (colored green for buy bids) */}
                      <div 
                        style={{ width: `${percent}%` }}
                        className="absolute left-0 top-0 bottom-0 bg-[#00FF41]/5 pointer-events-none transition-all duration-300"
                      ></div>

                      <div className="z-10 flex items-center space-x-2 text-[10px]">
                        <span className="text-[#00FF41] font-bold">{cSign}{bid.price.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                        {bid.isMyOrder && <span className="text-[7.5px] bg-[#00FF41]/20 border border-[#00FF41]/40 text-[#00FF41] px-0.5 leading-none">MINE</span>}
                      </div>
                      <div className="z-10 text-right text-gray-400 font-bold text-[10px]">
                        <span>{bid.quantity.toFixed(3)}</span>
                        <span className="text-[8.5px] text-[#555] font-normal ml-1">({bid.count})</span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>

          {/* CUMULATIVE MARKET DEPTH PROFILES (SVG AREA STAIRCASES) */}
          <div className="bg-[#0A0A0B] border border-[#2D2D33] p-2 flex flex-col space-y-1">
            <span className="text-[9px] font-bold text-amber-500 uppercase tracking-widest block select-none">HFT LIQUIDITY DEPTH CURVES — CUMULATIVE VOLUMES</span>
            
            {(() => {
              // Calculate cumulative curves
              const bidCum: { price: number; vol: number }[] = [];
              let runBid = 0;
              bids.forEach((b) => {
                runBid += b.quantity;
                bidCum.push({ price: b.price, vol: runBid });
              });

              const askCum: { price: number; vol: number }[] = [];
              let runAsk = 0;
              asks.forEach((a) => {
                runAsk += a.quantity;
                askCum.push({ price: a.price, vol: runAsk });
              });

              const totalMaxVolume = Math.max(runBid, runAsk, 10);

              // Render simple visual steps with an elegant SVG
              // Center is space. Left side is Bids (green), right side is Asks (red)
              const w = 400;
              const h = 55;

              // Build points for bids area: (0, h) -> step by step -> (w/2, h)
              let bidPoints = `0,${h}`;
              bidCum.forEach((pt, idx) => {
                const x = (w / 2) - ((idx + 1) / Math.max(1, bids.length)) * (w / 2 * 0.95);
                const y = h - (pt.vol / totalMaxVolume) * (h * 0.85);
                bidPoints += ` ${x},${y}`;
              });
              bidPoints += ` ${w / 2},${h}`;

              // Build points for asks area: (w/2, h) -> stepping to right -> (w, h)
              let askPoints = `${w / 2},${h}`;
              askCum.forEach((pt, idx) => {
                const x = (w / 2) + ((idx + 1) / Math.max(1, asks.length)) * (w / 2 * 0.95);
                const y = h - (pt.vol / totalMaxVolume) * (h * 0.85);
                askPoints += ` ${x},${y}`;
              });
              askPoints += ` ${w},${h}`;

              return (
                <div className="relative w-full h-[65px] bg-[#020507]">
                  {/* Grid background lines */}
                  <div className="absolute inset-0 flex justify-between pointer-events-none opacity-5">
                    <div className="border-r border-slate-500 w-1/4 h-full"></div>
                    <div className="border-r border-slate-500 w-1/4 h-full"></div>
                    <div className="border-r border-slate-500 w-1/4 h-full"></div>
                  </div>

                  <svg viewBox={`0 0 ${w} ${h}`} className="w-full h-full" preserveAspectRatio="none">
                    {/* Bids cumulative area */}
                    <polygon points={bidPoints} fill="rgba(16, 185, 129, 0.15)" stroke="#10b981" strokeWidth="1" />
                    
                    {/* Asks cumulative area */}
                    <polygon points={askPoints} fill="rgba(239, 68, 68, 0.15)" stroke="#ef4444" strokeWidth="1" />
                    
                    {/* Center separator representing mid price */}
                    <line x1={w/2} y1="0" x2={w/2} y2={h} stroke="#f59e0b" strokeWidth="1" strokeDasharray="2,2" />
                  </svg>
                  
                  {/* Labels overlays */}
                  <div className="absolute inset-x-2 top-0.5 flex justify-between items-center text-[7.5px] font-black pointer-events-none bg-transparent">
                    <span className="text-emerald-500 text-left uppercase">BUY ACC: {runBid.toFixed(2)} units</span>
                    <span className="text-amber-500 text-center uppercase bg-[#0B0E11] px-1 border border-[#1E232B]">{cSign}{midPrice.toFixed(2)}</span>
                    <span className="text-red-400 text-right uppercase">SELL ACC: {runAsk.toFixed(2)} units</span>
                  </div>
                </div>
              );
            })()}
          </div>
        </div>

        {/* RIGHT COLUMN: INTERACTIVE ORDER CREATOR (5/12) */}
        <div className="lg:col-span-5 flex flex-col space-y-3.5">
          
          <div className="bg-[#0A0A0B] p-3 border border-[#2D2D33]">
            <form onSubmit={handlePlaceOrder} className="space-y-3">
              
              <div className="flex justify-between items-center pb-1 border-b border-[#2D2D33]">
                <span className="text-[10px] uppercase font-bold text-[#888888] tracking-widest flex items-center gap-1">
                  <Sparkles size={11} className="text-[#00FF41]" />
                  <span>SIMULATE_LIMIT_ORDER</span>
                </span>
                <span className="text-[9px] text-[#888888]">O(1) Insertion Line</span>
              </div>

              {/* Side Selector */}
              <div className="grid grid-cols-2 gap-1.5">
                <button
                  type="button"
                  onClick={() => setOrderSide("BUY")}
                  className={`py-1 bg-transparent text-xs uppercase font-bold border transition rounded-none ${
                    orderSide === "BUY" 
                      ? "border-[#00FF41] text-[#00FF41] bg-[#00FF41]/5" 
                      : "border-[#2D2D33] text-[#888888] hover:text-white"
                  }`}
                >
                  LIMIT_BUY
                </button>
                <button
                  type="button"
                  onClick={() => setOrderSide("SELL")}
                  className={`py-1 bg-transparent text-xs uppercase font-bold border transition rounded-none ${
                    orderSide === "SELL" 
                      ? "border-[#FF3B30] text-[#FF3B30] bg-[#FF3B30]/5" 
                      : "border-[#2D2D33] text-[#888888] hover:text-white"
                  }`}
                >
                  LIMIT_SELL
                </button>
              </div>

              {/* Pricing & size */}
              <div className="grid grid-cols-2 gap-2 text-xs">
                <div>
                  <label className="block text-[9px] text-[#888888] uppercase mb-1">LIMIT PRICE ({cSign})</label>
                  <input
                    type="number"
                    step="any"
                    value={inputPrice}
                    onChange={(e) => setInputPrice(e.target.value)}
                    className="w-full bg-[#141417] border border-[#2D2D33] text-[#D1D1D1] placeholder-[#444] px-2 py-1.5 focus:outline-none focus:ring-1 focus:ring-[#00FF41] rounded-none font-bold"
                  />
                </div>
                <div>
                  <label className="block text-[9px] text-[#888888] uppercase mb-1">QUANTITY ({symbol.split("USDT")[0]})</label>
                  <input
                    type="number"
                    step="0.001"
                    value={inputQty}
                    onChange={(e) => setInputQty(e.target.value)}
                    className="w-full bg-[#141417] border border-[#2D2D33] text-[#D1D1D1] placeholder-[#444] px-2 py-1.5 focus:outline-none focus:ring-1 focus:ring-[#00FF41] rounded-none font-bold"
                  />
                </div>
              </div>

              {/* Execute Placement */}
              <button
                type="submit"
                className={`w-full py-2 cursor-pointer font-bold border transition text-xs tracking-widest ${
                  orderSide === "BUY" 
                    ? "bg-[#00FF41] border-[#00FF41] text-[#0A0A0B] hover:bg-[#00FF41]/85" 
                    : "bg-[#FF3B30] border-[#FF3B30] text-white hover:bg-[#FF3B30]/85"
                }`}
              >
                INSERT_ORDER_TO_BOOK
              </button>
            </form>
          </div>

          {/* HFT EXECUTION SLIPPAGE ESTIMATOR */}
          <div className="bg-[#0A0A0B] border border-[#2D2D33] p-3 space-y-3">
            <div className="border-b border-[#2D2D33] pb-1.5 flex justify-between items-center">
              <span className="text-[10px] uppercase font-bold text-amber-500 tracking-wider font-mono">HFT_EXECUTION_SLIPPAGE_ESTIMATOR</span>
              <span className="text-[8px] bg-amber-500/10 text-amber-500 px-1 border border-amber-500/30">REALTIME DISPATCH</span>
            </div>

            <div className="grid grid-cols-2 gap-2 text-xs">
              <div>
                <label className="block text-[8px] text-gray-500 uppercase mb-1">Simulated Sweep Side</label>
                <div className="grid grid-cols-2 gap-1 border border-[#2D2D33] p-[2px] bg-[#141417]">
                  <button
                    type="button"
                    onClick={() => setSlippageSide("BUY")}
                    className={`py-0.5 text-[9.5px] uppercase font-bold transition rounded-none ${
                      slippageSide === "BUY"
                        ? "bg-[#00FF41]/10 text-[#00FF41] border border-[#00FF41]/30"
                        : "text-gray-400 font-normal hover:text-white"
                    }`}
                  >
                    BUY ASK
                  </button>
                  <button
                    type="button"
                    onClick={() => setSlippageSide("SELL")}
                    className={`py-0.5 text-[9.5px] uppercase font-bold transition rounded-none ${
                      slippageSide === "SELL"
                        ? "bg-red-500/10 text-red-500 border border-red-500/30"
                        : "text-gray-400 font-normal hover:text-white"
                    }`}
                  >
                    SELL BID
                  </button>
                </div>
              </div>
              <div>
                <label className="block text-[8px] text-gray-500 uppercase mb-1">Order Sweep Size</label>
                <input
                  type="number"
                  step="0.01"
                  min="0.1"
                  value={slippageSize}
                  onChange={(e) => setSlippageSize(e.target.value)}
                  className="w-full bg-[#141417] border border-[#2D2D33] text-[#D1D1D1] px-2 py-[5px] focus:outline-none focus:ring-[1px] focus:ring-amber-500 rounded-none text-[11px] font-bold"
                />
              </div>
            </div>

            {/* Sweep Stats Display Grid */}
            <div className="bg-[#101012] border border-[#22252B] p-2 space-y-1.5 text-[10px]">
              <div className="flex justify-between">
                <span className="text-gray-500">Sweep Success / Filled:</span>
                <span className="font-bold text-white">
                  {slippageStats.matchedQty.toFixed(3)} / {slippageSize} units
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500">Est. Average Fill Price:</span>
                <span className="font-bold text-[#00FF41]">
                  {cSign}{slippageStats.avgPrice.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500">Execution Slippage Variance:</span>
                <span className="font-bold text-amber-500">
                  {cSign}{slippageStats.slippage.toFixed(4)}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500">Simulated Impact Cost %:</span>
                <span className={`font-bold ${slippageStats.impactPct > 0.05 ? "text-amber-500 animate-pulse" : "text-[#00FF41]"}`}>
                  {slippageStats.impactPct.toFixed(4)}%
                </span>
              </div>
              <div className="flex justify-between border-t border-[#2D2D33] pt-1.5 text-[9px]">
                <span className="text-gray-500 uppercase font-black">MATCHING RECONCILIATION:</span>
                {slippageStats.hasLeftover ? (
                  <span className="text-amber-400 font-extrabold uppercase animate-pulse">WARNING: LEFTOVER OUTSTANDING ({slippageStats.leftoverQty.toFixed(3)} Lacking Depth)</span>
                ) : (
                  <span className="text-[#00FF41] font-bold uppercase">100% DEPTH MATED (0% residual)</span>
                )}
              </div>
            </div>
          </div>

          {/* INSTITUTIONAL LIQUIDITY PROVIDER ACTIONS */}
          <div className="bg-[#0A0A0B] border border-[#2D2D33] p-3 space-y-2.5">
            <span className="text-[10px] uppercase font-bold text-[#80808a] tracking-wider block border-b border-[#2D2D33] pb-1.5">LIQUIDITY_PROVIDER_TELEPORT_ACTIONS</span>
            
            <div className="grid grid-cols-2 gap-2 text-[9.5px]">
              <button
                type="button"
                onClick={() => triggerSpoofing("BUY")}
                className="py-1.5 px-2 bg-[#00FF41]/5 hover:bg-[#00FF41]/10 text-[#00FF41] border border-[#00FF41]/20 rounded-none font-bold select-none text-left flex justify-between items-center transition"
              >
                <span>SPOOF BUY WALL</span>
                <span>+65.0</span>
              </button>
              <button
                type="button"
                onClick={() => triggerSpoofing("SELL")}
                className="py-1.5 px-2 bg-red-500/5 hover:bg-red-500/10 text-red-400 border border-red-500/20 rounded-none font-bold select-none text-left flex justify-between items-center transition"
              >
                <span>SPOOF SELL WALL</span>
                <span>-65.0</span>
              </button>
              <button
                type="button"
                onClick={injectLiquidityBurst}
                className="py-1.5 px-2 bg-amber-500/5 hover:bg-amber-500/10 text-amber-500 border border-amber-500/20 rounded-none font-bold select-none text-left flex justify-between items-center transition"
              >
                <span>BURST LIQUIDITY DEPTH</span>
                <span>+300%</span>
              </button>
              <button
                type="button"
                onClick={executeSimulatedBlockTrade}
                className="py-1.5 px-2 bg-purple-500/5 hover:bg-purple-500/10 text-purple-400 border border-purple-500/20 rounded-none font-bold select-none text-left flex justify-between items-center transition"
              >
                <span>CROSS DARK BLOCK</span>
                <span>1,500</span>
              </button>
            </div>
          </div>

          {/* MY ACTIVE PENDING ORDERS */}
          <div className="bg-[#141417] border border-[#2D2D33] p-2.5 flex flex-col h-[155px]">
            <div className="border-b border-[#2D2D33] pb-1.5 mb-1.5 flex justify-between items-center">
              <span className="text-[10px] uppercase font-bold text-[#80808a] tracking-wider">MY_LIMIT_QUEUE_STATUS</span>
              <span className="text-[8.5px] text-amber-500 font-bold uppercase">{userOrders.filter(o => o.status === "PENDING").length} PENDING</span>
            </div>

            <div className="flex-1 overflow-y-auto space-y-1">
              {userOrders.length === 0 ? (
                <div className="text-center text-[#555] py-7 text-[9px] font-normal leading-normal select-none">
                  No tracking orders. Input your limits above to watch real-time matching crossing thresholds.
                </div>
              ) : (
                <div className="space-y-1.5">
                  {userOrders.map((o) => (
                    <div 
                      key={o.id} 
                      className={`p-1.5 border flex justify-between items-center text-[9.5px] rounded-none ${
                        o.status === "FILLED" 
                          ? "bg-[#00FF41]/5 border-[#00FF41]/30" 
                          : o.status === "PARTIALLY_FILLED" 
                            ? "bg-amber-500/5 border-amber-500/30" 
                            : "bg-[#0A0A0B] border-[#2D2D33]"
                      }`}
                    >
                      <div className="space-y-[1px]">
                        <div className="flex items-center space-x-1">
                          <span className={`font-bold ${o.side === "BUY" ? "text-[#00FF41]" : "text-[#FF3B30]"}`}>{o.side}</span>
                          <span className="text-gray-400 font-bold">{o.id}</span>
                          <span className="text-[8px] bg-[#2D2D33] px-1 py-0.5 text-[#888888]">{o.status}</span>
                        </div>
                        <div className="text-[#888888] font-normal">
                          <span>{cSign}{o.price.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                          <span className="mx-1">•</span>
                          <span>Left: {o.remainingQuantity.toFixed(3)} / {o.quantity.toFixed(3)}</span>
                        </div>
                      </div>
                      
                      {o.status === "PENDING" && (
                        <button
                          onClick={() => handleClearOrder(o.id)}
                          className="p-1 hover:bg-[#FF3B30]/20 text-gray-500 hover:text-[#FF3B30] border border-transparent hover:border-[#FF3B30]/30 transition pb-1.5 cursor-pointer"
                          title="Instant cancellation"
                        >
                          <Trash2 size={11} />
                        </button>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

        </div>

      </div>

    </div>
  );
};
