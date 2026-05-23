# Low Latency Bloomberg Style Market Terminal

A production-grade, highly optimized market monitoring and options analytic terminal inspired by the Bloomberg Terminal infrastructure. This application replicates standard inter-bank multi-desk workstation routines, displaying global feed ticker desks (NYSE, NSE, SSE, EUR/Asia), microsecond-latency telemetry, real-time FX valuation matrix, and the custom F6 Option Valuation calculator.

## Project Description for GitHub

A high-performance Bloomberg-inspired market data terminal featuring real-time regional desk trackers, dynamic timezone conversions (Session Time vs. Base Time), instant valuation overlays, microsecond telemetry analytics, and an interactive options pricing matrix.

## Key Features

### Regional Market Desks with Live Session States
- **Exchange Groupings**: Instant feeds tracking US (NYSE), Indian (NSE), East Asian (SSE/JST/CHN), and global FX/Commodity assets.
- **Market Session Overlay**: Real-time integration of regional operational hours. The terminal dynamically visualizes active sessions and applies a grayed-out/dimmed layout mask for exchanges that are currently closed.
- **Interactive Multi-Base Valuation**: Support for real-time portfolio conversion across multiple currency bases (USD, INR, EUR) from the control head.

### Dynamic Clock and Timezone Matrix
- **Base Time Header System**: Visual indicators tracking central clock offsets.
- **Session Time Toggle**: Dynamic switching of tabular timestamp lines to the asset local exchange's operational timezone (e.g., IST for NSE tickers, EST for NYSE tickers).
- **Absolute Telemetry Clock**: Highly styled system clock panel running in the active footer.

### Advanced Analytical Modules
- **F6 Option Engine**: Built-in pricing desk computing continuous options metrics, modeling implied volatilities, and running option Greek outputs under variable interest rates.
- **High-Frequency Telemetry**: Realistic ring buffer occupancy monitors, SPSC (Single Producer Single Consumer) lock-free pipeline graphs, and latency trackers showing P50 and P99 intervals.
- **Always-Focused Command Interface**: Keyboard routing engine designed to receive focus on command line arrays directly when typing key codes.

---

## Technical Architecture

The terminal architecture is built for rapid frame assembly and minimal garbage collection pauses:
- **Framework**: React with TypeScript for scalable UI rendering.
- **Style Compilation**: Tailwind CSS utilizing atomic modern design layouts.
- **Animation Module**: Frame-by-frame orchestration of tick walk updates using high-contrast color transitions.

---

## Getting Started

Follow these instructions to set up and run the terminal workspace on your local environment.

### Prerequisites
- Node.js (v18 or higher recommended)
- npm or yarn package manager

### Installation

1. Clone or download the repository into your local workspace.
2. In the project directory, run the initialization command dependencies:
   ```bash
   npm install
   ```

### Execution

To run the development server:
```bash
npm run dev
```
The terminal will mount locally and bind to your configured host, opening a live portal usually available at:
`http://localhost:3000`

### Build and Compilation

To create a static production release containing index frames and optimized javascript build assets inside the `./dist` folder:
```bash
npm run build
```

---

## File Structure

- `src/App.tsx`: Central application dashboard containing UI matrices, layout panels, and high-frequency analytical states.
- `src/main.tsx`: Standard project bootstrapping.
- `src/index.css`: Global styles, themes, and font overrides mapping.
- `package.json`: System dependencies and runner commands.
