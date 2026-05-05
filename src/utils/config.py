"""
Configuration constants for the BTC quant research pipeline.
"""
import os

# ─── Exchange ─────────────────────────────────────────────────────────────────
SYMBOL = "BTCUSDT"
EXCHANGE = "Binance Futures (USDT-M)"
WS_URL = "wss://fstream.binance.com/ws/btcusdt@aggTrade"
REST_URL = "https://fapi.binance.com"
TRADES_ENDPOINT = f"{REST_URL}/fapi/v1/trades"
HISTORICAL_ENDPOINT = f"{REST_URL}/fapi/v1/historicalTrades"

# ─── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_RAW = os.path.join(PROJECT_ROOT, "data", "raw")
DATA_PROCESSED = os.path.join(PROJECT_ROOT, "data", "processed")
DATA_SAMPLE = os.path.join(PROJECT_ROOT, "data", "sample")
REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports")
ASSETS_DIR = os.path.join(PROJECT_ROOT, "assets")

# ─── Collection ───────────────────────────────────────────────────────────────
MAX_BUFFER = 50
FLUSH_INTERVAL = 5       # seconds
HEARTBEAT_INTERVAL = 5   # seconds
RECONNECT_DELAY = 3      # seconds
BATCH_DELAY = 2          # seconds between REST fetches
MAX_TRADES_PER_REQUEST = 1000

# ─── Feature Engineering ─────────────────────────────────────────────────────
ROLLING_WINDOWS = [5, 15, 30]        # seconds
PERCENTILES = [1, 5, 10, 90, 95, 99]
WINSOR_LOWER = 0.01                  # 1st percentile
WINSOR_UPPER = 0.99                  # 99th percentile

# ─── Validation ───────────────────────────────────────────────────────────────
FEATURE_STABILITY_SEGMENTS = 3       # A/B/C split
TEMPORAL_STABILITY_CHUNKS = 5
CORRELATION_REDUNDANCY_THRESHOLD = 0.9
FLATLINE_RUN_THRESHOLD = 10          # consecutive seconds with ≤1 trade
SPIKE_SIGMA_THRESHOLD = 5            # std devs above mean for spike detection
