# Portfolio Charts

These charts are generated from the pipeline's processed sample data. They show what the pipeline produces: clean, structured, visual outputs from raw trade data.

## How to Regenerate

```bash
pip install -r requirements.txt
python3 scripts/run_pipeline.py
python3 scripts/generate_portfolio_assets.py
```

## Charts

### Price Over Time
`price_over_time.png`

Shows the cleaned BTCUSDT price series after processing raw trade data into 1-second bars. The raw data arrives as individual trades — this chart shows the result after cleaning, sorting, and aggregating into a usable time series.

**What it demonstrates:** Ability to collect, clean, and structure raw market tick data.

### Volume Over Time
`volume_over_time.png`

Shows buy volume (green) vs sell volume (red) per second. Each bar is aggregated from multiple individual trades, classified by aggressor side.

**What it demonstrates:** Understanding of trade classification, volume aggregation, and market microstructure.

### Returns Over Time
`returns_over_time.png`

Shows 1-second log returns plotted over time. Returns are computed as log(price_t / price_t-1) — the standard way to measure price changes in quantitative finance.

**What it demonstrates:** Feature engineering skills, understanding of financial math, ability to compute derived metrics from raw data.

### Rolling Volatility
`rolling_volatility.png`

Shows realized volatility at 30-second and 60-second windows. Volatility is computed as the standard deviation of 1-second returns over a rolling prior-only window.

**What it demonstrates:** Rolling statistics, multi-scale feature engineering, anti-leakage design (all windows use prior data only).

### Cumulative Volume Delta (CVD)
`cvd_over_time.png`

Shows cumulative volume delta — the running sum of (buy volume − sell volume) per second. CVD tracks persistent buying or selling pressure over time.

**What it demonstrates:** Understanding of signed volume metrics, cumulative computations, market microstructure concepts.

### Feature Preview
`feature_preview.png`

Multi-panel overview showing 6 key engineered features: price, log returns, CVD, rolling volatility, volume imbalance, and trade intensity z-score.

**What it demonstrates:** End-to-end feature engineering pipeline — raw data in, structured analysis-ready features out.

## Using in Portfolio

**Upwork:** Add charts to your portfolio project gallery. Reference: "Python Market Data Pipeline — BTC Quant Research".

**GitHub README:** Charts are referenced in the main README.

**Resume / CV:** Include 2–3 best charts as proof of data engineering capability.

**LinkedIn:** Post a chart with a description of what it demonstrates about your Python/data skills.
