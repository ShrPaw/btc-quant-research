# Portfolio Description

Three versions for different contexts.

## Short Version (140 characters)

Python pipeline converting raw crypto trade data into clean research datasets with delta, CVD, volatility, and validation features.

## Medium Version

Python Market Data Pipeline that collects, cleans, aggregates, and structures high-volume crypto market data into validation-ready datasets. Demonstrates data engineering, feature construction, and research validation workflows.

## Long Version (Upwork/Portfolio)

Built a Python research pipeline to process raw crypto market data into structured datasets with features such as price, volume, delta, CVD, returns, trade count, volatility, and event labels.

The project demonstrates:
- Real-time WebSocket data collection from Binance Futures
- Data cleaning and quality assurance
- 1-second trade aggregation with CVD and VWAP
- 18 microstructure feature engineering (returns, volatility, CVD slopes, divergence, z-scores)
- Strict anti-leakage design (all features use prior data only)
- 6-check data integrity validation
- Statistical baseline tests (distribution, drift, correlation)
- Reproducible pipeline with saved winsorization bounds
- Professional documentation and data dictionary

Technologies: Python, pandas-style CSV workflows, WebSocket, REST APIs, matplotlib, data validation.

## Skills Tags

- Python
- Data Engineering
- Data Processing
- Data Cleaning
- Data Pipeline
- Feature Engineering
- Market Microstructure
- Validation
- Automation
- Research Documentation
