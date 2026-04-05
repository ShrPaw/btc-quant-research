#!/usr/bin/env python3
"""
Binance Futures BTCUSDT Trade Stream Collector
Streams aggTrades via WebSocket, writes to CSV.
"""

import websocket
import json
import csv
import time
import signal
import sys
import os

# --- Config ---
SYMBOL = "btcusdt"
WS_URL = f"wss://fstream.binance.com/ws/{SYMBOL}@aggTrade"
OUTPUT_DIR = "data/raw"

# --- State ---
trades_buffer = []
total_trades = 0
csv_file = None
csv_writer = None
start_time = 0
ws = None

def get_output_path():
    ts = time.strftime("%Y%m%d_%H%M%S")
    return os.path.join(OUTPUT_DIR, f"trades_{ts}.csv")

def init_csv():
    global csv_file, csv_writer
    path = get_output_path()
    csv_file = open(path, 'w', newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([
        'timestamp_ms', 'timestamp_utc', 'price', 'quantity',
        'is_buyer_maker', 'agggressor_side', 'trade_id'
    ])
    csv_file.flush()
    return path

def flush_buffer():
    global trades_buffer, total_trades, csv_writer, csv_file
    if not trades_buffer:
        return
    for row in trades_buffer:
        csv_writer.writerow(row)
    csv_file.flush()
    total_trades += len(trades_buffer)
    trades_buffer.clear()

def on_message(ws, message):
    global trades_buffer
    try:
        data = json.loads(message)
        ts_ms = data['T']
        ts_utc = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts_ms / 1000)) + f".{ts_ms % 1000:03d}"
        price = data['p']
        qty = data['q']
        is_buyer_maker = data['m']
        agg_side = 'SELL' if is_buyer_maker else 'BUY'
        trade_id = data['a']

        trades_buffer.append([
            ts_ms, ts_utc, price, qty, is_buyer_maker, agg_side, trade_id
        ])

        if len(trades_buffer) >= 100:
            flush_buffer()
            elapsed = time.time() - start_time
            rate = total_trades / elapsed if elapsed > 0 else 0
            print(f"\r  Trades: {total_trades:,} | Rate: {rate:.0f}/s | Elapsed: {elapsed:.0f}s", end='', flush=True)
    except Exception as e:
        print(f"\n  [parse error] {e}")

def on_error(ws, error):
    print(f"\n  [ws error] {error}")

def on_close(ws, close_status_code, close_msg):
    flush_buffer()
    if csv_file and not csv_file.closed:
        csv_file.close()
    print(f"\n  [ws closed] total={total_trades:,}")

def on_open(ws):
    print("  Connected! Streaming trades...")

def signal_handler(sig, frame):
    global ws
    print(f"\n  Shutting down...")
    flush_buffer()
    if csv_file and not csv_file.closed:
        csv_file.close()
    if ws:
        ws.close()
    sys.exit(0)

if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = init_csv()
    print(f"  Output: {path}")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    start_time = time.time()

    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever(ping_interval=30)
