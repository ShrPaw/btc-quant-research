#!/usr/bin/env python3
"""
Robust Binance Futures Trade Collector (Sandbox-Compatible)
- Foreground only
- Flush every 50 trades OR 5 seconds
- Auto-reconnect on failure
- Heartbeat every 5s
"""

import websocket
import json
import csv
import time
import os
import sys
import threading

WS_URL = "wss://fstream.binance.com/ws/btcusdt@aggTrade"
OUTPUT_DIR = "data/raw"
MAX_BUFFER = 50
FLUSH_INTERVAL = 5  # seconds
HEARTBEAT_INTERVAL = 5
RECONNECT_DELAY = 3

running = True
csv_file = None
csv_writer = None
buffer = []
total_written = 0
output_path = ""
last_flush_time = 0
last_heartbeat_time = 0


def get_path():
    ts = time.strftime("%Y%m%d_%H%M%S")
    return os.path.join(OUTPUT_DIR, f"trades_{ts}.csv")


def open_csv():
    global csv_file, csv_writer, output_path
    output_path = get_path()
    csv_file = open(output_path, "w", newline="")
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["timestamp_ms", "timestamp_utc", "price", "quantity", "is_buyer_maker", "agggressor_side", "trade_id"])
    csv_file.flush()


def flush_buffer():
    global buffer, total_written, csv_file, csv_writer, last_flush_time
    if not buffer:
        last_flush_time = time.time()
        return
    for row in buffer:
        csv_writer.writerow(row)
    csv_file.flush()
    total_written += len(buffer)
    buffer = []
    last_flush_time = time.time()


def maybe_flush():
    global last_flush_time
    now = time.time()
    if len(buffer) >= MAX_BUFFER or (now - last_flush_time) >= FLUSH_INTERVAL:
        flush_buffer()


def heartbeat():
    global last_heartbeat_time
    now = time.time()
    if (now - last_heartbeat_time) >= HEARTBEAT_INTERVAL:
        rate = total_written / (now - start_time) if start_time > 0 else 0
        print(f"  [HB] {total_written:,} trades | {rate:.0f}/s | buf={len(buffer)} | {now - start_time:.0f}s", flush=True)
        last_heartbeat_time = now


def on_message(ws, message):
    global buffer
    try:
        d = json.loads(message)
        ts_ms = d["T"]
        ts_utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts_ms / 1000)) + f".{ts_ms % 1000:03d}"
        side = "SELL" if d["m"] else "BUY"
        buffer.append([ts_ms, ts_utc, d["p"], d["q"], d["m"], side, d["a"]])
        maybe_flush()
        heartbeat()
    except Exception as e:
        print(f"  [ERR] parse: {e}", flush=True)


def on_error(ws, error):
    print(f"  [ERR] ws: {error}", flush=True)


def on_close(ws, close_status_code, close_msg):
    flush_buffer()
    print(f"  [CLOSE] code={close_status_code} total={total_written}", flush=True)


def on_open(ws):
    global start_time
    start_time = time.time()
    print(f"  [OPEN] Connected. Writing to {output_path}", flush=True)


start_time = 0


def run_collector():
    global running, csv_file, start_time

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    open_csv()

    attempt = 0
    while running:
        attempt += 1
        print(f"  [CONN] Attempt {attempt}...", flush=True)
        start_time = time.time()

        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(ping_interval=20)

        if not running:
            break

        # Reconnect
        flush_buffer()
        print(f"  [RECONNECT] Waiting {RECONNECT_DELAY}s...", flush=True)
        time.sleep(RECONNECT_DELAY)

    flush_buffer()
    if csv_file and not csv_file.closed:
        csv_file.close()
    print(f"  [DONE] Total: {total_written:,} trades → {output_path}", flush=True)


if __name__ == "__main__":
    try:
        run_collector()
    except KeyboardInterrupt:
        running = False
        flush_buffer()
        if csv_file and not csv_file.closed:
            csv_file.close()
        print(f"\n  [EXIT] Ctrl+C. Total: {total_written:,} → {output_path}", flush=True)
