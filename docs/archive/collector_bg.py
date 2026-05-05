#!/usr/bin/env python3
"""
Background Binance Futures trade stream collector.
Writes every trade immediately to CSV (no buffer).
Runs until killed.
"""

import websocket
import json
import csv
import time
import os
import signal
import sys

WS_URL = "wss://fstream.binance.com/ws/btcusdt@aggTrade"
OUTPUT_DIR = "data/raw"
LOG_EVERY_N = 500

csv_file = None
csv_writer = None
total = 0
start_time = 0
output_path = ""


def get_output_path():
    ts = time.strftime("%Y%m%d_%H%M%S")
    return os.path.join(OUTPUT_DIR, f"trades_stream_{ts}.csv")


def on_message(ws, message):
    global total
    try:
        d = json.loads(message)
        ts_ms = d["T"]
        ts_utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts_ms / 1000)) + f".{ts_ms % 1000:03d}"
        side = "SELL" if d["m"] else "BUY"

        csv_writer.writerow([ts_ms, ts_utc, d["p"], d["q"], d["m"], side, d["a"]])
        total += 1

        if total % LOG_EVERY_N == 0:
            csv_file.flush()
            elapsed = time.time() - start_time
            rate = total / elapsed
            status = f"trades={total} elapsed={elapsed:.0f}s rate={rate:.1f}/s"
            with open(os.path.join(OUTPUT_DIR, "stream_status.txt"), "w") as f:
                f.write(status)
            print(status, flush=True)

    except Exception as e:
        print(f"parse error: {e}", flush=True)


def on_open(ws):
    global start_time
    start_time = time.time()
    print(f"Connected. Writing to {output_path}", flush=True)


def on_close(ws, code, msg):
    if csv_file:
        csv_file.flush()
        csv_file.close()
    print(f"Closed. total={total}", flush=True)


def cleanup(sig, frame):
    if csv_file:
        csv_file.flush()
        csv_file.close()
    sys.exit(0)


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = get_output_path()
    csv_file = open(output_path, "w", newline="")
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["timestamp_ms", "timestamp_utc", "price", "quantity",
                         "is_buyer_maker", "agggressor_side", "trade_id"])
    csv_file.flush()

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_close=on_close
    )
    ws.run_forever(ping_interval=30)
