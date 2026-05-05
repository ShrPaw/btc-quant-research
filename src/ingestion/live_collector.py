"""
Live Binance Futures Trade Collector
WebSocket-based real-time trade stream with buffered writes and auto-reconnect.

Adapted from collector.py — foreground mode with heartbeat monitoring.
"""
import websocket
import json
import csv
import time
import os
import sys

from src.utils.config import WS_URL, MAX_BUFFER, FLUSH_INTERVAL, HEARTBEAT_INTERVAL, RECONNECT_DELAY
from src.utils.io import timestamp_filename


class LiveCollector:
    """Real-time trade collector with buffered CSV writes."""

    def __init__(self, output_dir=None):
        from src.utils.config import DATA_RAW
        self.output_dir = output_dir or DATA_RAW
        os.makedirs(self.output_dir, exist_ok=True)

        self.csv_file = None
        self.csv_writer = None
        self.buffer = []
        self.total_written = 0
        self.output_path = ""
        self.last_flush_time = 0
        self.last_heartbeat_time = 0
        self.start_time = 0
        self.running = True

    def _open_csv(self):
        self.output_path = os.path.join(self.output_dir, timestamp_filename("trades"))
        self.csv_file = open(self.output_path, "w", newline="")
        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow([
            "timestamp_ms", "timestamp_utc", "price", "quantity",
            "is_buyer_maker", "agggressor_side", "trade_id"
        ])
        self.csv_file.flush()

    def _flush_buffer(self):
        if not self.buffer:
            self.last_flush_time = time.time()
            return
        for row in self.buffer:
            self.csv_writer.writerow(row)
        self.csv_file.flush()
        self.total_written += len(self.buffer)
        self.buffer = []
        self.last_flush_time = time.time()

    def _maybe_flush(self):
        now = time.time()
        if len(self.buffer) >= MAX_BUFFER or (now - self.last_flush_time) >= FLUSH_INTERVAL:
            self._flush_buffer()

    def _heartbeat(self):
        now = time.time()
        if (now - self.last_heartbeat_time) >= HEARTBEAT_INTERVAL:
            elapsed = now - self.start_time
            rate = self.total_written / elapsed if elapsed > 0 else 0
            print(f"  [HB] {self.total_written:,} trades | {rate:.0f}/s | buf={len(self.buffer)} | {elapsed:.0f}s",
                  flush=True)
            self.last_heartbeat_time = now

    def _on_message(self, ws, message):
        try:
            d = json.loads(message)
            ts_ms = d["T"]
            ts_utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts_ms / 1000)) + f".{ts_ms % 1000:03d}"
            side = "SELL" if d["m"] else "BUY"
            self.buffer.append([ts_ms, ts_utc, d["p"], d["q"], d["m"], side, d["a"]])
            self._maybe_flush()
            self._heartbeat()
        except Exception as e:
            print(f"  [ERR] parse: {e}", flush=True)

    def _on_error(self, ws, error):
        print(f"  [ERR] ws: {error}", flush=True)

    def _on_close(self, ws, close_status_code, close_msg):
        self._flush_buffer()
        print(f"  [CLOSE] code={close_status_code} total={self.total_written}", flush=True)

    def _on_open(self, ws):
        self.start_time = time.time()
        print(f"  [OPEN] Connected. Writing to {self.output_path}", flush=True)

    def run(self):
        """Run the collector until interrupted."""
        self._open_csv()

        attempt = 0
        while self.running:
            attempt += 1
            print(f"  [CONN] Attempt {attempt}...", flush=True)
            self.start_time = time.time()

            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            ws.run_forever(ping_interval=20)

            if not self.running:
                break

            self._flush_buffer()
            print(f"  [RECONNECT] Waiting {RECONNECT_DELAY}s...", flush=True)
            time.sleep(RECONNECT_DELAY)

        self._flush_buffer()
        if self.csv_file and not self.csv_file.closed:
            self.csv_file.close()
        print(f"  [DONE] Total: {self.total_written:,} trades → {self.output_path}", flush=True)

    def stop(self):
        """Signal the collector to stop."""
        self.running = False
        self._flush_buffer()
        if self.csv_file and not self.csv_file.closed:
            self.csv_file.close()


if __name__ == "__main__":
    try:
        collector = LiveCollector()
        collector.run()
    except KeyboardInterrupt:
        print("\n  [EXIT] Ctrl+C detected.")
        collector.stop()
        print(f"  Total: {collector.total_written:,} → {collector.output_path}", flush=True)
