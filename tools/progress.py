"""Small dependency-free progress reporting for streaming capture parsing."""
import sys
import threading
import time
from pathlib import Path


class ProgressFile:
    def __init__(self, path, label="parse", enabled=False):
        self.path = Path(path)
        self.file = self.path.open("rb")
        self.total = self.path.stat().st_size
        self.label = label
        self.enabled = enabled
        self.read_bytes = 0
        self.started = time.monotonic()
        self.last_report = 0.0
        self.lock = threading.Lock()

    def read(self, size=-1):
        data = self.file.read(size)
        self.read_bytes += len(data)
        self._report()
        return data

    def _report(self):
        if not self.enabled:
            return
        now = time.monotonic()
        if self.read_bytes < self.total and now - self.last_report < 0.5:
            return
        self.last_report = now
        elapsed = max(now - self.started, 1e-6)
        ratio = self.read_bytes / self.total if self.total else 1.0
        rate = self.read_bytes / elapsed
        eta = (self.total - self.read_bytes) / rate if rate > 0 else None
        eta_text = "--:--" if eta is None else f"{int(eta // 60):02d}:{int(eta % 60):02d}"
        with self.lock:
            print(f"\r[{self.label}] {ratio * 100:6.2f}% ETA {eta_text}", end="", file=sys.stderr, flush=True)
            if self.read_bytes >= self.total:
                print(file=sys.stderr, flush=True)

    def close(self):
        self.file.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __getattr__(self, name):
        return getattr(self.file, name)
