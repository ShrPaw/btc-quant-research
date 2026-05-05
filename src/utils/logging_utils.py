"""
Logging utilities — consistent output formatting for pipeline stages.
"""
import sys
import time


class PipelineLogger:
    """Simple stage-based logger for pipeline output."""

    def __init__(self, name="pipeline"):
        self.name = name
        self.stage_start = None
        self.stage_num = 0

    def stage(self, msg):
        """Print a pipeline stage header."""
        print(f"\n  [{self.stage_num}] {msg}...")
        self.stage_start = time.time()
        sys.stdout.flush()

    def info(self, msg):
        """Print an info message."""
        print(f"      {msg}")
        sys.stdout.flush()

    def result(self, msg):
        """Print a result message."""
        elapsed = time.time() - self.stage_start if self.stage_start else 0
        print(f"      → {msg} ({elapsed:.1f}s)")
        sys.stdout.flush()

    def warn(self, msg):
        """Print a warning."""
        print(f"      ⚠ {msg}")
        sys.stdout.flush()

    def error(self, msg):
        """Print an error."""
        print(f"      ✗ {msg}")
        sys.stdout.flush()

    def success(self, msg):
        """Print a success message."""
        print(f"      ✓ {msg}")
        sys.stdout.flush()

    def header(self, title):
        """Print a section header."""
        print(f"\n{'='*60}")
        print(f"  {title}")
        print(f"{'='*60}")
        sys.stdout.flush()

    def set_stage(self, num):
        """Set current stage number."""
        self.stage_num = num


def print_banner(title):
    """Print a formatted banner."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")
