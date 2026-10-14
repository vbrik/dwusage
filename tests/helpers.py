"""Test helpers that use the code under test (`fakefs` must not, so it can be written first)."""

from concurrent.futures import ThreadPoolExecutor

from dwusage.capture import CaptureLimits, Capturer, CaptureResult
from fakefs import NOW, FakeFs


def capture(fs: FakeFs, roots: list[str], **limits: int) -> CaptureResult:
    """Run a capture of `fs` at time `NOW` with `CaptureLimits(**limits)`."""
    with ThreadPoolExecutor(4) as pool:
        return Capturer(fs, CaptureLimits(**limits), pool, now=lambda: NOW).run(roots)
