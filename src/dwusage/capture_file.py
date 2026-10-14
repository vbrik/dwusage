"""Capture file format: JSON Lines, gzip-compressed when the file name ends in ``.gz``.

Line 1 is a ``capture_start`` record, then one ``dir`` record per directory in `path_key`
order, then a ``capture_end`` record. Names that aren't valid UTF-8 are written as surrogate
escapes (``\\udcXX``), so `read_jsonl` returns the original names exactly; jq shows them
as U+FFFD.
"""

import dataclasses
import errno
import gzip
import json
import os
import sys
import tempfile
from collections.abc import Iterable, Iterator
from contextlib import nullcontext, suppress
from typing import Final

from dwusage.capture import CaptureLimits, CaptureResult, DirRecord

SCHEMA: Final = 1
"""Version of the record layout; bump on incompatible changes."""


def _dir_line(rec: DirRecord) -> dict:
    files = None
    if rec.files is not None:
        files = {
            "scanned": rec.files.scanned,
            "truncated": rec.files.truncated,
            "future": [
                {"name": e.name, "stat": dataclasses.asdict(e.stat)} for e in rec.files.future
            ],
        }
    return {
        "type": "dir",
        "path": rec.path,
        "root": rec.root,
        "depth": rec.depth,
        "reason": rec.reason,
        "captured_at": rec.captured_at,
        "stat": None if rec.stat is None else dataclasses.asdict(rec.stat),
        "xattrs": rec.xattrs,
        "subdirs": rec.subdirs,
        "skipped": rec.skipped,
        "files": files,
        "errors": rec.errors,
    }


def capture_lines(
    result: CaptureResult,
    *,
    roots: list[str],
    limits: CaptureLimits,
    host: str,
    version: str,
    openat2: bool,
) -> Iterator[dict]:
    """Yield the records of a capture file for `result`, in file order.

    `openat2` tells readers whether symlink protection covered every path component.
    """
    yield {
        "type": "capture_start",
        "schema": SCHEMA,
        "tool_version": version,
        "host": host,
        "roots": roots,
        "limits": dataclasses.asdict(limits),
        "openat2": openat2,
        "started_at": result.started_at,
    }
    for rec in result.records.values():
        yield _dir_line(rec)
    yield {
        "type": "capture_end",
        "elapsed_s": result.elapsed_s,
        "n_dirs": len(result.records),
        "n_errors": result.n_errors,
        "truncated_roots": result.truncated_roots,
        "chain_truncated_roots": result.chain_truncated_roots,
        "chain_ends": result.chain_ends,
    }


def _dumps(line: dict) -> str:
    # ensure_ascii (the default) escapes newlines and surrogates: one record, one ASCII line.
    return json.dumps(line, separators=(",", ":")) + "\n"


class Output:
    """Where a capture goes: a file, or stdout for "-".

    Create it before capturing, so a bad destination fails before the work starts. A file is
    written to a hidden temporary file (mode 0600, as captures reveal names and sizes of
    private directories) next to the destination and renamed into place by `write`. Leaving
    the context without a successful `write` removes the temporary file.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self._tmp: str | None = None
        self._fd: int | None = None
        if path == "-":
            return
        if os.path.isdir(path) or path.endswith("/"):
            raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), path)
        directory, name = os.path.split(os.path.abspath(path))
        # mkstemp uses O_EXCL and mode 0600; writing through its fd never follows a symlink.
        self._fd, self._tmp = tempfile.mkstemp(dir=directory, prefix=f".{name}.", suffix=".tmp")

    def __enter__(self) -> "Output":
        return self

    def __exit__(self, *exc_info: object) -> None:
        if self._fd is not None:
            os.close(self._fd)
        if self._tmp is not None:
            with suppress(FileNotFoundError):
                os.remove(self._tmp)

    def write(self, lines: Iterable[dict]) -> None:
        """Write `lines`; for a file, sync it and move it into place."""
        if self.path == "-" or self._fd is None:
            for line in lines:
                sys.stdout.write(_dumps(line))
            sys.stdout.flush()
            return
        fd, self._fd = self._fd, None
        with os.fdopen(fd, "wb") as raw:
            compress = gzip.GzipFile(fileobj=raw, mode="wb") if self.path.endswith(".gz") else None
            with compress or nullcontext(raw) as out:
                for line in lines:
                    out.write(_dumps(line).encode("ascii"))
            raw.flush()
            os.fsync(raw.fileno())
        os.replace(self._tmp, self.path)
        self._tmp = None


def read_jsonl(path: str) -> Iterator[dict]:
    """Yield the records of a JSON Lines file (gzip if `path` ends in ``.gz``)."""
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)
