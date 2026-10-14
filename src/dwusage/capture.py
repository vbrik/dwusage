"""Capture raw CephFS state for offline replay and diagnostics.

A capture walks breadth-first from each root. Every directory reached is *visited*: its
stat and all CephFS xattrs are recorded as raw values. Directories fewer than ``depth``
levels below their root are also *listed*: their subdirectory names are recorded and those
subdirectories visited.

Limits apply per root, so one big root can't starve the others. Per-directory failures are
recorded in `DirRecord.errors` and never stop the capture.
"""

import errno
import itertools
import os
import time
from collections import Counter
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import StrEnum
from operator import attrgetter
from typing import Final, NamedTuple

from dwusage import xattrs
from dwusage.fs.base import RawFs, RawStat, describe_oserror
from dwusage.paths import is_below, path_key

MAX_DIRS: Final = 100_000
"""Default visited dirs per root: roughly 150 MB of memory and a minute or two on CephFS."""

MAX_LIST_ENTRIES: Final = 1_000_000
"""Default limit on entries of a dir to list: readdir cost grows with all entries, files too."""

_BATCH: Final = 1024
"""Tasks handed to the pool at a time, so a huge level doesn't create all futures at once."""


class Reason(StrEnum):
    """Why a directory was visited."""

    ROOT = "root"
    DEPTH = "depth"


@dataclass(frozen=True, slots=True)
class CaptureLimits:
    """Bounds of one capture; see the module docstring for what each expansion does."""

    depth: int = 2
    max_dirs: int = MAX_DIRS  # visited dirs per root
    max_list_entries: int = MAX_LIST_ENTRIES


@dataclass(slots=True)
class DirRecord:
    """What a capture learned about one directory."""

    path: str
    root: str
    depth: int  # levels below root
    reason: Reason
    captured_at: float
    stat: RawStat | None = None
    xattrs: dict[str, str] = field(default_factory=dict)
    subdirs: list[str] | None = None  # sorted names; None unless listed
    skipped: str | None = None  # name of the limit that prevented listing
    errors: dict[str, str] = field(default_factory=dict)  # step -> description

    def xattr_int(self, name: str) -> int | None:
        """Return xattr `name` as an int, or None if it is absent or malformed."""
        value = self.xattrs.get(name)
        try:
            return None if value is None else int(value)
        except ValueError:
            return None


@dataclass(frozen=True, slots=True)
class CaptureResult:
    """Everything a capture found, and which limits cut it short."""

    records: dict[str, DirRecord]  # sorted by `path_key`
    started_at: float
    elapsed_s: float
    truncated_roots: tuple[str, ...]  # max_dirs reached: deeper dirs are missing

    @property
    def n_errors(self) -> int:
        """Return the number of directories with errors."""
        return sum(1 for rec in self.records.values() if rec.errors)


class _Pending(NamedTuple):
    """A directory waiting to be visited."""

    path: str
    root: str
    depth: int
    reason: Reason


class Capturer:
    """Runs one capture; create a new instance per run."""

    def __init__(
        self,
        fs: RawFs,
        limits: CaptureLimits,
        pool: ThreadPoolExecutor,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._fs = fs
        self._limits = limits
        self._pool = pool
        self._now = now
        self._records: dict[str, DirRecord] = {}
        self._visited: Counter[str] = Counter()
        self._truncated: set[str] = set()

    def run(self, roots: list[str]) -> CaptureResult:
        """Capture the trees under `roots`, which must pass `check_roots`."""
        started_at, start = self._now(), time.monotonic()
        pending = [_Pending(root, root, 0, Reason.ROOT) for root in roots]
        while pending:
            listed = self._choose_listed(self._visit_all(pending))
            self._map(self._list, listed)
            pending = self._children(listed)
        records = dict(sorted(self._records.items(), key=lambda item: path_key(item[0])))
        return CaptureResult(
            records=records,
            started_at=started_at,
            elapsed_s=time.monotonic() - start,
            truncated_roots=tuple(sorted(self._truncated)),
        )

    def _map[T, R](self, fn: Callable[[T], R], items: Iterable[T]) -> list[R]:
        """Return `fn` applied to `items` in the pool, in order, `_BATCH` tasks at a time."""
        results: list[R] = []
        for batch in itertools.batched(items, _BATCH):
            results.extend(self._pool.map(fn, batch))
        return results

    def _visit_all(self, pending: list[_Pending]) -> list[DirRecord]:
        """Visit `pending` in parallel, at most `max_dirs` per root; return the new records."""
        kept: list[_Pending] = []
        ordered = sorted(pending, key=lambda p: (p.root, path_key(p.path)))
        for root, group in itertools.groupby(ordered, key=attrgetter("root")):
            items = list(group)
            room = self._limits.max_dirs - self._visited[root]
            if len(items) > room:
                items = items[: max(room, 0)]
                self._truncated.add(root)
            self._visited[root] += len(items)
            kept += items
        visited = self._map(self._visit, kept)
        for rec in visited:
            self._records[rec.path] = rec
        return visited

    def _visit(self, item: _Pending) -> DirRecord:
        rec = DirRecord(item.path, item.root, item.depth, item.reason, captured_at=self._now())
        try:
            found = self._fs.inspect(item.path, xattrs.ALL)
        except OSError as exc:
            rec.errors["stat"] = describe_oserror(exc)
            return rec
        rec.stat, rec.xattrs = found.stat, found.xattrs
        if found.xattr_failure is not None:
            name, exc = found.xattr_failure
            rec.errors["xattr"] = f"{name}: {describe_oserror(exc)}"
        return rec

    def _choose_listed(self, visited: list[DirRecord]) -> list[DirRecord]:
        """Return the dirs among `visited` to list."""
        chosen = []
        for rec in visited:
            if rec.root in self._truncated or "stat" in rec.errors or "xattr" in rec.errors:
                continue
            if rec.depth >= self._limits.depth:
                continue
            entries = rec.xattr_int("ceph.dir.entries") or 0
            if entries > self._limits.max_list_entries:
                rec.skipped = "max_list_entries"
                continue
            chosen.append(rec)
        return chosen

    def _list(self, rec: DirRecord) -> None:
        """Record `rec`'s subdirs."""
        if rec.xattr_int("ceph.dir.subdirs") == 0:
            rec.subdirs = []
            return
        try:
            listing = self._fs.listdir(rec.path)
        except OSError as exc:
            rec.errors["list"] = describe_oserror(exc)
            return
        rec.subdirs = sorted(listing.subdirs)

    def _children(self, listed: list[DirRecord]) -> list[_Pending]:
        """Return the subdirs of the `listed` dirs, to visit next."""
        return [
            _Pending(os.path.join(rec.path, name), rec.root, rec.depth + 1, Reason.DEPTH)
            for rec in listed
            for name in rec.subdirs or ()
        ]


def check_roots(fs: RawFs, roots: list[str]) -> list[str]:
    """Return a description of each problem with `roots` (absolute, normalized paths).

    Problems: duplicates, roots inside other roots, roots that can't be opened as
    directories (missing, symlinks, files) and roots without CephFS xattrs. An empty list
    means all roots can be captured.
    """
    problems = [f"{root} is given more than once" for root, n in Counter(roots).items() if n > 1]
    unique = list(dict.fromkeys(roots))
    problems += [
        f"{inner} is inside {outer}; capture {outer} with a larger --depth instead"
        for outer in unique
        for inner in unique
        if is_below(inner, outer)
    ]
    for root in unique:
        try:
            found = fs.inspect(root, ["ceph.dir.rbytes"])
        except OSError as exc:
            problems.append(f"{root}: {describe_oserror(exc)}")
            continue
        failure = found.xattr_failure
        if failure is not None and failure[1].errno != errno.EOPNOTSUPP:
            problems.append(f"{root}: {describe_oserror(failure[1])}")
        elif "ceph.dir.rbytes" not in found.xattrs:
            problems.append(f"{root} is not on CephFS (no ceph.* extended attributes)")
    return problems
