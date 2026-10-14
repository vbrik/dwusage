"""Capture raw CephFS state for offline replay and diagnostics.

A capture walks breadth-first from each root. Every directory reached is *visited*: its
stat and all CephFS xattrs are recorded as raw values. Some are also *listed*: their
subdirectory names are recorded and those subdirectories visited. A directory is listed when

- it is fewer than ``depth`` levels below its root (reason ``depth``);
- its rctime is pinned in the future, at any depth (reason ``chain``). The chain of pinned
  dirs is followed down to its ends, and the non-directory entries of every pinned dir are
  stat-ed to record those with a future mtime or ctime, which pin rctime;
- after the walk, it is among the biggest (rbytes) unlisted dirs of its root, until
  ``largest`` more dirs have been visited (reason ``largest``).

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
from dwusage.bestfirst import best_first
from dwusage.fs.base import RawFs, RawStat, describe_oserror
from dwusage.paths import is_below, path_key
from dwusage.rctime import FUTURE_SLACK_S, is_future

MAX_DIRS: Final = 100_000
"""Default visited dirs per root: roughly 150 MB of memory and a minute or two on CephFS."""

MAX_LIST_ENTRIES: Final = 1_000_000
"""Default limit on entries of a dir to list: readdir cost grows with all entries, files too."""

CHAIN_MAX_DIRS: Final = 10_000
"""Default dirs per root visited only to follow pinned rctimes; chains are short and few."""

PINNED_MAX_FILES: Final = 100_000
"""Default non-directory entries stat-ed per pinned dir to find what pins its rctime."""

_BATCH: Final = 1024
"""Tasks handed to the pool at a time, so a huge level doesn't create all futures at once."""


class Reason(StrEnum):
    """Why a directory was visited."""

    ROOT = "root"
    DEPTH = "depth"
    CHAIN = "chain"
    LARGEST = "largest"


class ChainEnd(StrEnum):
    """What a pinned dir at the bottom of a chain shows."""

    CULPRIT = "culprit"  # the dir or one of its entries has a future mtime or ctime
    STALE = "stale"  # nothing visible is in the future: that entry was changed or removed
    UNDETERMINED = "undetermined"  # an unreadable or vanished subdir, or a truncated scan


@dataclass(frozen=True, slots=True)
class CaptureLimits:
    """Bounds of one capture; see the module docstring for what each expansion does."""

    depth: int = 2
    largest: int = 0  # extra dirs visited per root, biggest first
    max_dirs: int = MAX_DIRS  # visited dirs per root
    max_list_entries: int = MAX_LIST_ENTRIES
    chain_max_dirs: int = CHAIN_MAX_DIRS  # per root
    pinned_max_files: int = PINNED_MAX_FILES
    future_slack_s: float = FUTURE_SLACK_S


@dataclass(frozen=True, slots=True)
class FutureEntry:
    """An entry of a pinned dir with a future mtime or ctime; "." is the dir itself."""

    name: str
    stat: RawStat


@dataclass(frozen=True, slots=True)
class FileScan:
    """The stat-ed non-directory entries of a pinned dir."""

    scanned: int
    truncated: bool  # stopped at CaptureLimits.pinned_max_files
    future: tuple[FutureEntry, ...]  # sorted by name


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
    files: FileScan | None = None  # pinned, listed dirs only
    errors: dict[str, str] = field(default_factory=dict)  # step -> description

    def xattr_int(self, name: str) -> int | None:
        """Return xattr `name` as an int, or None if it is absent or malformed."""
        value = self.xattrs.get(name)
        try:
            return None if value is None else int(value)
        except ValueError:
            return None

    def rctime(self) -> float | None:
        """Return ceph.dir.rctime in seconds, or None if it is absent or malformed."""
        value = self.xattrs.get("ceph.dir.rctime")
        try:
            return None if value is None else xattrs.parse_time(value)
        except ValueError:
            return None


@dataclass(frozen=True, slots=True)
class CaptureResult:
    """Everything a capture found, and which limits cut it short."""

    records: dict[str, DirRecord]  # sorted by `path_key`
    started_at: float
    elapsed_s: float
    truncated_roots: tuple[str, ...]  # max_dirs reached: deeper dirs are missing
    chain_truncated_roots: tuple[str, ...]  # chain_max_dirs reached: chains not followed
    chain_ends: dict[str, ChainEnd]  # pinned dirs with no pinned subdir, by path

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
        self._chain_dirs: Counter[str] = Counter()
        self._truncated: set[str] = set()
        self._chain_truncated: set[str] = set()

    def run(self, roots: list[str]) -> CaptureResult:
        """Capture the trees under `roots`, which must pass `check_roots`."""
        started_at, start = self._now(), time.monotonic()
        pending = [_Pending(root, root, 0, Reason.ROOT) for root in roots]
        while pending:
            listed = self._choose_listed(self._visit_all(pending))
            self._map(self._list, listed)
            pending = self._children(listed)
        for root in sorted(roots):
            self._expand_largest(root)
        records = dict(sorted(self._records.items(), key=lambda item: path_key(item[0])))
        return CaptureResult(
            records=records,
            started_at=started_at,
            elapsed_s=time.monotonic() - start,
            truncated_roots=tuple(sorted(self._truncated)),
            chain_truncated_roots=tuple(sorted(self._chain_truncated)),
            chain_ends=self._chain_ends(records),
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
        if "ceph.dir.rctime" in rec.xattrs and rec.rctime() is None:
            rec.errors["rctime"] = f"malformed: {rec.xattrs['ceph.dir.rctime']!r}"
        return rec

    def _is_future(self, st: RawStat, now: float) -> bool:
        """Return True if the newer of `st`'s mtime and ctime is implausibly after `now`."""
        newest = max(st.mtime_ns, st.ctime_ns) / 1e9
        return is_future(newest, now, self._limits.future_slack_s)

    def _is_pinned(self, rec: DirRecord) -> bool:
        """Return True if `rec`'s rctime is in the future."""
        rctime = rec.rctime()
        return rctime is not None and is_future(
            rctime, rec.captured_at, self._limits.future_slack_s
        )

    def _choose_listed(self, visited: list[DirRecord]) -> list[DirRecord]:
        """Return the dirs among `visited` to list, charging chain dirs in path order."""
        chosen = []
        for rec in visited:
            if rec.root in self._truncated or "stat" in rec.errors or "xattr" in rec.errors:
                continue
            within_depth = rec.depth < self._limits.depth
            if not within_depth and not self._is_pinned(rec):
                continue
            entries = rec.xattr_int("ceph.dir.entries") or 0
            if entries > self._limits.max_list_entries:
                rec.skipped = "max_list_entries"
                continue
            if not within_depth:
                n_subdirs = rec.xattr_int("ceph.dir.subdirs") or 0
                if self._chain_dirs[rec.root] + n_subdirs > self._limits.chain_max_dirs:
                    rec.skipped = "chain_max_dirs"
                    self._chain_truncated.add(rec.root)
                    continue
                self._chain_dirs[rec.root] += n_subdirs
            chosen.append(rec)
        return chosen

    def _list(self, rec: DirRecord) -> None:
        """Record `rec`'s subdirs; for a pinned dir, also its entries with future times."""
        pinned = self._is_pinned(rec)
        if not pinned and rec.xattr_int("ceph.dir.subdirs") == 0:
            rec.subdirs = []
            return
        try:
            listing = self._fs.listdir(rec.path, self._limits.pinned_max_files if pinned else 0)
        except OSError as exc:
            rec.errors["list"] = describe_oserror(exc)
            return
        rec.subdirs = sorted(listing.subdirs)
        if pinned:
            future = [
                FutureEntry(name, st)
                for name, st in listing.nondirs
                if self._is_future(st, rec.captured_at)
            ]
            if rec.stat is not None and self._is_future(rec.stat, rec.captured_at):
                future.append(FutureEntry(".", rec.stat))
            future.sort(key=attrgetter("name"))
            rec.files = FileScan(len(listing.nondirs), listing.nondirs_truncated, tuple(future))

    def _children(self, listed: list[DirRecord]) -> list[_Pending]:
        """Return the subdirs of the `listed` dirs, to visit next."""
        return [
            _Pending(
                os.path.join(rec.path, name),
                rec.root,
                rec.depth + 1,
                Reason.DEPTH if rec.depth < self._limits.depth else Reason.CHAIN,
            )
            for rec in listed
            for name in rec.subdirs or ()
        ]

    def _expandable(self, rec: DirRecord) -> bool:
        """Return True if `rec` can be listed by `_expand_largest`."""
        n_subdirs = rec.xattr_int("ceph.dir.subdirs") or 0
        entries = rec.xattr_int("ceph.dir.entries") or 0
        return (
            rec.subdirs is None
            and not rec.errors
            and rec.skipped is None
            and rec.xattr_int("ceph.dir.rbytes") is not None
            and n_subdirs > 0
            and entries <= self._limits.max_list_entries
        )

    def _expand_largest(self, root: str) -> None:
        """Visit up to `largest` more dirs under `root`, listing the biggest dirs first."""
        remaining = min(self._limits.largest, self._limits.max_dirs - self._visited[root])
        if remaining <= 0 or root in self._truncated:
            return

        def expand(rec: DirRecord) -> list[DirRecord]:
            nonlocal remaining
            if (rec.xattr_int("ceph.dir.subdirs") or 0) > remaining:
                return []  # doesn't fit; smaller dirs may
            self._list(rec)
            children = [
                _Pending(os.path.join(rec.path, name), root, rec.depth + 1, Reason.LARGEST)
                for name in rec.subdirs or ()
            ]
            visited = self._visit_all(children)
            remaining -= len(visited)
            return [child for child in visited if self._expandable(child)]

        start = [
            rec for rec in self._records.values() if rec.root == root and self._expandable(rec)
        ]
        for _ in best_first(
            start,
            weight=lambda rec: rec.xattr_int("ceph.dir.rbytes") or 0,
            key=lambda rec: path_key(rec.path),
            expand=expand,
        ):
            if remaining <= 0 or root in self._truncated:
                break

    def _chain_ends(self, records: dict[str, DirRecord]) -> dict[str, ChainEnd]:
        """Classify each listed pinned dir that has no pinned subdir."""
        ends = {}
        for path, rec in records.items():
            if rec.subdirs is None or not self._is_pinned(rec):
                continue
            children = [records.get(os.path.join(path, name)) for name in rec.subdirs]
            if any(child is not None and self._is_pinned(child) for child in children):
                continue  # the chain goes on below
            if rec.files is not None and rec.files.future:
                ends[path] = ChainEnd.CULPRIT
            elif (
                rec.files is None
                or rec.files.truncated
                or any(child is None or child.errors for child in children)
            ):
                ends[path] = ChainEnd.UNDETERMINED
            else:
                ends[path] = ChainEnd.STALE
        return ends


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
