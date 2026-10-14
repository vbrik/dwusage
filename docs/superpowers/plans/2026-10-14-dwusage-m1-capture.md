# dwusage Milestone 1 — Scaffold, CephFS reader, capture — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `dwusage capture`, which records raw CephFS state (lstat, ceph xattrs, subdir names) to a JSON Lines file. Captures can then be taken repeatedly while the live mounts are still reachable, and replayed offline later.

**Architecture:**
- **One raw access layer:** a `RawFs` protocol with `lstat`, `getxattr`, `subdirs` and `nondirs`, implemented by `CephFs` (real) and `FakeFs` (tests). Everything above it is testable without a live system.
- **`Capturer`** walks breadth-first through a thread pool. It expands dirs that are shallower than `--depth` or whose rctime is in the future (it follows pinned chains down to their endpoints), and then expands the N largest remaining dirs per root.
- **`capture_file`** serializes the result to JSONL: gzip when the name ends in `.gz`, written atomically.

**Tech Stack:** Python ≥ 3.12, standard library only (argparse, concurrent.futures, ctypes, gzip, json), pytest, ruff, hatchling (build backend).

**Spec:** `docs/superpowers/specs/2026-10-14-dwusage-design.md` (approved). This plan implements its Milestone 1. Later milestones get their own plans.

## Global Constraints

- `requires-python = ">=3.12"`. Don't use syntax or stdlib APIs newer than 3.12 (this host runs 3.14).
- No runtime dependencies in this milestone.
- `ruff format` and `ruff check` must pass, with the config in `pyproject.toml`. Checkpoints run `ruff check --fix` first, because isort treats `tests/fakefs.py` as first-party (`src = ["src", "tests"]`) and reorders the test imports.
- Tests must not need a live CephFS. They use `tests/fakefs.py` or the local filesystem (`tmp_path`).
- **Never run `git commit`** (the user's AGENTS.md rule). The project isn't a git repo, so each task ends with a checkpoint step (ruff and pytest) instead of a commit.
- Output streams:
  - stdout carries only results; with `-o -` that is the capture JSONL.
  - Notes go to stderr. Errors start with `ERROR: `, warnings with `WARNING: `.
  - Exit codes: 0 means success, 1 means a fatal error, 2 means a usage error, 130 means interrupted.
- Output is deterministic: dir records are sorted by path, and expansion order is fixed.
- Time units:
  - Raw stat times are integer **nanoseconds** (`*_ns`).
  - `captured_at`, `started_at` and `finished_at` are float **seconds** since the epoch.
- Capture is read-only toward the filesystem. Its only write is the output file, which is written to `OUTPUT.tmp.PID` and renamed into place.
- Run everything from the project root with `PYTHONPATH=src` (pytest picks this up from `pyproject.toml`).

## Review Focus

These failure modes are implied by the spec but no feature test would naturally cover them. Each has a test in its owning task.

1. **Dir names with newlines or non-UTF-8 bytes** (surrogate-escaped `str`) must round-trip through the JSONL file unchanged. → Task 6, `test_odd_names_round_trip`.
2. **A dir removed between readdir and visit** must be recorded with an `ENOENT` error, and the capture must continue. → Task 3, `test_dir_removed_after_listing_is_recorded`.
3. **An interrupted or failed write** (Ctrl-C, disk full) must not leave a partial or temporary file at the output path. → Task 6, `test_interrupted_write_leaves_no_file`.
4. **A missing or unwritable output dir** must be reported as `ERROR:` *before* a capture that may take an hour. → Task 7, `test_missing_output_dir_is_an_error`.
5. **Symlinks to directories** must never be followed; following one could loop or escape the tree. → Task 2, `test_listing_splits_dirs_and_nondirs_without_following_symlinks`.

---

## File map

| File | Responsibility |
|---|---|
| `pyproject.toml` | Package metadata, entry point, pytest and ruff configuration |
| `src/dwusage/__init__.py` | `__version__` (single source, read by hatch) |
| `src/dwusage/__main__.py` | `python -m dwusage` |
| `src/dwusage/cli.py` | argparse, `main()`, `run_capture()`, `capture_summary()` (pure; returns lines) |
| `src/dwusage/xattrs.py` | CephFS xattr names (`ALL`), `parse_time()` |
| `src/dwusage/rctime.py` | `FUTURE_SLACK_S`, `is_future()`; grows in Milestone 5 |
| `src/dwusage/fs/__init__.py` | Empty |
| `src/dwusage/fs/base.py` | `RawStat`, `RawFs` Protocol, `describe_oserror()` |
| `src/dwusage/fs/statx.py` | `btime_ns()` via ctypes `statx(2)` |
| `src/dwusage/fs/ceph.py` | `CephFs`, the real `RawFs` |
| `src/dwusage/capture.py` | `CaptureLimits`, `DirRecord`, `EndpointScan`, `FutureEntry`, `CaptureResult`, `Capturer`, `check_roots()` |
| `src/dwusage/capture_file.py` | `SCHEMA`, `capture_lines()`, `write_jsonl()`, `read_jsonl()` |
| `tests/fakefs.py` | `FakeFs`, `capture()` helper, `NOW`, `PINNED` |
| `tests/test_*.py` | One test file per module |
| `README.md`, `docs/terms.md`, `docs/conventions.md` | User docs, nomenclature, project conventions (required by AGENTS.md) |

---

### Task 1: Project scaffold

**Files:**
- Create: `pyproject.toml`, `src/dwusage/__init__.py`, `src/dwusage/__main__.py`, `src/dwusage/cli.py` (minimal; replaced in Task 7), `src/dwusage/fs/__init__.py`, `README.md`, `docs/terms.md`, `docs/conventions.md`
- Test: `tests/test_cli.py`

**Interfaces:**
- Produces: `dwusage.__version__: str`; `dwusage.cli.main(argv: Sequence[str] | None = None) -> int`; `dwusage.cli.build_parser() -> argparse.ArgumentParser`.

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "dwusage"
dynamic = ["version"]
description = "Track how CephFS directory usage changes over time"
readme = "README.md"
requires-python = ">=3.12"
dependencies = []

[project.optional-dependencies]
postgres = ["psycopg[binary]>=3.2"]

[project.scripts]
dwusage = "dwusage.cli:main"

[tool.hatch.version]
path = "src/dwusage/__init__.py"

[tool.hatch.build.targets.wheel]
packages = ["src/dwusage"]

[tool.pytest.ini_options]
pythonpath = ["src", "tests"]
testpaths = ["tests"]

[tool.ruff]
line-length = 100
target-version = "py312"
src = ["src", "tests"]

[tool.ruff.lint]
select = ["E", "F", "W", "I", "B", "UP", "SIM", "C4", "RUF"]
```

- [ ] **Step 2: Write the package files**

`src/dwusage/__init__.py`:
```python
"""Track how CephFS directory usage changes over time."""

__version__ = "0.1.0"
```

`src/dwusage/__main__.py`:
```python
from dwusage.cli import main

raise SystemExit(main())
```

`src/dwusage/fs/__init__.py`: an empty file.

`src/dwusage/cli.py`:
```python
"""Command-line interface: parses arguments, calls the library, prints results."""

import argparse
from collections.abc import Sequence

from dwusage import __version__


def build_parser() -> argparse.ArgumentParser:
    """Return the top-level parser with one subparser per command."""
    parser = argparse.ArgumentParser(
        prog="dwusage", description="Track how CephFS directory usage changes over time."
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_subparsers(dest="command", required=True, metavar="COMMAND")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command line; return the process exit status."""
    build_parser().parse_args(argv)
    return 0
```

- [ ] **Step 3: Write the failing test** `tests/test_cli.py`

```python
import pytest

from dwusage import __version__, cli


def test_version(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"dwusage {__version__}"


def test_command_is_required(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main([])
    assert exc.value.code == 2
```

- [ ] **Step 4: Run the tests**

Run: `pytest tests/test_cli.py -v`
Expected: 2 passed. (If you wrote the test before `cli.py`, the first run fails with `ModuleNotFoundError: dwusage`.)

- [ ] **Step 5: Write the docs skeletons**

`README.md`:
```markdown
# dwusage

Tracks how CephFS directory usage (bytes, file and dir counts, owner, times) changes over
time, for dashboards and alerts built by other tools.

Status: milestone 1 — `dwusage capture` only. Design: `docs/superpowers/specs/2026-10-14-dwusage-design.md`.

## Running

No installation is needed for development:

    PYTHONPATH=src python3 -m dwusage --help

Or install it: `pip install -e .` (add `[postgres]` for the PostgreSQL sink, later).

## Documentation

- `docs/terms.md` — terms used in output, docs and code
- `docs/conventions.md` — project conventions for contributors
```

`docs/terms.md`:
```markdown
# Terms

| Term | Meaning |
|---|---|
| root | A directory a capture (or, later, a tree) starts from. |
| visit | Read a directory's lstat fields and CephFS xattrs. Cheap: cost does not depend on subtree size. |
| expand | List a directory's subdirectories (readdir) and visit each one. The expensive operation. |
| rstats | CephFS recursive statistics (`ceph.dir.rbytes`, `rentries`, `rfiles`, `rsubdirs`, `rctime`), maintained by the MDS for the whole subtree. Propagated lazily: a parent may briefly disagree with the sum of its children. |
| pinned rctime | An rctime in the future (more than `FUTURE_SLACK_S` after the observation). rctime never decreases, so one entry with a bogus future ctime pins rctime of all its ancestors permanently. Observed: `4294967295` on `/mnt/ceph1/npx`. |
| chain | The pinned ancestors of such an entry, from a root down to the entry's directory. |
| chain endpoint | A pinned, expanded directory none of whose subdirectories is pinned. The entry that pins the chain is the directory itself (its own ctime) or one of its non-directory entries. |
| capture | A JSON Lines file of raw filesystem state written by `dwusage capture`, for offline replay. |
```

`docs/conventions.md`:
```markdown
# Conventions

- Filesystem access goes through `dwusage.fs.base.RawFs`. `CephFs` is the real implementation;
  tests use `tests/fakefs.py`. Nothing above `dwusage.fs` calls `os` filesystem functions.
- Read CephFS stats with `getxattr` only. `cat <dir>` (`dirstat` mount option) and `st_size` of
  directories (`rbytes` mount option) depend on how the filesystem is mounted.
- Times: raw stat times are integer nanoseconds (`*_ns`); observation times (`captured_at`,
  `started_at`, `finished_at`) are float seconds since the epoch.
- Per-directory failures are data, not exceptions: record `describe_oserror()` text in the
  record's `errors` and continue. Only configuration and usage problems end a run.
- stdout carries results only. Messages go to stderr, prefixed `ERROR:` or `WARNING:` where
  they are one.
- Output is deterministic: sort by path; break ties by name.
- Separate deciding from printing: library functions return data; `cli.py` renders it.
- Before handing off: `ruff format`, `ruff check`, `pytest` all clean. No automatic git commits.
```

- [ ] **Step 6: Checkpoint**

Run: `ruff check --fix . && ruff format . && pytest -q`
Expected: no ruff errors; 2 passed. Don't commit.

---

### Task 2: Raw filesystem layer — `RawFs`, xattr parsing, `CephFs`, `statx`, `FakeFs`

**Files:**
- Create: `src/dwusage/fs/base.py`, `src/dwusage/xattrs.py`, `src/dwusage/rctime.py`, `src/dwusage/fs/statx.py`, `src/dwusage/fs/ceph.py`, `tests/fakefs.py`
- Test: `tests/test_xattrs.py`, `tests/test_rctime.py`, `tests/test_statx.py`, `tests/test_cephfs.py`, `tests/test_fakefs.py`

**Interfaces:**
- Produces:
  - `RawStat(ino, mode, uid, gid, nlink, size, atime_ns, mtime_ns, ctime_ns, btime_ns: int | None)`, a frozen dataclass.
  - `RawFs` Protocol:
    - `lstat(path) -> RawStat`
    - `getxattr(path, name) -> str | None` (None means the xattr is absent)
    - `subdirs(path) -> list[str]`
    - `nondirs(path) -> Iterator[str]`
    - All of them raise `OSError`.
  - `describe_oserror(exc: OSError) -> str`
  - `xattrs.ALL: tuple[str, ...]`, `xattrs.parse_time(value: str) -> float`
  - `rctime.FUTURE_SLACK_S: float`, `rctime.is_future(t: float, now: float, slack: float = FUTURE_SLACK_S) -> bool`
  - `statx.btime_ns(path: str) -> int | None`
  - `CephFs()`, which implements `RawFs`
  - `tests/fakefs.py: FakeFs(ceph: bool = True)`, with `mkdir(path, *, ctime=1000.0, rbytes=0, rctime=None, deny=False)`, `mkfile(path, *, ctime=1000.0)` and `listed: list[str]`, plus the constants `NOW = 2_000_000_000.0` and `PINNED = 4294967295.0`. Task 3 adds `capture(fs, roots, **limits) -> CaptureResult`.

- [ ] **Step 1: Write the failing tests for parsing and `is_future`**

`tests/test_xattrs.py`:
```python
import pytest

from dwusage.xattrs import ALL, parse_time


def test_parse_time_real_values():
    assert parse_time("1791990850.239100976") == pytest.approx(1791990850.239100976)
    assert parse_time("4294967295.000000000") == 4294967295.0
    assert parse_time("1697051072") == 1697051072.0
    assert parse_time("5.5") == 5.5


@pytest.mark.parametrize("bad", ["", "abc", "1.2.3", "inf", "nan", "-5.0", "1.", "1e9", " 1"])
def test_parse_time_rejects_other_formats(bad):
    with pytest.raises(ValueError, match="not a ceph timestamp"):
        parse_time(bad)


def test_all_names_are_unique_ceph_xattrs():
    assert len(set(ALL)) == len(ALL)
    assert all(name.startswith("ceph.") for name in ALL)
    assert "ceph.dir.rctime" in ALL and "ceph.quota.max_bytes" in ALL
```

`tests/test_rctime.py`:
```python
from dwusage.rctime import FUTURE_SLACK_S, is_future


def test_is_future_allows_clock_skew_up_to_slack():
    now = 1_000_000.0
    assert not is_future(now - 10, now)
    assert not is_future(now + FUTURE_SLACK_S, now)
    assert is_future(now + FUTURE_SLACK_S + 1, now)


def test_observed_pinned_value_is_future():
    assert is_future(4294967295.0, 1791990850.0)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/test_xattrs.py tests/test_rctime.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dwusage.xattrs'`.

- [ ] **Step 3: Implement `xattrs.py` and `rctime.py`**

`src/dwusage/xattrs.py`:
```python
"""Names and value formats of the CephFS virtual extended attributes."""

import re
from typing import Final

DIR_STATS: Final = (
    "ceph.dir.entries",
    "ceph.dir.files",
    "ceph.dir.subdirs",
    "ceph.dir.rentries",
    "ceph.dir.rfiles",
    "ceph.dir.rsubdirs",
    "ceph.dir.rbytes",
    "ceph.dir.rctime",
)
"""Immediate and recursive directory statistics, maintained by the MDS."""

OPTIONAL: Final = (
    "ceph.quota.max_bytes",
    "ceph.quota.max_files",
    "ceph.dir.layout.pool",
    "ceph.dir.pin",
)
"""Attributes that may be absent (ENODATA) unless set on, or inherited by, the directory."""

ALL: Final = DIR_STATS + OPTIONAL

_TIME_RE: Final = re.compile(r"(\d+)(?:\.(\d{1,9}))?", re.ASCII)


def parse_time(value: str) -> float:
    """Return a ceph timestamp such as ``1791990850.239100976`` as float seconds since the epoch.

    Raises ValueError for anything other than ``SECONDS[.FRACTION]`` in decimal digits.
    """
    match = _TIME_RE.fullmatch(value)
    if match is None:
        raise ValueError(f"not a ceph timestamp: {value!r}")
    seconds, fraction = match.groups()
    return int(seconds) + (int(fraction) / 10 ** len(fraction) if fraction else 0.0)
```

`src/dwusage/rctime.py`:
```python
"""Handling of CephFS directories whose recursive ctime (rctime) is stuck in the future.

rctime is the maximum ctime in a subtree and never decreases, so a single entry with a bogus
future ctime pins the rctime of every ancestor permanently (observed: ``4294967295`` on
``/mnt/ceph1/npx``). Such values are recognized by comparing them with the observation time.
"""

from typing import Final

FUTURE_SLACK_S: Final = 86_400.0
"""Timestamps more than this many seconds after the observation are bogus; absorbs clock skew."""


def is_future(t: float, now: float, slack: float = FUTURE_SLACK_S) -> bool:
    """Return True if timestamp `t` (seconds since the epoch) is implausibly far after `now`."""
    return t > now + slack
```

- [ ] **Step 4: Run them to verify they pass**

Run: `pytest tests/test_xattrs.py tests/test_rctime.py -v`
Expected: all pass.

- [ ] **Step 5: Write the failing tests for `CephFs` and `statx`** (they run on the local filesystem)

`tests/test_statx.py`:
```python
import ctypes
import time

import pytest

from dwusage.fs.statx import _Statx, btime_ns


def test_struct_matches_kernel_abi():
    assert ctypes.sizeof(_Statx) == 256


def test_btime_of_new_file_is_recent(tmp_path):
    path = tmp_path / "f"
    path.write_text("")
    btime = btime_ns(str(path))
    if btime is None:
        pytest.skip("filesystem does not report birth time")
    assert abs(btime - time.time_ns()) < 60 * 1_000_000_000


def test_btime_of_missing_path_is_none(tmp_path):
    assert btime_ns(str(tmp_path / "missing")) is None
```

`tests/test_cephfs.py`:
```python
import errno
import os
import stat

import pytest

from dwusage.fs.base import describe_oserror
from dwusage.fs.ceph import CephFs


def test_listing_splits_dirs_and_nondirs_without_following_symlinks(tmp_path):
    (tmp_path / "d1").mkdir()
    (tmp_path / "d2").mkdir()
    (tmp_path / "f").write_text("x")
    (tmp_path / "link").symlink_to(tmp_path / "d1")
    fs = CephFs()
    assert sorted(fs.subdirs(str(tmp_path))) == ["d1", "d2"]
    assert sorted(fs.nondirs(str(tmp_path))) == ["f", "link"]


def test_lstat_does_not_follow_symlinks(tmp_path):
    (tmp_path / "f").write_text("x")
    (tmp_path / "link").symlink_to(tmp_path / "f")
    fs = CephFs()
    st = fs.lstat(str(tmp_path / "f"))
    assert stat.S_ISREG(st.mode) and st.size == 1 and st.ctime_ns > 0
    assert stat.S_ISLNK(fs.lstat(str(tmp_path / "link")).mode)


def test_lstat_missing_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        CephFs().lstat(str(tmp_path / "missing"))


def test_getxattr_present_and_absent(tmp_path):
    path = tmp_path / "f"
    path.write_text("")
    try:
        os.setxattr(path, "user.dwusage", b"42\0")
    except OSError as exc:
        if exc.errno in (errno.EOPNOTSUPP, errno.EPERM):
            pytest.skip("filesystem has no user xattrs")
        raise
    fs = CephFs()
    assert fs.getxattr(str(path), "user.dwusage") == "42"
    assert fs.getxattr(str(path), "user.missing") is None


def test_describe_oserror():
    exc = PermissionError(errno.EACCES, os.strerror(errno.EACCES), "/x")
    assert describe_oserror(exc) == "EACCES: Permission denied"
    assert describe_oserror(OSError("no errno")) == "OSError: no errno"
```

- [ ] **Step 6: Run them to verify they fail**

Run: `pytest tests/test_statx.py tests/test_cephfs.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dwusage.fs.statx'`.

- [ ] **Step 7: Implement `fs/base.py`, `fs/statx.py`, `fs/ceph.py`**

`src/dwusage/fs/base.py`:
```python
"""Raw filesystem access used by everything above ``dwusage.fs``.

`RawFs` is the seam that keeps the rest of the code testable: `CephFs` reads a live mount,
tests use an in-memory fake, and (later) a replay reader serves a capture file.
"""

import errno
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class RawStat:
    """lstat fields of one entry. Times are integer nanoseconds since the epoch."""

    ino: int
    mode: int
    uid: int
    gid: int
    nlink: int
    size: int
    atime_ns: int
    mtime_ns: int
    ctime_ns: int
    btime_ns: int | None  # None when the platform or filesystem doesn't report it


class RawFs(Protocol):
    """Minimal read-only filesystem interface. Every method raises OSError on failure."""

    def lstat(self, path: str) -> RawStat:
        """Return the entry's own stat, not following symlinks."""
        ...

    def getxattr(self, path: str, name: str) -> str | None:
        """Return extended attribute `name` of `path`, or None if it is absent (ENODATA)."""
        ...

    def subdirs(self, path: str) -> list[str]:
        """Return names of the immediate subdirectories, excluding symlinks, in any order."""
        ...

    def nondirs(self, path: str) -> Iterator[str]:
        """Yield names of immediate entries that are not directories (symlinks included)."""
        ...


def describe_oserror(exc: OSError) -> str:
    """Return a short description such as ``EACCES: Permission denied`` for records and logs."""
    if exc.errno is None:
        return f"{type(exc).__name__}: {exc}"
    name = errno.errorcode.get(exc.errno, f"errno {exc.errno}")
    return f"{name}: {exc.strerror}"
```

`src/dwusage/fs/statx.py`:
```python
"""Birth time via statx(2), which the os module only exposes from Python 3.15."""

import ctypes
import os
from typing import Final

_AT_FDCWD: Final = -100
_AT_SYMLINK_NOFOLLOW: Final = 0x100
_STATX_BTIME: Final = 0x800


class _Timestamp(ctypes.Structure):
    _fields_ = (
        ("tv_sec", ctypes.c_int64),
        ("tv_nsec", ctypes.c_uint32),
        ("_reserved", ctypes.c_int32),
    )


class _Statx(ctypes.Structure):
    """``struct statx`` from <linux/stat.h>; 256 bytes, layout stable since Linux 4.11."""

    _fields_ = (
        ("stx_mask", ctypes.c_uint32),
        ("stx_blksize", ctypes.c_uint32),
        ("stx_attributes", ctypes.c_uint64),
        ("stx_nlink", ctypes.c_uint32),
        ("stx_uid", ctypes.c_uint32),
        ("stx_gid", ctypes.c_uint32),
        ("stx_mode", ctypes.c_uint16),
        ("_spare0", ctypes.c_uint16),
        ("stx_ino", ctypes.c_uint64),
        ("stx_size", ctypes.c_uint64),
        ("stx_blocks", ctypes.c_uint64),
        ("stx_attributes_mask", ctypes.c_uint64),
        ("stx_atime", _Timestamp),
        ("stx_btime", _Timestamp),
        ("stx_ctime", _Timestamp),
        ("stx_mtime", _Timestamp),
        ("stx_rdev_major", ctypes.c_uint32),
        ("stx_rdev_minor", ctypes.c_uint32),
        ("stx_dev_major", ctypes.c_uint32),
        ("stx_dev_minor", ctypes.c_uint32),
        ("_spare2", ctypes.c_uint64 * 14),
    )


def _load_statx():
    try:
        fn = ctypes.CDLL(None, use_errno=True).statx
    except (OSError, AttributeError):  # not glibc >= 2.28 / not Linux
        return None
    fn.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_uint,
        ctypes.POINTER(_Statx),
    )
    fn.restype = ctypes.c_int
    return fn


_statx = _load_statx()


def btime_ns(path: str) -> int | None:
    """Return the birth time of `path` (symlinks not followed) in ns since the epoch.

    Returns None if libc, the kernel or the filesystem doesn't report it, or the call fails.
    """
    if _statx is None:
        return None
    buf = _Statx()
    if _statx(_AT_FDCWD, os.fsencode(path), _AT_SYMLINK_NOFOLLOW, _STATX_BTIME, buf) != 0:
        return None
    if not buf.stx_mask & _STATX_BTIME:
        return None
    return buf.stx_btime.tv_sec * 1_000_000_000 + buf.stx_btime.tv_nsec
```

`src/dwusage/fs/ceph.py`:
```python
"""`RawFs` over a mounted CephFS (kernel or FUSE client)."""

import errno
import os
from collections.abc import Iterator

from dwusage.fs.base import RawStat
from dwusage.fs.statx import btime_ns


class CephFs:
    """Reads a live mount with lstat, statx, getxattr and readdir only.

    The ``dirstat`` and ``rbytes`` mount options change what ``read()`` and ``st_size`` return
    for directories; nothing here depends on them.
    """

    def lstat(self, path: str) -> RawStat:
        st = os.lstat(path)
        return RawStat(
            ino=st.st_ino,
            mode=st.st_mode,
            uid=st.st_uid,
            gid=st.st_gid,
            nlink=st.st_nlink,
            size=st.st_size,
            atime_ns=st.st_atime_ns,
            mtime_ns=st.st_mtime_ns,
            ctime_ns=st.st_ctime_ns,
            btime_ns=btime_ns(path),
        )

    def getxattr(self, path: str, name: str) -> str | None:
        try:
            value = os.getxattr(path, name, follow_symlinks=False)
        except OSError as exc:
            if exc.errno == errno.ENODATA:
                return None
            raise
        return value.decode("utf-8", "surrogateescape").rstrip("\0")

    def subdirs(self, path: str) -> list[str]:
        # d_type from readdir avoids a stat per entry (CephFS fills it in).
        with os.scandir(path) as entries:
            return [e.name for e in entries if e.is_dir(follow_symlinks=False)]

    def nondirs(self, path: str) -> Iterator[str]:
        with os.scandir(path) as entries:
            for entry in entries:
                if not entry.is_dir(follow_symlinks=False):
                    yield entry.name
```

- [ ] **Step 8: Run them to verify they pass**

Run: `pytest tests/test_statx.py tests/test_cephfs.py -v`
Expected: all pass. A skip is acceptable only for `test_btime_of_new_file_is_recent` or `test_getxattr_present_and_absent`, and only if the local filesystem lacks the feature.

- [ ] **Step 9: Write `tests/fakefs.py` and its test**

`tests/fakefs.py`:
```python
"""In-memory `RawFs` and helpers for tests."""

import errno
import os
import stat
from collections.abc import Iterator
from dataclasses import dataclass, field

from dwusage.fs.base import RawStat

NOW = 2_000_000_000.0
"""Observation time used by tests (2033-05-18)."""

PINNED = 4294967295.0
"""rctime observed on /mnt/ceph1/npx: UINT32_MAX, far in the future."""


@dataclass
class FakeEntry:
    is_dir: bool
    ctime: float = 1000.0
    rbytes: int = 0
    rctime: float | None = None  # dirs only; defaults to ctime
    deny: bool = False  # EACCES on xattrs and listing, like an unreadable dir
    ino: int = 0
    children: dict[str, "FakeEntry"] = field(default_factory=dict)


class FakeFs:
    """Tree of entries under "/". Immediate counts in xattrs are computed from the tree.

    ``listed`` records every path passed to `subdirs`, so tests can count readdirs.
    """

    def __init__(self, *, ceph: bool = True) -> None:
        self.ceph = ceph  # False: getxattr fails with EOPNOTSUPP like a non-CephFS mount
        self.listed: list[str] = []
        self._root = FakeEntry(is_dir=True, ino=1)
        self._next_ino = 2

    def mkdir(self, path: str, **attrs) -> None:
        """Create or replace dir `path`; missing parents are created with defaults."""
        self._add(path, FakeEntry(is_dir=True, **attrs))

    def mkfile(self, path: str, *, ctime: float = 1000.0) -> None:
        self._add(path, FakeEntry(is_dir=False, ctime=ctime))

    def lstat(self, path: str) -> RawStat:
        entry = self._get(path)
        ns = int(entry.ctime * 1_000_000_000)
        mode = stat.S_IFDIR | 0o755 if entry.is_dir else stat.S_IFREG | 0o644
        return RawStat(entry.ino, mode, 0, 0, 1, len(entry.children), ns, ns, ns, None)

    def getxattr(self, path: str, name: str) -> str | None:
        entry = self._get(path)
        if not self.ceph:
            raise OSError(errno.EOPNOTSUPP, os.strerror(errno.EOPNOTSUPP), path)
        if entry.deny:
            raise PermissionError(errno.EACCES, os.strerror(errno.EACCES), path)
        if not entry.is_dir:
            return None
        n_dirs = sum(child.is_dir for child in entry.children.values())
        n_files = len(entry.children) - n_dirs
        rctime = entry.ctime if entry.rctime is None else entry.rctime
        values = {
            "ceph.dir.entries": len(entry.children),
            "ceph.dir.files": n_files,
            "ceph.dir.subdirs": n_dirs,
            "ceph.dir.rentries": len(entry.children),
            "ceph.dir.rfiles": n_files,
            "ceph.dir.rsubdirs": n_dirs,
            "ceph.dir.rbytes": entry.rbytes,
            "ceph.dir.rctime": f"{rctime:.9f}",
        }
        value = values.get(name)
        return None if value is None else str(value)

    def subdirs(self, path: str) -> list[str]:
        entry = self._listable(path)
        self.listed.append(path)
        return [name for name, child in entry.children.items() if child.is_dir]

    def nondirs(self, path: str) -> Iterator[str]:
        entry = self._listable(path)
        return iter([name for name, child in entry.children.items() if not child.is_dir])

    def _add(self, path: str, entry: FakeEntry) -> None:
        *parents, name = [part for part in path.split("/") if part]
        node = self._root
        for part in parents:
            if part not in node.children:
                node.children[part] = FakeEntry(is_dir=True, ino=self._ino())
            node = node.children[part]
        entry.ino = self._ino()
        node.children[name] = entry

    def _ino(self) -> int:
        self._next_ino += 1
        return self._next_ino - 1

    def _get(self, path: str) -> FakeEntry:
        node = self._root
        for part in filter(None, path.split("/")):
            if not node.is_dir or part not in node.children:
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
            node = node.children[part]
        return node

    def _listable(self, path: str) -> FakeEntry:
        entry = self._get(path)
        if not entry.is_dir:
            raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), path)
        if entry.deny:
            raise PermissionError(errno.EACCES, os.strerror(errno.EACCES), path)
        return entry

```


`tests/test_fakefs.py`:
```python
import pytest
from fakefs import PINNED, FakeFs


def test_xattrs_reflect_tree():
    fs = FakeFs()
    fs.mkdir("/r/a", rbytes=7, rctime=PINNED)
    fs.mkdir("/r/a/x")
    fs.mkfile("/r/a/f")
    assert fs.getxattr("/r/a", "ceph.dir.subdirs") == "1"
    assert fs.getxattr("/r/a", "ceph.dir.files") == "1"
    assert fs.getxattr("/r/a", "ceph.dir.rbytes") == "7"
    assert fs.getxattr("/r/a", "ceph.dir.rctime") == "4294967295.000000000"
    assert fs.getxattr("/r/a", "ceph.quota.max_bytes") is None
    assert fs.subdirs("/r/a") == ["x"] and list(fs.nondirs("/r/a")) == ["f"]


def test_failures_mimic_real_errno():
    fs = FakeFs()
    fs.mkdir("/r/secret", deny=True)
    with pytest.raises(PermissionError):
        fs.getxattr("/r/secret", "ceph.dir.rbytes")
    with pytest.raises(FileNotFoundError):
        fs.lstat("/r/missing")
    with pytest.raises(OSError, match="not supported"):
        FakeFs(ceph=False).getxattr("/", "ceph.dir.rbytes")
```

- [ ] **Step 10: Checkpoint**

Run: `ruff check --fix . && ruff format . && pytest -q`
Expected: all pass. Don't commit.

---

### Task 3: Capture walk — visiting, depth expansion, errors, `max_dirs`

**Files:**
- Create: `src/dwusage/capture.py`
- Modify: `tests/fakefs.py` (add the `capture()` helper)
- Test: `tests/test_capture.py`

**Interfaces:**
- Consumes: `RawFs`, `RawStat`, `describe_oserror` (fs/base.py); `xattrs.ALL`, `xattrs.parse_time`; `rctime.FUTURE_SLACK_S`, `rctime.is_future`.
- Produces (Tasks 4–7 rely on these exact names):
  - `CaptureLimits(depth=2, expand_largest=0, max_dirs=200_000, chain_max_dirs=10_000, endpoint_max_files=100_000, future_slack_s=FUTURE_SLACK_S)`, frozen.
  - `FutureEntry(name: str, stat: RawStat)`, frozen. `name == "."` means the endpoint dir itself.
  - `EndpointScan(files_scanned: int, truncated: bool, future_entries: tuple[FutureEntry, ...])`, frozen.
  - `DirRecord`:
    - fields `path`, `root`, `depth`, `reason` (`"root" | "depth" | "chain" | "largest"`), `captured_at`, `stat: RawStat | None`, `xattrs: dict[str, str]`, `subdirs: list[str] | None`, `endpoint: EndpointScan | None`, `errors: dict[str, str]`;
    - methods `xattr_int(name) -> int | None` and `rctime() -> float | None`.
  - `CaptureResult(records: dict[str, DirRecord] sorted by path, started_at, finished_at, truncated: bool, chain_truncated: bool)`, with the property `chain_endpoints -> list[str]`.
  - `Capturer(fs, limits, pool: ThreadPoolExecutor, now=time.time).run(roots: list[str]) -> CaptureResult`.
  - `check_roots(fs, roots: list[str]) -> list[str]` (problem descriptions; empty if all are usable).

- [ ] **Step 1: Add the `capture()` helper to `tests/fakefs.py`**

Add these imports at the top of the file:
```python
from concurrent.futures import ThreadPoolExecutor

from dwusage.capture import CaptureLimits, CaptureResult, Capturer
```
and append this at the end:
```python
def capture(fs: FakeFs, roots: list[str], **limits) -> CaptureResult:
    """Run a capture of `fs` at time `NOW` with `CaptureLimits(**limits)`."""
    with ThreadPoolExecutor(4) as pool:
        return Capturer(fs, CaptureLimits(**limits), pool, now=lambda: NOW).run(roots)
```

- [ ] **Step 2: Write the failing tests** `tests/test_capture.py`

```python
from fakefs import FakeFs, capture

from dwusage.capture import check_roots


def test_depth_expands_only_dirs_above_limit():
    fs = FakeFs()
    fs.mkdir("/r/a/x")
    fs.mkdir("/r/b")
    result = capture(fs, ["/r"], depth=1)
    assert list(result.records) == ["/r", "/r/a", "/r/b"]
    root, a = result.records["/r"], result.records["/r/a"]
    assert (root.reason, root.depth, root.subdirs) == ("root", 0, ["a", "b"])
    assert (a.reason, a.depth, a.subdirs, a.root) == ("depth", 1, None, "/r")
    assert a.xattrs["ceph.dir.subdirs"] == "1"
    assert a.stat is not None and not a.errors
    assert not result.truncated and result.started_at <= result.finished_at


def test_depth_zero_visits_root_only():
    fs = FakeFs()
    fs.mkdir("/r/a")
    result = capture(fs, ["/r"], depth=0)
    assert list(result.records) == ["/r"]
    assert result.records["/r"].subdirs is None
    assert fs.listed == []


def test_dir_without_subdirs_is_not_listed():
    fs = FakeFs()
    fs.mkdir("/r/leaf")
    fs.mkfile("/r/leaf/f")
    result = capture(fs, ["/r"], depth=5)
    assert result.records["/r/leaf"].subdirs == []
    assert fs.listed == ["/r"]


def test_unreadable_dir_is_recorded_and_not_expanded():
    fs = FakeFs()
    fs.mkdir("/r/secret", deny=True)
    fs.mkdir("/r/secret/inner")
    fs.mkdir("/r/ok")
    result = capture(fs, ["/r"], depth=3)
    secret = result.records["/r/secret"]
    assert secret.errors == {"xattr": "ceph.dir.entries: EACCES: Permission denied"}
    assert secret.subdirs is None
    assert "/r/secret/inner" not in result.records
    assert not result.records["/r/ok"].errors


class VanishingFs(FakeFs):
    """Lists a subdir "ghost" that is gone by the time it is visited."""

    def subdirs(self, path: str) -> list[str]:
        return [*super().subdirs(path), "ghost"]


def test_dir_removed_after_listing_is_recorded():
    fs = VanishingFs()
    fs.mkdir("/r/a")
    result = capture(fs, ["/r"], depth=1)
    ghost = result.records["/r/ghost"]
    assert ghost.errors["lstat"].startswith("ENOENT:")
    assert ghost.stat is None and ghost.subdirs is None


def test_max_dirs_truncates_deterministically():
    fs = FakeFs()
    for name in "dcba":
        fs.mkdir(f"/r/{name}")
    result = capture(fs, ["/r"], depth=1, max_dirs=3)
    assert list(result.records) == ["/r", "/r/a", "/r/b"]
    assert result.truncated


def test_multiple_roots():
    fs = FakeFs()
    fs.mkdir("/x/a")
    fs.mkdir("/y/b")
    result = capture(fs, ["/y", "/x"], depth=1)
    assert list(result.records) == ["/x", "/x/a", "/y", "/y/b"]
    assert result.records["/y/b"].root == "/y"


def test_check_roots_reports_each_problem():
    fs = FakeFs()
    fs.mkdir("/r/sub")
    fs.mkfile("/file")
    assert check_roots(fs, ["/r", "/r/sub", "/missing", "/file", "/r"]) == [
        "/r is given more than once",
        "/r/sub is inside /r; capture /r with a larger --depth instead",
        "/missing: ENOENT: No such file or directory",
        "/file is not a directory",
    ]


def test_check_roots_rejects_non_ceph():
    fs = FakeFs(ceph=False)
    fs.mkdir("/r")
    assert check_roots(fs, ["/r"]) == ["/r is not on CephFS (no ceph.* extended attributes)"]


def test_check_roots_accepts_good_roots():
    fs = FakeFs()
    fs.mkdir("/r")
    fs.mkdir("/rx")
    assert check_roots(fs, ["/r", "/rx"]) == []
```

- [ ] **Step 3: Run them to verify they fail**

Run: `pytest tests/test_capture.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dwusage.capture'`.

- [ ] **Step 4: Implement `src/dwusage/capture.py`** (the complete module; Tasks 4 and 5 add tests only)

```python
"""Capture raw CephFS state for offline replay and testing.

A capture walks breadth-first from each root. A directory is *visited* (lstat plus all
CephFS xattrs, as raw strings) or additionally *expanded* (its subdirectories are listed and
visited). Directories are expanded when they are:

- fewer than ``depth`` levels below their root (reason ``depth``);
- pinned, i.e. their rctime is in the future, regardless of depth (reason ``chain``). The
  chain of pinned dirs is followed down to its endpoints, and the non-directory entries of
  each endpoint are lstat-ed to find the one with a future ctime;
- among the ``expand_largest`` biggest (rbytes) unexpanded dirs of their root, chosen
  best-first after the walk (reason ``largest``). This lets size-weighted budgets be replayed
  deeper than ``depth``.

Per-directory failures are recorded in `DirRecord.errors`; they never stop the capture.
"""

import errno
import heapq
import os
import stat
import time
from collections import Counter
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Final, NamedTuple

from dwusage import xattrs
from dwusage.fs.base import RawFs, RawStat, describe_oserror
from dwusage.rctime import FUTURE_SLACK_S, is_future

LARGEST_MAX_FANOUT: Final = 10_000
"""--expand-largest skips dirs with more subdirs: visiting them all would exhaust the budget."""


@dataclass(frozen=True, slots=True)
class CaptureLimits:
    """Bounds of one capture; see the module docstring for what each expansion does."""

    depth: int = 2
    expand_largest: int = 0  # per root
    max_dirs: int = 200_000  # total visited dirs; the walk stops when reached
    chain_max_dirs: int = 10_000  # dirs visited only because their parent is pinned
    endpoint_max_files: int = 100_000  # entries lstat-ed per chain endpoint
    future_slack_s: float = FUTURE_SLACK_S


@dataclass(frozen=True, slots=True)
class FutureEntry:
    """An entry of a chain endpoint with a future ctime; name "." is the endpoint itself."""

    name: str
    stat: RawStat


@dataclass(frozen=True, slots=True)
class EndpointScan:
    """Result of lstat-ing the non-directory entries of a chain endpoint."""

    files_scanned: int
    truncated: bool  # stopped at CaptureLimits.endpoint_max_files
    future_entries: tuple[FutureEntry, ...]


@dataclass(slots=True)
class DirRecord:
    """What a capture learned about one directory."""

    path: str
    root: str
    depth: int  # levels below root
    reason: str  # why it was visited: "root", "depth", "chain" or "largest"
    captured_at: float
    stat: RawStat | None = None
    xattrs: dict[str, str] = field(default_factory=dict)
    subdirs: list[str] | None = None  # sorted names; None if not expanded
    endpoint: EndpointScan | None = None  # set on chain endpoints only
    errors: dict[str, str] = field(default_factory=dict)  # step -> describe_oserror() text

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


@dataclass(slots=True)
class CaptureResult:
    records: dict[str, DirRecord]  # sorted by path
    started_at: float
    finished_at: float
    truncated: bool  # max_dirs reached; deeper dirs are missing
    chain_truncated: bool  # chain_max_dirs reached; some chains were not followed to the end

    @property
    def chain_endpoints(self) -> list[str]:
        return [path for path, rec in self.records.items() if rec.endpoint is not None]


class _Pending(NamedTuple):
    path: str
    root: str
    depth: int
    reason: str


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
        self._truncated = False
        self._chain_dirs = 0
        self._chain_truncated = False

    def run(self, roots: list[str]) -> CaptureResult:
        """Capture the trees under `roots`, which must pass `check_roots`."""
        started_at = self._now()
        pending = [_Pending(root, root, 0, "root") for root in roots]
        pinned_parents: list[DirRecord] = []
        while pending or pinned_parents:
            visited = self._visit_all(pending)
            # Children of the previous level's pinned dirs are visited now, so endpoints are known.
            self._scan_endpoints(pinned_parents)
            expanded = [rec for rec in visited if self._should_expand(rec)]
            self._map(self._list_subdirs, expanded)
            pinned_parents = [rec for rec in expanded if self._is_pinned(rec)]
            pending = self._children(expanded)
        self._expand_largest()
        return CaptureResult(
            records=dict(sorted(self._records.items())),
            started_at=started_at,
            finished_at=self._now(),
            truncated=self._truncated,
            chain_truncated=self._chain_truncated,
        )

    def _map(self, fn: Callable, items: Iterable) -> list:
        return list(self._pool.map(fn, items))

    def _visit_all(self, pending: list[_Pending]) -> list[DirRecord]:
        room = self._limits.max_dirs - len(self._records)
        if len(pending) > room:
            pending = sorted(pending)[: max(room, 0)]
            self._truncated = True
        visited = self._map(self._visit, pending)
        for rec in visited:
            self._records[rec.path] = rec
        return visited

    def _visit(self, item: _Pending) -> DirRecord:
        rec = DirRecord(item.path, item.root, item.depth, item.reason, captured_at=self._now())
        try:
            rec.stat = self._fs.lstat(item.path)
        except OSError as exc:
            rec.errors["lstat"] = describe_oserror(exc)
            return rec
        for name in xattrs.ALL:
            try:
                value = self._fs.getxattr(item.path, name)
            except OSError as exc:
                # CephFS virtual xattrs fail together (permissions, removal); one error suffices.
                rec.errors["xattr"] = f"{name}: {describe_oserror(exc)}"
                break
            if value is not None:
                rec.xattrs[name] = value
        return rec

    def _is_pinned(self, rec: DirRecord) -> bool:
        rctime = rec.rctime()
        return rctime is not None and is_future(
            rctime, rec.captured_at, self._limits.future_slack_s
        )

    def _should_expand(self, rec: DirRecord) -> bool:
        if rec.errors:
            return False
        return rec.depth < self._limits.depth or self._is_pinned(rec)

    def _list_subdirs(self, rec: DirRecord) -> None:
        if rec.xattr_int("ceph.dir.subdirs") == 0:
            rec.subdirs = []
            return
        try:
            rec.subdirs = sorted(self._fs.subdirs(rec.path))
        except OSError as exc:
            rec.errors["subdirs"] = describe_oserror(exc)

    def _children(self, parents: list[DirRecord]) -> list[_Pending]:
        pending = []
        for parent in parents:
            if not parent.subdirs:
                continue
            if parent.depth < self._limits.depth:
                reason = "depth"
            else:
                reason = "chain"
                if self._chain_dirs + len(parent.subdirs) > self._limits.chain_max_dirs:
                    self._chain_truncated = True
                    continue
                self._chain_dirs += len(parent.subdirs)
            pending.extend(
                _Pending(os.path.join(parent.path, name), parent.root, parent.depth + 1, reason)
                for name in parent.subdirs
            )
        return pending

    def _scan_endpoints(self, pinned_parents: list[DirRecord]) -> None:
        endpoints = []
        for parent in pinned_parents:
            if parent.subdirs is None:
                continue
            children = [self._records.get(os.path.join(parent.path, n)) for n in parent.subdirs]
            if None in children:  # not followed (chain or max_dirs limit): can't tell
                continue
            if not any(self._is_pinned(child) for child in children):
                endpoints.append(parent)
        self._map(self._scan_endpoint, endpoints)

    def _scan_endpoint(self, rec: DirRecord) -> None:
        slack = self._limits.future_slack_s
        future = []
        if rec.stat is not None and is_future(rec.stat.ctime_ns / 1e9, rec.captured_at, slack):
            future.append(FutureEntry(".", rec.stat))
        scanned, truncated = 0, False
        try:
            for name in self._fs.nondirs(rec.path):
                if scanned == self._limits.endpoint_max_files:
                    truncated = True
                    break
                scanned += 1
                try:
                    st = self._fs.lstat(os.path.join(rec.path, name))
                except OSError:
                    continue  # removed since readdir
                if is_future(st.ctime_ns / 1e9, rec.captured_at, slack):
                    future.append(FutureEntry(name, st))
        except OSError as exc:
            rec.errors["endpoint"] = describe_oserror(exc)
            return
        future.sort(key=lambda entry: entry.name)
        rec.endpoint = EndpointScan(scanned, truncated, tuple(future))

    def _is_largest_candidate(self, rec: DirRecord) -> bool:
        n_subdirs = rec.xattr_int("ceph.dir.subdirs")
        return (
            rec.subdirs is None
            and not rec.errors
            and rec.xattr_int("ceph.dir.rbytes") is not None
            and n_subdirs is not None
            and 0 < n_subdirs <= LARGEST_MAX_FANOUT
        )

    def _push_candidates(self, heaps: dict[str, list], records: Iterable[DirRecord]) -> None:
        for rec in records:
            if self._is_largest_candidate(rec):
                heap = heaps.setdefault(rec.root, [])
                heapq.heappush(heap, (-rec.xattr_int("ceph.dir.rbytes"), rec.path))

    def _expand_largest(self) -> None:
        if not self._limits.expand_largest:
            return
        heaps: dict[str, list] = {}
        self._push_candidates(heaps, self._records.values())
        for root in sorted(heaps):
            heap = heaps[root]
            for _ in range(self._limits.expand_largest):
                if not heap or self._truncated:
                    break
                rec = self._records[heapq.heappop(heap)[1]]
                self._list_subdirs(rec)
                children = [
                    _Pending(os.path.join(rec.path, name), rec.root, rec.depth + 1, "largest")
                    for name in rec.subdirs or ()
                ]
                self._push_candidates(heaps, self._visit_all(children))


def _is_within(path: str, parent: str) -> bool:
    return path.startswith(parent.rstrip("/") + "/")


def check_roots(fs: RawFs, roots: list[str]) -> list[str]:
    """Return a description of each problem with `roots` (absolute, normalized paths).

    Problems: duplicates, roots inside other roots, missing roots, non-directories and roots
    without CephFS xattrs. An empty list means all roots can be captured.
    """
    problems = [f"{root} is given more than once" for root, n in Counter(roots).items() if n > 1]
    unique = list(dict.fromkeys(roots))
    problems += [
        f"{inner} is inside {outer}; capture {outer} with a larger --depth instead"
        for outer in unique
        for inner in unique
        if _is_within(inner, outer)
    ]
    for root in unique:
        try:
            if not stat.S_ISDIR(fs.lstat(root).mode):
                problems.append(f"{root} is not a directory")
                continue
            if fs.getxattr(root, "ceph.dir.rbytes") is None:
                raise OSError(errno.EOPNOTSUPP, "no ceph.dir.rbytes")
        except OSError as exc:
            if exc.errno == errno.EOPNOTSUPP:
                problems.append(f"{root} is not on CephFS (no ceph.* extended attributes)")
            else:
                problems.append(f"{root}: {describe_oserror(exc)}")
    return problems
```


- [ ] **Step 5: Run the tests to verify they pass**

Run: `pytest tests/test_capture.py -v`
Expected: all 10 pass.

- [ ] **Step 6: Checkpoint**

Run: `ruff check --fix . && ruff format . && pytest -q`
Expected: all pass. Don't commit.

---

### Task 4: Pinned-rctime chain following and endpoint scan (tests)

The behavior is implemented in Task 3's `capture.py` (`_is_pinned`, `_children`, `_scan_endpoints`, `_scan_endpoint`). This task pins it down with tests that use the real failure shape seen on `/mnt/ceph1/npx`. If a test fails, fix `capture.py`, not the test.

**Files:**
- Test: `tests/test_capture_chain.py`

**Interfaces:**
- Consumes: `capture()`, `FakeFs`, `PINNED` (tests/fakefs.py); `CaptureResult.chain_endpoints`, `DirRecord.endpoint`, `EndpointScan`, `FutureEntry`.

- [ ] **Step 1: Write the tests**

```python
from fakefs import PINNED, FakeFs, capture


def make_pinned_tree() -> FakeFs:
    """/r, /r/a, /r/a/b are pinned by file /r/a/b/bad; /r/c and /r/a/ok are healthy."""
    fs = FakeFs()
    for path in ("/r", "/r/a", "/r/a/b"):
        fs.mkdir(path, rctime=PINNED)
    fs.mkdir("/r/c")
    fs.mkdir("/r/c/deep")
    fs.mkdir("/r/a/ok")
    fs.mkfile("/r/a/b/good")
    fs.mkfile("/r/a/b/bad", ctime=PINNED)
    return fs


def test_pinned_chain_is_followed_to_its_endpoint_regardless_of_depth():
    result = capture(make_pinned_tree(), ["/r"], depth=0)
    assert result.chain_endpoints == ["/r/a/b"]
    assert result.records["/r/a"].reason == "chain"
    assert result.records["/r/a/ok"].subdirs is None  # healthy: visited, not expanded
    assert result.records["/r/c"].subdirs is None
    assert "/r/c/deep" not in result.records
    scan = result.records["/r/a/b"].endpoint
    assert [entry.name for entry in scan.future_entries] == ["bad"]
    assert scan.future_entries[0].stat.ctime_ns == int(PINNED * 1e9)
    assert (scan.files_scanned, scan.truncated) == (2, False)
    assert not result.chain_truncated


def test_pinned_parent_does_not_make_healthy_siblings_endpoints():
    result = capture(make_pinned_tree(), ["/r"], depth=0)
    assert result.records["/r"].endpoint is None
    assert result.records["/r/a"].endpoint is None


def test_dir_with_future_ctime_is_its_own_culprit():
    fs = FakeFs()
    fs.mkdir("/r", rctime=PINNED)
    fs.mkdir("/r/bad", ctime=PINNED)  # rctime defaults to its own ctime
    fs.mkdir("/r/bad/x")
    result = capture(fs, ["/r"], depth=0)
    assert result.chain_endpoints == ["/r/bad"]
    assert [entry.name for entry in result.records["/r/bad"].endpoint.future_entries] == ["."]


def test_chain_budget_stops_following_and_says_so():
    result = capture(make_pinned_tree(), ["/r"], depth=0, chain_max_dirs=3)
    assert result.chain_truncated
    assert result.chain_endpoints == []
    assert "/r/a/b" not in result.records


def test_endpoint_scan_stops_at_file_limit():
    result = capture(make_pinned_tree(), ["/r"], depth=0, endpoint_max_files=1)
    scan = result.records["/r/a/b"].endpoint
    assert (scan.files_scanned, scan.truncated) == (1, True)


def test_pinned_root_within_depth_is_still_traced():
    result = capture(make_pinned_tree(), ["/r"], depth=5)
    assert result.chain_endpoints == ["/r/a/b"]
    assert result.records["/r/a"].reason == "depth"
    assert result.records["/r/c/deep"].reason == "depth"
```

- [ ] **Step 2: Run them**

Run: `pytest tests/test_capture_chain.py -v`
Expected: 6 passed. If one fails, debug `capture.py` with superpowers:systematic-debugging; don't weaken the test.

- [ ] **Step 3: Checkpoint**

Run: `ruff check --fix . && ruff format . && pytest -q`. Don't commit.

---

### Task 5: `--expand-largest` (tests)

**Files:**
- Test: `tests/test_capture_largest.py`

**Interfaces:**
- Consumes: `capture()`, `FakeFs`; `CaptureLimits.expand_largest`; `capture.LARGEST_MAX_FANOUT`.

- [ ] **Step 1: Write the tests**

```python
from fakefs import FakeFs, capture

from dwusage import capture as capture_module


def make_sized_tree() -> FakeFs:
    fs = FakeFs()
    fs.mkdir("/r")
    fs.mkdir("/r/small", rbytes=1)
    fs.mkdir("/r/small/s1")
    fs.mkdir("/r/big", rbytes=100)
    fs.mkdir("/r/big/b1", rbytes=60)
    fs.mkdir("/r/big/b1/deep")
    fs.mkdir("/r/big/b2", rbytes=40)
    return fs


def test_expand_largest_goes_best_first_by_rbytes():
    result = capture(make_sized_tree(), ["/r"], depth=1, expand_largest=2)
    assert result.records["/r/big"].subdirs == ["b1", "b2"]
    assert result.records["/r/big/b1"].reason == "largest"
    assert result.records["/r/big/b1"].subdirs == ["deep"]
    assert result.records["/r/big/b1/deep"].depth == 3
    assert result.records["/r/small"].subdirs is None


def test_expand_largest_zero_adds_nothing():
    result = capture(make_sized_tree(), ["/r"], depth=1)
    assert list(result.records) == ["/r", "/r/big", "/r/small"]


def test_expand_largest_budget_is_per_root():
    fs = make_sized_tree()
    fs.mkdir("/q/only", rbytes=5)
    fs.mkdir("/q/only/x")
    result = capture(fs, ["/r", "/q"], depth=1, expand_largest=1)
    assert result.records["/q/only"].subdirs == ["x"]
    assert result.records["/r/big"].subdirs == ["b1", "b2"]
    assert result.records["/r/big/b1"].subdirs is None


def test_expand_largest_skips_huge_fanout(monkeypatch):
    monkeypatch.setattr(capture_module, "LARGEST_MAX_FANOUT", 1)
    result = capture(make_sized_tree(), ["/r"], depth=1, expand_largest=1)
    assert result.records["/r/big"].subdirs is None  # 2 subdirs > fanout limit
    assert result.records["/r/small"].subdirs == ["s1"]


def test_expand_largest_respects_max_dirs():
    result = capture(make_sized_tree(), ["/r"], depth=1, expand_largest=5, max_dirs=4)
    assert len(result.records) == 4 and result.truncated
```

- [ ] **Step 2: Run them**

Run: `pytest tests/test_capture_largest.py -v`
Expected: 5 passed. `test_expand_largest_skips_huge_fanout` needs `_is_largest_candidate` to read the *module global* `LARGEST_MAX_FANOUT` at call time; the Task 3 code does.

- [ ] **Step 3: Checkpoint**

Run: `ruff check --fix . && ruff format . && pytest -q`. Don't commit.

---

### Task 6: Capture file format — `capture_file.py`

**Files:**
- Create: `src/dwusage/capture_file.py`
- Test: `tests/test_capture_file.py`

**Interfaces:**
- Consumes: `CaptureResult`, `CaptureLimits`, `DirRecord` (capture.py).
- Produces:
  - `SCHEMA: int = 1`
  - `capture_lines(result, *, roots: list[str], limits: CaptureLimits, host: str, version: str) -> Iterator[dict]`
  - `write_jsonl(lines: Iterable[dict], output: str) -> None` (`"-"` means stdout)
  - `read_jsonl(path: str) -> Iterator[dict]`

  The Milestone 2 replay reader will consume `read_jsonl` together with this record layout:
  ```
  {"type":"capture_start","schema":1,"tool_version":…,"host":…,"roots":[…],"limits":{…},"started_at":…}
  {"type":"dir","path":…,"root":…,"depth":…,"reason":…,"captured_at":…,"stat":{RawStat fields}|null,
   "xattrs":{name: raw string},"subdirs":[…]|null,
   "endpoint":{"files_scanned":…,"truncated":…,"future_entries":[{"name":…,"stat":{…}}]}|null,
   "errors":{step: text}}
  {"type":"capture_end","finished_at":…,"n_dirs":…,"n_errors":…,"truncated":…,"chain_truncated":…,
   "chain_endpoints":[…]}
  ```

- [ ] **Step 1: Write the failing tests**

```python
import json

import pytest
from fakefs import PINNED, FakeFs, capture

from dwusage.capture import CaptureLimits
from dwusage.capture_file import SCHEMA, capture_lines, read_jsonl, write_jsonl


def lines_for(fs: FakeFs, **limits) -> list[dict]:
    result = capture(fs, ["/r"], **limits)
    return list(
        capture_lines(result, roots=["/r"], limits=CaptureLimits(**limits), host="h", version="v")
    )


def test_record_layout():
    fs = FakeFs()
    fs.mkdir("/r", rctime=PINNED)
    fs.mkfile("/r/bad", ctime=PINNED)
    fs.mkdir("/r/a")
    start, root, a, end = lines_for(fs, depth=1)
    assert start["type"] == "capture_start" and start["schema"] == SCHEMA
    assert start["limits"]["depth"] == 1 and start["roots"] == ["/r"]
    assert root["type"] == "dir" and root["subdirs"] == ["a"]
    assert root["stat"]["ctime_ns"] == 1000 * 10**9
    assert root["xattrs"]["ceph.dir.rctime"] == "4294967295.000000000"
    assert root["endpoint"]["future_entries"][0]["name"] == "bad"
    assert a["subdirs"] is None and a["endpoint"] is None and a["errors"] == {}
    assert end == {
        "type": "capture_end",
        "finished_at": end["finished_at"],
        "n_dirs": 2,
        "n_errors": 0,
        "truncated": False,
        "chain_truncated": False,
        "chain_endpoints": ["/r"],
    }


def test_gzip_round_trip_is_atomic(tmp_path):
    fs = FakeFs()
    fs.mkdir("/r/a")
    out = tmp_path / "cap.jsonl.gz"
    lines = lines_for(fs, depth=1)
    write_jsonl(lines, str(out))
    assert out.read_bytes()[:2] == b"\x1f\x8b"  # gzip magic
    assert list(read_jsonl(str(out))) == lines
    assert [p.name for p in tmp_path.iterdir()] == ["cap.jsonl.gz"]


def test_plain_file_and_stdout(tmp_path, capsys):
    lines = [{"type": "x", "n": 1}]
    write_jsonl(lines, str(tmp_path / "cap.jsonl"))
    assert (tmp_path / "cap.jsonl").read_text() == '{"type":"x","n":1}\n'
    write_jsonl(lines, "-")
    assert capsys.readouterr().out == '{"type":"x","n":1}\n'


def test_odd_names_round_trip(tmp_path):
    fs = FakeFs()
    fs.mkdir("/r/new\nline")
    fs.mkdir("/r/latin1-\udce9")  # undecodable byte 0xE9, as os.scandir returns it
    out = tmp_path / "cap.jsonl"
    lines = lines_for(fs, depth=1)
    write_jsonl(lines, str(out))
    paths = [line["path"] for line in read_jsonl(str(out)) if line["type"] == "dir"]
    assert paths == ["/r", "/r/latin1-\udce9", "/r/new\nline"]
    assert len(out.read_text().splitlines()) == 5  # newline in a name doesn't split a record


def test_interrupted_write_leaves_no_file(tmp_path):
    def lines():
        yield {"type": "capture_start"}
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        write_jsonl(lines(), str(tmp_path / "cap.jsonl.gz"))
    assert list(tmp_path.iterdir()) == []


def test_read_jsonl_reports_bad_line(tmp_path):
    path = tmp_path / "bad.jsonl"
    path.write_text('{"type":"capture_start"}\nnot json\n')
    with pytest.raises(json.JSONDecodeError):
        list(read_jsonl(str(path)))
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/test_capture_file.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dwusage.capture_file'`.

- [ ] **Step 3: Implement `src/dwusage/capture_file.py`**

```python
"""Capture file format: JSON Lines, gzip-compressed when the file name ends in ``.gz``.

Line 1 is a ``capture_start`` record, then one ``dir`` record per directory sorted by path,
then a ``capture_end`` record. Paths that are not valid UTF-8 are kept as surrogate escapes
(``\\udcXX`` in JSON), so they map back to the original bytes with ``os.fsencode``.
"""

import dataclasses
import gzip
import json
import os
import sys
from collections.abc import Iterable, Iterator
from contextlib import suppress
from typing import Final

from dwusage.capture import CaptureLimits, CaptureResult, DirRecord

SCHEMA: Final = 1
"""Version of the record layout; bump on incompatible changes."""


def _dir_line(rec: DirRecord) -> dict:
    endpoint = None
    if rec.endpoint is not None:
        endpoint = {
            "files_scanned": rec.endpoint.files_scanned,
            "truncated": rec.endpoint.truncated,
            "future_entries": [
                {"name": e.name, "stat": dataclasses.asdict(e.stat)}
                for e in rec.endpoint.future_entries
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
        "endpoint": endpoint,
        "errors": rec.errors,
    }


def capture_lines(
    result: CaptureResult, *, roots: list[str], limits: CaptureLimits, host: str, version: str
) -> Iterator[dict]:
    """Yield the records of a capture file for `result`, in file order."""
    yield {
        "type": "capture_start",
        "schema": SCHEMA,
        "tool_version": version,
        "host": host,
        "roots": roots,
        "limits": dataclasses.asdict(limits),
        "started_at": result.started_at,
    }
    for rec in result.records.values():
        yield _dir_line(rec)
    yield {
        "type": "capture_end",
        "finished_at": result.finished_at,
        "n_dirs": len(result.records),
        "n_errors": sum(1 for rec in result.records.values() if rec.errors),
        "truncated": result.truncated,
        "chain_truncated": result.chain_truncated,
        "chain_endpoints": result.chain_endpoints,
    }


def _dumps(line: dict) -> str:
    # ensure_ascii (the default) escapes surrogates and newlines, so one record = one line.
    return json.dumps(line, separators=(",", ":")) + "\n"


def write_jsonl(lines: Iterable[dict], output: str) -> None:
    """Write `lines` to file `output`, or to stdout if it is "-".

    A file is written under a temporary name and renamed into place, so a failed or
    interrupted run never leaves a partial file at `output`.
    """
    if output == "-":
        for line in lines:
            sys.stdout.write(_dumps(line))
        sys.stdout.flush()
        return
    tmp = f"{output}.tmp.{os.getpid()}"
    opener = gzip.open if output.endswith(".gz") else open
    try:
        with opener(tmp, "wt", encoding="utf-8") as f:
            for line in lines:
                f.write(_dumps(line))
        os.replace(tmp, output)
    except BaseException:
        with suppress(FileNotFoundError):
            os.remove(tmp)
        raise


def read_jsonl(path: str) -> Iterator[dict]:
    """Yield the records of a JSON Lines file (gzip if `path` ends in ``.gz``)."""
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/test_capture_file.py -v`
Expected: 6 passed.

- [ ] **Step 5: Checkpoint**

Run: `ruff check --fix . && ruff format . && pytest -q`. Don't commit.

---

### Task 7: `dwusage capture` command

**Files:**
- Modify: `src/dwusage/cli.py` (replace it entirely with the code below)
- Modify: `tests/test_cli.py` (append the tests below)

**Interfaces:**
- Consumes: `CephFs`; `Capturer`, `CaptureLimits`, `CaptureResult`, `check_roots`; `capture_lines`, `write_jsonl`; `describe_oserror`.
- Produces:
  - `cli.main(argv) -> int`
  - `cli.capture_summary(result: CaptureResult, n_roots: int) -> list[str]`, which is pure and returns stderr lines.
  - Tests monkeypatch `cli.CephFs` to return a `FakeFs`.

- [ ] **Step 1: Write the failing tests**

Add these imports at the top of `tests/test_cli.py` (`ruff check --fix` sorts them):
```python
import json

from fakefs import PINNED, FakeFs

from dwusage.capture_file import read_jsonl
```
and append the rest:
```python
@pytest.fixture
def fake(monkeypatch):
    fs = FakeFs()
    fs.mkdir("/r", rctime=PINNED)
    fs.mkdir("/r/a")
    fs.mkdir("/r/b", rctime=PINNED)
    fs.mkfile("/r/b/bad", ctime=PINNED)
    monkeypatch.setattr(cli, "CephFs", lambda: fs)
    return fs


def squash(text: str) -> str:
    return " ".join(text.split())


def test_capture_writes_file_and_summary(fake, tmp_path, capsys):
    out = tmp_path / "c.jsonl.gz"
    assert cli.main(["capture", "/r", "--depth", "1", "-o", str(out)]) == 0
    captured = capsys.readouterr()
    assert captured.out == ""
    err = squash(captured.err)
    assert "captured 3 dirs under 1 root" in err
    assert "rctime pinned by /r/b/bad (ctime 2106-02-07)" in err
    assert [line["path"] for line in read_jsonl(str(out)) if line["type"] == "dir"] == [
        "/r",
        "/r/a",
        "/r/b",
    ]


def test_capture_to_stdout(fake, capsys):
    assert cli.main(["capture", "/r", "--depth", "0", "-o", "-"]) == 0
    lines = [json.loads(raw) for raw in capsys.readouterr().out.splitlines()]
    assert lines[0]["type"] == "capture_start" and lines[1]["path"] == "/r"


def test_bad_root_is_an_error(fake, tmp_path, capsys):
    assert cli.main(["capture", "/missing", "-o", str(tmp_path / "c.jsonl")]) == 1
    assert capsys.readouterr().err.startswith("ERROR: /missing: ENOENT")
    assert list(tmp_path.iterdir()) == []


def test_missing_output_dir_is_an_error(fake, tmp_path, capsys):
    assert cli.main(["capture", "/r", "-o", str(tmp_path / "no" / "c.jsonl")]) == 1
    assert squash(capsys.readouterr().err).startswith("ERROR: output directory")
    assert fake.listed == []  # failed before capturing anything


@pytest.mark.parametrize("arg", ["--depth=-1", "--workers=0", "--max-dirs=x"])
def test_bad_numbers_are_usage_errors(fake, arg, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["capture", "/r", arg, "-o", "-"])
    assert exc.value.code == 2


def test_capture_summary_reports_stale_rctime(monkeypatch, capsys):
    fs = FakeFs()
    fs.mkdir("/r", rctime=PINNED)  # set by an entry that no longer exists
    fs.mkdir("/r/a")
    monkeypatch.setattr(cli, "CephFs", lambda: fs)
    assert cli.main(["capture", "/r", "--depth", "0", "-o", "-"]) == 0
    assert "stuck rctime with no current culprit in 1 dir (" in squash(capsys.readouterr().err)


def test_capture_summary_mentions_truncation(fake, tmp_path, capsys):
    assert cli.main(["capture", "/r", "--depth", "1", "--max-dirs", "1", "-o", "-"]) == 0
    assert "WARNING: stopped at --max-dirs" in squash(capsys.readouterr().err)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/test_cli.py -v`
Expected: the new tests FAIL (`argument COMMAND: invalid choice: 'capture'`, exit 2).

- [ ] **Step 3: Replace `src/dwusage/cli.py`**

```python
"""Command-line interface: parses arguments, calls the library, prints results."""

import argparse
import os
import socket
import sys
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from dwusage import __version__
from dwusage.capture import CaptureLimits, CaptureResult, Capturer, check_roots
from dwusage.capture_file import capture_lines, write_jsonl
from dwusage.fs.base import describe_oserror
from dwusage.fs.ceph import CephFs

DEFAULT_WORKERS = 16
"""Parallel filesystem operations; enough to hide MDS latency without flooding it."""

CAPTURE_DESCRIPTION = """\
Record lstat fields, CephFS xattrs and subdirectory names of directories under each ROOT,
for replay and testing without access to the filesystem. Read-only except for OUTPUT.

Dirs fewer than --depth levels below ROOT are expanded: their subdirs are listed and visited.
Dirs whose rctime is in the future (stuck rctime) are expanded regardless of depth, down to
the dir holding the entry that pins it; that entry is recorded and reported.
--expand-largest N then expands the N biggest remaining dirs per ROOT.

Output is JSON Lines: capture_start, one "dir" record per directory, capture_end. Example:
  zcat cap.jsonl.gz | jq -r 'select(.type=="dir" and .errors!={}) | .path'
A summary goes to stderr. Unreadable dirs are recorded in .errors and don't fail the run.
"""


def _int_at_least(minimum: int) -> Callable[[str], int]:
    def parse(text: str) -> int:
        try:
            value = int(text)
        except ValueError:
            raise argparse.ArgumentTypeError(f"not an integer: {text!r}") from None
        if value < minimum:
            raise argparse.ArgumentTypeError(f"must be at least {minimum}: {value}")
        return value

    return parse


def build_parser() -> argparse.ArgumentParser:
    """Return the top-level parser with one subparser per command."""
    parser = argparse.ArgumentParser(
        prog="dwusage", description="Track how CephFS directory usage changes over time."
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    commands = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    defaults = CaptureLimits()
    nonneg, positive = _int_at_least(0), _int_at_least(1)
    cap = commands.add_parser(
        "capture",
        help="record raw CephFS state for offline replay",
        description=CAPTURE_DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    cap.add_argument("roots", nargs="+", metavar="ROOT", help="directory to capture")
    cap.add_argument(
        "-o", "--output", required=True, help="output file (.gz compresses); - for stdout"
    )
    cap.add_argument(
        "--depth",
        type=nonneg,
        default=defaults.depth,
        help="expand dirs fewer than N levels below ROOT (default: %(default)s)",
    )
    cap.add_argument(
        "--expand-largest",
        type=nonneg,
        default=defaults.expand_largest,
        metavar="N",
        help="then expand the N largest unexpanded dirs per ROOT (default: %(default)s)",
    )
    cap.add_argument(
        "--max-dirs",
        type=positive,
        default=defaults.max_dirs,
        metavar="N",
        help="stop after visiting N dirs in total (default: %(default)s)",
    )
    cap.add_argument(
        "--chain-max-dirs",
        type=nonneg,
        default=defaults.chain_max_dirs,
        metavar="N",
        help="visit at most N dirs only to follow stuck rctimes (default: %(default)s)",
    )
    cap.add_argument(
        "--endpoint-max-files",
        type=nonneg,
        default=defaults.endpoint_max_files,
        metavar="N",
        help="lstat at most N files per stuck-rctime endpoint (default: %(default)s)",
    )
    cap.add_argument(
        "--workers",
        type=positive,
        default=DEFAULT_WORKERS,
        metavar="N",
        help="parallel filesystem operations (default: %(default)s)",
    )
    return parser


def _error(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)


def _date(t: float) -> str:
    try:
        return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%d")
    except (OverflowError, OSError, ValueError):
        return f"{t:.0f}"


def capture_summary(result: CaptureResult, n_roots: int) -> list[str]:
    """Return the stderr summary of a capture: counts, truncation warnings, pinned rctimes."""
    n_errors = sum(1 for rec in result.records.values() if rec.errors)
    elapsed = result.finished_at - result.started_at
    lines = [
        f"captured {len(result.records)} dirs under {n_roots} root{'s' * (n_roots != 1)} "
        f"in {elapsed:.0f}s; {n_errors} with errors (see .errors in the output)"
    ]
    if result.truncated:
        lines.append(
            "WARNING: stopped at --max-dirs; deeper dirs are missing. "
            "Raise --max-dirs or lower --depth."
        )
    if result.chain_truncated:
        lines.append(
            "WARNING: stopped following stuck rctimes at --chain-max-dirs; "
            "some culprits are not recorded. Raise --chain-max-dirs."
        )
    n_stale = 0
    for path in result.chain_endpoints:
        scan = result.records[path].endpoint
        culprits = [
            f"{path if e.name == '.' else os.path.join(path, e.name)} "
            f"(ctime {_date(e.stat.ctime_ns / 1e9)})"
            for e in scan.future_entries
        ]
        if culprits:
            more = f" and {len(culprits) - 3} more" if len(culprits) > 3 else ""
            lines.append(f"rctime pinned by {', '.join(culprits[:3])}{more}")
        elif scan.truncated:
            lines.append(
                f"rctime pinned below {path}; no culprit among the first "
                f"{scan.files_scanned} files (--endpoint-max-files)"
            )
        else:
            n_stale += 1
    if n_stale:
        # Observed on /mnt/ceph1/npx: every endpoint was stale.
        lines.append(
            f"stuck rctime with no current culprit in {n_stale} dir{'s' * (n_stale != 1)} "
            "(the entry that set it was removed or moved; rctime never decreases). "
            "List: jq -c 'select(.type==\"capture_end\").chain_endpoints'"
        )
    return lines


def _output_problem(output: str) -> str | None:
    if output == "-":
        return None
    directory = os.path.dirname(os.path.abspath(output))
    if not os.path.isdir(directory):
        return f"output directory {directory} does not exist"
    if not os.access(directory, os.W_OK):
        return f"output directory {directory} is not writable"
    return None


def run_capture(args: argparse.Namespace) -> int:
    """Run `dwusage capture`; return the exit status."""
    fs = CephFs()
    roots = [os.path.abspath(root) for root in args.roots]
    problems = check_roots(fs, roots)
    if problem := _output_problem(args.output):
        problems.append(problem)
    if problems:
        for problem in problems:
            _error(problem)
        return 1
    limits = CaptureLimits(
        depth=args.depth,
        expand_largest=args.expand_largest,
        max_dirs=args.max_dirs,
        chain_max_dirs=args.chain_max_dirs,
        endpoint_max_files=args.endpoint_max_files,
    )
    pool = ThreadPoolExecutor(args.workers)
    try:
        result = Capturer(fs, limits, pool).run(roots)
    finally:
        pool.shutdown(cancel_futures=True)
    lines = capture_lines(
        result, roots=roots, limits=limits, host=socket.gethostname(), version=__version__
    )
    try:
        write_jsonl(lines, args.output)
    except OSError as exc:
        _error(f"cannot write {args.output}: {describe_oserror(exc)}")
        return 1
    for line in capture_summary(result, len(roots)):
        print(line, file=sys.stderr)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command line; return the process exit status."""
    args = build_parser().parse_args(argv)
    try:
        match args.command:
            case "capture":
                return run_capture(args)
    except KeyboardInterrupt:
        _error("interrupted; nothing was written")
        return 130
    raise AssertionError(f"unhandled command {args.command}")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/test_cli.py -v`
Expected: all pass. `fake.listed == []` in the missing-output test proves the check happens before any readdir.

- [ ] **Step 5: Check help text and manual use**

Run: `PYTHONPATH=src python3 -m dwusage capture --help`
Expected: the description and all options are shown, with one-line option help.

- [ ] **Step 6: Checkpoint**

Run: `ruff check --fix . && ruff format . && pytest -q`. Don't commit.

---

### Task 8: Live verification, capture schedule, docs

This task needs the live mounts (`/mnt/ceph1`, `/mnt/ceph2`, both mounted read-only). Every command here only reads the filesystem.

**Files:**
- Modify: `README.md` (add a "Capturing" section)

- [ ] **Step 1: Smoke-test on a small live tree**

```bash
S=/tmp/claude-1000/-home-vbrik-proj-datawarehouse-usage-tracker/ab1a298b-79ba-4788-a377-4e2b58fe3dc9/scratchpad
time PYTHONPATH=src python3 -m dwusage capture /mnt/ceph1/volumes --depth 1 -o $S/volumes.jsonl.gz
zcat $S/volumes.jsonl.gz | jq -c 'select(.type=="capture_end")'
zcat $S/volumes.jsonl.gz | jq -r 'select(.type=="dir" and .errors!={}) | [.path, (.errors|tostring)] | @tsv'
```
Expected:
- exit 0;
- `n_dirs` ≈ 1 + 3 subdirs (`_deleting`, `_index`, `csi`);
- `_deleting` and `_index` show `EACCES` errors when run as a non-root user.

- [ ] **Step 2: Trace the stuck rctime on `/mnt/ceph1/npx`**

```bash
time PYTHONPATH=src python3 -m dwusage capture /mnt/ceph1/npx --depth 0 -o $S/npx-chain.jsonl.gz
zcat $S/npx-chain.jsonl.gz | jq -c 'select(.type=="capture_end") | {n_dirs, chain_truncated, chain_endpoints}'
```
Expected (a dry run during planning gave 5,130 dirs in 3 s and 23 endpoints, all stale): the summary has `rctime pinned by …` lines for any culprits that still exist, and a `stuck rctime with no current culprit in N dirs` line for the rest. If `chain_truncated`, rerun with a larger `--chain-max-dirs` and note the value that was needed; Milestone 5's `rctime_trace_max_dirs` default depends on it. **Report the culprits to the user.**

- [ ] **Step 3: Measure capture size and time for the real trees**

Run each command and note `n_dirs`, wall time and file size:
```bash
for spec in "/mnt/ceph1/npx/user 2" "/mnt/ceph1/npx/ana 3" "/mnt/ceph1/npx/wipac 3" "/mnt/ceph2/exp 5"; do
  set -- $spec
  name=$(echo $1 | tr / _)
  /usr/bin/time -f "%e s" env PYTHONPATH=src python3 -m dwusage capture $1 --depth $2 --expand-largest 500 \
    -o $S/m$name.jsonl.gz
  ls -l $S/m$name.jsonl.gz
done
```
Expected: each command finishes, and either `truncated` is false or the summary warns. Use these numbers to propose depths for the cron schedule (Step 4) that keep each run well under an hour.

- [ ] **Step 4: Add a "Capturing" section to `README.md`**

Use the numbers measured in Step 3 to fill in the depths below:

````markdown
## Capturing

`dwusage capture` records raw CephFS state for offline replay (see `dwusage capture --help`).
Take captures repeatedly while the filesystems are reachable; a series of captures is what
later milestones replay as history. Run as root for a complete view (otherwise unreadable
dirs are recorded with `EACCES` errors).

Example crontab (hourly; `%` must be escaped in crontab):

    0 * * * * cd /path/to/dwusage && PYTHONPATH=src python3 -m dwusage capture /mnt/ceph1/npx/user /mnt/ceph1/npx/ana /mnt/ceph1/npx/wipac /mnt/ceph1/volumes --depth 3 --expand-largest 2000 -o $HOME/dwusage-captures/ceph1-$(date +\%Y\%m\%dT\%H).jsonl.gz 2>>$HOME/dwusage-captures/capture.log
    30 * * * * cd /path/to/dwusage && PYTHONPATH=src python3 -m dwusage capture /mnt/ceph2/exp --depth 5 --expand-largest 2000 -o $HOME/dwusage-captures/ceph2-$(date +\%Y\%m\%dT\%H).jsonl.gz 2>>$HOME/dwusage-captures/capture.log

Quick looks:

    zcat cap.jsonl.gz | jq -c 'select(.type=="capture_end")'
    zcat cap.jsonl.gz | jq -r 'select(.type=="dir") | [.path, .xattrs["ceph.dir.rbytes"]] | @tsv'
````

- [ ] **Step 5: Final checkpoint**

Run: `ruff format --check . && ruff check . && pytest -q`
Expected: clean, all pass. Report the results of Steps 1–3 to the user (culprits, sizes, timings) and propose the crontab. **Do not install the crontab yourself**: that is outward-facing and the user decides. Don't commit.
