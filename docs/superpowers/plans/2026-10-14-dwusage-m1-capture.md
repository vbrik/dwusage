# dwusage Milestone 1 — capture — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `dwusage capture`, which records raw CephFS state (stat, ceph xattrs, subdir names, and the entries that pin rctime) to a JSON Lines file, so the filesystems can be replayed and studied offline.

**Architecture:**
- **Filesystem access:** everything reads the filesystem through `RawFs` (`inspect`, `listdir`).
  - `CephFs` opens each directory below its root with openat2 and reads through the fd, so it never follows a symlink or enters another mount.
  - Tests use an in-memory `FakeFs`.
- **`Capturer`** walks breadth-first in a thread pool, with all limits applied per root. It lists:
  - dirs shallower than `--depth`;
  - dirs with a pinned (future) rctime, down to each chain's end;
  - with `--largest`, more dirs chosen best-first by size.
- **`Output`** writes the result atomically with mode 0600.

**Tech Stack:** Python ≥ 3.12, standard library only (argparse, concurrent.futures, ctypes, gzip, json, tempfile), pytest, ruff, hatchling.

**Spec:** `docs/superpowers/specs/2026-10-14-dwusage-design.md`. This plan is its Milestone 1; later milestones get their own plans.

## Global Constraints

- **Python:** `requires-python = ">=3.12"`. Don't use syntax or stdlib APIs newer than 3.12 (this host runs 3.14).
- **Dependencies:** none at runtime.
- **Platform:** Linux only. Full symlink protection needs openat2 (Linux ≥ 5.6); without it the code falls back and warns.
- **Ruff:** `ruff check .` and `ruff format --check .` are clean. The code below is already in its formatted form; paste it verbatim.
- **Tests:** they need no live CephFS; they use `tests/fakefs.py` or `tmp_path`.
- **No git commits.** AGENTS.md forbids automatic commits; the user commits.
- **Public repo:** never put names or paths from the live systems into committed files. Captures (`*.jsonl`, `*.jsonl.gz`) are git-ignored.
- **Live systems:** never install crontabs, modify the CephFS mounts, or use elevated privileges. Task 8 only reads the mounts.
- **Streams and exit codes:** stdout carries results only. Messages go to stderr, prefixed `ERROR:` or `WARNING:`. Exit 0 done, 1 error, 2 usage, 3 a limit left work undone, 130 interrupted.
- **Determinism:** records are in `path_key` order.
- **Units:** raw stat times are integer ns (`*_ns`); `captured_at` and `started_at` are float seconds; durations use `time.monotonic`.
- **Output files** are created with mode 0600 and renamed into place.
- **Where to run:** from the project root. `pyproject.toml` puts `src` and `tests` on pytest's path; for the CLI use `PYTHONPATH=src python3 -m dwusage`.

## Review Focus

These failure modes are not covered by the feature tests; each has its own test in the owning task:

1. **A directory swapped for a symlink between readdir and visit** must be refused, not followed. → Task 3 `test_dir_replaced_by_symlink_after_listing_is_refused`; Task 2 `test_paths_through_symlinks_or_to_nondirs_fail` and `test_openat2_refuses_symlinks_and_escapes`.
2. **An unreadable subdir on a pinned chain** must make the chain end *undetermined*, never *stale*. → Task 4 `test_unreadable_or_vanished_subdir_makes_the_end_undetermined`.
3. **A bad destination** (`-o DIR`, `-o DIR/`, a missing parent) must give `ERROR:` before any capturing and leave nothing behind. → Task 6 `test_bad_destinations_fail_before_anything_is_written`; Task 7 `test_bad_output_fails_before_capturing` and `test_bad_root_is_an_error`.
4. **Ctrl-C or SIGTERM mid-capture** must leave no partial or temporary file. → Task 7 `test_interrupt_removes_partial_output`. `entry()`'s immediate exit isn't unit-tested, because it would end pytest; Task 8 Step 2 checks it with real signals.
5. **Names with newlines or invalid UTF-8** must round-trip exactly. → Task 6 `test_odd_names_round_trip`.

---

## File map

| File | Responsibility | Task |
|---|---|---|
| `pyproject.toml`, `.gitignore` | Metadata, entry point, pytest and ruff config; ignore caches and captures | 1 |
| `README.md`, `docs/terms.md`, `docs/conventions.md` | User docs, nomenclature, contributor conventions (AGENTS.md) | 1, 8 |
| `src/dwusage/__init__.py`, `__main__.py` | `__version__`; `python -m dwusage` | 1 |
| `src/dwusage/cli.py` | argparse, `main`, `entry`, `run_capture`, `capture_summary` | 1, 7 |
| `src/dwusage/xattrs.py` | CephFS xattr names, `parse_time` | 2 |
| `src/dwusage/rctime.py` | `FUTURE_SLACK_S`, `is_future` | 2 |
| `src/dwusage/paths.py` | `path_key`, `is_below` | 2 |
| `src/dwusage/fs/base.py` | `RawStat`, `Inspection`, `Listing`, `RawFs`, error helpers | 2 |
| `src/dwusage/fs/syscalls.py` | `openat2`, `stat_at` (statx) via ctypes | 2 |
| `src/dwusage/fs/ceph.py` | `CephFs` | 2 |
| `src/dwusage/capture.py` | `Capturer`, limits, records, `check_roots` | 3, 4, 5 |
| `src/dwusage/bestfirst.py` | `best_first` (shared with Milestone 6 budget rules) | 5 |
| `src/dwusage/capture_file.py` | Capture JSONL format, `Output`, `read_jsonl` | 6 |
| `tests/fakefs.py` | `FakeFs`, `NOW`, `PINNED` (imports no code under test) | 2 |
| `tests/helpers.py` | `capture()` test helper | 3 |

---

### Task 1: Project scaffold

**Files:**
- Create: `pyproject.toml`, `.gitignore`, `README.md`, `docs/terms.md`, `docs/conventions.md`, `src/dwusage/__init__.py`, `src/dwusage/__main__.py`, `src/dwusage/fs/__init__.py` (empty), `src/dwusage/cli.py` (first version; Task 7 replaces it)
- Test: `tests/test_cli.py` (first version; Task 7 replaces it)

**Interfaces:**
- Produces: `dwusage.__version__: str`; `dwusage.cli.main(argv: Sequence[str] | None = None) -> int`; `dwusage.cli.entry() -> NoReturn` (console script and `python -m dwusage`).

- [ ] **Step 1: Write `pyproject.toml` and `.gitignore`**

`pyproject.toml`:
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
dwusage = "dwusage.cli:entry"

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

`.gitignore`:
```text
__pycache__/
*.egg-info/
.pytest_cache/
.ruff_cache/
.venv/
build/
dist/
# Captures hold names from live filesystems; never commit them (or their temporary files).
*.jsonl
*.jsonl.gz
*.tmp
```

- [ ] **Step 2: Write the failing test** `tests/test_cli.py`

```python
import pytest

from dwusage import __version__, cli


def test_version(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"dwusage {__version__}"


def test_command_is_required():
    with pytest.raises(SystemExit) as exc:
        cli.main([])
    assert exc.value.code == 2
```

- [ ] **Step 3: Run it to verify it fails**

Run: `pytest tests/test_cli.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dwusage'`.

- [ ] **Step 4: Write the package**

`src/dwusage/__init__.py`:
```python
"""Track how CephFS directory usage changes over time."""

__version__ = "0.1.0"
```

`src/dwusage/__main__.py`:
```python
from dwusage.cli import entry

entry()
```

`src/dwusage/fs/__init__.py`: an empty file.

`src/dwusage/cli.py`:
```python
"""Command-line interface: parses arguments, calls the library, prints results."""

import argparse
import sys
from collections.abc import Sequence
from typing import NoReturn

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
    """Run the command line; return the exit status."""
    build_parser().parse_args(argv)
    return 0


def entry() -> NoReturn:
    """Console entry point."""
    sys.exit(main())
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `pytest tests/test_cli.py -v`
Expected: 2 passed.

- [ ] **Step 6: Write the docs**

`README.md` (Task 8 adds the "Capturing" section):
````markdown
# dwusage

Tracks how CephFS directory usage (bytes, file and directory counts, owner, times) changes
over time, for dashboards and alerts built by other tools.

Status: milestone 1, in progress. Design:
`docs/superpowers/specs/2026-10-14-dwusage-design.md`.

## Running

Linux only. No installation is needed for development:

    PYTHONPATH=src python3 -m dwusage --help

Or install it with `pip install -e .`. Full protection against symlinks swapped in during a
walk needs openat2 (Linux 5.6 or later).

## Documentation

- `docs/terms.md`: terms used in output, docs and code
- `docs/conventions.md`: conventions for contributors
````

`docs/terms.md`:
````markdown
# Terms

| Term | Meaning |
|---|---|
| root | A directory a capture (later: a tree) starts from. Paths are always opened below a root. |
| visit | Read a directory's stat and CephFS xattrs. The cost doesn't depend on the subtree's size. |
| list | Read a directory's entries (readdir) and visit its subdirectories. The cost grows with the number of entries, files included. |
| rstats | CephFS recursive statistics (`ceph.dir.rbytes`, `rentries`, `rfiles`, `rsubdirs`, `rctime`), kept by the MDS for a whole subtree and propagated lazily: a parent may briefly disagree with its children. `rentries` and `rsubdirs` count the directory itself (an empty directory has 1 of each). `rbytes` is the sum of file sizes, not pool usage. |
| rctime | The newest ctime or mtime in a subtree. It never decreases. |
| pinned rctime | An rctime more than `FUTURE_SLACK_S` after the observation time. Set by an entry with a future mtime (`touch -d`, archives unpacked with bogus dates) or ctime; it stays after that entry is fixed or removed. |
| chain | The pinned directories from a root down to where the entry that pins them is. |
| chain end | A pinned, listed directory without pinned subdirectories: *culprit* (it or one of its entries has a future mtime or ctime), *stale* (nothing visible is in the future), or *undetermined* (an unreadable or vanished subdirectory, or a truncated file scan). |
| capture | A JSON Lines file of raw filesystem state written by `dwusage capture`, for replay and diagnostics. |
````

`docs/conventions.md`:
````markdown
# Conventions

- Only `dwusage.fs` reads the tracked filesystem, through `RawFs`: `CephFs` on a live mount,
  `tests/fakefs.py` in tests. Other modules use `os` only for their own files.
- `CephFs` opens every directory below its root with openat2(2) and never follows a symlink
  or enters another mount: the tool runs as root over user-writable trees. All reads use the
  resulting file descriptor.
- Read CephFS stats with getxattr only. What `read()` and `st_size` return for directories
  depends on the `dirstat` and `rbytes` mount options.
- Times: raw stat times are integer nanoseconds (`*_ns`); observation times (`captured_at`,
  `started_at`) are float seconds since the epoch; durations come from `time.monotonic`.
- Per-directory failures are data, not exceptions: record `describe_oserror()` text in the
  record's `errors` and continue. Only usage, configuration and output problems end a run.
- Capture files keep names exactly (invalid UTF-8 as `\udcXX` surrogate escapes).
- Output files are created mode 0600 under a temporary name and renamed into place.
- stdout carries results only. Messages go to stderr, one paragraph each, prefixed `ERROR:` or
  `WARNING:` where they are one.
- Output is deterministic: sort paths with `paths.path_key`; break ties by name.
- Separate deciding from printing: library functions return data; `cli.py` renders it.
- Committed files never contain names or paths from live systems (see
  `AGENTS.md.d/LIVE-SYSTEMS-v1.md`); captures are git-ignored.
- Before handing off: `ruff format`, `ruff check` and `pytest` are clean. No automatic git
  commits.
````

- [ ] **Step 7: Checkpoint**

Run: `ruff check . && ruff format --check . && pytest -q`
Expected: clean; 2 passed. Don't commit.

---

### Task 2: Raw filesystem layer

**Files:**
- Create: `src/dwusage/xattrs.py`, `src/dwusage/rctime.py`, `src/dwusage/paths.py`, `src/dwusage/fs/base.py`, `src/dwusage/fs/syscalls.py`, `src/dwusage/fs/ceph.py`, `tests/fakefs.py`
- Test: `tests/test_xattrs.py`, `tests/test_rctime.py`, `tests/test_paths.py`, `tests/test_syscalls.py`, `tests/test_cephfs.py`, `tests/test_fakefs.py`

**Interfaces:**
- Produces:
  - `xattrs.ALL: tuple[str, ...]` and `xattrs.parse_time(value: str) -> float` (raises ValueError).
  - `rctime.FUTURE_SLACK_S: float` and `rctime.is_future(t, now, slack=FUTURE_SLACK_S) -> bool`.
  - `paths.path_key(path) -> tuple[str, ...]` and `paths.is_below(path, parent) -> bool`.
  - `fs.base`:
    - `RawStat(ino, mode, uid, gid, nlink, size, atime_ns, mtime_ns, ctime_ns, btime_ns: int | None)`;
    - `Inspection(stat, xattrs: dict[str, str], xattr_failure: tuple[str, OSError] | None)`;
    - `Listing(subdirs: list[str], nondirs: list[tuple[str, RawStat]], nondirs_truncated: bool)`;
    - `RawFs` Protocol: `inspect(path, xattr_names) -> Inspection` and `listdir(path, stat_nondirs=0) -> Listing`, both raising OSError;
    - `symlink_error(path)` (ELOOP), `mount_error(path)` (EXDEV), `describe_oserror(exc) -> str`.
  - `fs.syscalls`: `openat2(dir_fd, path, flags, resolve) -> int`; `stat_at(dir_fd, name="") -> RawStat`; `RESOLVE_*` constants.
  - `fs.ceph.CephFs(roots)`: implements `RawFs`. It is a context manager; `openat2: bool` is False once it has fallen back.
  - `tests/fakefs.py`:
    - `FakeFs(ceph=True)`, which implements `RawFs` and the context-manager surface and `openat2` attribute of `CephFs`;
    - its builders `mkdir(path, **attrs)`, `mkfile(path, **attrs)` and `symlink(path)`; the attrs are `ctime`, `mtime`, `rbytes`, `rctime`, `deny` and `mount`;
    - `listed: list[str]`, the paths it has listed;
    - the constants `NOW` and `PINNED`.

- [ ] **Step 1: Write the failing tests for parsing and paths**

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


def test_pinned_values_seen_on_cephfs_are_future():
    assert is_future(4294967295.0, 1791990850.0)
    assert is_future(2147483648.0, 1791990850.0)
```

`tests/test_paths.py`:
```python
from dwusage.paths import is_below, path_key


def test_path_key_keeps_subtrees_contiguous():
    paths = ["/r/a b", "/r/a/x", "/r/a", "/r", "/r/a-b"]
    assert sorted(paths, key=path_key) == ["/r", "/r/a", "/r/a/x", "/r/a b", "/r/a-b"]


def test_is_below():
    assert is_below("/r/a", "/r")
    assert is_below("/r", "/")
    assert not is_below("/r", "/r")
    assert not is_below("/", "/")
    assert not is_below("/rx", "/r")
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/test_xattrs.py tests/test_rctime.py tests/test_paths.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dwusage.xattrs'` (and likewise for `rctime` and `paths`).

- [ ] **Step 3: Implement them**

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
"""Immediate and recursive directory statistics, maintained by the MDS.

``rentries`` and ``rsubdirs`` count the directory itself: an empty dir has 1 of each.
"""

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
"""Recognizing CephFS recursive ctimes (rctime) that are pinned in the future.

The MDS sets a directory's rctime to the newest ctime *or mtime* in its subtree, and never
lowers it. mtime is user-settable (``touch -d``, unpacking archives with preserved dates), so
one entry with a future mtime pins the rctime of every ancestor, and the value stays even
after that entry is fixed or removed. Such values are recognized by comparing them with the
observation time.
"""

from typing import Final

FUTURE_SLACK_S: Final = 86_400.0
"""Timestamps more than this many seconds after the observation are bogus; absorbs clock skew."""


def is_future(t: float, now: float, slack: float = FUTURE_SLACK_S) -> bool:
    """Return True if timestamp `t` (seconds since the epoch) is implausibly far after `now`."""
    return t > now + slack
```

`src/dwusage/paths.py`:
```python
"""Helpers for the absolute, normalized paths used throughout."""


def path_key(path: str) -> tuple[str, ...]:
    """Return a sort key that keeps every subtree contiguous: "/a", "/a/x", "/a b"."""
    return tuple(path.split("/"))


def is_below(path: str, parent: str) -> bool:
    """Return True if `path` is strictly inside directory `parent`."""
    return path != parent and path.startswith(parent.rstrip("/") + "/")
```

- [ ] **Step 4: Run them to verify they pass**

Run: `pytest tests/test_xattrs.py tests/test_rctime.py tests/test_paths.py -v`
Expected: all pass.

- [ ] **Step 5: Write the failing tests for syscalls and `CephFs`.** They run on the local filesystem.

`tests/test_syscalls.py`:
```python
import ctypes
import errno
import os
import stat
import time

import pytest

from dwusage.fs import syscalls
from dwusage.fs.syscalls import _Statx

RESOLVE = syscalls.RESOLVE_BENEATH | syscalls.RESOLVE_NO_SYMLINKS | syscalls.RESOLVE_NO_XDEV


def test_statx_struct_matches_kernel_abi():
    assert ctypes.sizeof(_Statx) == 256
    assert _Statx.stx_btime.offset == 0x50


def test_stat_at_reads_dir_itself_and_entries_without_following(tmp_path):
    (tmp_path / "f").write_text("x")
    (tmp_path / "link").symlink_to(tmp_path / "f")
    fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        assert stat.S_ISDIR(syscalls.stat_at(fd).mode)
        f = syscalls.stat_at(fd, "f")
        assert stat.S_ISREG(f.mode) and f.size == 1
        assert f.ctime_ns == os.stat(tmp_path / "f").st_ctime_ns
        assert stat.S_ISLNK(syscalls.stat_at(fd, "link").mode)
        with pytest.raises(FileNotFoundError):
            syscalls.stat_at(fd, "missing")
    finally:
        os.close(fd)


def test_btime_of_new_file_is_recent(tmp_path):
    (tmp_path / "f").write_text("")
    fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        btime = syscalls.stat_at(fd, "f").btime_ns
    finally:
        os.close(fd)
    if btime is None:
        pytest.skip("filesystem does not report birth time")
    assert abs(btime - time.time_ns()) < 60 * 1_000_000_000


def _openat2_or_skip(dir_fd, path):
    try:
        return syscalls.openat2(dir_fd, path, os.O_RDONLY | os.O_DIRECTORY, RESOLVE)
    except OSError as exc:
        if exc.errno in (errno.ENOSYS, errno.EPERM):
            pytest.skip("kernel lacks openat2")
        raise


def test_openat2_refuses_symlinks_and_escapes(tmp_path):
    (tmp_path / "real" / "sub").mkdir(parents=True)
    (tmp_path / "link").symlink_to("real")
    anchor = os.open(tmp_path, os.O_PATH | os.O_DIRECTORY)
    try:
        os.close(_openat2_or_skip(anchor, "real/sub"))
        for path, code in (("link", errno.ELOOP), ("link/sub", errno.ELOOP), ("..", errno.EXDEV)):
            with pytest.raises(OSError) as exc:
                _openat2_or_skip(anchor, path)
            assert exc.value.errno == code, path
    finally:
        os.close(anchor)
```

`tests/test_cephfs.py`:
```python
import errno
import os
import stat

import pytest

from dwusage.fs.base import describe_oserror
from dwusage.fs.ceph import CephFs


@pytest.fixture
def tree(tmp_path):
    (tmp_path / "d1" / "x").mkdir(parents=True)
    (tmp_path / "d2").mkdir()
    (tmp_path / "f").write_text("x")
    (tmp_path / "link").symlink_to(tmp_path / "d1")
    return tmp_path


def test_listdir_splits_dirs_and_nondirs_without_following_symlinks(tree):
    with CephFs([str(tree)]) as fs:
        listing = fs.listdir(str(tree), stat_nondirs=10)
    assert sorted(listing.subdirs) == ["d1", "d2"]
    assert sorted(name for name, _ in listing.nondirs) == ["f", "link"]
    assert not listing.nondirs_truncated
    modes = {name: st.mode for name, st in listing.nondirs}
    assert stat.S_ISLNK(modes["link"]) and stat.S_ISREG(modes["f"])


def test_listdir_stats_at_most_the_requested_nondirs(tree):
    with CephFs([str(tree)]) as fs:
        assert fs.listdir(str(tree)).nondirs == []
        listing = fs.listdir(str(tree), stat_nondirs=1)
    assert len(listing.nondirs) == 1 and listing.nondirs_truncated


def test_inspect_reads_stat_and_reports_missing_ceph_xattrs(tree):
    with CephFs([str(tree)]) as fs:
        found = fs.inspect(str(tree / "d1"), ["ceph.dir.rbytes"])
    assert stat.S_ISDIR(found.stat.mode)
    assert found.stat.ino == os.stat(tree / "d1").st_ino
    # A local filesystem either lacks the attribute or rejects the ceph.* namespace.
    assert found.xattrs == {}
    assert found.xattr_failure is None or found.xattr_failure[1].errno == errno.EOPNOTSUPP


def test_inspect_reads_present_xattrs(tree):
    try:
        os.setxattr(tree / "d2", "user.dwusage", b"42\0")
    except OSError as exc:
        if exc.errno in (errno.EOPNOTSUPP, errno.EPERM):
            pytest.skip("filesystem has no user xattrs")
        raise
    with CephFs([str(tree)]) as fs:
        found = fs.inspect(str(tree / "d2"), ["user.missing", "user.dwusage"])
    assert found.xattrs == {"user.dwusage": "42"} and found.xattr_failure is None


@pytest.mark.parametrize(
    ("relative", "message"),
    [
        ("link", "ELOOP: symlink, not followed"),
        ("link/x", "ELOOP: symlink, not followed"),
        ("f", "ENOTDIR: Not a directory"),
        ("missing", "ENOENT: No such file or directory"),
    ],
)
def test_paths_through_symlinks_or_to_nondirs_fail(tree, relative, message):
    with CephFs([str(tree)]) as fs, pytest.raises(OSError) as exc:
        fs.inspect(str(tree / relative), [])
    assert describe_oserror(exc.value) == message


def test_symlink_root_is_refused(tree):
    with CephFs([str(tree / "link")]) as fs, pytest.raises(OSError) as exc:
        fs.inspect(str(tree / "link"), [])
    assert describe_oserror(exc.value) == "ELOOP: is a symlink; give its target instead"


@pytest.mark.skipif(os.geteuid() == 0, reason="root can read any directory")
def test_unreadable_dir_still_has_a_stat(tree):
    secret = tree / "d2"
    secret.chmod(0)
    try:
        with CephFs([str(tree)]) as fs:
            found = fs.inspect(str(secret), ["user.x"])
            with pytest.raises(PermissionError):
                fs.listdir(str(secret))
    finally:
        secret.chmod(0o755)
    assert stat.S_ISDIR(found.stat.mode)
    assert found.xattr_failure is not None and found.xattr_failure[0] == "open"


def test_paths_outside_the_roots_are_a_bug(tree):
    with CephFs([str(tree / "d1")]) as fs, pytest.raises(ValueError, match="not below any root"):
        fs.inspect(str(tree / "d2"), [])


def test_describe_oserror():
    exc = PermissionError(errno.EACCES, os.strerror(errno.EACCES), "/x")
    assert describe_oserror(exc) == "EACCES: Permission denied"
    assert describe_oserror(OSError("no errno")) == "OSError: no errno"
```

- [ ] **Step 6: Run them to verify they fail**

Run: `pytest tests/test_syscalls.py tests/test_cephfs.py -v`
Expected: FAIL with `ImportError: cannot import name 'syscalls' from 'dwusage.fs'` and `ModuleNotFoundError: No module named 'dwusage.fs.base'`.

- [ ] **Step 7: Implement `fs/base.py`, `fs/syscalls.py`, `fs/ceph.py`**

`src/dwusage/fs/base.py`:
```python
"""Raw, read-only access to the filesystem being tracked.

`RawFs` is the seam that keeps the rest of the code testable: `CephFs` reads a live mount,
tests use an in-memory fake, and the replay reader (milestone 2) serves a capture file.
"""

import errno
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class RawStat:
    """Stat fields of one entry, symlinks not followed. Times are ns since the epoch."""

    ino: int
    mode: int
    uid: int
    gid: int
    nlink: int
    size: int
    atime_ns: int
    mtime_ns: int
    ctime_ns: int
    btime_ns: int | None  # None when the kernel or filesystem doesn't report it


@dataclass(frozen=True, slots=True)
class Inspection:
    """A directory's stat and extended attributes."""

    stat: RawStat
    xattrs: dict[str, str]  # the requested attributes that are present
    # Attribute name (or "open") and the error that stopped reading attributes, if any.
    xattr_failure: tuple[str, OSError] | None


@dataclass(frozen=True, slots=True)
class Listing:
    """A directory's entries."""

    subdirs: list[str]  # subdirectory names (symlinks excluded), in readdir order
    nondirs: list[tuple[str, RawStat]]  # name and stat of the first non-directory entries
    nondirs_truncated: bool  # more non-directory entries exist than were stat-ed


class RawFs(Protocol):
    """Read-only access to directories, by absolute path.

    Methods raise OSError, with ELOOP if a component of the path is a symlink, EXDEV if it
    is on another mount (see `symlink_error`, `mount_error`), ENOTDIR if not a directory.
    """

    def inspect(self, path: str, xattr_names: Sequence[str]) -> Inspection:
        """Return the stat of dir `path` and those of `xattr_names` that are present.

        Reading attributes stops at the first failure other than ENODATA (absent); the
        failure is reported in the result, which still carries the stat.
        """
        ...

    def listdir(self, path: str, stat_nondirs: int = 0) -> Listing:
        """Return the entries of dir `path`, stat-ing up to `stat_nondirs` non-directories."""
        ...


def symlink_error(path: str) -> OSError:
    """Return the error for a path that leads through a symlink, which is never followed."""
    return OSError(errno.ELOOP, "symlink, not followed", path)


def mount_error(path: str) -> OSError:
    """Return the error for a path on another mount, which is never entered."""
    return OSError(errno.EXDEV, "other filesystem, not entered", path)


def describe_oserror(exc: OSError) -> str:
    """Return a short description such as ``EACCES: Permission denied`` for records and logs."""
    if exc.errno is None:
        return f"{type(exc).__name__}: {exc}"
    name = errno.errorcode.get(exc.errno, f"errno {exc.errno}")
    return f"{name}: {exc.strerror}"
```

`src/dwusage/fs/syscalls.py`:
```python
"""Linux system calls that the os module lacks: openat2(2), and statx(2) before Python 3.15."""

import ctypes
import os
from typing import Final

from dwusage.fs.base import RawStat

_libc = ctypes.CDLL(None, use_errno=True)

_SYS_OPENAT2: Final = 437  # the same on every architecture except alpha
RESOLVE_NO_XDEV: Final = 0x01
RESOLVE_NO_SYMLINKS: Final = 0x04
RESOLVE_BENEATH: Final = 0x08

_AT_SYMLINK_NOFOLLOW: Final = 0x100
_AT_EMPTY_PATH: Final = 0x1000
_STATX_BASIC_STATS: Final = 0x7FF
_STATX_BTIME: Final = 0x800


class _OpenHow(ctypes.Structure):
    """``struct open_how`` from <linux/openat2.h>."""

    _fields_ = (
        ("flags", ctypes.c_uint64),
        ("mode", ctypes.c_uint64),
        ("resolve", ctypes.c_uint64),
    )


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
        ("_spare1", ctypes.c_uint64 * 16),  # rdev, dev, mnt_id, dio alignment, ...
    )


_libc.syscall.restype = ctypes.c_long
_statx_fn = getattr(_libc, "statx", None)  # glibc >= 2.28
if _statx_fn is not None:
    _statx_fn.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_uint,
        ctypes.POINTER(_Statx),
    )
    _statx_fn.restype = ctypes.c_int


def _raise_errno(path: str) -> None:
    err = ctypes.get_errno()
    raise OSError(err, os.strerror(err), path)


def openat2(dir_fd: int, path: str, flags: int, resolve: int) -> int:
    """Open `path` relative to `dir_fd` under openat2(2) `resolve` restrictions; return the fd.

    Raises OSError: ELOOP for a symlink and EXDEV for a mount or ".." under the RESOLVE_*
    flags above; ENOSYS (or EPERM under some seccomp filters) if the kernel lacks openat2.
    """
    how = _OpenHow(flags, 0, resolve)
    fd = _libc.syscall(
        ctypes.c_long(_SYS_OPENAT2),
        ctypes.c_long(dir_fd),
        ctypes.c_char_p(os.fsencode(path)),
        ctypes.byref(how),
        ctypes.c_size_t(ctypes.sizeof(how)),
    )
    if fd < 0:
        _raise_errno(path)
    return fd


def _ns(t: _Timestamp) -> int:
    return t.tv_sec * 1_000_000_000 + t.tv_nsec


def stat_at(dir_fd: int, name: str = "") -> RawStat:
    """Return the stat of `name` in directory `dir_fd`, or of `dir_fd` itself if `name` is "".

    Symlinks are not followed. Uses statx(2) for the birth time; without it (libc older than
    glibc 2.28) `btime_ns` is None. Raises OSError.
    """
    if _statx_fn is None:
        st = os.stat(name, dir_fd=dir_fd, follow_symlinks=False) if name else os.fstat(dir_fd)
        return RawStat(
            st.st_ino, st.st_mode, st.st_uid, st.st_gid, st.st_nlink, st.st_size,
            st.st_atime_ns, st.st_mtime_ns, st.st_ctime_ns, None,
        )  # fmt: skip
    buf = _Statx()
    flags = _AT_SYMLINK_NOFOLLOW if name else _AT_EMPTY_PATH
    if _statx_fn(dir_fd, os.fsencode(name), flags, _STATX_BASIC_STATS | _STATX_BTIME, buf):
        _raise_errno(name)
    return RawStat(
        ino=buf.stx_ino,
        mode=buf.stx_mode,
        uid=buf.stx_uid,
        gid=buf.stx_gid,
        nlink=buf.stx_nlink,
        size=buf.stx_size,
        atime_ns=_ns(buf.stx_atime),
        mtime_ns=_ns(buf.stx_mtime),
        ctime_ns=_ns(buf.stx_ctime),
        btime_ns=_ns(buf.stx_btime) if buf.stx_mask & _STATX_BTIME else None,
    )
```

`src/dwusage/fs/ceph.py`:
```python
"""`RawFs` over a mounted CephFS (kernel or FUSE client)."""

import errno
import os
import stat
import threading
from collections.abc import Sequence
from contextlib import suppress
from typing import Final

from dwusage.fs import syscalls
from dwusage.fs.base import Inspection, Listing, mount_error, symlink_error
from dwusage.paths import is_below

_RESOLVE: Final = syscalls.RESOLVE_BENEATH | syscalls.RESOLVE_NO_SYMLINKS | syscalls.RESOLVE_NO_XDEV
"""Stay below the root, follow no symlink in any path component, enter no other mount.

Production runs as root over user-writable trees, so a directory that a user swaps for a
symlink mid-walk must not redirect reads elsewhere.
"""


class CephFs:
    """Reads directories at or below `roots` through file descriptors.

    Every path is opened relative to its root with openat2(2) and `_RESOLVE`. Without
    openat2 (Linux < 5.6, or blocked by a seccomp filter) a path is opened with O_NOFOLLOW
    and must be on the root's device; `openat2` is then False, and symlinks swapped into
    intermediate components during a walk are not detected.

    Only open, statx, getxattr and getdents are used, so results don't depend on mount
    options such as ``dirstat`` or ``rbytes``. Close the instance (or use it as a context
    manager) to release the root descriptors.
    """

    def __init__(self, roots: Sequence[str]) -> None:
        self.openat2 = True
        self._roots = sorted(roots, key=len, reverse=True)
        self._anchors: dict[str, tuple[int, int]] = {}  # root -> (O_PATH fd, st_dev)
        self._lock = threading.Lock()

    def __enter__(self) -> "CephFs":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            for fd, _ in self._anchors.values():
                os.close(fd)
            self._anchors.clear()

    def inspect(self, path: str, xattr_names: Sequence[str]) -> Inspection:
        try:
            fd = self._open(path, os.O_RDONLY)
        except PermissionError as exc:
            fd = self._open(path, os.O_PATH)  # stat needs no read permission
            try:
                return Inspection(syscalls.stat_at(fd), {}, ("open", exc))
            finally:
                os.close(fd)
        try:
            xattrs: dict[str, str] = {}
            failure = None
            for name in xattr_names:
                try:
                    value = os.getxattr(fd, name)
                except OSError as exc:
                    if exc.errno == errno.ENODATA:
                        continue
                    failure = (name, exc)
                    break
                xattrs[name] = value.decode("utf-8", "surrogateescape").rstrip("\0")
            return Inspection(syscalls.stat_at(fd), xattrs, failure)
        finally:
            os.close(fd)

    def listdir(self, path: str, stat_nondirs: int = 0) -> Listing:
        fd = self._open(path, os.O_RDONLY)
        try:
            subdirs: list[str] = []
            nondirs = []
            truncated = False
            with os.scandir(fd) as entries:
                for entry in entries:
                    if entry.is_dir(follow_symlinks=False):  # d_type: no stat per entry
                        subdirs.append(entry.name)
                    elif len(nondirs) < stat_nondirs:
                        with suppress(FileNotFoundError):  # removed since readdir
                            nondirs.append((entry.name, syscalls.stat_at(fd, entry.name)))
                    elif stat_nondirs:
                        truncated = True
            return Listing(subdirs, nondirs, truncated)
        finally:
            os.close(fd)

    def _open(self, path: str, flags: int) -> int:
        """Open dir `path` with `flags` below its root; see the class docstring."""
        root = next((r for r in self._roots if path == r or is_below(path, r)), None)
        if root is None:
            raise ValueError(f"{path} is not below any root of this CephFs")
        anchor_fd, anchor_dev = self._anchor(root)
        relative = os.path.relpath(path, root)
        flags |= os.O_DIRECTORY | os.O_CLOEXEC
        try:
            if self.openat2:
                try:
                    return syscalls.openat2(anchor_fd, relative, flags, _RESOLVE)
                except OSError as exc:
                    if exc.errno not in (errno.ENOSYS, errno.EPERM):
                        raise
                    self.openat2 = False
            fd = os.open(relative, flags | os.O_NOFOLLOW, dir_fd=anchor_fd)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise symlink_error(path) from None
            if exc.errno == errno.EXDEV:
                raise mount_error(path) from None
            raise
        if os.fstat(fd).st_dev != anchor_dev:
            os.close(fd)
            raise mount_error(path)
        return fd

    def _anchor(self, root: str) -> tuple[int, int]:
        """Return the O_PATH fd and device of `root`, opening it on first use."""
        with self._lock:
            if root not in self._anchors:
                if stat.S_ISLNK(os.lstat(root).st_mode):
                    raise OSError(errno.ELOOP, "is a symlink; give its target instead", root)
                fd = os.open(root, os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
                self._anchors[root] = (fd, os.fstat(fd).st_dev)
            return self._anchors[root]
```

- [ ] **Step 8: Run them to verify they pass**

Run: `pytest tests/test_syscalls.py tests/test_cephfs.py -v`
Expected: all pass. A skip is acceptable only where the local system lacks a feature: no birth time, no user xattrs, no openat2, or running as root (`test_unreadable_dir_still_has_a_stat`).

- [ ] **Step 9: Write `tests/test_fakefs.py`, run it (it fails: no `fakefs`), then write `tests/fakefs.py`**

`tests/test_fakefs.py`:
```python
import pytest

from fakefs import PINNED, FakeFs


def test_xattrs_reflect_tree():
    fs = FakeFs()
    fs.mkdir("/r/a", rbytes=7, rctime=PINNED)
    fs.mkdir("/r/a/x")
    fs.mkfile("/r/a/f")
    found = fs.inspect("/r/a", ["ceph.dir.subdirs", "ceph.dir.rsubdirs", "ceph.dir.rctime"])
    assert found.xattrs == {
        "ceph.dir.subdirs": "1",
        "ceph.dir.rsubdirs": "2",
        "ceph.dir.rctime": "4294967295.000000000",
    }
    assert fs.inspect("/r/a", ["ceph.quota.max_bytes"]).xattrs == {}
    listing = fs.listdir("/r/a", stat_nondirs=5)
    assert listing.subdirs == ["x"] and [name for name, _ in listing.nondirs] == ["f"]


def test_rctime_defaults_to_newest_of_ctime_and_mtime():
    fs = FakeFs()
    fs.mkdir("/r", ctime=10.0, mtime=PINNED)
    assert fs.inspect("/r", ["ceph.dir.rctime"]).xattrs["ceph.dir.rctime"].startswith("42949")


def test_mkdir_on_existing_dir_keeps_children():
    fs = FakeFs()
    fs.mkdir("/r/a/x")
    fs.mkdir("/r/a", rbytes=5)
    assert fs.listdir("/r/a").subdirs == ["x"]


def test_failures_mimic_cephfs():
    fs = FakeFs()
    fs.mkdir("/r/secret", deny=True)
    fs.symlink("/r/link")
    fs.mkdir("/r/mnt", mount=True)
    found = fs.inspect("/r/secret", ["ceph.dir.rbytes"])
    assert found.xattr_failure[0] == "open" and isinstance(found.xattr_failure[1], PermissionError)
    with pytest.raises(PermissionError):
        fs.listdir("/r/secret")
    with pytest.raises(OSError, match="symlink, not followed"):
        fs.inspect("/r/link", [])
    with pytest.raises(OSError, match="other filesystem"):
        fs.inspect("/r/mnt", [])
    with pytest.raises(FileNotFoundError):
        fs.inspect("/r/missing", [])
    failure = FakeFs(ceph=False).inspect("/", ["ceph.dir.rbytes"]).xattr_failure
    assert failure[0] == "ceph.dir.rbytes" and "not supported" in str(failure[1])
```

`tests/fakefs.py`:
```python
"""In-memory `RawFs` for tests."""

import errno
import os
import stat
from collections.abc import Sequence
from dataclasses import dataclass, field

from dwusage.fs.base import Inspection, Listing, RawStat, mount_error, symlink_error

NOW = 2_000_000_000.0
"""Observation time used by tests (2033-05-18)."""

PINNED = 4294967295.0
"""A pinned rctime (UINT32_MAX seconds, in 2106)."""


@dataclass
class FakeEntry:
    kind: str  # "dir", "file" or "symlink"
    ctime: float = 1000.0
    mtime: float | None = None  # defaults to ctime
    rbytes: int = 0
    rctime: float | None = None  # dirs only; defaults to the newer of ctime and mtime
    deny: bool = False  # unreadable dir: opening it for reading fails with EACCES
    mount: bool = False  # another filesystem is mounted here
    ino: int = 0
    children: dict[str, "FakeEntry"] = field(default_factory=dict)


def _error(code: int, path: str) -> OSError:
    return OSError(code, os.strerror(code), path)


class FakeFs:
    """`RawFs` over a tree of `FakeEntry` under "/"; xattr counts come from the tree.

    ``listed`` records every path passed to `listdir`, so tests can count readdirs.
    """

    def __init__(self, *, ceph: bool = True) -> None:
        self.ceph = ceph  # False: xattrs fail with EOPNOTSUPP, like a non-CephFS mount
        self.openat2 = True  # like CephFs: symlinks are refused in every path component
        self.listed: list[str] = []
        self._root = FakeEntry("dir", ino=1)
        self._next_ino = 2

    def __enter__(self) -> "FakeFs":  # used like CephFs by the CLI
        return self

    def __exit__(self, *exc_info: object) -> None:
        pass

    def mkdir(self, path: str, **attrs: object) -> None:
        """Create dir `path` and missing parents, or update an existing dir's attributes."""
        self._put(path, "dir", attrs)

    def mkfile(self, path: str, **attrs: object) -> None:
        self._put(path, "file", attrs)

    def symlink(self, path: str) -> None:
        self._put(path, "symlink", {})

    def inspect(self, path: str, xattr_names: Sequence[str]) -> Inspection:
        entry = self._dir(path)
        st = self._stat(entry)
        if entry.deny:
            return Inspection(st, {}, ("open", _error(errno.EACCES, path)))
        xattrs: dict[str, str] = {}
        for name in xattr_names:
            if not self.ceph:
                return Inspection(st, xattrs, (name, _error(errno.EOPNOTSUPP, path)))
            if (value := self._xattr(entry, name)) is not None:
                xattrs[name] = value
        return Inspection(st, xattrs, None)

    def listdir(self, path: str, stat_nondirs: int = 0) -> Listing:
        entry = self._dir(path)
        if entry.deny:
            raise _error(errno.EACCES, path)
        self.listed.append(path)
        subdirs = [name for name, child in entry.children.items() if child.kind == "dir"]
        others = [
            (name, self._stat(child))
            for name, child in entry.children.items()
            if child.kind != "dir"
        ]
        return Listing(subdirs, others[:stat_nondirs], len(others) > stat_nondirs > 0)

    def _put(self, path: str, kind: str, attrs: dict) -> None:
        *parents, name = [part for part in path.split("/") if part]
        node = self._root
        for part in parents:
            if part not in node.children:
                node.children[part] = FakeEntry("dir", ino=self._ino())
            node = node.children[part]
        existing = node.children.get(name)
        if existing is not None and existing.kind == kind:
            for key, value in attrs.items():
                setattr(existing, key, value)
        else:
            node.children[name] = FakeEntry(kind, ino=self._ino(), **attrs)

    def _ino(self) -> int:
        self._next_ino += 1
        return self._next_ino - 1

    def _dir(self, path: str) -> FakeEntry:
        node = self._root
        for part in filter(None, path.split("/")):
            if part not in node.children:
                raise _error(errno.ENOENT, path)
            node = node.children[part]
            if node.kind == "symlink":
                raise symlink_error(path)
            if node.mount:
                raise mount_error(path)
        if node.kind != "dir":
            raise _error(errno.ENOTDIR, path)
        return node

    @staticmethod
    def _stat(entry: FakeEntry) -> RawStat:
        ctime = int(entry.ctime * 1e9)
        mtime = ctime if entry.mtime is None else int(entry.mtime * 1e9)
        kind = {"dir": stat.S_IFDIR, "file": stat.S_IFREG, "symlink": stat.S_IFLNK}[entry.kind]
        return RawStat(
            entry.ino, kind | 0o755, 0, 0, 1, len(entry.children), mtime, mtime, ctime, None
        )

    @staticmethod
    def _xattr(entry: FakeEntry, name: str) -> str | None:
        n_dirs = sum(child.kind == "dir" for child in entry.children.values())
        n_files = len(entry.children) - n_dirs
        newest = max(entry.ctime, entry.mtime or 0.0)
        values = {
            "ceph.dir.entries": len(entry.children),
            "ceph.dir.files": n_files,
            "ceph.dir.subdirs": n_dirs,
            "ceph.dir.rentries": 1 + len(entry.children),  # like CephFS, counts the dir itself
            "ceph.dir.rfiles": n_files,
            "ceph.dir.rsubdirs": 1 + n_dirs,
            "ceph.dir.rbytes": entry.rbytes,
            "ceph.dir.rctime": f"{newest if entry.rctime is None else entry.rctime:.9f}",
        }
        value = values.get(name)
        return None if value is None else str(value)
```

Run: `pytest tests/test_fakefs.py -v`
Expected: first FAIL with `ModuleNotFoundError: No module named 'fakefs'`, then 4 passed.

- [ ] **Step 10: Checkpoint**

Run: `ruff check . && ruff format --check . && pytest -q`
Expected: clean; all pass. Don't commit.

---

### Task 3: Capture walk — visiting, depth listing, errors, per-root limits

**Files:**
- Create: `src/dwusage/capture.py` (first version; Tasks 4 and 5 replace it), `tests/helpers.py`
- Test: `tests/test_capture.py`

**Interfaces:**
- Consumes: `RawFs`, `RawStat` and `describe_oserror` (`fs/base.py`); `xattrs.ALL`; `paths.path_key` and `paths.is_below`.
- Produces (Tasks 4–7 rely on these names):
  - `MAX_DIRS` and `MAX_LIST_ENTRIES`;
  - `Reason` (StrEnum: `root`, `depth`);
  - `CaptureLimits(depth=2, max_dirs=MAX_DIRS, max_list_entries=MAX_LIST_ENTRIES)`;
  - `DirRecord`: `path`, `root`, `depth`, `reason`, `captured_at`, `stat`, `xattrs`, `subdirs`, `skipped`, `errors`, plus `xattr_int(name)`;
  - `CaptureResult`: `records`, `started_at`, `elapsed_s`, `truncated_roots`, plus the `n_errors` property;
  - `Capturer(fs, limits, pool, now=time.time).run(roots) -> CaptureResult`;
  - `check_roots(fs, roots) -> list[str]`;
  - in tests, `helpers.capture(fs, roots, **limits)`.

- [ ] **Step 1: Write the failing tests and the helper**

`tests/helpers.py`:
```python
"""Test helpers that use the code under test (`fakefs` must not, so it can be written first)."""

from concurrent.futures import ThreadPoolExecutor

from dwusage.capture import CaptureLimits, Capturer, CaptureResult
from fakefs import NOW, FakeFs


def capture(fs: FakeFs, roots: list[str], **limits: int) -> CaptureResult:
    """Run a capture of `fs` at time `NOW` with `CaptureLimits(**limits)`."""
    with ThreadPoolExecutor(4) as pool:
        return Capturer(fs, CaptureLimits(**limits), pool, now=lambda: NOW).run(roots)
```

`tests/test_capture.py`:
```python
from dwusage.capture import check_roots
from dwusage.fs.base import Listing
from fakefs import FakeFs
from helpers import capture


def test_depth_lists_only_dirs_above_limit():
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
    assert result.truncated_roots == () and result.n_errors == 0


def test_depth_zero_visits_root_only():
    fs = FakeFs()
    fs.mkdir("/r/a")
    result = capture(fs, ["/r"], depth=0)
    assert list(result.records) == ["/r"]
    assert result.records["/r"].subdirs is None
    assert fs.listed == []


def test_dir_without_subdirs_is_not_read():
    fs = FakeFs()
    fs.mkdir("/r/leaf")
    fs.mkfile("/r/leaf/f")
    result = capture(fs, ["/r"], depth=5)
    assert result.records["/r/leaf"].subdirs == []
    assert fs.listed == ["/r"]


def test_records_are_in_subtree_order():
    fs = FakeFs()
    for path in ("/r/a b", "/r/a/x", "/r/a-b"):
        fs.mkdir(path)
    result = capture(fs, ["/r"], depth=2)
    assert list(result.records) == ["/r", "/r/a", "/r/a/x", "/r/a b", "/r/a-b"]


def test_unreadable_dir_is_recorded_and_not_listed():
    fs = FakeFs()
    fs.mkdir("/r/secret/inner")
    fs.mkdir("/r/secret", deny=True)
    fs.mkdir("/r/ok")
    result = capture(fs, ["/r"], depth=3)
    secret = result.records["/r/secret"]
    assert secret.errors == {"xattr": "open: EACCES: Permission denied"}
    assert secret.stat is not None and secret.subdirs is None
    assert "/r/secret/inner" not in result.records
    assert result.n_errors == 1


class VanishingFs(FakeFs):
    """Lists a subdir "ghost" that is gone by the time it is visited."""

    def listdir(self, path: str, stat_nondirs: int = 0) -> Listing:
        listing = super().listdir(path, stat_nondirs)
        return Listing([*listing.subdirs, "ghost"], listing.nondirs, listing.nondirs_truncated)


def test_dir_removed_after_listing_is_recorded():
    fs = VanishingFs()
    fs.mkdir("/r/a")
    result = capture(fs, ["/r"], depth=1)
    ghost = result.records["/r/ghost"]
    assert ghost.errors["stat"].startswith("ENOENT:")
    assert ghost.stat is None and ghost.subdirs is None


def test_mounts_and_symlinks_are_not_entered():
    fs = FakeFs()
    fs.mkdir("/r/m/inner")
    fs.mkdir("/r/m", mount=True)
    fs.symlink("/r/link")
    result = capture(fs, ["/r"], depth=3)
    assert result.records["/r/m"].errors == {"stat": "EXDEV: other filesystem, not entered"}
    assert "/r/m/inner" not in result.records
    assert "/r/link" not in result.records  # readdir reports it as a non-directory


def test_dir_replaced_by_symlink_after_listing_is_refused():
    class SwapFs(FakeFs):
        def listdir(self, path: str, stat_nondirs: int = 0) -> Listing:
            listing = super().listdir(path, stat_nondirs)
            if path == "/r":
                self.symlink("/r/a")  # swapped between readdir and visit
            return listing

    fs = SwapFs()
    fs.mkdir("/r/a/private")
    result = capture(fs, ["/r"], depth=3)
    assert result.records["/r/a"].errors == {"stat": "ELOOP: symlink, not followed"}
    assert "/r/a/private" not in result.records


def test_max_dirs_applies_per_root_and_stops_listing():
    fs = FakeFs()
    for name in "dcba":
        fs.mkdir(f"/x/{name}/deep")
    fs.mkdir("/y/only")
    result = capture(fs, ["/y", "/x"], depth=2, max_dirs=3)
    assert list(result.records) == ["/x", "/x/a", "/x/b", "/y", "/y/only"]
    assert result.truncated_roots == ("/x",)
    assert "/x/a" not in fs.listed  # its children could never be visited


def test_huge_dirs_are_not_listed():
    fs = FakeFs()
    fs.mkdir("/r/big/sub")
    for i in range(3):
        fs.mkfile(f"/r/big/f{i}")
    result = capture(fs, ["/r"], depth=3, max_list_entries=3)
    big = result.records["/r/big"]
    assert (big.subdirs, big.skipped) == (None, "max_list_entries")
    assert "/r/big" not in fs.listed


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
    fs.symlink("/link")
    assert check_roots(fs, ["/r", "/r/sub", "/missing", "/file", "/link", "/r"]) == [
        "/r is given more than once",
        "/r/sub is inside /r; capture /r with a larger --depth instead",
        "/missing: ENOENT: No such file or directory",
        "/file: ENOTDIR: Not a directory",
        "/link: ELOOP: symlink, not followed",
    ]


def test_check_roots_rejects_non_ceph_and_unreadable():
    fs = FakeFs(ceph=False)
    fs.mkdir("/r")
    assert check_roots(fs, ["/r"]) == ["/r is not on CephFS (no ceph.* extended attributes)"]
    fs = FakeFs()
    fs.mkdir("/r", deny=True)
    assert check_roots(fs, ["/r"]) == ["/r: EACCES: Permission denied"]


def test_check_roots_accepts_good_roots_including_slash():
    fs = FakeFs()
    fs.mkdir("/r")
    fs.mkdir("/rx")
    assert check_roots(fs, ["/r", "/rx"]) == []
    assert check_roots(fs, ["/"]) == []
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/test_capture.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dwusage.capture'`.

- [ ] **Step 3: Implement `src/dwusage/capture.py`**

```python
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
```

- [ ] **Step 4: Run them to verify they pass**

Run: `pytest tests/test_capture.py -v`
Expected: 14 passed.

- [ ] **Step 5: Checkpoint**

Run: `ruff check . && ruff format --check . && pytest -q`. Don't commit.

---

### Task 4: Pinned-rctime chains

A dir is *pinned* when its rctime is more than a day in the future. CephFS raises rctime to the newest ctime **or mtime** below a dir, and never lowers it. Capture follows pinned dirs at any depth, records each pinned dir's entries that have a future mtime or ctime, and classifies the ends of each chain as `culprit`, `stale` or `undetermined`.

**Files:**
- Modify: `src/dwusage/capture.py` (replace it with the version below)
- Test: `tests/test_capture_chain.py`

**Interfaces:**
- Consumes: `rctime.FUTURE_SLACK_S` and `rctime.is_future`.
- Adds to `capture`:
  - `CHAIN_MAX_DIRS` and `PINNED_MAX_FILES`;
  - `Reason.CHAIN`;
  - `ChainEnd` (StrEnum: `culprit`, `stale`, `undetermined`);
  - `CaptureLimits` fields `chain_max_dirs`, `pinned_max_files` and `future_slack_s`;
  - `FutureEntry(name, stat)`, where the name `"."` means the dir itself;
  - `FileScan(scanned, truncated, future)`;
  - `DirRecord.files` and `DirRecord.rctime()`;
  - `CaptureResult` fields `chain_truncated_roots` and `chain_ends: dict[str, ChainEnd]`.
- Changes compared with Task 3:
  - `_visit` records a malformed rctime as an error;
  - `_choose_listed` also lists pinned dirs and charges them to `chain_max_dirs` *before* reading them;
  - `_list` stats the non-directories of pinned dirs in the same readdir pass;
  - `_children` marks chain children;
  - `_chain_ends` is new.

- [ ] **Step 1: Write the failing tests** `tests/test_capture_chain.py`

```python
from dwusage.capture import ChainEnd
from fakefs import PINNED, FakeFs
from helpers import capture


def make_pinned_tree() -> FakeFs:
    """/r, /r/a, /r/a/b are pinned by /r/a/b/bad, a file with a future mtime but normal ctime;
    /r/c and /r/a/ok are healthy."""
    fs = FakeFs()
    for path in ("/r", "/r/a", "/r/a/b"):
        fs.mkdir(path, rctime=PINNED)
    fs.mkdir("/r/c/deep")
    fs.mkdir("/r/a/ok")
    fs.mkfile("/r/a/b/good")
    fs.mkfile("/r/a/b/bad", mtime=PINNED)
    return fs


def test_pinned_chain_is_followed_to_the_culprit_regardless_of_depth():
    result = capture(make_pinned_tree(), ["/r"], depth=0)
    assert result.chain_ends == {"/r/a/b": ChainEnd.CULPRIT}
    assert result.records["/r/a"].reason == "chain"
    assert result.records["/r/a/ok"].subdirs is None  # healthy: visited, not listed
    assert result.records["/r/c"].subdirs is None
    assert "/r/c/deep" not in result.records
    files = result.records["/r/a/b"].files
    assert [entry.name for entry in files.future] == ["bad"]
    assert files.future[0].stat.mtime_ns == int(PINNED * 1e9)
    assert (files.scanned, files.truncated) == (2, False)
    assert result.chain_truncated_roots == ()


def test_only_pinned_dirs_have_their_files_scanned():
    result = capture(make_pinned_tree(), ["/r"], depth=2)
    assert result.records["/r/a"].files is not None
    assert result.records["/r/c"].files is None


def test_dir_with_its_own_future_mtime_is_its_own_culprit():
    fs = FakeFs()
    fs.mkdir("/r", rctime=PINNED)
    fs.mkdir("/r/bad", mtime=PINNED)  # rctime defaults to the newest of ctime and mtime
    fs.mkdir("/r/bad/x")
    result = capture(fs, ["/r"], depth=0)
    assert result.chain_ends == {"/r/bad": ChainEnd.CULPRIT}
    assert [entry.name for entry in result.records["/r/bad"].files.future] == ["."]


def test_pinned_dir_with_nothing_in_the_future_is_stale():
    fs = FakeFs()
    fs.mkdir("/r", rctime=PINNED)  # the entry that pinned it is gone
    fs.mkdir("/r/a")
    fs.mkfile("/r/f")
    result = capture(fs, ["/r"], depth=0)
    assert result.chain_ends == {"/r": ChainEnd.STALE}


def test_unreadable_or_vanished_subdir_makes_the_end_undetermined():
    fs = FakeFs()
    fs.mkdir("/r", rctime=PINNED)
    fs.mkdir("/r/secret", rctime=PINNED, deny=True)  # might be the next link of the chain
    fs.mkdir("/r/ok")
    result = capture(fs, ["/r"], depth=0)
    assert result.chain_ends == {"/r": ChainEnd.UNDETERMINED}


def test_truncated_file_scan_without_culprit_is_undetermined():
    fs = FakeFs()
    fs.mkdir("/r", rctime=PINNED)
    fs.mkfile("/r/a")
    fs.mkfile("/r/b", mtime=PINNED)
    result = capture(fs, ["/r"], depth=0, pinned_max_files=1)
    files = result.records["/r"].files
    assert (files.scanned, files.truncated, files.future) == (1, True, ())
    assert result.chain_ends == {"/r": ChainEnd.UNDETERMINED}


def test_chain_budget_stops_following_and_says_so():
    result = capture(make_pinned_tree(), ["/r"], depth=0, chain_max_dirs=3)
    assert result.chain_truncated_roots == ("/r",)
    assert result.records["/r/a"].skipped == "chain_max_dirs"
    assert result.chain_ends == {}
    assert "/r/a/b" not in result.records


def test_pinned_root_within_depth_is_still_traced():
    result = capture(make_pinned_tree(), ["/r"], depth=5)
    assert result.chain_ends == {"/r/a/b": ChainEnd.CULPRIT}
    assert result.records["/r/a"].reason == "depth"
    assert result.records["/r/c/deep"].reason == "depth"


def test_malformed_rctime_is_an_error_not_healthy():
    fs = FakeFs()
    fs.mkdir("/r", rctime=PINNED)
    fs.mkdir("/r/odd")

    class OddFs(FakeFs):
        def inspect(self, path, xattr_names):
            found = fs.inspect(path, xattr_names)
            if path == "/r/odd":
                found.xattrs["ceph.dir.rctime"] = "garbage"
            return found

        def listdir(self, path, stat_nondirs=0):
            return fs.listdir(path, stat_nondirs)

    result = capture(OddFs(), ["/r"], depth=0)
    assert result.records["/r/odd"].errors == {"rctime": "malformed: 'garbage'"}
    assert result.chain_ends == {"/r": ChainEnd.UNDETERMINED}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/test_capture_chain.py -v`
Expected: FAIL with `ImportError: cannot import name 'ChainEnd' from 'dwusage.capture'`.

- [ ] **Step 3: Replace `src/dwusage/capture.py`**

```python
"""Capture raw CephFS state for offline replay and diagnostics.

A capture walks breadth-first from each root. Every directory reached is *visited*: its
stat and all CephFS xattrs are recorded as raw values. Some are also *listed*: their
subdirectory names are recorded and those subdirectories visited. A directory is listed when

- it is fewer than ``depth`` levels below its root (reason ``depth``);
- its rctime is pinned in the future, at any depth (reason ``chain``). The chain of pinned
  dirs is followed down to its ends, and the non-directory entries of every pinned dir are
  stat-ed to record those with a future mtime or ctime, which pin rctime.

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


class ChainEnd(StrEnum):
    """What a pinned dir at the bottom of a chain shows."""

    CULPRIT = "culprit"  # the dir or one of its entries has a future mtime or ctime
    STALE = "stale"  # nothing visible is in the future: that entry was changed or removed
    UNDETERMINED = "undetermined"  # an unreadable or vanished subdir, or a truncated scan


@dataclass(frozen=True, slots=True)
class CaptureLimits:
    """Bounds of one capture; see the module docstring for what each expansion does."""

    depth: int = 2
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
```

- [ ] **Step 4: Run all capture tests**

Run: `pytest tests/test_capture.py tests/test_capture_chain.py -v`
Expected: 23 passed.

- [ ] **Step 5: Checkpoint**

Run: `ruff check . && ruff format --check . && pytest -q`. Don't commit.

---

### Task 5: Best-first expansion and `--largest`

`best_first` yields a tree's nodes heaviest first and expands a node only when its children could be next. `capture --largest N` uses it to visit N more dirs per root, biggest first. Milestone 6's budget rules will reuse it unchanged.

**Files:**
- Create: `src/dwusage/bestfirst.py`
- Modify: `src/dwusage/capture.py` (replace it with the final version below)
- Test: `tests/test_bestfirst.py`, `tests/test_capture_largest.py`

**Interfaces:**
- Produces: `best_first(start, *, weight, key, expand) -> Iterator[T]`.
- Adds to `capture`:
  - `Reason.LARGEST` and the `CaptureLimits.largest` field;
  - `Capturer._expandable` and `Capturer._expand_largest`, called from `run` once per root in sorted order.

  A dir that doesn't fit in the remaining budget is skipped (`expand` returns `[]`), and smaller dirs are tried.

- [ ] **Step 1: Write the failing tests**

`tests/test_bestfirst.py`:
```python
import itertools

from dwusage.bestfirst import best_first

TREE = {"r": ["a", "b"], "a": ["a1", "a2"], "b": ["b1"], "a1": [], "a2": [], "b1": []}
SIZE = {"r": 100, "a": 60, "b": 40, "a1": 35, "a2": 25, "b1": 39}


def walk(start, size=SIZE, calls=None):
    def expand(node):
        if calls is not None:
            calls.append(node)
        return TREE[node]

    return best_first(start, weight=size.__getitem__, key=str, expand=expand)


def test_yields_heaviest_first_across_branches():
    assert list(walk(["r"])) == ["r", "a", "b", "b1", "a1", "a2"]


def test_expands_lazily():
    calls = []
    assert list(itertools.islice(walk(["r"], calls=calls), 3)) == ["r", "a", "b"]
    # "a" (60) must be expanded before "b" (40) is known to be next; "b" never is.
    assert calls == ["r", "a"]


def test_parents_come_before_children():
    order = list(walk(["r"]))
    parent = {child: node for node, children in TREE.items() for child in children}
    assert all(order.index(parent[n]) < order.index(n) for n in order if n in parent)


def test_ties_are_broken_by_key():
    assert list(walk(["b1", "a1", "a2"], size=dict.fromkeys(SIZE, 1))) == ["a1", "a2", "b1"]


def test_empty_start():
    assert list(walk([])) == []
```

`tests/test_capture_largest.py`:
```python
from fakefs import FakeFs
from helpers import capture


def make_sized_tree() -> FakeFs:
    fs = FakeFs()
    fs.mkdir("/r")
    fs.mkdir("/r/small/s1", rbytes=1)
    fs.mkdir("/r/small", rbytes=1)
    fs.mkdir("/r/big/b1/deep")
    fs.mkdir("/r/big/b2", rbytes=40)
    fs.mkdir("/r/big/b1", rbytes=60)
    fs.mkdir("/r/big", rbytes=100)
    return fs


def test_largest_visits_n_more_dirs_biggest_first():
    result = capture(make_sized_tree(), ["/r"], depth=1, largest=3)
    big, b1 = result.records["/r/big"], result.records["/r/big/b1"]
    assert big.subdirs == ["b1", "b2"] and b1.subdirs == ["deep"]
    assert (b1.reason, result.records["/r/big/b1/deep"].depth) == ("largest", 3)
    assert result.records["/r/small"].subdirs is None


def test_largest_never_exceeds_its_budget():
    result = capture(make_sized_tree(), ["/r"], depth=1, largest=1)
    # /r/big has 2 subdirs and doesn't fit; the next biggest that fits is /r/small.
    assert result.records["/r/big"].subdirs is None
    assert result.records["/r/small"].subdirs == ["s1"]
    assert len(result.records) == 4


def test_largest_zero_adds_nothing():
    result = capture(make_sized_tree(), ["/r"], depth=1)
    assert list(result.records) == ["/r", "/r/big", "/r/small"]


def test_largest_budget_is_per_root():
    fs = make_sized_tree()
    fs.mkdir("/q/only/x")
    fs.mkdir("/q/only", rbytes=5)
    result = capture(fs, ["/r", "/q"], depth=1, largest=2)
    assert result.records["/q/only"].subdirs == ["x"]
    assert result.records["/r/big"].subdirs == ["b1", "b2"]


def test_largest_respects_max_dirs():
    result = capture(make_sized_tree(), ["/r"], depth=1, largest=10, max_dirs=4)
    assert len(result.records) == 4
    assert result.records["/r/small"].subdirs == ["s1"]
    assert result.truncated_roots == ()
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/test_bestfirst.py tests/test_capture_largest.py -v`
Expected: FAIL. Collection stops with `ModuleNotFoundError: No module named 'dwusage.bestfirst'`. Run alone, `test_capture_largest.py` fails with `TypeError: CaptureLimits.__init__() got an unexpected keyword argument 'largest'`.

- [ ] **Step 3: Write `src/dwusage/bestfirst.py`**

```python
"""Best-first expansion of a tree by weight, shared by ``capture --largest`` and budget rules."""

import heapq
from collections.abc import Callable, Hashable, Iterable, Iterator


def best_first[T](
    start: Iterable[T],
    *,
    weight: Callable[[T], float],
    key: Callable[[T], Hashable],
    expand: Callable[[T], Iterable[T]],
) -> Iterator[T]:
    """Yield the `start` nodes and their descendants, heaviest first.

    `expand(node)` returns a node's children. It is called lazily, once every waiting node
    at least as heavy as `node` has been yielded, so a consumer that stops early never pays
    for expansions it didn't need. The order is exact if no child outweighs its parent (true
    for rbytes and rentries), and a node is always yielded after its parent. Ties are
    broken by `key`, which must be unique per node and orderable.
    """
    heap: list[tuple[float, bool, Hashable, T]] = []
    for node in start:
        heapq.heappush(heap, (-weight(node), False, key(node), node))
    while heap:
        neg_weight, is_expansion, node_key, node = heapq.heappop(heap)
        if is_expansion:
            for child in expand(node):
                heapq.heappush(heap, (-weight(child), False, key(child), child))
        else:
            yield node
            # Children weigh no more than their parent: expanding can wait until it is next.
            heapq.heappush(heap, (neg_weight, True, node_key, node))
```

- [ ] **Step 4: Replace `src/dwusage/capture.py` with its final version**

```python
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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pytest tests/test_bestfirst.py tests/test_capture_largest.py tests/test_capture.py tests/test_capture_chain.py -v`
Expected: 33 passed.

- [ ] **Step 6: Checkpoint**

Run: `ruff check . && ruff format --check . && pytest -q`. Don't commit.

---

### Task 6: Capture file — format and `Output`

**Files:**
- Create: `src/dwusage/capture_file.py`
- Test: `tests/test_capture_file.py`

**Interfaces:**
- Consumes: `CaptureLimits`, `CaptureResult` and `DirRecord`.
- Produces:
  - `SCHEMA = 1`;
  - `capture_lines(result, *, roots, limits, host, version, openat2) -> Iterator[dict]`;
  - `Output(path)`:
    - a context manager; `"-"` means stdout;
    - it raises OSError *on creation* for a bad destination;
    - `write(lines)` writes the file and renames it into place;
  - `read_jsonl(path) -> Iterator[dict]`.
- Record layout, which Milestone 2's replay reader will consume:
  ```
  {"type":"capture_start","schema":1,"tool_version":…,"host":…,"roots":[…],"limits":{…},"openat2":…,"started_at":…}
  {"type":"dir","path":…,"root":…,"depth":…,"reason":…,"captured_at":…,"stat":{RawStat fields}|null,
   "xattrs":{name: raw string},"subdirs":[…]|null,"skipped":null|"max_list_entries"|"chain_max_dirs",
   "files":{"scanned":…,"truncated":…,"future":[{"name":…,"stat":{…}}]}|null,"errors":{step: text}}
  {"type":"capture_end","elapsed_s":…,"n_dirs":…,"n_errors":…,"truncated_roots":[…],
   "chain_truncated_roots":[…],"chain_ends":{path: "culprit"|"stale"|"undetermined"}}
  ```

- [ ] **Step 1: Write the failing tests** `tests/test_capture_file.py`

```python
import json
import stat

import pytest

from dwusage.capture import CaptureLimits
from dwusage.capture_file import SCHEMA, Output, capture_lines, read_jsonl
from fakefs import PINNED, FakeFs
from helpers import capture


def lines_for(fs: FakeFs, **limits: int) -> list[dict]:
    result = capture(fs, ["/r"], **limits)
    return list(
        capture_lines(
            result,
            roots=["/r"],
            limits=CaptureLimits(**limits),
            host="h",
            version="v",
            openat2=True,
        )
    )


def test_record_layout():
    fs = FakeFs()
    fs.mkdir("/r", rctime=PINNED)
    fs.mkfile("/r/bad", mtime=PINNED)
    fs.mkdir("/r/a")
    start, root, a, end = lines_for(fs, depth=1)
    assert start["type"] == "capture_start" and start["schema"] == SCHEMA
    assert start["limits"]["depth"] == 1 and start["roots"] == ["/r"] and start["openat2"]
    assert root["type"] == "dir" and root["subdirs"] == ["a"] and root["reason"] == "root"
    assert root["stat"]["ctime_ns"] == 1000 * 10**9
    assert root["xattrs"]["ceph.dir.rctime"] == "4294967295.000000000"
    assert root["files"]["future"][0]["name"] == "bad"
    assert a["subdirs"] is None and a["files"] is None and a["errors"] == {}
    assert a["skipped"] is None
    assert end == {
        "type": "capture_end",
        "elapsed_s": end["elapsed_s"],
        "n_dirs": 2,
        "n_errors": 0,
        "truncated_roots": (),
        "chain_truncated_roots": (),
        "chain_ends": {"/r": "culprit"},
    }


def test_gzip_file_is_private_and_round_trips(tmp_path):
    fs = FakeFs()
    fs.mkdir("/r/a")
    out = tmp_path / "cap.jsonl.gz"
    lines = lines_for(fs, depth=1)
    with Output(str(out)) as output:
        output.write(lines)
    assert out.read_bytes()[:2] == b"\x1f\x8b"  # gzip magic
    assert stat.S_IMODE(out.stat().st_mode) == 0o600
    assert list(read_jsonl(str(out))) == json.loads(json.dumps(lines))
    assert [p.name for p in tmp_path.iterdir()] == ["cap.jsonl.gz"]


def test_plain_file_and_stdout(tmp_path, capsys):
    lines = [{"type": "x", "n": 1}]
    with Output(str(tmp_path / "cap.jsonl")) as output:
        output.write(lines)
    assert (tmp_path / "cap.jsonl").read_text() == '{"type":"x","n":1}\n'
    with Output("-") as output:
        output.write(lines)
    assert capsys.readouterr().out == '{"type":"x","n":1}\n'


def test_bad_destinations_fail_before_anything_is_written(tmp_path):
    with pytest.raises(IsADirectoryError):
        Output(str(tmp_path))
    with pytest.raises(IsADirectoryError):
        Output(str(tmp_path / "new") + "/")
    with pytest.raises(FileNotFoundError):
        Output(str(tmp_path / "missing" / "cap.jsonl"))
    assert list(tmp_path.iterdir()) == []


def test_unwritten_or_interrupted_output_leaves_nothing(tmp_path):
    with Output(str(tmp_path / "cap.jsonl.gz")):
        pass

    def lines():
        yield {"type": "capture_start"}
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt), Output(str(tmp_path / "cap.jsonl")) as output:
        output.write(lines())
    assert list(tmp_path.iterdir()) == []


def test_odd_names_round_trip(tmp_path):
    fs = FakeFs()
    fs.mkdir("/r/new\nline")
    fs.mkdir("/r/latin1-\udce9")  # undecodable byte 0xE9, as os.scandir returns it
    out = tmp_path / "cap.jsonl"
    with Output(str(out)) as output:
        output.write(lines_for(fs, depth=1))
    paths = [line["path"] for line in read_jsonl(str(out)) if line["type"] == "dir"]
    assert paths == ["/r", "/r/latin1-\udce9", "/r/new\nline"]
    assert len(out.read_text().splitlines()) == 5  # a newline in a name doesn't split a record


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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/test_capture_file.py -v`
Expected: 7 passed.

- [ ] **Step 5: Checkpoint**

Run: `ruff check . && ruff format --check . && pytest -q`. Don't commit.

---

### Task 7: `dwusage capture` command

**Files:**
- Modify: `src/dwusage/cli.py` and `tests/test_cli.py` (replace both with the versions below)

**Interfaces:**
- Consumes: `CephFs`; `Capturer`, `CaptureLimits`, `CaptureResult`, `ChainEnd` and `check_roots`; `Output` and `capture_lines`; `describe_oserror`.
- Produces:
  - `cli.main(argv) -> int`, which lets KeyboardInterrupt propagate;
  - `cli.entry()`, which makes SIGTERM act like Ctrl-C and ends with `os._exit` so it never waits for blocked worker threads;
  - `cli.capture_summary(result, *, n_roots, output, openat2) -> list[str]`, which is pure.
  - Tests replace `cli.CephFs` with a factory that returns a `FakeFs`.

- [ ] **Step 1: Write the failing tests.** Replace `tests/test_cli.py`:

```python
import json

import pytest

from dwusage import __version__, cli
from dwusage.capture_file import read_jsonl
from fakefs import PINNED, FakeFs
from helpers import capture


def test_version(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"dwusage {__version__}"


def test_command_is_required():
    with pytest.raises(SystemExit) as exc:
        cli.main([])
    assert exc.value.code == 2


def squash(text: str) -> str:
    return " ".join(text.split())


def use(monkeypatch, fs: FakeFs) -> FakeFs:
    monkeypatch.setattr(cli, "CephFs", lambda roots: fs)
    return fs


@pytest.fixture
def fake(monkeypatch):
    fs = FakeFs()
    fs.mkdir("/r", rctime=PINNED)
    fs.mkdir("/r/a")
    fs.mkdir("/r/b", rctime=PINNED)
    fs.mkfile("/r/b/bad", mtime=PINNED)
    return use(monkeypatch, fs)


def test_capture_writes_file_and_summary(fake, tmp_path, capsys):
    out = tmp_path / "c.jsonl.gz"
    assert cli.main(["capture", "/r", "--depth", "1", "-o", str(out)]) == 0
    captured = capsys.readouterr()
    assert captured.out == ""
    err = squash(captured.err)
    assert "captured 3 dirs under 1 root" in err
    assert "along 1 chain: 1 end at entries with a future mtime or ctime" in err
    assert "(e.g. /r/b/bad, mtime 2106-02-07)" in err
    assert f"zcat {out} | jq" in err
    paths = [line["path"] for line in read_jsonl(str(out)) if line["type"] == "dir"]
    assert paths == ["/r", "/r/a", "/r/b"]


def test_capture_to_stdout(fake, capsys):
    assert cli.main(["capture", "/r", "--depth", "0", "-o", "-"]) == 0
    lines = [json.loads(raw) for raw in capsys.readouterr().out.splitlines()]
    assert lines[0]["type"] == "capture_start" and lines[1]["path"] == "/r"


def test_bad_root_is_an_error(fake, tmp_path, capsys):
    assert cli.main(["capture", "/missing", "-o", str(tmp_path / "c.jsonl")]) == 1
    assert capsys.readouterr().err.startswith("ERROR: /missing: ENOENT")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("output", ["no/c.jsonl", "", "dir/"])
def test_bad_output_fails_before_capturing(fake, tmp_path, capsys, output):
    (tmp_path / "dir").mkdir()
    assert cli.main(["capture", "/r", "-o", str(tmp_path / output)]) == 1
    assert squash(capsys.readouterr().err).startswith("ERROR: cannot write")
    assert fake.listed == []


@pytest.mark.parametrize("arg", ["--depth=-1", "--workers=0", "--max-dirs=x"])
def test_bad_numbers_are_usage_errors(fake, arg):
    with pytest.raises(SystemExit) as exc:
        cli.main(["capture", "/r", arg, "-o", "-"])
    assert exc.value.code == 2


def test_limit_reached_exits_3_and_names_the_root(fake, capsys):
    assert cli.main(["capture", "/r", "--depth", "1", "--max-dirs", "1", "-o", "-"]) == 3
    assert "WARNING: --max-dirs reached under /r;" in squash(capsys.readouterr().err)


def test_stale_and_undetermined_chains_are_counted(monkeypatch, capsys):
    fs = FakeFs()
    fs.mkdir("/r/stale", rctime=PINNED)
    fs.mkdir("/r/stale/a")
    fs.mkdir("/r/unknown", rctime=PINNED)
    fs.mkdir("/r/unknown/secret", rctime=PINNED, deny=True)
    use(monkeypatch, fs)
    assert cli.main(["capture", "/r", "--depth", "1", "-o", "-"]) == 0
    err = squash(capsys.readouterr().err)
    assert "along 2 chains: 1 are stale" in err and "1 are undetermined" in err


def test_interrupt_removes_partial_output(monkeypatch, tmp_path, capsys):
    class InterruptingFs(FakeFs):
        def inspect(self, path, xattr_names):
            if path == "/r/a":
                raise KeyboardInterrupt
            return super().inspect(path, xattr_names)

    fs = InterruptingFs()
    fs.mkdir("/r/a")
    use(monkeypatch, fs)
    with pytest.raises(KeyboardInterrupt):
        cli.main(["capture", "/r", "-o", str(tmp_path / "c.jsonl")])
    assert capsys.readouterr().err == "ERROR: interrupted; nothing written\n"
    assert list(tmp_path.iterdir()) == []


def test_summary_warns_without_openat2():
    fs = FakeFs()
    fs.mkdir("/r")
    lines = cli.capture_summary(capture(fs, ["/r"]), n_roots=1, output="-", openat2=False)
    assert any(line.startswith("WARNING: openat2 is unavailable") for line in lines)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/test_cli.py -v`
Expected: the two Task 1 tests pass; the others FAIL with `AttributeError: <module 'dwusage.cli' ...> has no attribute 'CephFs'` (from the fixture) or `... has no attribute 'capture_summary'`.

- [ ] **Step 3: Replace `src/dwusage/cli.py`**

```python
"""Command-line interface: parses arguments, calls the library, prints results."""

import argparse
import os
import signal
import socket
import sys
from collections import Counter
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import UTC, datetime
from typing import Final, NoReturn

from dwusage import __version__
from dwusage.capture import CaptureLimits, Capturer, CaptureResult, ChainEnd, check_roots
from dwusage.capture_file import Output, capture_lines
from dwusage.fs.base import describe_oserror
from dwusage.fs.ceph import CephFs

DEFAULT_WORKERS: Final = 16
"""Parallel filesystem operations; enough to hide MDS latency without flooding it."""

EXIT_INCOMPLETE: Final = 3
"""Exit status when a run finished but a limit left part of the requested work undone."""

EXIT_INTERRUPTED: Final = 130

CAPTURE_DESCRIPTION: Final = """\
Record the stat, CephFS xattrs and subdirectory names of directories under each ROOT, for
replay and diagnostics without access to the filesystem. Read-only except for OUTPUT.

Dirs fewer than --depth levels below ROOT are listed: their subdirs are recorded and
visited. Dirs whose rctime is in the future (pinned) are listed at any depth, down to the end
of each chain, and their files with a future mtime or ctime are recorded: those pin rctime.
--largest N then visits up to N more dirs per ROOT, listing the biggest dirs first.

Output is JSON Lines (capture_start, one "dir" record per directory, capture_end), mode 0600
because it reveals names and sizes of private directories. Example:
  zcat cap.jsonl.gz | jq -r 'select(.type=="dir" and .errors!={}) | .path'
A summary goes to stderr. Unreadable dirs are recorded in .errors and don't fail the run.
Exit status: 0 done, 1 error, 2 usage error, 3 a limit left work undone, 130 interrupted.
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
    options = (
        ("--depth", nonneg, defaults.depth, "list dirs fewer than N levels below ROOT"),
        ("--largest", nonneg, defaults.largest, "then visit N more dirs per ROOT, biggest first"),
        ("--max-dirs", positive, defaults.max_dirs, "visit at most N dirs per ROOT"),
        (
            "--max-list-entries",
            nonneg,
            defaults.max_list_entries,
            "don't list dirs with more than N entries",
        ),
        (
            "--chain-max-dirs",
            nonneg,
            defaults.chain_max_dirs,
            "visit at most N dirs per ROOT only to follow pinned rctimes",
        ),
        (
            "--pinned-max-files",
            nonneg,
            defaults.pinned_max_files,
            "stat at most N files per pinned dir",
        ),
        ("--workers", positive, DEFAULT_WORKERS, "parallel filesystem operations"),
    )
    for flag, parse, default, text in options:
        metavar = None if flag == "--depth" else "N"
        cap.add_argument(
            flag,
            type=parse,
            default=default,
            metavar=metavar,
            help=f"{text} (default: %(default)s)",
        )
    return parser


def _error(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)


def _date(t: float) -> str:
    try:
        return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%d")
    except (OverflowError, OSError, ValueError):
        return f"{t:.0f}"


def _chain_summary(result: CaptureResult, output: str) -> str | None:
    if not result.chain_ends:
        return None
    counts = Counter(result.chain_ends.values())
    parts = []
    if n := counts[ChainEnd.CULPRIT]:
        path = next(p for p, end in result.chain_ends.items() if end is ChainEnd.CULPRIT)
        entry = result.records[path].files.future[0]
        which = "mtime" if entry.stat.mtime_ns >= entry.stat.ctime_ns else "ctime"
        newest = max(entry.stat.mtime_ns, entry.stat.ctime_ns) / 1e9
        culprit = path if entry.name == "." else os.path.join(path, entry.name)
        parts.append(
            f"{n} end at entries with a future mtime or ctime "
            f"(e.g. {culprit}, {which} {_date(newest)})"
        )
    if n := counts[ChainEnd.STALE]:
        parts.append(f"{n} are stale (that entry was changed or removed; rctime never decreases)")
    if n := counts[ChainEnd.UNDETERMINED]:
        parts.append(f"{n} are undetermined (unreadable or vanished subdirs; run as root)")
    query = "jq -c 'select(.type==\"capture_end\").chain_ends'"
    if output != "-":
        query = f"{'zcat' if output.endswith('.gz') else 'cat'} {output} | {query}"
    return (
        f"rctime is pinned in the future along {len(result.chain_ends)} "
        f"chain{'s' * (len(result.chain_ends) != 1)}: "
        f"{'; '.join(parts)}. List them: {query}"
    )


def capture_summary(
    result: CaptureResult, *, n_roots: int, output: str, openat2: bool
) -> list[str]:
    """Return the stderr summary of a capture: counts, limits reached, pinned rctimes."""
    lines = [
        f"captured {len(result.records)} dirs under {n_roots} root{'s' * (n_roots != 1)} "
        f"in {result.elapsed_s:.0f}s; {result.n_errors} with errors (see .errors)"
    ]
    if result.truncated_roots:
        lines.append(
            f"WARNING: --max-dirs reached under {', '.join(result.truncated_roots)}; "
            "deeper dirs are missing. Raise --max-dirs, or lower --depth or --largest."
        )
    if result.chain_truncated_roots:
        lines.append(
            f"WARNING: --chain-max-dirs reached under {', '.join(result.chain_truncated_roots)}; "
            "some pinned-rctime chains were not followed to the end."
        )
    if not openat2:
        lines.append(
            "WARNING: openat2 is unavailable (Linux < 5.6 or a seccomp filter); symlinks "
            "swapped into intermediate path components during the capture went undetected."
        )
    if chains := _chain_summary(result, output):
        lines.append(chains)
    return lines


def run_capture(args: argparse.Namespace) -> int:
    """Run `dwusage capture`; return the exit status."""
    roots = [os.path.abspath(root) for root in args.roots]
    limits = CaptureLimits(
        depth=args.depth,
        largest=args.largest,
        max_dirs=args.max_dirs,
        max_list_entries=args.max_list_entries,
        chain_max_dirs=args.chain_max_dirs,
        pinned_max_files=args.pinned_max_files,
    )
    with ExitStack() as cleanup:
        fs = cleanup.enter_context(CephFs(roots))
        problems = check_roots(fs, roots)
        try:
            output = cleanup.enter_context(Output(args.output))
        except OSError as exc:
            problems.append(f"cannot write {args.output}: {describe_oserror(exc)}")
        if problems:
            for problem in problems:
                _error(problem)
            return 1
        pool = ThreadPoolExecutor(args.workers)
        writing_stdout = False
        try:
            result = Capturer(fs, limits, pool).run(roots)
            lines = capture_lines(
                result,
                roots=roots,
                limits=limits,
                host=socket.gethostname(),
                version=__version__,
                openat2=fs.openat2,
            )
            writing_stdout = args.output == "-"
            output.write(lines)
        except KeyboardInterrupt:
            _error(
                f"interrupted; {'stdout is incomplete' if writing_stdout else 'nothing written'}"
            )
            raise
        except OSError as exc:  # only writing the output can fail as a whole
            _error(f"cannot write {args.output}: {describe_oserror(exc)}")
            return 1
        finally:
            pool.shutdown(wait=False, cancel_futures=True)
    for line in capture_summary(result, n_roots=len(roots), output=args.output, openat2=fs.openat2):
        print(line, file=sys.stderr)
    return EXIT_INCOMPLETE if result.truncated_roots or result.chain_truncated_roots else 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command line; return the exit status. KeyboardInterrupt propagates."""
    args = build_parser().parse_args(argv)
    match args.command:
        case "capture":
            return run_capture(args)
    raise AssertionError(f"unhandled command {args.command}")


def _interrupt(signum: int, frame: object) -> NoReturn:
    raise KeyboardInterrupt


def entry() -> NoReturn:
    """Console entry point: `main`, but SIGTERM acts like Ctrl-C and either exits at once.

    Worker threads may be blocked in a slow filesystem call; waiting for them would delay
    the exit for minutes, and they hold nothing that needs cleaning up.
    """
    signal.signal(signal.SIGTERM, _interrupt)
    try:
        status = main()
    except KeyboardInterrupt:
        status = EXIT_INTERRUPTED
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(status)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/test_cli.py -v`
Expected: all pass. In `test_bad_output_fails_before_capturing`, `fake.listed == []` proves that the output check runs before any readdir.

- [ ] **Step 5: Check the help text**

Run: `PYTHONPATH=src python3 -m dwusage capture --help`
Expected: the description, then every option with one line of help and its default.

- [ ] **Step 6: Checkpoint**

Run: `ruff check . && ruff format --check . && pytest -q`
Expected: clean; 90 passed (a few may be skipped where the local system lacks a feature). Don't commit.

---

### Task 8: Live verification and README

This task reads the live mounts (`/mnt/ceph1`, `/mnt/ceph2`, mounted read-only) and nothing else. Each command below is self-contained, because shell variables don't survive between separate tool calls. The commands use `--workers 4` to go easy on the production MDS. **Don't install any crontab, write to the mounts, or use sudo.** When reporting results, don't paste paths that contain user names into committed files.

**Files:**
- Modify: `README.md` (replace it with the version below; adjust the crontab depths to what Step 3 measured)

- [ ] **Step 1: Smoke test on a small tree**

```bash
D="${TMPDIR:-/tmp}/dwusage-m1"; mkdir -p "$D"; PYTHONPATH=src python3 -m dwusage capture /mnt/ceph1/volumes --depth 1 --workers 4 -o "$D/volumes.jsonl.gz"; echo "exit $?"; stat -c '%a' "$D/volumes.jsonl.gz"; zcat "$D/volumes.jsonl.gz" | jq -r 'select(.type=="dir") | [.path, (.xattrs["ceph.dir.rbytes"] // "-"), (.errors|tostring), (.stat.btime_ns != null)] | @tsv'
```

Expected:
- exit 0 and mode `600`;
- 4 dirs (`volumes` and its 3 subdirs);
- run as a non-root user, `_deleting` and `_index` show `{"xattr":"open: EACCES: Permission denied"}`;
- `btime` is present (`true`) for all of them.

- [ ] **Step 2: Trace pinned rctimes on `/mnt/ceph1/npx`**

```bash
D="${TMPDIR:-/tmp}/dwusage-m1"; mkdir -p "$D"; time PYTHONPATH=src python3 -m dwusage capture /mnt/ceph1/npx --depth 0 --workers 4 -o "$D/npx.jsonl.gz"; zcat "$D/npx.jsonl.gz" | jq -c 'select(.type=="capture_end") | {n_dirs, n_errors, chain_truncated_roots, ends: ([.chain_ends[]] | group_by(.) | map({(.[0]): length}) | add)}'
```

Expected:
- exit 0 and about 5,000 dirs, in tens of seconds;
- about 23 chains. During planning, as a non-root user, the split was 17 culprit, 4 stale and 2 undetermined.

Then interrupt reruns with SIGINT (Ctrl-C) and SIGTERM after 3 seconds:

```bash
D="${TMPDIR:-/tmp}/dwusage-m1"; mkdir -p "$D"; for sig in INT TERM; do timeout --preserve-status -s "$sig" 3 env PYTHONPATH=src python3 -m dwusage capture /mnt/ceph1/npx --depth 0 --workers 4 -o "$D/npx-interrupted.jsonl.gz"; echo "SIG$sig exit $?"; ls -A "$D" | grep -c interrupted || true; done
```

Expected, for each signal: `ERROR: interrupted; nothing written`, then `exit 130` right after the 3 seconds, then `0`, meaning no output or temporary file is left. (`--preserve-status` keeps the 130; without it `timeout` reports 124.)

- [ ] **Step 3: Measure capture size and time for the real trees**

```bash
D="${TMPDIR:-/tmp}/dwusage-m1"; mkdir -p "$D"; for spec in "/mnt/ceph1/npx/user 2" "/mnt/ceph1/npx/ana 3" "/mnt/ceph1/npx/wipac 3" "/mnt/ceph2/exp 5"; do set -- $spec; out="$D/m$(echo "$1" | tr / _).jsonl.gz"; /usr/bin/time -f "%e s wall" env PYTHONPATH=src python3 -m dwusage capture "$1" --depth "$2" --largest 500 --workers 4 -o "$out"; echo "exit $?"; ls -l "$out"; done
```

Expected: each run finishes with exit 0, or exit 3 with a WARNING naming the limit. Note the dirs, wall time and file size of each. Use them to choose the crontab depths in Step 4, so that each run stays well under its interval.

- [ ] **Step 4: Replace `README.md`** (with the depths chosen from Step 3):

````markdown
# dwusage

Tracks how CephFS directory usage (bytes, file and directory counts, owner, times) changes
over time, for dashboards and alerts built by other tools.

Status: milestone 1, `dwusage capture` only. Design:
`docs/superpowers/specs/2026-10-14-dwusage-design.md`.

## Running

Linux only. No installation is needed for development:

    PYTHONPATH=src python3 -m dwusage --help

Or install it with `pip install -e .`. Full protection against symlinks swapped in during a
walk needs openat2 (Linux 5.6 or later).

## Capturing

`dwusage capture` records raw CephFS state for offline replay and diagnostics (see
`dwusage capture --help`). Take captures repeatedly while the filesystems are reachable: a
series of captures is what later milestones replay as history. Run as root for a complete
view; otherwise unreadable directories are recorded with `EACCES` errors.

Captures are mode 0600 because they reveal names and sizes of directories other users may
not read. Keep them out of the repository (`*.jsonl.gz` is git-ignored).

Example crontab entries (documentation only: install them yourself if you want them).
`flock -n` skips a run while the previous one is still going (keep the lock file in a
directory only you can write); `%` must be escaped:

    0 * * * * cd /path/to/dwusage && flock -n $HOME/dwusage-captures/ceph1.lock env PYTHONPATH=src python3 -m dwusage capture /mnt/ceph1/npx/user /mnt/ceph1/npx/ana /mnt/ceph1/npx/wipac /mnt/ceph1/volumes --depth 3 --largest 2000 --workers 8 -o $HOME/dwusage-captures/ceph1-$(date +\%Y\%m\%dT\%H).jsonl.gz 2>>$HOME/dwusage-captures/capture.log
    30 * * * * cd /path/to/dwusage && flock -n $HOME/dwusage-captures/ceph2.lock env PYTHONPATH=src python3 -m dwusage capture /mnt/ceph2/exp --depth 5 --largest 2000 --workers 8 -o $HOME/dwusage-captures/ceph2-$(date +\%Y\%m\%dT\%H).jsonl.gz 2>>$HOME/dwusage-captures/capture.log

Quick looks:

    zcat cap.jsonl.gz | jq -c 'select(.type=="capture_end")'
    zcat cap.jsonl.gz | jq -r 'select(.type=="dir") | [.path, .xattrs["ceph.dir.rbytes"]] | @tsv'

## Documentation

- `docs/terms.md`: terms used in output, docs and code
- `docs/conventions.md`: conventions for contributors
````

- [ ] **Step 5: Final checkpoint**

Run: `ruff check . && ruff format --check . && pytest -q`
Expected: clean; all pass. Report the results of Steps 1–3 to the user: counts, chain-end classes, timings and sizes. Suggest the crontab, but don't install it. Don't commit.
