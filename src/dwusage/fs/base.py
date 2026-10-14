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
