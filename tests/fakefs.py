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
