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
