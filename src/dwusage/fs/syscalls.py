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
