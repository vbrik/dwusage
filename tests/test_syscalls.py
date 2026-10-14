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
