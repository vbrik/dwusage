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
