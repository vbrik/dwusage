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


def test_nondirs_not_stated_are_reported_as_truncated():
    fs = FakeFs()
    fs.mkfile("/r/f")
    fs.mkdir("/r/d")
    assert fs.listdir("/r").nondirs_truncated
    assert not fs.listdir("/r/d").nondirs_truncated
