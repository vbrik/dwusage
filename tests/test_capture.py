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
