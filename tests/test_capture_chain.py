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
