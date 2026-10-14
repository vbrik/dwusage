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
