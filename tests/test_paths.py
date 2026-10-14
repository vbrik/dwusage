from dwusage.paths import is_below, path_key


def test_path_key_keeps_subtrees_contiguous():
    paths = ["/r/a b", "/r/a/x", "/r/a", "/r", "/r/a-b"]
    assert sorted(paths, key=path_key) == ["/r", "/r/a", "/r/a/x", "/r/a b", "/r/a-b"]


def test_is_below():
    assert is_below("/r/a", "/r")
    assert is_below("/r", "/")
    assert not is_below("/r", "/r")
    assert not is_below("/", "/")
    assert not is_below("/rx", "/r")
