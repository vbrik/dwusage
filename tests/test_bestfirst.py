import itertools

from dwusage.bestfirst import best_first

TREE = {"r": ["a", "b"], "a": ["a1", "a2"], "b": ["b1"], "a1": [], "a2": [], "b1": []}
SIZE = {"r": 100, "a": 60, "b": 40, "a1": 35, "a2": 25, "b1": 39}


def walk(start, size=SIZE, calls=None):
    def expand(node):
        if calls is not None:
            calls.append(node)
        return TREE[node]

    return best_first(start, weight=size.__getitem__, key=str, expand=expand)


def test_yields_heaviest_first_across_branches():
    assert list(walk(["r"])) == ["r", "a", "b", "b1", "a1", "a2"]


def test_expands_lazily():
    calls = []
    assert list(itertools.islice(walk(["r"], calls=calls), 3)) == ["r", "a", "b"]
    # "a" (60) must be expanded before "b" (40) is known to be next; "b" never is.
    assert calls == ["r", "a"]


def test_parents_come_before_children():
    order = list(walk(["r"]))
    parent = {child: node for node, children in TREE.items() for child in children}
    assert all(order.index(parent[n]) < order.index(n) for n in order if n in parent)


def test_ties_are_broken_by_key():
    assert list(walk(["b1", "a1", "a2"], size=dict.fromkeys(SIZE, 1))) == ["a1", "a2", "b1"]


def test_empty_start():
    assert list(walk([])) == []
