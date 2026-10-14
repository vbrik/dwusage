"""Best-first expansion of a tree by weight, shared by ``capture --largest`` and budget rules."""

import heapq
from collections.abc import Callable, Hashable, Iterable, Iterator


def best_first[T](
    start: Iterable[T],
    *,
    weight: Callable[[T], float],
    key: Callable[[T], Hashable],
    expand: Callable[[T], Iterable[T]],
) -> Iterator[T]:
    """Yield the `start` nodes and their descendants, heaviest first.

    `expand(node)` returns a node's children. It is called lazily, once every waiting node
    at least as heavy as `node` has been yielded, so a consumer that stops early never pays
    for expansions it didn't need. The order is exact if no child outweighs its parent (true
    for rbytes and rentries), and a node is always yielded after its parent. Ties are
    broken by `key`, which must be unique per node and orderable.
    """
    heap: list[tuple[float, bool, Hashable, T]] = []
    for node in start:
        heapq.heappush(heap, (-weight(node), False, key(node), node))
    while heap:
        neg_weight, is_expansion, node_key, node = heapq.heappop(heap)
        if is_expansion:
            for child in expand(node):
                heapq.heappush(heap, (-weight(child), False, key(child), child))
        else:
            yield node
            # Children weigh no more than their parent: expanding can wait until it is next.
            heapq.heappush(heap, (neg_weight, True, node_key, node))
