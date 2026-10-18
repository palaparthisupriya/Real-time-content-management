import threading
from typing import Any, Dict, Optional
from src.services.metrics import metrics


class Node:
    """Doubly Linked List Node to maintain LRU access order."""

    def __init__(self, key: Optional[str] = None, value: Any = None) -> None:
        self.key: Optional[str] = key
        self.value: Any = value
        self.prev: Optional["Node"] = None
        self.next: Optional["Node"] = None


class LRUCache:
    """
    In-memory L1 cache implementing a custom Least Recently Used (LRU) eviction policy
    using a Hash Map (dictionary) and a Doubly Linked List for strict O(1) operations.
    Standard library LRU implementations are strictly avoided.
    """

    def __init__(self, capacity: int = 100) -> None:
        if capacity <= 0:
            raise ValueError("LRUCache capacity must be greater than 0")
        self.capacity: int = capacity
        self._map: Dict[str, Node] = {}
        self._lock = threading.Lock()

        # Dummy head and tail nodes to eliminate edge-case boundary checks
        self.head: Node = Node(None, None)
        self.tail: Node = Node(None, None)
        self.head.next = self.tail
        self.tail.prev = self.head

    def _remove_node(self, node: Node) -> None:
        """Removes an existing node from the doubly linked list in O(1)."""
        prev_node = node.prev
        next_node = node.next
        if prev_node is not None:
            prev_node.next = next_node
        if next_node is not None:
            next_node.prev = prev_node

    def _add_node_to_head(self, node: Node) -> None:
        """Adds a node immediately after the dummy head (marked Most Recently Used) in O(1)."""
        node.prev = self.head
        node.next = self.head.next
        if self.head.next is not None:
            self.head.next.prev = node
        self.head.next = node

    def _move_to_head(self, node: Node) -> None:
        """Moves an existing node to head (most recently used)."""
        self._remove_node(node)
        self._add_node_to_head(node)

    def _pop_tail(self) -> Optional[Node]:
        """Removes and returns the least recently used node (immediately before dummy tail)."""
        res = self.tail.prev
        if res is not None and res != self.head:
            self._remove_node(res)
            return res
        return None

    def get(self, key: str) -> Optional[Any]:
        """
        Retrieves the value for the given key and updates its recency.
        Returns None on cache miss.
        """
        with self._lock:
            node = self._map.get(key)
            if node is None:
                return None

            # Move accessed node to head (most recently used)
            self._move_to_head(node)
            return node.value

    def put(self, key: str, value: Any) -> None:
        """
        Inserts or updates the value for the given key.
        If key exists, updates its value and moves to head.
        If key is new, adds it to head and checks capacity.
        If capacity is exceeded, evicts the least recently used item (tail)
        and increments the l1_evictions metric.
        """
        with self._lock:
            node = self._map.get(key)
            if node is not None:
                # Key exists, update value and move to head
                node.value = value
                self._move_to_head(node)
            else:
                # New key
                new_node = Node(key=key, value=value)
                self._map[key] = new_node
                self._add_node_to_head(new_node)

                # Check eviction
                if len(self._map) > self.capacity:
                    tail_node = self._pop_tail()
                    if tail_node is not None and tail_node.key is not None:
                        del self._map[tail_node.key]
                        metrics.increment_l1_evictions()

    def clear(self) -> None:
        """Clears all entries from the cache."""
        with self._lock:
            self._map.clear()
            self.head.next = self.tail
            self.tail.prev = self.head

    def __len__(self) -> int:
        with self._lock:
            return len(self._map)

    def contains(self, key: str) -> bool:
        with self._lock:
            return key in self._map
