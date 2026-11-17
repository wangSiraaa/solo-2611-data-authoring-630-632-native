"""Validation primitives, canonical hashing and the forest topology."""
from __future__ import annotations

import hashlib
import json

from .errors import UnsafeConnection, ValidationError

# Time, revision, modulus and rate are non-bool integers with fixed ranges.
TIME_MIN, TIME_MAX = 0, 1_000_000
REV_MIN, REV_MAX = 1, 1_000_000
M_MIN, M_MAX = 2, 1_000_000_000
R_MIN, R_MAX = 1, 1_000_000
VALUE_MIN = 0
MAX_NODES, MAX_EDGES = 12, 16
MAX_CHANGES, MAX_POINTS_PER_METER = 20, 200
MAX_METERS = 10
DISCONNECTED = "DISCONNECTED"


def nat_int(value, lo: int, hi: int, name: str) -> int:
    """Validate a non-bool integer inside [lo, hi]."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{name} must be an integer, got {type(value).__name__}")
    if not lo <= value <= hi:
        raise ValidationError(f"{name}={value} outside [{lo}, {hi}]")
    return value


def state_int(value, name: str) -> int:
    """Edge targets and booleans accept 0/1 (including bool) and normalize to int."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int) and value in (0, 1):
        return value
    raise ValidationError(f"{name} must be 0/1, got {value!r}")


def identifier(value, name: str) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValidationError(f"{name} must be a non-empty str/int id")
    text = str(value)
    if not text:
        raise ValidationError(f"{name} must be non-empty")
    return text


def canonical_hash(payload) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class DSU:
    def __init__(self, nodes):
        self.parent = {n: n for n in nodes}
        self.rank = {n: 0 for n in nodes}

    def find(self, x):
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != x:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return False
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1
        return True


class Topology:
    """Immutable-ish view of a facility; only the closed-edge set moves.

    A legal connection graph is an undirected forest and every connected
    component contains at most one feeder.  Loads in a feeder-less component
    are de-energized.
    """

    def __init__(self, nodes: dict, edges: dict, initial_closed: set):
        if not 1 <= len(nodes) <= MAX_NODES:
            raise ValidationError(f"node count must be 1..{MAX_NODES}")
        if len(edges) > MAX_EDGES:
            raise ValidationError(f"at most {MAX_EDGES} registered edges")
        kinds = set(nodes.values())
        if not kinds <= {"feeder", "junction", "load"}:
            raise ValidationError(f"unknown node kind in {kinds}")
        for edge_id, endpoints in edges.items():
            if (not isinstance(endpoints, (list, tuple)) or len(endpoints) != 2
                    or endpoints[0] not in nodes or endpoints[1] not in nodes):
                raise ValidationError(f"edge {edge_id!r} endpoints must be two known nodes")
            if endpoints[0] == endpoints[1]:
                raise ValidationError(f"self-loop edge {edge_id!r} is not a registered edge")
        if not initial_closed <= set(edges):
            raise ValidationError("initial closed set references an unknown edge")
        self.nodes = dict(nodes)
        self.edges = {eid: tuple(endpoints) for eid, endpoints in edges.items()}
        self.loads = sorted(n for n, k in nodes.items() if k == "load")
        self.feeders = sorted(n for n, k in nodes.items() if k == "feeder")
        # The initial graph (effective at minute 0) must itself be legal.
        self.evaluate(initial_closed)

    def evaluate(self, closed: set) -> dict:
        """Return load -> feeder (None = de-energized) or raise UnsafeConnection."""
        dsu = DSU(self.nodes)
        for edge_id in closed:
            u, v = self.edges[edge_id]
            if not dsu.union(u, v):
                raise UnsafeConnection(f"closing edge {edge_id!r} creates a cycle")
        feeder_of_component: dict = {}
        for feeder in self.feeders:
            root = dsu.find(feeder)
            if root in feeder_of_component:
                raise UnsafeConnection(
                    f"component contains two feeders {feeder_of_component[root]!r}, {feeder!r}")
            feeder_of_component[root] = feeder
        return {load: feeder_of_component.get(dsu.find(load)) for load in self.loads}

    def apply_actions(self, closed: set, actions: list) -> set:
        """Apply a whole action group at once; open/close order is irrelevant."""
        trial = set(closed)
        seen = set()
        for edge_id, target in actions:
            if edge_id not in self.edges:
                raise ValidationError(f"unknown edge id {edge_id!r}")
            if edge_id in seen:
                raise ValidationError(f"edge {edge_id!r} specified twice in one change")
            seen.add(edge_id)
            if target == 1:
                trial.add(edge_id)
            else:
                trial.discard(edge_id)
        return trial
