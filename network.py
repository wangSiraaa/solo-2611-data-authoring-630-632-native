"""Existing small switch register: ordinary ordered changes already work."""
from copy import deepcopy
from hashlib import sha256
import json


class UnsafeConnection(ValueError):
    pass


class SwitchRegister:
    def __init__(self, nodes, edges, closed):
        self.nodes = dict(nodes)
        self.edges = {k: tuple(v) for k, v in edges.items()}
        self.closed = set(closed)
        self.timeline = [(0, self._owners(self.closed))]

    def _owners(self, closed):
        parent = {node: node for node in self.nodes}

        def find(node):
            while parent[node] != node:
                node = parent[node]
            return node

        for edge in sorted(closed):
            left, right = self.edges[edge]
            a, b = find(left), find(right)
            if a == b:
                raise UnsafeConnection('cycle')
            parent[a] = b
        feeders = {}
        for node, kind in self.nodes.items():
            if kind == 'feeder':
                component = find(node)
                if component in feeders:
                    raise UnsafeConnection('two feeders')
                feeders[component] = node
        return {node: feeders.get(find(node)) for node, kind in self.nodes.items()
                if kind == 'load'}

    def preview(self, actions):
        trial = set(self.closed)
        for edge, close in actions:
            if close:
                trial.add(edge)
            else:
                trial.discard(edge)
            self._owners(trial)
        return trial, self._owners(trial)

    def commit(self, at, actions):
        trial, owners = self.preview(actions)
        self.closed = trial
        self.timeline.append((at, owners))

    def publish(self):
        slices = [{'start': at, 'owners': owners}
                  for at, owners in sorted(self.timeline)]
        raw = json.dumps(slices, sort_keys=True).encode()
        return {'product_id': sha256(raw).hexdigest(), 'slices': deepcopy(slices)}
