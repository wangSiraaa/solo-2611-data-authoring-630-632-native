"""Distribution connection ledger.

Changes arrive as whole open/close groups at integer minutes.  Acceptance is
always decided by replaying *every current head version* from the initial
graph in time order, so "close before open" intermediate states can never
reject a legal transfer and a request can never leave partial actions.
"""
from __future__ import annotations

import json

from .errors import (LedgerRejected, ProductError, UnsafeConnection,
                     ValidationError)
from .topology import (DISCONNECTED, TIME_MAX, canonical_hash, nat_int,
                       identifier, state_int, Topology)


class SwitchRegister:
    def __init__(self, store):
        self.store = store
        facility = store.load_facility()
        self.topo = Topology(facility["nodes"], facility["edges"],
                             facility["initial_closed"])
        self._initial_owners = self.topo.evaluate(facility["initial_closed"])

    # -- input validation -------------------------------------------------
    def _validate_change(self, change_id, revision, minute, actions):
        cid = identifier(change_id, "change_id")
        rev = nat_int(revision, 1, 1_000_000, "revision")
        minute = nat_int(minute, 0, TIME_MAX, "minute")
        if minute <= 0:
            raise ValidationError("change minute must be > 0 (initial graph owns minute 0)")
        if not isinstance(actions, (list, tuple)):
            raise ValidationError("actions must be a list of [edge_id, target] pairs")
        normalized, seen = [], set()
        for pair in actions:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                raise ValidationError("each action must be an [edge_id, target] pair")
            edge_id = identifier(pair[0], "edge_id")
            if edge_id not in self.topo.edges:
                raise ValidationError(f"unknown edge id {edge_id!r}")
            if edge_id in seen:
                raise ValidationError(f"edge {edge_id!r} specified twice in one change")
            seen.add(edge_id)
            normalized.append([edge_id, state_int(pair[1], "action target")])
        # Business content: the target set.  Action order never matters.
        normalized.sort()
        content = {"minute": minute, "actions": normalized}
        return cid, rev, minute, normalized, canonical_hash(content)

    # -- replay -----------------------------------------------------------
    def _projected_heads(self, candidate):
        """Merge one candidate version over the persisted head rows."""
        cid, rev, minute, actions, content_hash = candidate
        heads = {}
        for row in self.store.all_heads():
            heads[row["change_id"]] = {
                "change_id": row["change_id"], "revision": row["revision"],
                "minute": row["minute"],
                "actions": json.loads(row["actions_json"]),
                "content_hash": row["content_hash"]}
        heads[cid] = {"change_id": cid, "revision": rev, "minute": minute,
                      "actions": actions, "content_hash": content_hash}
        return list(heads.values())

    def _replay(self, heads):
        """Replay heads from the initial graph; return slices or raise."""
        ordered = sorted(heads, key=lambda h: (h["minute"], h["change_id"]))
        minutes = [h["minute"] for h in ordered]
        if len(set(minutes)) != len(minutes):
            raise LedgerRejected("two distinct changes occupy the same minute")
        closed = set(self.store.load_facility()["initial_closed"])
        slices = [{"start": 0, "owners": dict(self._initial_owners)}]
        for head in ordered:
            closed = self.topo.apply_actions(closed, head["actions"])
            owners = self.topo.evaluate(closed)  # raises UnsafeConnection
            slices.append({"start": head["minute"],
                           "owners": dict(owners),
                           "change_id": head["change_id"],
                           "revision": head["revision"]})
        slices.sort(key=lambda s: s["start"])
        return closed, slices

    def _evaluate(self, candidate):
        cid, rev, minute, actions, content_hash = candidate
        head_rev = self.store.head_revision(cid)
        if head_rev is not None and rev < head_rev:
            # Structural input was already validated above; the stale version
            # is ignored and must not occupy its minute.
            return {"allowed": True, "decision": "stale",
                    "reason": f"revision {rev} < head {head_rev}",
                    "slices": self._slices_of_current()}
        if head_rev is not None and rev == head_rev:
            row = self.store.change_row(cid, rev)
            if row["content_hash"] != content_hash:
                return {"allowed": False, "decision": "rejected",
                        "reason": "same revision with different content",
                        "slices": None}
            return {"allowed": True, "decision": "replayed",
                    "reason": "identical version replayed",
                    "slices": self._slices_of_current()}
        if self.store.count_heads() >= 20 and head_rev is None:
            return {"allowed": False, "decision": "rejected",
                    "reason": "ledger holds the maximum of 20 changes",
                    "slices": None}
        try:
            _, slices = self._replay(self._projected_heads(candidate))
        except (LedgerRejected, UnsafeConnection, ValidationError) as exc:
            return {"allowed": False, "decision": "rejected",
                    "reason": str(exc), "slices": None}
        return {"allowed": True, "decision": "accepted", "reason": None,
                "slices": slices}

    def _slices_of_current(self):
        heads = [{"change_id": r["change_id"], "revision": r["revision"],
                  "minute": r["minute"], "actions": json.loads(r["actions_json"]),
                  "content_hash": r["content_hash"]}
                 for r in self.store.all_heads()]
        _, slices = self._replay(heads)
        return slices

    # -- public API -------------------------------------------------------
    def preview(self, change_id, revision, minute, actions):
        """Judge a version without touching any state (no meter data needed)."""
        candidate = self._validate_change(change_id, revision, minute, actions)
        result = self._evaluate(candidate)
        result["change_id"], result["revision"] = candidate[0], candidate[1]
        return result

    def submit(self, change_id, revision, minute, actions):
        payload = {"change_id": change_id, "revision": revision,
                   "minute": minute, "actions": actions}
        try:
            candidate = self._validate_change(change_id, revision, minute, actions)
            cid, rev, minute2, actions2, content_hash = candidate
            payload = {"change_id": cid, "revision": rev, "minute": minute2,
                       "actions": actions2}
            verdict = self._evaluate(candidate)
            if verdict["decision"] == "rejected":
                raise LedgerRejected(verdict["reason"])
            if verdict["decision"] == "accepted":
                self.store.insert_change(
                    cid, rev, minute2, json.dumps(actions2, sort_keys=True),
                    content_hash)
            self.store.audit("change_submit", payload, verdict["decision"],
                             verdict["reason"])
            self.store.commit()
            return verdict
        except Exception:
            self.store.rollback()
            self.store.audit("change_submit", payload, "rejected")
            self.store.commit()
            raise

    # -- published products ----------------------------------------------
    def _build_product(self):
        facility = self.store.load_facility()
        head_rows = self.store.all_heads()
        heads = [{"change_id": r["change_id"], "revision": r["revision"],
                  "minute": r["minute"],
                  "actions": sorted(json.loads(r["actions_json"]))}
                 for r in head_rows]
        # One full replay from the initial graph gives the canonical timeline.
        replay_heads = [{"change_id": r["change_id"], "revision": r["revision"],
                         "minute": r["minute"],
                         "actions": json.loads(r["actions_json"])}
                        for r in head_rows]
        _, slices_full = self._replay(replay_heads)
        slices = [{"start": s["start"], "owners": s["owners"]}
                  for s in slices_full]
        content = {"facility": {"nodes": facility["nodes"],
                                "edges": {e: list(p) for e, p in facility["edges"].items()},
                                "closed": sorted(facility["initial_closed"])},
                   "changes": heads, "slices": slices}
        product_id = canonical_hash(content)
        spec = {"product_id": product_id, "facility": content["facility"],
                "changes": heads, "slices": slices, "content_hash": product_id}
        return product_id, spec

    def publish(self):
        """Freeze the current timeline into an immutable, re-openable product."""
        product_id, spec = self._build_product()
        self.store.insert_product(
            product_id, json.dumps(spec, sort_keys=True), product_id,
            self.store.max_cut_id())
        self.store.audit("product_publish", {"product_id": product_id}, "published")
        self.store.commit()
        return spec

    def get_product(self, product_id) -> dict:
        spec = self.store.product(product_id)
        if spec is None:
            raise ProductError(f"unknown product {product_id!r}")
        return spec

    def state_at(self, product_id, minute) -> dict:
        """Load -> feeder name or None (de-energized) at an integer minute."""
        nat_int(minute, 0, TIME_MAX, "minute")
        spec = self.get_product(product_id)
        owners = spec["slices"][0]["owners"]
        for slc in spec["slices"]:
            if slc["start"] <= minute:
                owners = slc["owners"]
            else:
                break
        return {load: (owner if owner is not None else None)
                for load, owner in owners.items()}

    def state_label_at(self, product_id, minute):
        """Same mapping with de-energized loads labeled DISCONNECTED."""
        owners = self.state_at(product_id, minute)
        return {load: (owner if owner is not None else DISCONNECTED)
                for load, owner in owners.items()}
