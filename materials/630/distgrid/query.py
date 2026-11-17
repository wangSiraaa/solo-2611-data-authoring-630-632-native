"""Time-of-use energy queries pinned to one connection product + one reading cut.

A query is a continuous-time measure on [start, end).  Both the connection
timeline (from the immutable product) and the reading-segment boundaries
(from the immutable cut) split the interval; each resulting piece is then
attributed to a feeder, to DISCONNECTED energy, or to unknown time.  Isolated
half-open endpoints contribute no duration.
"""
from __future__ import annotations

import json
from fractions import Fraction

from .errors import ProductError, ValidationError
from .meters import MeterWarehouse
from .topology import (DISCONNECTED, TIME_MAX, canonical_hash, nat_int)


class QueryEngine:
    def __init__(self, store):
        self.store = store
        self.warehouse = MeterWarehouse(store)

    # -- helpers ----------------------------------------------------------
    def _product(self, product_id):
        spec = self.store.product(product_id)
        if spec is None:
            raise ProductError(f"unknown product {product_id!r}")
        return spec

    @staticmethod
    def _owner_on(slices, minute):
        owners = slices[0]["owners"]
        for slc in slices:
            if slc["start"] <= minute:
                owners = slc["owners"]
            else:
                break
        return owners

    def run(self, product_id, start, end, max_gap, cut_id=None):
        start = nat_int(start, 0, TIME_MAX - 1, "start")
        end = nat_int(end, 1, TIME_MAX, "end")
        if not start < end:
            raise ValidationError("require 0 <= start < end <= 1000000")
        max_gap = nat_int(max_gap, 1, 1_000_000, "max_gap")
        spec = self._product(product_id)
        if cut_id is None:
            cut_id = self.store.max_cut_id()
        cut = self.warehouse.get_cut(cut_id)
        slices = spec["slices"]

        facet = canonical_hash({"product_id": product_id, "cut_id": cut_id,
                                "start": start, "end": end, "max_gap": max_gap})
        prior = self.store.query_row(facet)
        if prior is not None:
            return json.loads(prior["result"])

        energy: dict = {}
        # Every load appearing in the product gets a stats row, even un-metered.
        load_stats = {load: {"known_minutes": 0, "unknown_minutes": 0}
                      for load in self._all_loads(spec)}
        pieces_out = []

        for meter_id, meter_cut in cut["meters"].items():
            segments = self.warehouse.build_segments(cut, meter_id)
            load = meter_cut["load"]
            # Reading boundaries inside (start, end); points outside coverage
            # simply leave the meter unknown there.
            boundaries = {start, end}
            for slc in slices:
                if start < slc["start"] < end:
                    boundaries.add(slc["start"])
            for seg in segments:
                for t in (seg["start"], seg["end"]):
                    if start < t < end:
                        boundaries.add(t)
            ordered = sorted(boundaries)

            for lo, hi in zip(ordered, ordered[1:]):
                if lo >= hi:
                    continue
                mid = lo  # constant connection state on [lo, hi); left-closed
                owners = self._owner_on(slices, mid)
                owner = owners.get(load)
                key = owner if owner is not None else DISCONNECTED
                seg = self._segment_covering(segments, lo, hi)
                piece = {"meter_id": meter_id, "load": load, "start": lo,
                         "end": hi, "duration": hi - lo, "owner": key}
                duration = hi - lo
                if seg is None or seg["kind"] != "normal":
                    piece["status"] = "unknown"
                    piece["energy"] = "0"
                    load_stats[load]["unknown_minutes"] += duration
                elif seg["gap"] > max_gap:
                    # The whole segment is unknown when its gap exceeds max_gap.
                    piece["status"] = "unknown"
                    piece["energy"] = "0"
                    piece["reason"] = "gap exceeds max_gap"
                    load_stats[load]["unknown_minutes"] += duration
                else:
                    share = seg["rate_energy"] * duration
                    piece["status"] = "known"
                    piece["energy"] = str(share)
                    energy[key] = energy.get(key, Fraction(0)) + share
                    load_stats[load]["known_minutes"] += duration
                pieces_out.append(piece)

        # Un-metered loads: their entire queried span is unknown, never zero.
        metered_loads = {m["load"] for m in cut["meters"].values()}
        for load in load_stats:
            if load not in metered_loads:
                load_stats[load]["unknown_minutes"] += end - start

        result = {
            "query_id": facet,
            "product_id": product_id,
            "cut_id": cut_id,
            "start": start, "end": end, "max_gap": max_gap,
            "energy": {k: str(v) for k, v in sorted(energy.items())},
            "load_minutes": {
                load: {"known_minutes": st["known_minutes"],
                       "unknown_minutes": st["unknown_minutes"]}
                for load, st in sorted(load_stats.items())},
            "pieces": pieces_out,
            "retracted_points_preserved": cut.get("retracted_kept", []),
        }
        self.store.insert_query(facet, product_id, cut_id, start, end,
                                max_gap, result)
        self.store.audit("query_run",
                         {"product_id": product_id, "cut_id": cut_id}, "recorded")
        self.store.commit()
        return result

    @staticmethod
    def _all_loads(spec):
        return sorted(n for n, kind in spec["facility"]["nodes"].items()
                      if kind == "load")

    @staticmethod
    def _segment_covering(segments, lo, hi):
        for seg in segments:
            if seg["start"] <= lo and hi <= seg["end"]:
                return seg
        return None

    # -- facet verification ----------------------------------------------
    def verify_facet(self, query_id) -> dict:
        """Recompute a stored query and compare it with its recorded result."""
        row = self.store.query_row(query_id)
        if row is None:
            raise ProductError(f"unknown query {query_id!r}")
        recomputed = self.run(row["product_id"], row["start"], row["end"],
                              row["max_gap"], cut_id=row["cut_id"])
        stored = json.loads(row["result"])
        return {"query_id": query_id,
                "matches": recomputed == stored,
                "stored": stored, "recomputed": recomputed}

    @staticmethod
    def check_product_content(product_id, claimed_spec) -> None:
        """Raise if a consumer's internal connection content is not the product.

        This is how "request new product id but silently use old connection
        content" becomes visible: the id is a canonical hash over the full
        product content, so re-hashing what the consumer actually holds must
        reproduce it.
        """
        claimed_hash = canonical_hash({
            "facility": claimed_spec["facility"],
            "changes": claimed_spec["changes"],
            "slices": claimed_spec["slices"]})
        if claimed_hash != product_id:
            raise ValidationError(
                "product content mismatch: id does not match supplied connection"
                f" content (id={product_id[:12]}..., content={claimed_hash[:12]}...)")
