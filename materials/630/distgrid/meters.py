"""Meter fact warehouse.

Cumulative readings are *mod-M counters*.  A point version only means
something through the sequence of its meter's surviving effective points;
connection changes never appear here as readings.  Every accepted batch
produces an immutable reading cut that a query facet can pin forever.
"""
from __future__ import annotations

import json
from fractions import Fraction

from .errors import MeterRejected, ProductError, ValidationError
from .topology import (REV_MAX, REV_MIN, TIME_MAX, TIME_MIN,
                       canonical_hash, identifier, nat_int, state_int)


class MeterWarehouse:
    def __init__(self, store):
        self.store = store
        facility = store.load_facility()
        self.meters = facility["meters"]
        self.load_to_meter = {spec["load"]: mid for mid, spec in self.meters.items()}
        if len(self.meters) > 10:
            raise ValidationError("at most 10 meters")
        # One meter is fixed to one load; we enforce a strict 1:1 binding.
        if len(self.load_to_meter) != len(self.meters):
            raise ValidationError("two meters bound to the same load")

    # -- validation -------------------------------------------------------
    def _validate_point(self, point):
        if not isinstance(point, dict):
            raise ValidationError("each reading must be an object")
        meter_id = identifier(point.get("meter_id"), "meter_id")
        if meter_id not in self.meters:
            raise ValidationError(f"unknown meter {meter_id!r}")
        point_no = nat_int(point["point_no"], 1, 1_000_000, "point_no")
        revision = nat_int(point["revision"], REV_MIN, REV_MAX, "revision")
        minute = nat_int(point["minute"], TIME_MIN, TIME_MAX, "minute")
        modulus = self.meters[meter_id]["modulus"]
        value = nat_int(point["value"], 0, modulus - 1, "value")
        reset = state_int(point.get("reset", 0), "reset")
        retracted = state_int(point.get("retracted", 0), "retracted")
        content = {"minute": minute, "value": value,
                   "reset": reset, "retracted": retracted}
        return {
            "meter_id": meter_id, "point_no": point_no, "revision": revision,
            "minute": minute, "value": value, "reset": reset,
            "retracted": retracted,
            "content_hash": canonical_hash(content),
            "content": content,
        }

    # -- projection -------------------------------------------------------
    def _load_projection(self):
        """meter -> point_no -> head version dict (highest revision wins)."""
        heads: dict = {mid: {} for mid in self.meters}
        for row in self.store.all_points():
            bucket = heads[row["meter_id"]]
            old = bucket.get(row["point_no"])
            if old is None or row["revision"] > old["revision"]:
                bucket[row["point_no"]] = dict(row)
        return heads

    def put_points(self, batch):
        """Apply one whole reading batch or reject the whole thing atomically."""
        validated = []
        try:
            if not isinstance(batch, (list, tuple)) or not batch:
                raise ValidationError("a reading batch must be a non-empty list")
            validated = [self._validate_point(p) for p in batch]
            # Same-version conflicts within the batch are judged in input
            # order: even a later high version cannot rescue the batch.
            in_batch = {}
            for p in validated:
                key = (p["meter_id"], p["point_no"])
                if key in in_batch and in_batch[key]["revision"] == p["revision"]:
                    if in_batch[key]["content_hash"] != p["content_hash"]:
                        raise MeterRejected(
                            f"same revision {p['revision']} with different content"
                            f" for point {p['point_no']} of {p['meter_id']}")
                    raise MeterRejected(
                        f"point {p['point_no']} revision {p['revision']} duplicated"
                        " within the batch")
                in_batch.setdefault(key, p)
                # A point number belongs to one fixed meter forever.
                owner = self.store.point_owner(p["point_no"])
                if owner is not None and owner != p["meter_id"]:
                    raise MeterRejected(
                        f"point_no {p['point_no']} already belongs to meter {owner!r}")

            heads = self._load_projection()
            projected_counts = {mid: len(heads[mid]) for mid in self.meters}
            to_insert, changed = [], False

            for p in validated:  # input order = version interpretation order
                bucket = heads[p["meter_id"]]
                existing = bucket.get(p["point_no"])
                if existing is not None and p["revision"] < existing["revision"]:
                    continue  # validated above; stale version is ignored
                if existing is not None and p["revision"] == existing["revision"]:
                    if existing["content_hash"] != p["content_hash"]:
                        raise MeterRejected(
                            f"same revision with different content for point"
                            f" {p['point_no']} of {p['meter_id']}")
                    continue  # same-version same-content replay
                if existing is None:
                    if projected_counts[p["meter_id"]] >= 200:
                        raise MeterRejected(
                            f"meter {p['meter_id']!r} already holds 200 points")
                    projected_counts[p["meter_id"]] += 1
                bucket[p["point_no"]] = p
                to_insert.append(p)
                changed = True

            # Final per-meter minute uniqueness over effective (non-retracted) points.
            effective = self._effective_points(heads)
            for meter_id, points in effective.items():
                minutes = [p["minute"] for p in points]
                if len(set(minutes)) != len(minutes):
                    raise MeterRejected(
                        f"two effective points of meter {meter_id!r} share a minute")

            if not changed:
                # Pure replay / stale batch: no new rows and no new cut.
                self.store.audit("meter_batch",
                                 {"size": len(validated)}, "ignored")
                self.store.commit()
                return {"cut_id": self.store.max_cut_id(),
                        "accepted": 0,
                        "effective": {mid: len(pts)
                                      for mid, pts in effective.items()}}

            # Persist the genuinely new versions, then freeze the new cut.
            for p in to_insert:
                self.store.insert_point(
                    p["meter_id"], p["point_no"], p["revision"], p["minute"],
                    p["value"], p["reset"], p["retracted"], p["content_hash"])
            cut_id = self.store.next_cut_id()
            snapshot = self._snapshot(effective)
            self.store.insert_cut(cut_id, len(self.store.all_points()),
                                  json.dumps(snapshot, sort_keys=True))
            self.store.audit("meter_batch",
                             {"size": len(validated)}, "accepted")
            self.store.commit()
            return {"cut_id": cut_id, "accepted": len(to_insert),
                    "effective": {mid: len(pts) for mid, pts in effective.items()}}
        except Exception:
            self.store.rollback()
            self.store.audit("meter_batch", {"size": len(validated)}, "rejected")
            self.store.commit()
            raise

    @staticmethod
    def _effective_points(heads):
        effective = {}
        for meter_id, bucket in heads.items():
            pts = [p for p in bucket.values() if not p["retracted"]]
            pts.sort(key=lambda p: (p["minute"], p["point_no"]))
            effective[meter_id] = pts
        return effective

    def _snapshot(self, effective) -> dict:
        return {
            "meters": {
                mid: {"load": spec["load"], "modulus": spec["modulus"],
                      "rate": spec["rate"],
                      "points": [{"point_no": p["point_no"], "minute": p["minute"],
                                  "value": p["value"], "reset": bool(p["reset"])}
                                 for p in effective.get(mid, [])]}
                for mid, spec in self.meters.items()},
            "retracted_kept": [
                {"meter_id": r["meter_id"], "point_no": r["point_no"],
                 "revision": r["revision"], "minute": r["minute"],
                 "value": r["value"], "reset": bool(r["reset"])}
                for r in self.store.all_points() if r["retracted"]],
        }

    # -- cuts -------------------------------------------------------------
    def latest_cut_id(self) -> int:
        return self.store.max_cut_id()

    def get_cut(self, cut_id: int) -> dict:
        cut = self.store.cut(cut_id)
        if cut is None:
            raise ProductError(f"unknown reading cut {cut_id}")
        return cut

    def ensure_initial_cut(self) -> int:
        """Cut 0: the empty reading facet existing before any batch."""
        if self.store.cut(0) is None:
            self.store.insert_cut(0, 0, json.dumps(self._snapshot(
                {mid: [] for mid in self.meters}), sort_keys=True))
            self.store.commit()
        return 0

    # -- segment interpretation ------------------------------------------
    def build_segments(self, cut: dict, meter_id: str):
        """Interpret cumulative points independently of any connection product.

        Segment kinds:
          normal       unique non-negative mod-M difference within R*gap;
          bad          counter difference exceeds R*gap (uniqueness holds);
          unknown      gap > max_gap is the query's concern, but non-unique
                       interpretations (R*gap >= M) are unknown here;
          reset_break  a meter-replacement first point cuts its left segment.
        """
        spec = cut["meters"][meter_id]
        modulus, rate = spec["modulus"], spec["rate"]
        points = spec["points"]
        segments = []
        for left, right in zip(points, points[1:]):
            gap = right["minute"] - left["minute"]
            base = {
                "meter_id": meter_id, "load": spec["load"],
                "start": left["minute"], "end": right["minute"], "gap": gap,
                "left_point": left["point_no"], "right_point": right["point_no"],
                "reset_right": right["reset"],
            }
            if right["reset"]:
                segments.append({**base, "kind": "reset_break",
                                 "energy": None})
                continue
            if rate * gap >= modulus:
                # The number of wraps is not uniquely determined: do not guess.
                segments.append({**base, "kind": "unknown", "energy": None,
                                 "reason": "non-unique wraps"})
                continue
            energy = (right["value"] - left["value"]) % modulus
            if energy > rate * gap:
                segments.append({**base, "kind": "bad", "energy": None,
                                 "reason": "delta exceeds R*gap"})
            else:
                segments.append({**base, "kind": "normal",
                                 "energy": Fraction(energy),
                                 "rate_energy": Fraction(energy, gap)})
        return segments
