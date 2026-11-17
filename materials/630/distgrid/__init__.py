"""High-level facade: one SQLite file holds the whole re-openable system."""
from __future__ import annotations

from .errors import (DistGridError, LedgerRejected, MeterRejected,
                     ProductError, ValidationError)
from .ledger import SwitchRegister
from .meters import MeterWarehouse
from .query import QueryEngine
from .store import Store
from .topology import DISCONNECTED, UnsafeConnection

__all__ = ["DistGrid", "SwitchRegister", "MeterWarehouse", "QueryEngine",
           "Store", "DistGridError", "ValidationError", "LedgerRejected",
           "MeterRejected", "ProductError", "UnsafeConnection",
           "DISCONNECTED"]


class DistGrid:
    """Create with a facility spec, or reopen an existing database file."""

    def __init__(self, path, setup=None):
        self.path = str(path)
        self.store = Store(self.path)
        if setup is not None:
            if not self.store.fresh:
                raise ValidationError(
                    f"database {self.path!r} already exists; reopen without setup")
            self._init_facility(setup)
        elif self.store.fresh:
            raise ValidationError(
                f"database {self.path!r} is empty; a facility setup is required")
        self.ledger = SwitchRegister(self.store)
        self.meters = MeterWarehouse(self.store)
        self.queries = QueryEngine(self.store)
        # Cut 0 always exists so even the first query has a named facet.
        self.meters.ensure_initial_cut()

    def _init_facility(self, setup):
        nodes = dict(setup["nodes"])
        edges = {str(e): tuple(p) for e, p in setup["edges"].items()}
        closed = {str(e) for e in setup.get("closed", [])}
        meter_specs = {}
        for meter in setup.get("meters", []):
            mid = str(meter["meter_id"])
            if meter["load"] not in nodes or nodes[meter["load"]] != "load":
                raise ValidationError(f"meter {mid!r} must bind to a load node")
            modulus = meter["modulus"]
            meter_specs[mid] = {"load": meter["load"],
                                "modulus": modulus, "rate": meter["rate"]}
        self.store.save_facility(nodes, edges, closed, meter_specs)
        self.store.commit()

    # -- ledger pass-through ---------------------------------------------
    def preview(self, *args, **kwargs):
        return self.ledger.preview(*args, **kwargs)

    def submit(self, *args, **kwargs):
        return self.ledger.submit(*args, **kwargs)

    def publish(self):
        return self.ledger.publish()

    def state_at(self, product_id, minute):
        return self.ledger.state_at(product_id, minute)

    def state_label_at(self, product_id, minute):
        return self.ledger.state_label_at(product_id, minute)

    # -- meter pass-through ----------------------------------------------
    def put_points(self, batch):
        return self.meters.put_points(batch)

    def latest_cut_id(self):
        return self.meters.latest_cut_id()

    def get_cut(self, cut_id):
        return self.meters.get_cut(cut_id)

    # -- query pass-through ----------------------------------------------
    def query(self, product_id, start, end, max_gap, cut_id=None):
        return self.queries.run(product_id, start, end, max_gap, cut_id)

    def verify_facet(self, query_id):
        return self.queries.verify_facet(query_id)

    def close(self):
        self.store.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
