"""Unit tests covering the numeric/range/atomicity edge rules of the brief."""
from __future__ import annotations

import tempfile
import unittest
from fractions import Fraction
from pathlib import Path

from distgrid import (DistGrid, DISCONNECTED, LedgerRejected, MeterRejected,
                      ProductError, UnsafeConnection, ValidationError)


def base_setup(**over):
    setup = {
        "nodes": {"F1": "feeder", "F2": "feeder", "J": "junction",
                  "L1": "load", "L2": "load"},
        "edges": {"a": ["F1", "J"], "b": ["F2", "J"], "c": ["J", "L1"],
                  "d": ["L1", "L2"]},
        "closed": ["a", "c", "d"],
        "meters": [
            {"meter_id": "m1", "load": "L1", "modulus": 1000, "rate": 10},
            {"meter_id": "m2", "load": "L2", "modulus": 100, "rate": 5},
        ],
    }
    setup.update(over)
    return setup


class GridTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "g.sqlite3")
        self.grid = DistGrid(self.db, base_setup())

    def tearDown(self):
        self.grid.close()
        self.tmp.cleanup()

    def reopen(self):
        self.grid.close()
        self.grid = DistGrid(self.db)

    # -- facility / range checks -----------------------------------------
    def test_bool_rejected_as_int(self):
        # 时间、修订等是非 bool 整数；True/False 不得冒充 1/0。
        with self.assertRaises(ValidationError):
            self.grid.ledger._validate_change("X", True, 10, [["c", 0]])
        with self.assertRaises(ValidationError):
            self.grid.ledger._validate_change("X", 1, False, [["c", 0]])

    def _validate(self, cid, kwargs):
        self.grid.ledger._validate_change(
            cid, kwargs.get("revision", 1), kwargs.get("minute", 10),
            kwargs.get("actions", [["c", 0]]))

    def test_ranges_enforced(self):
        with self.assertRaises(ValidationError):
            self._validate("X", {"minute": 0})                 # 初始图独占 0
        with self.assertRaises(ValidationError):
            self._validate("X", {"minute": 1_000_001})
        with self.assertRaises(ValidationError):
            self._validate("X", {"revision": 1_000_001})
        with self.assertRaises(ValidationError):
            self._validate("X", {"actions": [["zzz", 0]]})     # 未知边

    def test_self_loop_and_initial_illegal_graph(self):
        bad = base_setup(edges={"a": ["F1", "F1"]}, closed=[])
        with self.assertRaises(ValidationError):
            DistGrid(str(Path(self.tmp.name) / "bad.sqlite3"), bad)
        cyclic = base_setup(
            nodes={"F1": "feeder", "A": "junction", "B": "junction",
                   "L1": "load"},
            edges={"a": ["F1", "A"], "b": ["A", "B"], "c": ["B", "F1"],
                   "d": ["A", "L1"]},
            closed=["a", "b", "c", "d"], meters=[])
        with self.assertRaises(UnsafeConnection):
            DistGrid(str(Path(self.tmp.name) / "cyc.sqlite3"), cyclic)

    def test_meter_must_bind_to_load(self):
        bad = base_setup(meters=[{"meter_id": "mx", "load": "F1",
                                  "modulus": 10, "rate": 1}])
        with self.assertRaises(ValidationError):
            DistGrid(str(Path(self.tmp.name) / "m.sqlite3"), bad)

    def test_reopen_requires_no_setup(self):
        self.reopen()
        with self.assertRaises(ValidationError):
            DistGrid(self.db, base_setup())

    # -- ledger semantics -------------------------------------------------
    def test_preview_does_not_change_state(self):
        before = self.grid.publish()
        verdict = self.grid.preview("P", 1, 10, [["b", 1]])  # 非法双馈线
        self.assertFalse(verdict["allowed"])
        after = self.grid.publish()
        self.assertEqual(before["product_id"], after["product_id"])

    def test_grouped_transfer_intermediate_double_feeder_allowed(self):
        # J 同时可接 F1/F2；整组换接的中间态短暂双馈线不构成拒绝理由。
        verdict = self.grid.preview("T", 1, 30, [["b", 1], ["a", 0]])
        self.assertTrue(verdict["allowed"], verdict.get("reason"))
        self.grid.submit("T", 1, 30, [["b", 1], ["a", 0]])
        p = self.grid.publish()
        self.assertEqual(self.grid.state_label_at(p["product_id"], 29)["L1"], "F1")
        self.assertEqual(self.grid.state_label_at(p["product_id"], 30)["L1"], "F2")

    def test_cycle_rejected_atomically(self):
        # F1-J-L1 已连通；新增一条 J->L1 边制造环，需先注册。
        setup = base_setup(
            edges={"a": ["F1", "J"], "b": ["F2", "J"], "c": ["J", "L1"],
                   "d": ["L1", "L2"], "e": ["J", "L1"]},
            closed=["a", "c", "d"])
        g = DistGrid(str(Path(self.tmp.name) / "cyc2.sqlite3"), setup)
        before = g.publish()
        with self.assertRaises(LedgerRejected):
            g.submit("CY", 1, 10, [["e", 1]])
        self.assertEqual(g.publish()["product_id"], before["product_id"])

    def test_high_revision_can_move_minute_and_actions(self):
        self.grid.submit("C", 1, 10, [["d", 0]])
        self.grid.submit("C", 2, 20, [["d", 1]])
        p = self.grid.publish()
        # 10 分钟不再有事件；L2 在 20 分钟前一直通电。
        self.assertEqual(self.grid.state_label_at(p["product_id"], 15)["L2"], "F1")
        self.assertEqual(self.grid.state_label_at(p["product_id"], 20)["L2"], "F1")

    def test_deenergized_load_reports_disconnected(self):
        self.grid.submit("OFF", 1, 10, [["d", 0]])
        p = self.grid.publish()
        self.assertIsNone(self.grid.state_at(p["product_id"], 10)["L2"])
        self.assertEqual(
            self.grid.state_label_at(p["product_id"], 10)["L2"], DISCONNECTED)

    def test_insert_replace_illegal_any_point_rejects_whole_request(self):
        self.grid.submit("C1", 1, 10, [["d", 0]])
        # 在更早插入一个制造双馈线的变更：重放任一时点违法 -> 整拒。
        with self.assertRaises(LedgerRejected):
            self.grid.submit("C0", 1, 5, [["b", 1]])
        p = self.grid.publish()
        self.assertEqual(self.grid.state_label_at(p["product_id"], 5)["L1"], "F1")

    def test_twenty_change_cap_and_duplicate_edge(self):
        for i in range(20):
            self.grid.submit(f"E{i}", 1, 100 + i, [["d", i % 2]])
        with self.assertRaises(LedgerRejected):
            self.grid.submit("E20", 1, 500, [["d", 0]])
        with self.assertRaises(ValidationError):
            self.grid.submit("EX", 1, 600, [["d", 0], ["d", 1]])

    def test_action_target_equivalence(self):
        self.grid.submit("C", 1, 10, [["d", 0]])
        # 同版同内容重放（目标状态集合相同）。
        verdict = self.grid.submit("C", 1, 10, [["d", 0]])
        self.assertEqual(verdict["decision"], "replayed")

    # -- meter semantics --------------------------------------------------
    def _put_m1(self, points):
        return self.grid.put_points([{"meter_id": "m1", **p} for p in points])

    def test_value_range_and_point_owner_fixed(self):
        with self.assertRaises(ValidationError):
            self._put_m1([{"point_no": 1, "revision": 1, "minute": 0,
                           "value": 1000, "reset": True}])  # M=1000
        self._put_m1([{"point_no": 1, "revision": 1, "minute": 0,
                       "value": 0, "reset": True}])
        with self.assertRaises(MeterRejected):
            self.grid.put_points([{"meter_id": "m2", "point_no": 1,
                                   "revision": 1, "minute": 0, "value": 0,
                                   "reset": True}])  # 点号已属 m1

    def test_same_revision_conflict_kills_batch_even_with_later_high(self):
        self._put_m1([{"point_no": 1, "revision": 1, "minute": 0,
                       "value": 0, "reset": True}])
        before = self.grid.latest_cut_id()
        with self.assertRaises(MeterRejected):
            self.grid.put_points([
                {"meter_id": "m1", "point_no": 2, "revision": 1,
                 "minute": 5, "value": 5},
                {"meter_id": "m1", "point_no": 2, "revision": 1,
                 "minute": 5, "value": 6},          # 同版异文
                {"meter_id": "m1", "point_no": 2, "revision": 9,
                 "minute": 5, "value": 7},          # 后续高版也救不了
            ])
        self.assertEqual(self.grid.latest_cut_id(), before)  # 无半批

    def test_duplicate_effective_minute_rejected(self):
        self._put_m1([
            {"point_no": 1, "revision": 1, "minute": 0, "value": 0,
             "reset": True},
            {"point_no": 2, "revision": 1, "minute": 10, "value": 10}])
        with self.assertRaises(MeterRejected):
            self._put_m1([{"point_no": 3, "revision": 1, "minute": 10,
                           "value": 11}])

    def test_two_hundred_point_cap(self):
        pts = [{"point_no": i + 1, "revision": 1, "minute": i,
                "value": i % 1000, "reset": i == 0} for i in range(200)]
        self._put_m1(pts)
        with self.assertRaises(MeterRejected):
            self._put_m1([{"point_no": 201, "revision": 1, "minute": 500,
                           "value": 1}])

    def test_revision_correction_and_retraction_kept(self):
        self._put_m1([
            {"point_no": 1, "revision": 1, "minute": 0, "value": 0,
             "reset": True},
            {"point_no": 2, "revision": 1, "minute": 10, "value": 999}])
        p = self.grid.publish()
        bad = self.grid.query(p["product_id"], 0, 10, 100)
        self.assertEqual(bad["load_minutes"]["L1"],
                         {"known_minutes": 0, "unknown_minutes": 10})
        self.grid.put_points([{"meter_id": "m1", "point_no": 2,
                               "revision": 2, "minute": 10, "value": 20}])
        good = self.grid.query(p["product_id"], 0, 10, 100)
        self.assertEqual(Fraction(good["energy"]["F1"]), 20)
        # 旧切面仍复现坏段。
        pinned = self.grid.query(p["product_id"], 0, 10, 100, cut_id=1)
        self.assertEqual(pinned, bad)

    def test_stale_revision_validated_then_ignored(self):
        self._put_m1([{"point_no": 1, "revision": 2, "minute": 0,
                       "value": 0, "reset": True}])
        # 合法低修订被忽略，不产生新切面。
        cut = self.grid.latest_cut_id()
        self.grid.put_points([{"meter_id": "m1", "point_no": 1,
                               "revision": 1, "minute": 0, "value": 0,
                               "reset": True}])
        self.assertEqual(self.grid.latest_cut_id(), cut)
        # 非法低修订照样先被校验。
        with self.assertRaises(ValidationError):
            self.grid.put_points([{"meter_id": "m1", "point_no": 1,
                                   "revision": 1, "minute": 0,
                                   "value": 10_000}])

    # -- query semantics --------------------------------------------------
    def test_isolated_endpoint_no_duration(self):
        self._put_m1([
            {"point_no": 1, "revision": 1, "minute": 0, "value": 0,
             "reset": True},
            {"point_no": 2, "revision": 1, "minute": 10, "value": 10}])
        p = self.grid.publish()
        # 末点 10 之后未知：[10,11) 不产生已知时长（孤立端点不贡献）。
        r = self.grid.query(p["product_id"], 10, 11, 100)
        self.assertEqual(r["load_minutes"]["L1"],
                         {"known_minutes": 0, "unknown_minutes": 1})

    def test_unknown_before_first_and_after_last(self):
        self._put_m1([
            {"point_no": 1, "revision": 1, "minute": 10, "value": 0,
             "reset": True},
            {"point_no": 2, "revision": 1, "minute": 20, "value": 10}])
        p = self.grid.publish()
        r = self.grid.query(p["product_id"], 0, 30, 100)
        self.assertEqual(r["load_minutes"]["L1"],
                         {"known_minutes": 10, "unknown_minutes": 20})
        self.assertEqual(Fraction(r["energy"]["F1"]), 10)

    def test_disconnected_energy_attribution_and_unmetered_load(self):
        # 独立设施：L2 挂表（M=1000, R=5）；10 分钟断开 d，L2 断电但表仍有
        # 读数 -> 前半归馈线，后半归 DISCONNECTED；L1 未挂表全程未知。
        setup = base_setup(
            meters=[{"meter_id": "m2", "load": "L2",
                     "modulus": 1000, "rate": 5}])
        g = DistGrid(str(Path(self.tmp.name) / "disc.sqlite3"), setup)
        g.put_points([
            {"meter_id": "m2", "point_no": 1, "revision": 1, "minute": 0,
             "value": 0, "reset": True},
            {"meter_id": "m2", "point_no": 2, "revision": 1, "minute": 20,
             "value": 20}])
        g.submit("OFF", 1, 10, [["d", 0]])
        p = g.publish()
        r = g.query(p["product_id"], 0, 20, 100)
        # m2 的 20 度按连接切半：通电 10 度归 F1，断电 10 度归 DISCONNECTED。
        self.assertEqual(Fraction(r["energy"]["F1"]), 10)
        self.assertEqual(Fraction(r["energy"][DISCONNECTED]), 10)
        # L1 未挂表：整个窗口未知，绝不按零电量算已知。
        self.assertEqual(r["load_minutes"]["L1"],
                         {"known_minutes": 0, "unknown_minutes": 20})
        self.assertEqual(r["load_minutes"]["L2"],
                         {"known_minutes": 20, "unknown_minutes": 0})

    def test_query_input_validation(self):
        p = self.grid.publish()
        with self.assertRaises(ValidationError):
            self.grid.query(p["product_id"], 10, 10, 100)
        with self.assertRaises(ValidationError):
            self.grid.query(p["product_id"], 20, 10, 100)
        with self.assertRaises(ValidationError):
            self.grid.query(p["product_id"], 0, 10, 0)
        with self.assertRaises(ProductError):
            self.grid.query("deadbeef", 0, 10, 100)

    def test_reopen_preserves_products_cuts_and_queries(self):
        self._put_m1([
            {"point_no": 1, "revision": 1, "minute": 0, "value": 0,
             "reset": True},
            {"point_no": 2, "revision": 1, "minute": 10, "value": 30}])
        p = self.grid.publish()
        first = self.grid.query(p["product_id"], 0, 10, 100)
        self.reopen()
        again = self.grid.query(p["product_id"], 0, 10, 100)
        self.assertEqual(first, again)
        self.assertTrue(self.grid.verify_facet(first["query_id"])["matches"])

    def test_unknown_cut_rejected(self):
        p = self.grid.publish()
        with self.assertRaises(ProductError):
            self.grid.query(p["product_id"], 0, 10, 100, cut_id=999)

    def test_exact_rational_sharing(self):
        # 7 度 / 3 分钟这类除不尽的分摊必须精确。
        self.grid.put_points([
            {"meter_id": "m2", "point_no": 1, "revision": 1, "minute": 0,
             "value": 0, "reset": True},
            {"meter_id": "m2", "point_no": 2, "revision": 1, "minute": 3,
             "value": 7}])
        p = self.grid.publish()
        r = self.grid.query(p["product_id"], 0, 3, 100)
        self.assertEqual(Fraction(r["energy"]["F1"]), 7)
        one_minute = self.grid.query(p["product_id"], 0, 1, 100)
        self.assertEqual(Fraction(one_minute["energy"]["F1"]),
                         Fraction(7, 3))

    def test_feederless_island_is_deenergized(self):
        g = DistGrid(str(Path(self.tmp.name) / "island.sqlite3"), base_setup(
            nodes={"F1": "feeder", "A": "junction", "L1": "load",
                   "L2": "load"},
            edges={"a": ["F1", "A"], "c": ["A", "L1"], "d": ["L1", "L2"]},
            closed=["a", "c", "d"]))
        g.submit("OFF", 1, 10, [["c", 0]])
        p = g.publish()
        self.assertEqual(g.state_at(p["product_id"], 10),
                         {"L1": None, "L2": None})
        self.assertEqual(g.state_label_at(p["product_id"], 10),
                         {"L1": DISCONNECTED, "L2": DISCONNECTED})

    def test_transfer_at_query_and_reading_boundary(self):
        g = DistGrid(str(Path(self.tmp.name) / "algn.sqlite3"), base_setup(
            nodes={"F1": "feeder", "F2": "feeder", "L": "load"},
            edges={"a": ["F1", "L"], "b": ["F2", "L"]}, closed=["a"],
            meters=[{"meter_id": "m", "load": "L",
                     "modulus": 1000, "rate": 10}]))
        g.submit("T", 1, 10, [["b", 1], ["a", 0]])
        g.put_points([
            {"meter_id": "m", "point_no": 1, "revision": 1, "minute": 0,
             "value": 0, "reset": True},
            {"meter_id": "m", "point_no": 2, "revision": 1, "minute": 10,
             "value": 20},
            {"meter_id": "m", "point_no": 3, "revision": 1, "minute": 20,
             "value": 60}])
        p = g.publish()
        r = g.query(p["product_id"], 10, 20, 100)
        self.assertEqual(set(r["energy"]), {"F2"})
        self.assertEqual(Fraction(r["energy"]["F2"]), 40)

    def test_empty_action_group_occupies_minute(self):
        verdict = self.grid.submit("EMPTY", 1, 1, [])
        self.assertEqual(verdict["decision"], "accepted")
        with self.assertRaises(LedgerRejected):
            self.grid.submit("OTHER", 1, 1, [["c", 0]])

    def test_meter_batch_validation_failure_is_atomic_and_audited(self):
        before = self.grid.latest_cut_id()
        with self.assertRaises(ValidationError):  # value 越界
            self.grid.put_points([
                {"meter_id": "m1", "point_no": 9, "revision": 1,
                 "minute": 0, "value": 1000, "reset": True}])
        self.assertEqual(self.grid.latest_cut_id(), before)
        audit = self.grid.store.conn.execute(
            "SELECT outcome FROM audit WHERE kind='meter_batch'").fetchall()
        self.assertIn(("rejected",), [tuple(r) for r in audit])

    def test_content_hash_detects_stale_connection_swap(self):
        p = self.grid.publish()
        tampered = dict(p)
        tampered["slices"] = [{"start": 0,
                               "owners": {"L1": "F2", "L2": "F2"}}]
        with self.assertRaises(ValidationError):
            self.grid.queries.check_product_content(p["product_id"], tampered)
        # 正确内容自检通过。
        self.grid.queries.check_product_content(p["product_id"], p)


if __name__ == "__main__":
    unittest.main()
