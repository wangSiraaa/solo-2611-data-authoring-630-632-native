"""Re-runnable scenario entries for the connection ledger + meter fact store.

Run:  python3 run_scenarios.py [--keep-db] [--out-dir OUT]

Each scenario builds its own SQLite database, closes it, reopens it (proving
the history products / reading cuts / query facets survive a restart) and
asserts the expected business result.  Exit code is non-zero on any failure.
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
import traceback
from fractions import Fraction
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from distgrid import (DistGrid, DISCONNECTED, LedgerRejected,
                      ProductError, ValidationError)

CHECKS: list[tuple[str, bool, str]] = []


def check(name, condition, detail=""):
    CHECKS.append((name, bool(condition), detail))
    flag = "PASS" if condition else "FAIL"
    print(f"  [{flag}] {name}" + (f" -- {detail}" if detail and not condition else ""))
    return condition


# ----------------------------------------------------------------------
def scenario_single_feeder(out_dir: Path):
    print("\n=== 1. 正常单馈线查询（含重开） ===")
    db = out_dir / "single_feeder.sqlite3"
    setup = {
        "nodes": {"F1": "feeder", "J": "junction", "L1": "load"},
        "edges": {"e1": ["F1", "J"], "e2": ["J", "L1"]},
        "closed": ["e1", "e2"],
        "meters": [{"meter_id": "m1", "load": "L1", "modulus": 1000, "rate": 10}],
    }
    grid = DistGrid(db, setup)
    grid.put_points([
        {"meter_id": "m1", "point_no": 1, "revision": 1, "minute": 0,
         "value": 0, "reset": True},
        {"meter_id": "m1", "point_no": 2, "revision": 1, "minute": 10, "value": 30},
        {"meter_id": "m1", "point_no": 3, "revision": 1, "minute": 20, "value": 90},
    ])
    product = grid.publish()
    result = grid.query(product["product_id"], 0, 20, 100)
    grid.close()

    grid = DistGrid(db)  # reopen: no setup argument
    reopened = grid.query(product["product_id"], 0, 20, 100)
    check("重开后查询结果完全一致", reopened == result)
    check("馈线 F1 总电量 90", Fraction(result["energy"]["F1"]) == 90,
          result["energy"])
    check("L1 已知 20 分钟 / 未知 0 分钟",
          result["load_minutes"]["L1"] == {"known_minutes": 20,
                                           "unknown_minutes": 0},
          str(result["load_minutes"]["L1"]))
    # 半区间切分：[0,10) 得 30，[10,20) 得 60，精确有理数。
    half = grid.query(product["product_id"], 5, 15, 100)
    check("线性分摊 [5,15)=15+60=... 得 45",
          Fraction(half["energy"]["F1"]) == Fraction(45), half["energy"])
    grid.close()
    return result


# ----------------------------------------------------------------------
def scenario_same_minute_transfer(out_dir: Path):
    print("\n=== 2. 同一分钟换接（先合后开的整组提交） ===")
    db = out_dir / "transfer.sqlite3"
    setup = {
        "nodes": {"F1": "feeder", "F2": "feeder", "L": "load"},
        "edges": {"a": ["F1", "L"], "b": ["F2", "L"]},
        "closed": ["a"],
        "meters": [{"meter_id": "m", "load": "L", "modulus": 1000, "rate": 10}],
    }
    grid = DistGrid(db, setup)
    # 逐动作的中间状态（先合 b）会出现双馈线，预演必须按整组后的图判定。
    preview = grid.preview("T1", 1, 30, [["b", 1], ["a", 0]])
    check("预演允许整组换接", preview["allowed"] is True, preview.get("reason"))
    before = grid.publish()  # 变更前的正式产品
    verdict = grid.submit("T1", 1, 30, [["b", 1], ["a", 0]])
    check("提交成功", verdict["decision"] == "accepted", verdict)
    after = grid.publish()
    check("两个产品身份不同", before["product_id"] != after["product_id"])
    check("换接前 L 连 F1", grid.state_label_at(after["product_id"], 29)["L"] == "F1")
    check("换接后 L 连 F2", grid.state_label_at(after["product_id"], 30)["L"] == "F2")
    # 动作顺序不构成异文。
    rev = grid.submit("T1", 1, 30, [["a", 0], ["b", 1]])
    check("同版同内容（仅顺序不同）重放", rev["decision"] == "replayed")
    # 一次变更不能重复指定同一边（即使目标状态相同）。
    try:
        grid.submit("T1", 1, 30, [["b", 1], ["a", 0], ["b", 1]])
        check("同边重复指定被拒绝", False)
    except ValidationError:
        check("同边重复指定被拒绝", True)
    grid.close()
    return before, after


def scenario_illegal_double_feeder(out_dir: Path):
    print("\n=== 3. 非法双馈线整批拒绝（已发布历史不变） ===")
    db = out_dir / "illegal.sqlite3"
    setup = {
        "nodes": {"F1": "feeder", "F2": "feeder", "J": "junction", "L": "load"},
        "edges": {"a": ["F1", "J"], "b": ["F2", "J"], "c": ["J", "L"]},
        "closed": ["a", "c"],
        "meters": [{"meter_id": "m", "load": "L", "modulus": 100, "rate": 5}],
    }
    grid = DistGrid(db, setup)
    product_before = grid.publish()
    try:
        grid.submit("BAD", 1, 10, [["b", 1]])  # F1 与 F2 同分量
        check("双馈线变更被拒绝", False, "no exception")
    except LedgerRejected as exc:
        check("双馈线变更被拒绝", True, str(exc))
    after = grid.publish()
    check("拒绝后已发布产品身份不变",
          after["product_id"] == product_before["product_id"])
    # 同版异文拒绝；低修订先核输入再忽略，不占时点。
    grid.submit("C1", 1, 20, [["c", 0]])
    try:
        grid.submit("C1", 1, 20, [["c", 1]])
        check("同版异文拒绝", False)
    except LedgerRejected:
        check("同版异文拒绝", True)
    stale = grid.submit("C1", 1, 20, [["c", 0]])  # identical replay
    check("同版同内容重放", stale["decision"] == "replayed")
    # 先用高修订改时点，再演示合法的低修订被忽略且不占时点。
    grid.submit("C1", 2, 25, [["c", 0]])
    low = grid.submit("C1", 1, 5, [["c", 0]])
    check("低修订被忽略且不占时点", low["decision"] == "stale", low["decision"])
    check("当前时点仍是高修订的 25",
          grid.state_label_at(grid.publish()["product_id"], 24)["L"] == "F1" and
          grid.state_label_at(grid.publish()["product_id"], 25)["L"]
          == DISCONNECTED)
    # 低修订同样先做输入合法性检查（未知边 / 超域均直接拒绝）。
    try:
        grid.submit("C1", 1, 5, [["unknown-edge", 0]])
        check("低修订非法输入仍被校验", False)
    except ValidationError:
        check("低修订非法输入仍被校验", True)
    # 不同变更不能占用同一分钟（C1 头版本当前在 25 分钟）。
    try:
        grid.submit("C2", 1, 25, [["a", 0]])
        check("同一分钟冲突拒绝", False)
    except LedgerRejected:
        check("同一分钟冲突拒绝", True)
    grid.close()


# ----------------------------------------------------------------------
def scenario_wraparound(out_dir: Path):
    print("\n=== 4. 累计值绕回（模 M 差唯一时方可解释） ===")
    db = out_dir / "wrap.sqlite3"
    M, R = 100, 3
    setup = {
        "nodes": {"F1": "feeder", "L": "load"},
        "edges": {"a": ["F1", "L"]}, "closed": ["a"],
        "meters": [{"meter_id": "m", "load": "L", "modulus": M, "rate": R}],
    }
    grid = DistGrid(db, setup)
    grid.put_points([
        {"meter_id": "m", "point_no": 1, "revision": 1, "minute": 0,
         "value": 90, "reset": True},
        {"meter_id": "m", "point_no": 2, "revision": 1, "minute": 5, "value": 5},
    ])
    product = grid.publish()
    result = grid.query(product["product_id"], 0, 5, 100)
    # (5-90) mod 100 = 15；R*gap=15 < M=100，绕一圈唯一，合法。
    check("绕回电量解释为 15", Fraction(result["energy"]["F1"]) == 15,
          result["energy"])
    check("L 全程已知", result["load_minutes"]["L"] ==
          {"known_minutes": 5, "unknown_minutes": 0})

    # 非唯一：R*gap >= M 时不能猜绕圈次数 -> 未知。
    grid.put_points([
        {"meter_id": "m", "point_no": 3, "revision": 1, "minute": 40,
         "value": 20},  # gap 35, R*gap=105 >= M
    ])
    result2 = grid.query(product["product_id"], 5, 40, 1000)
    check("不满足唯一性 -> 未知段",
          result2["load_minutes"]["L"] == {"known_minutes": 0,
                                           "unknown_minutes": 35},
          str(result2["load_minutes"]["L"]))
    check("未知段不产生任何馈线电量", result2["energy"] == {}, str(result2["energy"]))

    # 坏段：差超过 R*gap。
    grid.put_points([
        {"meter_id": "m", "point_no": 4, "revision": 1, "minute": 45,
         "value": 99},  # gap 5, diff 79 > 15
    ])
    result3 = grid.query(product["product_id"], 40, 45, 1000)
    check("坏段计未知、不按零电量算已知",
          result3["load_minutes"]["L"] == {"known_minutes": 0,
                                           "unknown_minutes": 5})
    grid.close()


# ----------------------------------------------------------------------
def scenario_meter_reset(out_dir: Path):
    print("\n=== 5. 换表（reset 首点切断左侧段） ===")
    db = out_dir / "reset.sqlite3"
    setup = {
        "nodes": {"F1": "feeder", "L": "load"},
        "edges": {"a": ["F1", "L"]}, "closed": ["a"],
        "meters": [{"meter_id": "m", "load": "L", "modulus": 1000, "rate": 10}],
    }
    grid = DistGrid(db, setup)
    grid.put_points([
        {"meter_id": "m", "point_no": 1, "revision": 1, "minute": 0,
         "value": 500, "reset": True},
        {"meter_id": "m", "point_no": 2, "revision": 1, "minute": 10, "value": 560},
        {"meter_id": "m", "point_no": 3, "revision": 1, "minute": 20,
         "value": 20, "reset": True},   # 换表首点
        {"meter_id": "m", "point_no": 4, "revision": 1, "minute": 30, "value": 40},
    ])
    product = grid.publish()
    # [0,10) 已知 60；[10,20) reset 断点 -> 未知；[20,30) 新表段 20 已知。
    result = grid.query(product["product_id"], 0, 30, 100)
    check("电量只来自断点两侧的正常段（60+20=80）",
          Fraction(result["energy"]["F1"]) == 80, result["energy"])
    check("换表断点 10 分钟计未知",
          result["load_minutes"]["L"] == {"known_minutes": 20,
                                          "unknown_minutes": 10},
          str(result["load_minutes"]["L"]))
    # max_gap：正常段间隔超过 max_gap 时整段未知。
    gapped = grid.query(product["product_id"], 0, 10, 5)
    check("间隔超过 max_gap 的整段未知",
          gapped["load_minutes"]["L"] == {"known_minutes": 0,
                                          "unknown_minutes": 10})
    grid.close()


def scenario_correction(out_dir: Path):
    print("\n=== 6. 历史读数更正（切面固化，旧结果不变，新查询重算） ===")
    db = out_dir / "correction.sqlite3"
    setup = {
        "nodes": {"F1": "feeder", "L": "load"},
        "edges": {"a": ["F1", "L"]}, "closed": ["a"],
        "meters": [{"meter_id": "m", "load": "L", "modulus": 1000, "rate": 10}],
    }
    grid = DistGrid(db, setup)
    grid.put_points([
        {"meter_id": "m", "point_no": 1, "revision": 1, "minute": 0,
         "value": 0, "reset": True},
        {"meter_id": "m", "point_no": 2, "revision": 1, "minute": 10,
         "value": 999},  # 坏段：999 > R*gap=100
    ])
    product = grid.publish()
    cut_before = grid.latest_cut_id()
    old = grid.query(product["product_id"], 0, 10, 100)
    old_query_id = old["query_id"]
    check("更正前为坏段：全程未知",
          old["load_minutes"]["L"] == {"known_minutes": 0,
                                       "unknown_minutes": 10})
    # 高修订把末点改成合法值；旧切面与已返回结果保持不变。
    grid.put_points([
        {"meter_id": "m", "point_no": 2, "revision": 2, "minute": 10,
         "value": 40},
    ])
    cut_after = grid.latest_cut_id()
    check("生成新的读数切面", cut_after > cut_before)
    new = grid.query(product["product_id"], 0, 10, 100)  # 默认最新切面
    check("更正后电量 40、全程已知",
          Fraction(new["energy"]["F1"]) == 40 and
          new["load_minutes"]["L"] == {"known_minutes": 10,
                                       "unknown_minutes": 0},
          str(new))
    pinned = grid.query(product["product_id"], 0, 10, 100, cut_id=cut_before)
    check("显式固定旧切面仍复现坏段结果", pinned == old)
    row = grid.store.query_row(old_query_id)
    check("已返回的查询记录原样保留", json.loads(row["result"]) == old)
    verify = grid.verify_facet(old_query_id)
    check("切面重算与存档一致", verify["matches"])
    # 撤回点：撤回仍带完整数据，但离开有效集。
    grid.put_points([
        {"meter_id": "m", "point_no": 2, "revision": 3, "minute": 10,
         "value": 40, "retracted": True},
    ])
    retracted = grid.query(product["product_id"], 0, 10, 100)
    check("撤回后无相邻有效点 -> 未知",
          retracted["load_minutes"]["L"] == {"known_minutes": 0,
                                             "unknown_minutes": 10})
    check("撤回点仍完整保留在切面中",
          any(p["point_no"] == 2 for p in
              grid.get_cut(grid.latest_cut_id())["retracted_kept"]))
    grid.close()


# ----------------------------------------------------------------------
def scenario_two_products_same_readings(out_dir: Path):
    print("\n=== 7. 同组读数对照换接前后两个正式产品 ===")
    db = out_dir / "two_products.sqlite3"
    setup = {
        "nodes": {"F1": "feeder", "F2": "feeder", "L": "load"},
        "edges": {"a": ["F1", "L"], "b": ["F2", "L"]},
        "closed": ["a"],
        "meters": [{"meter_id": "m", "load": "L", "modulus": 1000, "rate": 10}],
    }
    grid = DistGrid(db, setup)
    grid.put_points([
        {"meter_id": "m", "point_no": 1, "revision": 1, "minute": 0,
         "value": 0, "reset": True},
        {"meter_id": "m", "point_no": 2, "revision": 1, "minute": 40,
         "value": 80},
    ])
    p_before = grid.publish()
    grid.submit("T1", 1, 20, [["b", 1], ["a", 0]])
    p_after = grid.publish()
    q_before = grid.query(p_before["product_id"], 0, 40, 100)
    q_after = grid.query(p_after["product_id"], 0, 40, 100)
    check("真实产品变更改变馈线归属",
          q_before["energy"] == {"F1": "80"} and
          q_after["energy"] == {"F1": "40", "F2": "40"},
          f"{q_before['energy']} vs {q_after['energy']}")
    check("两产品总电量相同（读数未变，仅归属变化）",
          sum(map(Fraction, q_before["energy"].values())) ==
          sum(map(Fraction, q_after["energy"].values())) == 80)

    # 消费者请求新产品身份，内部却误用旧连接内容 -> 必须显出错误。
    tampered = dict(p_after)
    tampered["slices"] = p_before["slices"]
    tampered["changes"] = p_before["changes"]
    try:
        grid.queries.check_product_content(p_after["product_id"], tampered)
        check("误用旧连接内容可被检出", False)
    except ValidationError as exc:
        check("误用旧连接内容可被检出", True, str(exc)[:60])
    # 对照：内容与身份一致时校验通过。
    grid.queries.check_product_content(p_after["product_id"], p_after)
    check("内容与产品身份一致时校验通过", True)
    # 明确请求旧产品本来合法。
    legit = grid.query(p_before["product_id"], 0, 40, 100)
    check("明确请求旧产品合法且结果不变", legit == q_before)
    # 未知产品 / 未知切面报错。
    try:
        grid.query("nope", 0, 40, 100)
        check("未知产品被拒绝", False)
    except ProductError:
        check("未知产品被拒绝", True)
    grid.close()


# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep-db", action="store_true")
    parser.add_argument("--out-dir", default=None)
    args = parser.parse_args()

    if args.out_dir:
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        cleanup = False
    else:
        out_dir = Path(tempfile.mkdtemp(prefix="distgrid630_"))
        cleanup = not args.keep_db

    try:
        scenario_single_feeder(out_dir)
        scenario_same_minute_transfer(out_dir)
        scenario_illegal_double_feeder(out_dir)
        scenario_wraparound(out_dir)
        scenario_meter_reset(out_dir)
        scenario_correction(out_dir)
        scenario_two_products_same_readings(out_dir)
    except Exception:
        traceback.print_exc()
        return 2
    finally:
        if cleanup:
            for f in out_dir.glob("*.sqlite3"):
                f.unlink()
            out_dir.rmdir()

    passed = sum(1 for _, ok, _ in CHECKS if ok)
    print(f"\n总计 {passed}/{len(CHECKS)} 项检查通过"
          + (f"；数据库位于 {out_dir}" if not cleanup else ""))
    return 0 if passed == len(CHECKS) else 1


if __name__ == "__main__":
    sys.exit(main())
