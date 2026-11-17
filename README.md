# 承保修订后已分期赔案的重新结算

离线、可运行、可追溯的演示实现。仓库（warehouse）负责承保去重、修订归并、
接收历史与可复现的保障查询；赔案应用（claims）负责案件修订、支付承诺台账、
追偿、可付余额和人工暂停。两者共用同一个 SQLite 事件库，但事件命名空间分离。

> 全部金额为同币种整数分；日期为 UTC 日（输入使用整数日号）。
> 材料仅用于离线开发，不连接任何真实保险系统。

## 1. 运行

只依赖 Python 3.11 标准库（`sqlite3`、`unittest`），无需安装任何包。

```bash
# 完整演示（首次运行 → 重启重放 → 全新库对比；包含全部核对点）
python3 -m settlement demo --db /tmp/demo.db --reset --journal /tmp/demo.jsonl

# 机器可读输出
python3 -m settlement demo --db /tmp/demo.db --reset --json

# 题面 case.json 的 before / after 核对点
python3 -m settlement case --case case.json

# 承保修订与支付并发提交的先后顺序可复现
python3 -m settlement race --iterations 20

# 单元 + 集成测试（32 项）
python3 -m unittest discover -s tests -v
```

单步操作（可跨进程、反复执行；每次都是一个持久事务）：

```bash
python3 -m settlement ingest  --db s.db --file scenarios/coverage_batch_example.json
python3 -m settlement claim   --db s.db --claim C1 --revision 1 --day 3 --loss 150 --policy P1 --device D1
python3 -m settlement pay     --db s.db --payment pay-1 --claim C1 --amount 40
python3 -m settlement dispatch--db s.db --payment pay-1 [--drop-response]
python3 -m settlement pause   --db s.db --claim C1 [--resume]
python3 -m settlement coverage--db s.db --policy P1 --device D1 --day 3 [--batch N]
python3 -m settlement history --db s.db --policy P1 --device D1 --day 3
python3 -m settlement show    --db s.db --claim C1
python3 -m settlement events  --db s.db
```

`pay` 退出码：`0` = 承诺已接受（含幂等重试），`2` = 已持久化的拒绝或格式冲突。
被拒绝的请求同样落库，因此重试会返回**原来的**结果，而不是重新判定。

## 2. 输入说明

### 承保批次 `ingest`

一个批次是 JSON 数组（或 `{"coverage": [...]}` / `{"envelopes": [...]}`）：

```json
[
  {"policy": "P1", "device": "D1", "revision": 2,
   "fragments": [{"from": 1, "to": 5, "deductible": 20, "limit": 60}]}
]
```

* 每个 envelope 是该修订的**完整**片段集合，空数组表示撤销该保单/设备。
* 片段 `[from, to)` 左闭右开；同一修订内部不得重叠，可相邻；`deductible`、
  `limit` 为非负整数。
* 修订号越大整体替换越小修订，**不合并**旧片段；与当前已存内容不同的同一
  修订号（同案同内容）视为冲突，不会静默覆盖。
* 完全相同内容的重复投递（含乱序到达）是幂等 no-op，但仍写入接收批次历史。
* 每个批次都会分配 `batch_id`、内容摘要与接收时间，事件行记录来源批次。

### 案件修订 `claim`

```json
{"claim_id": "C1", "revision": 2, "day": 8, "loss": 150,
 "policy": "P1", "device": "D1"}
```

或使用 CLI 参数。新修订是完整镜像（可更正事故日或损失）。`revision 0`
必须给出 `policy`/`device`；后续修订可省略并沿用。相同修订号相同内容幂等，
相同修订号不同内容冲突。

### 支付 `pay`

```json
{"payment_id": "pay-3", "claim_id": "C1", "amount": 20}
```

可附带 `seen_coverage_revision` 或 `expected_warehouse_seq`：提交端以它声明
“我看到的权威版本”，服务端若已向前推进则拒绝，避免使用陈旧界面余额。

### 人工暂停 / 恢复 `pause`

`{"claim_id": "C1", "action": "pause"|"resume", "request_id": "pause-1"}`。
`request_id` 是幂等键；CLI 不提供时自动生成递增序号。

## 3. 规则实现与关键设计

### 3.1 保障判定

* 当前版本 = 该保单/设备收到过的最大修订号（与到达顺序无关）。
* 当前保障 = 该版本片段集合中覆盖事故日的片段；无覆盖则“无保障”。
* 应赔额 = `min(max(核定损失 - 免赔额, 0), 单次赔付上限)`；无保障为 0。
  案件之间不共享额度。
* `coverage_history` 按修订号列出每个历史版本及其对某日的判定，可直接复现。
* `coverage_at_batch(batch_id, …)` 只折叠 `batch_id` 及其之前批次的事件，
  用于“按接收批次回看当时的判定”。
* 支付决策记录 `coverage_digest`、`coverage_revision`、`coverage_seq`，
  把输入事件（片段 + 案件修订）到支付决定连成可审计链。

### 3.2 结算余额

对任一案件，令 `paid` = 已生效承诺总额，`auth` = 最新权威应赔额：

```
recovery = max(0, paid - auth)      # 追偿余额（已多付、需追回）
payable  = max(0, auth - paid)      # 可新支付的余额
```

* 追偿优先于新支付：`recovery > 0` 时任何 `amount > 0` 的新请求都被拒绝
  （`recovery_active`）。`amount == 0` 不创造金额，属合法承诺。
* 应赔额回升时追偿先被抵消，只有超过 `paid` 的部分才回到 `payable`。
* 已生效的钱不会因后续修订消失：因此存在追偿，而不是“回滚支付”。

### 3.3 支付承诺：唯一生效点

`accept_payment` 在单个 `BEGIN IMMEDIATE` 事务内完成：

1. **幂等判定**：同 `payment_id` + 同内容返回原先持久化决定（接受或拒绝）；
   同 id 不同内容 -> `PaymentIdConflict`。拒绝决定是“粘性”的：之后即使
   保障恢复，同一 id 仍然返回原拒绝，不会翻转为接受。
2. **当前权威状态**：从事件日志重新折叠出最新案件修订与最新承保修订，
   重算 `auth`；不使用任何缓存余额。
3. **已付总额**：从承诺台账重新求和，且排除本 `payment_id` 自身，因此
   “响应丢失后的重试”不会重复占用额度，也不可能重复扣款。
4. 校验 `paid_before + amount <= auth`；通过后**原子保存**：
   `payment_intent` 事件 + 外部接收端行 + outbox 行三者在同一事务提交。

因此：承诺先提交时，之后的撤销只产生追偿；撤销先提交时，支付被拒绝且
拒绝被持久化。并发时 SQLite 的写锁给出单一的提交顺序，事件 `seq` 是
“谁先提交”的可复现依据（`race` 命令验证：支付 seq 在撤销 seq 之前时按旧
权威判定，之后时按新权威判定）。

### 3.4 投递与接收端

台账分别显示三种事实：

* `committed`：本系统已接受的支付承诺（结算余额只按此计算）；
* `delivery`：`pending` / `delivered`（outbox 状态）；
* `receiver_accepted`：模拟外部接收端是否已接受。

投递按 `payment_id` 幂等。`--drop-response` 模拟“接收端已接受但响应丢失”：
外部记账已发生，outbox 仍为 `pending`；再次投递同一 id 得到
`duplicate_ack`/`already_delivered`，接收端只有一条记录，业务金额不变。

### 3.5 人工暂停

暂停只阻止新支付，不改变应赔额、已付记录或历史；重算、案件修订、承保修订
都不会清除暂停记录（事件日志保留全部 pause/resume 动作），恢复后恢复正常支付。

### 3.6 仓库独立性与应用独立性

* 无任何赔案时，`coverage`/`history` 仍可独立查询历史保障。
* 无新承保事件时，仍可修订案件、暂停/恢复、支付。

### 3.7 重启与重放

所有业务写入以稳定业务键去重（承保批次键含内容指纹、案件修订键、
支付 id、暂停 request_id、拒绝决定键）。`demo` 第二阶段在同一数据库上把
整份动作清单再放一遍：业务快照哈希不变、事件数不变；第三阶段在全新库重放
得到逐字节一致的全量快照。

接收批次表在重放时仍会追加行——这是“接收历史”本身，属于要求保留的事实；
它不影响业务状态，所以快照分为 `business_snapshot`（不含接收台账）与
`full_snapshot`（含全部事件与批次）两类，二者在“全新库首跑 vs 重放”中
保持一致；同一库的重放只增加接收批次行。

## 4. 核对过程（demo 输出）

演示动作分三段（`settlement/scenario.py: demo_actions`）：

**A 段（核心链路，C1，P1/D1）**

| 步骤 | 动作 | 期望 |
|---|---|---|
| A1 | 接收 rev1：`[1,5)` 免赔 0、上限 100 | 当前保障 100 |
| A2 | C1 rev1：事故日 3、损失 150 | 应赔 100 |
| A3/A5 | 支付 pay-1=40、pay-2=30 并投递 | 已付 70，可付 30 |
| A7 | 追溯缩小 rev2：上限 50 | 已付 70 > 应赔 50 -> 追偿 20，可付 0 |
| A8 | 乱序重放 rev1 | 幂等 no-op，仍以 rev2 为准，历史两者都在 |
| A9/A10 | 暂停；尝试支付 | 拒绝 `paused`，应赔/已付不变 |
| A11 | 暂停中更正事故日为 8（当前片段不覆盖） | 应赔 0，追偿 70，暂停仍在 |
| A12/A13 | 恢复；尝试支付 | 恢复仅解除暂停；支付被 `recovery_active` 拒绝 |
| A14–A16 | rev3（day3 上限 90）、rev4 空集合撤销、rev5 覆盖 day8 | 每版整体替换；最终 day8 应赔 100 |
| A17 | 支付 30（已处于追偿状态） | 拒绝，不会“重复许诺” |
| A17b | 案件 rev3：损失改 80 | 应赔 70 < 已付 100 -> 追偿 30、可付 0 |
| A17c | 在追偿下支付 5 | 拒绝 `recovery_active` |
| A17d | 案件 rev4：损失改 120 | 应赔 100，追偿清零，可付 0 |
| A18/A19 | 无新承保事件时再次暂停/恢复 | 仅人工状态变化，历史保留 |

**B 段（撤销先提交，C2，P2/D2）**

1. 接收 rev1（上限 100），建案 C2（日 5、损失 80）。
2. 接收 rev2 空集合（撤销）。
3. 支付 10 -> 拒绝（无保障；`paid_before 0, payable 0`）；同 id 重试返回原拒绝；
   同 id 换内容被拒。
4. 接收 rev3 恢复保障（上限 70）；新支付 id 支付 70 -> 接受。

**C 段（支付先提交 + 响应丢失 + 事后撤销，C3，P3/D3）**

1. 接收 rev1（上限 100），建案 C3（日 4、损失 80 -> 应赔 80）。
2. 支付 pay-C1=50 -> 接受（承诺生效）。
3. 投递并丢弃响应：接收端已接受、outbox 仍 pending。
4. 接收 rev2 空集合（撤销）：已付 50 不变，追偿 50，可付 0。
5. 同 id 同内容重试：返回原接受结果，已付仍 50；同 id 换内容被拒。
6. 重新投递：接收端去重返回 duplicate，不重复业务效果。
7. 带陈旧版本（rev1）提交：拒绝 `stale submission`；用最新版本（rev2）提交
   新 id：`recovery_active`。
8. 接收 rev3（上限 60）：追偿清零、可付 10；支付 10 -> 接受。

最终核对（`settlement/scenario.py: evaluate_final_state`）：

| 案件 | 当前承保版本 | 应赔额 | 累计已付 | 追偿 | 可付 | 人工状态 |
|---|---|---|---|---|---|---|
| C1 | rev5 | 100 | 100 | 0 | 0 | 未暂停（4 次暂停历史保留） |
| C2 | rev3 | 70 | 70 | 0 | 0 | 未暂停 |
| C3 | rev3 | 60 | 60 | 0 | 0 | 未暂停 |

并核对：历史保障（rev1=100、rev2=50、rev3=90、rev4 空、rev5 对 day3 不覆盖）、
批次 1 的历史视角仍为上限 100、仓库在无赔案时仍可查询（P9/D9）、
pay-C1 的承诺 seq 早于撤销 seq、接收端对 pay-C1 只有一条金额 50 的记录、
每个已承诺支付都有 outbox 行、重放后事件数 45 不变、业务快照一致。

`python3 -m settlement demo` 会打印每一条 `[PASS]/[FAIL]` 以及
`DEMO RESULT: PASS/FAIL`；`--json` 给出同样信息的机读版本。

## 5. 题面 `case.json` 核对

`python3 -m settlement case --case case.json` 独立构造两个状态（各自独立的库，
相同两笔已有承诺 40 + 30，绝不把两个对照当成连续接受同一笔 pay-3）：

* `before`：最新权威上限 100 -> 应赔 100，可付 30，追偿 0，接受 pay-3=20；
  重试幂等且不重复扣款。
* `after`：最新权威上限 50 -> 应赔 50，已付 70，可付 0，追偿 20，拒绝 pay-3；
  同 id 重试返回原拒绝，换内容拒绝。
* 附加：之后把承保恢复到上限 100，pay-3 的拒绝仍保持（粘性拒绝），不产生新支付。
* 输入第三个修订（上限 90）单独核对：可付 20。

## 6. 成本与取舍

* **历史查询开销**：事件日志按 `seq` 追加；查询会折叠该命名空间的事件。
  当前实现是“全量重算”（简单、绝对可复现），复杂度 O(事件数)；承保查询
  在折叠后按 `(policy, device)` 取最大修订，O(1) 定位片段（片段线性扫描）。
  `coverage_history` 为每个修订做一次判定，O(修订数 × 片段数)。
  `coverage_at_batch` 折叠到该批次为止，O(批次位置)。
  若事件量增长，可按 `(policy, device, revision)` 建物化快照或增量投影，
  代价是需要维护投影的一致性与重建路径；本实现优先正确性与可审计性。
* **修订传播延迟**：承保修订在一个事务内持久化后立即对后续支付可见；
  支付在事务内读取最新修订，所以没有异步传播窗口。代价是每次支付要折叠
  一次承保/案件事件（当前数据规模下可忽略）。若改为异步投影，必须让支付
  等待投影水位，否则会读到陈旧权威——本实现刻意避免这条路径。
* **支付提交成本**：一次提交 = 一次 `BEGIN IMMEDIATE` + 若干行读写 +
  outbox 写；承诺的原子性来自单事务。并发提交被 SQLite 串行化，吞吐受写锁
  限制；换 PostgreSQL 时可用行锁/可串行化事务保持同样的“先提交者获胜”语义。
* **恢复成本**：重启时无需专门恢复步骤——状态由事件日志与 outbox 重建；
  `dispatch_all_pending`（以及演示中的重投）把 pending 消息重发，接收端按
  `payment_id` 去重。因此“响应丢失”最多造成一次重投，不会重复扣款。
* **追偿语义**：已付的钱不会因修订消失，因此实现对“从承诺台账重算余额”，
  而不是保存可变的“界面余额”；这也是不能通过回滚/重放重复记账的原因。

## 7. 目录

```
settlement/
  core.py       事件信封、片段校验、金额公式、纯折叠的 Warehouse / Claims
  store.py      SQLite 持久层：事件、接收批次、模拟接收端、outbox、事务锁
  service.py    两个应用的服务层：摄取、案件修订、支付承诺、投递、视图
  scenario.py   确定性 demo 动作、核对点、不变量、重放与并发验证、case 检查
  cli.py        命令行入口
tests/
  test_settlement.py   32 项 unittest（含重启、并发、接收端去重、金额公式）
scenarios/
  coverage_batch_example.json
case.json       题面输入（只读）
```
