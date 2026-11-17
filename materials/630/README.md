# 园区换接与电表事实仓起点

Python 3.11 标准库即可运行。`SwitchRegister` 已能预演和提交普通开关变更，并发布按分钟分段的连接产品。`MeterWarehouse` 已能将普通单调读数分时归属到实际连接产品。`example.json` 提供一个负荷在两条馈线间换接及累计计数绕回的数据。

这些是现有普通路径，不是完整目标。当前开关预演逐动作判断，不能正确处理所有合法整组换接；表仓尚未实现模计数器、修订/撤回和持久切面。完整目标以题面为准，公开方法可扩展，不要求保留内部结构。

快速检查：

```python
import json
from network import SwitchRegister
from meter_warehouse import MeterWarehouse

d = json.load(open('example.json'))
register = SwitchRegister(d['nodes'], d['edges'], d['closed'])
warehouse = MeterWarehouse(**d['meter'])
warehouse.put(d['ordinary_points'])
print(warehouse.query(register.publish(), 0, 10, 10))
```
