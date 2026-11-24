# 单网关 IPv6 地址池换池演练（Flask + SQLite）

模拟单网关一次 IPv6 委派前缀池更换的值班演练。SQLite 持久化**模拟时钟、地址池、租约、
空闲区间和撤路发布**，内置旧池及其租约；网页录入已上线的新池 CIDR 与委派长度（/56 或 /60），
配置无效则整单回滚、保留原状。

## 进程划分

| 进程 | 端口 | 职责 | 数据库打开方式 |
|---|---|---|---|
| `web`（值班台，写入端） | 8000 | 换池、领址、模拟时钟两阶段推进、撤路数据发布 | 读写（WAL，单写者 gunicorn 1 worker） |
| `gateway`（网关，独立进程） | 8001 | 只读查询已发布最早撤路时刻与未被新汇总完整覆盖的有效旧租约 | `mode=ro` 只读 URI |

两容器共享命名卷 `drill-data` 上的同一个 SQLite 文件；WAL 允许网关在写入期间并发读。

## 启动

```bash
docker compose up --build
# 值班台：     http://localhost:8000/
# 网关撤路视图：http://localhost:8001/withdrawal
```

无 Docker 时本地运行（需要 Python 3.11，`pip install -r requirements.txt`）：

```bash
export DRILL_DB=/tmp/pool_drill.db
gunicorn --workers 1 --threads 8 -b 0.0.0.0:8000 "app.web:create_app" &
gunicorn --workers 1 --threads 4 -b 0.0.0.0:8001 "app.gateway:create_app" &
```

## 内置数据（模拟时钟 t=0 起，单位秒）

- 旧池 `2001:db8:aaaa::/48`，委派 `/60`，共 4096 个槽位。
- 旧租约：slot 0/1 到期 3600，slot 2/5 到期 7200，slot 6 到期 10800；
  slot 4 的租约在演练开始前已到期（演示历史到期数据）。
- 旧池初始空闲区间 `[3,5)`、`[7,4096)`（半开区间，按槽位编号）。

## 关键规则与实现位置

1. **换池校验，无效保留原状**：`app/common.py::validate_pool` 严格 CIDR 解析（拒绝主机位、
   非 IPv6），委派长度仅允许 56/60 且不得短于池前缀；`app/logic.py::change_pool` 在
   `BEGIN IMMEDIATE` 事务内先全部校验再写入，任何异常都 ROLLBACK。单次演练只允许换一次。
2. **换池后仅从新池发放 60 分钟租约，避开未到期前缀，池满提示**：`meta.new_pool_id` 切换
   活动池；可分配空间来自持久化的 `free_intervals`（半开 `[start,end)` 槽位区间），
   first-fit 只取首区间首槽，池满（无空闲行）返回 400 且不写入。
3. **旧租约按原期限到期**：到期时刻在建租约时固定写入 `expires_at`，时钟推进不修改未到期租约。
4. **网关独立进程只读查撤路**：`app/gateway.py` 以 `?mode=ro` 打开库，`GET /withdrawal`
   返回发布行中的最早安全撤路时刻，以及有效（active）且委派前缀不被新池 CIDR 完整包含
   （`subnet_of`）的旧租约清单；全部旧租约到期后最早撤路时刻为 `null`（可立即撤）。
5. **模拟时钟两阶段推进，只核算本次到期租约，暂停期间照常服务**：
   - `POST /api/advance` 暂停：快照 `expires_at <= 目标时刻` 的 active 租约 id 集合与最大租约 id，
     写入 `clock_advance(pending)`，**不改动时钟与租约**；
   - 暂停期间领址与网关查撤路均正常；
   - `POST /api/advance/commit`：在一个事务内重新计算到期集合——
     - 暂停期间新领且在本次到期的租约自动进入集合并计入（响应字段 `added`）；
     - 若快照中应有成员从当前到期集合消失（名单变化），返回 **409 退回重做**，
       原时钟与已领租约全部保留；
     - 提交成功则同一事务内完成：租约置 expired、槽位释放并归并空闲区间、时钟前进、
       撤路结论重新发布。
   - `POST /api/advance/abort` 手动退回；重启时启动恢复（`db.recover_pending`）把崩溃遗留的
     pending 行标记 aborted，两端按同一已提交状态继续。
6. **提交中断原子性**：提交接口支持 `fault_inject` 演练——在 COMMIT 前关闭连接模拟进程丢失，
   SQLite 自动丢弃未提交事务；重启恢复后时钟、可领范围、撤路发布仍为上一个已提交状态。
7. **不展开候选前缀、重启/首笔领址不回扫在租明细、找空位开销不随连续占用段增长**：
   - 新池只写一行覆盖整池的空闲区间，前缀文本由整数运算 `slot_prefix` 现算，绝不枚举；
   - 空闲区间持久化在表中，重启直接读取，无任何租约扫描重建；
   - 分配只查 `ORDER BY start_slot LIMIT 1` 的首空闲行，开销取决于空闲碎片数，
     与连续占用段内的租约数量无关（自测 500 连占后实测约 17ms）。

## HTTP API（均支持 JSON；表单提交后 302 回值班台）

| 方法 路径 | 说明 |
|---|---|
| `POST /api/pool` | `{cidr, deleg_len}` 新池上线 |
| `POST /api/lease` | `{count}` 从活动池发放 60 分钟租约 |
| `POST /api/advance` | `{target_seconds}` 暂停并核算本次到期 |
| `POST /api/advance/commit` | 提交（可带 `{"fault_inject": true}` 演练中断） |
| `POST /api/advance/abort` | 退回重做 |
| `GET  /withdrawal`（网关 8001） | 最早撤路时刻 + 未覆盖有效旧租约 |

## 自测

```bash
python3 selftest.py
```

9 组 57 项断言，覆盖：种子数据、只读隔离、无效配置回滚、换池交叠覆盖、/56 与 /60 边界、
领址与池满、暂停期间新领计入到期、名单变化 409 退回、故障注入提交中断与重启恢复、
重启不回扫与分配开销、手动退回与非法参数。
