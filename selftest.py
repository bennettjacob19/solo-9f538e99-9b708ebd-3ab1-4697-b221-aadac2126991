"""End-to-end self tests for the IPv6 pool-swap drill (stdlib only).

Run:  python3 selftest.py
Covers: seed data, invalid config rollback, pool swap, overlap/coverage,
first-fit leasing and pool-full, clock expiry including leases taken during
the pause, list-changed redo, fault-injected commit interruption, read-only
gateway, restart recovery without lease scans, and O(1)-ish allocation cost.
"""
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.db import connect, init_db, recover_pending          # noqa: E402
from app.gateway import create_app as gateway_app             # noqa: E402
from app.web import create_app as web_app                     # noqa: E402

PASS, FAIL = 0, 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  PASS  %s %s" % (name, detail))
    else:
        FAIL += 1
        print("  FAIL  %s %s" % (name, detail))


def fresh_db():
    tmp = tempfile.mkdtemp(prefix="drill-")
    path = os.path.join(tmp, "test.db")
    init_db(path, force=True).close()
    return path


def jpost(client, url, payload=None, raw=False):
    if raw:
        resp = client.post(url, data=payload or {})
    else:
        resp = client.post(url, json=payload or {})
    return resp.status_code, resp.get_json()


# --------------------------------------------------------------------------
def test_seed_and_gateway():
    print("[1] 种子数据与只读网关撤路视图")
    path = fresh_db()
    conn = connect(path)
    rows = conn.execute("SELECT slot, status FROM leases ORDER BY slot").fetchall()
    check("内置 6 条旧租约（5 active + 1 expired）", len(rows) == 6
          and sum(1 for r in rows if r["status"] == "active") == 5)
    fr = conn.execute(
        "SELECT start_slot,end_slot FROM free_intervals ORDER BY start_slot"
    ).fetchall()
    # slot 4's lease is already expired at drill start, so its slot is free;
    # free ranges are [3,5) and [7,4096). No candidate prefix list is built.
    check("旧池空闲区间为 [3,5) 与 [7,4096)，未展开候选前缀",
          [(r["start_slot"], r["end_slot"]) for r in fr] == [(3, 5), (7, 4096)])
    check("模拟时钟初始为 0",
          conn.execute("SELECT now_seconds FROM meta").fetchone()[0] == 0)
    conn.close()

    gw = gateway_app(path).test_client()
    resp = gw.get("/withdrawal")
    body = resp.get_json()
    check("无新池时最早撤路时刻=最早旧租约到期 3600",
          body["earliest_withdrawal_seconds"] == 3600)
    check("无新池时 5 条有效旧租约全部未覆盖", body["uncovered_count"] == 5)

    ro = connect(path, readonly=True)
    try:
        blocked = False
        try:
            ro.execute("UPDATE meta SET now_seconds = 1")
        except sqlite3.OperationalError:
            blocked = True
        check("网关连接为只读，写入被 SQLite 拒绝", blocked)
    finally:
        ro.close()


def test_invalid_config_keeps_state():
    print("[2] 无效新池配置保留原状")
    path = fresh_db()
    client = web_app(path).test_client()
    before = connect(path).execute(
        "SELECT now_seconds, new_pool_id FROM meta").fetchone()
    bad_inputs = [
        ("not-a-cidr", 60),
        ("2001:db8::/128", 60),          # deleg shorter than pool prefix
        ("2001:db8::/56", 64),           # deleg not 56/60
        ("2001:db8::1/56", 56),          # host bits set, strict parse fails
        ("10.0.0.0/24", 60),             # IPv4
    ]
    for cidr, deleg in bad_inputs:
        code, body = jpost(client, "/api/pool", {"cidr": cidr, "deleg_len": deleg})
        check("拒绝无效配置 %s /%d" % (cidr, deleg),
              code == 400 and body["ok"] is False)
    after = connect(path).execute(
        "SELECT now_seconds, new_pool_id FROM meta").fetchone()
    check("全部拒绝后时钟与池状态不变", tuple(before) == tuple(after))


def test_swap_and_overlap():
    print("[3] 换池与交叠覆盖核算")
    path = fresh_db()
    client = web_app(path).test_client()
    gw = gateway_app(path).test_client()

    # New /60 pool covering exactly old slot 2, whose delegated prefix is
    # 2001:db8:aaaa:20::/60 (verified via the same integer math as leasing).
    code, body = jpost(client, "/api/pool",
                       {"cidr": "2001:db8:aaaa:20::/60", "deleg_len": 60})
    check("交叠新池换池成功", code == 200 and body["slots"] == 1
          and body["cidr"] == "2001:db8:aaaa:20::/60")
    view = gw.get("/withdrawal").get_json()
    check("新汇总仅覆盖旧租约 slot2，未覆盖=4", view["uncovered_count"] == 4)
    check("最早撤路时刻仍为 3600（slot0/1）",
          view["earliest_withdrawal_seconds"] == 3600)
    check("未覆盖名单前缀正确",
          sorted(x["prefix"] for x in view["uncovered_leases"]) == sorted([
              "2001:db8:aaaa::/60", "2001:db8:aaaa:10::/60",
              "2001:db8:aaaa:50::/60", "2001:db8:aaaa:60::/60"]))

    code, body = jpost(client, "/api/pool",
                       {"cidr": "2001:db8:zzzz::/56", "deleg_len": 56})
    check("单次演练禁止二次换池", code == 400)


def test_overlap_capacity_and_release():
    print("[3c] 余量与领址排除未到期租约，分批到期后解挡")
    path = fresh_db()
    client = web_app(path).test_client()
    # /55 pool with /60 delegation = 32 slots. Old slots 0,1,2,5,6 are active;
    # each maps to one new slot. Remaining capacity must be 27, and the
    # blocked slots must not be handed out.
    code, body = jpost(client, "/api/pool",
                       {"cidr": "2001:db8:aaaa::/55", "deleg_len": 60})
    check("换池响应余量=32-5 且排除数=5", code == 200
          and body["slots"] == 32 and body["blocked"] == 5
          and body["free"] == 27)
    conn = connect(path)
    fr = [tuple(r) for r in conn.execute(
        "SELECT start_slot,end_slot FROM free_intervals"
        " WHERE pool_id = 2 ORDER BY start_slot")]
    check("新池空闲区间已扣掉被未到期旧租约占用的槽位",
          fr == [(3, 5), (7, 32)])
    check("阻挡槽位与旧租约一一对应",
          [(r["slot"], r["lease_id"]) for r in conn.execute(
              "SELECT slot, lease_id FROM pool_blocked_slots"
              " WHERE pool_id = 2 ORDER BY slot")]
          == [(0, 1), (1, 2), (2, 3), (5, 5), (6, 6)])
    conn.close()

    # Dashboard remaining capacity must equal the free-range total.
    html = client.get("/").get_data(as_text=True)
    check("页面余量显示 27 且列出阻挡 5", ">27<" in html and ">5<" in html)

    # 3600 batch: old leases 1(slot0) and 2(slot1) expire -> new slots 0,1
    # become free; leases on new slots 2/5/6 keep blocking.
    jpost(client, "/api/advance", {"target_seconds": 3600})
    code, body = jpost(client, "/api/advance/commit", {})
    check("第一批按名单提交", code == 200 and sorted(body["settled"]) == [1, 2])
    conn = connect(path)
    fr = [tuple(r) for r in conn.execute(
        "SELECT start_slot,end_slot FROM free_intervals"
        " WHERE pool_id = 2 ORDER BY start_slot")]
    blocked = [r["slot"] for r in conn.execute(
        "SELECT slot FROM pool_blocked_slots WHERE pool_id = 2 ORDER BY slot")]
    check("到期旧租约对应槽位解挡并入空闲区间", fr == [(0, 2), (3, 5), (7, 32)])
    check("未到期的阻挡租约仍保留", blocked == [2, 5, 6])
    conn.close()
    code, body = jpost(client, "/api/lease", {"count": 1})
    check("解挡后的首槽可领且不与在租旧前缀相交",
          code == 200 and body["issued"][0]["slot"] == 0)

    # 7200 batch: old slot2 and slot5 and the new lease on slot0 expire.
    jpost(client, "/api/advance", {"target_seconds": 7200})
    jpost(client, "/api/advance/commit", {})
    conn = connect(path)
    check("第二批后仅 slot6 仍阻挡", [r["slot"] for r in conn.execute(
        "SELECT slot FROM pool_blocked_slots WHERE pool_id = 2")] == [6])
    conn.close()

    # Last blocker (old slot6, expiry 10800) must not be offered before it
    # expires; once the advance commits, its slot becomes available.
    jpost(client, "/api/advance", {"target_seconds": 10800})
    jpost(client, "/api/advance/commit", {})
    conn = connect(path)
    check("最后一笔阻挡租约到期提交后全部槽位可领", conn.execute(
        "SELECT COUNT(*) c FROM pool_blocked_slots WHERE pool_id = 2"
    ).fetchone()["c"] == 0 and conn.execute(
        "SELECT SUM(end_slot-start_slot) s FROM free_intervals WHERE pool_id=2"
    ).fetchone()["s"] == 32)
    conn.close()


def test_batch_unblocks_only_due_leases():
    print("[3d] 推进只处理本次到期租约及其挡住的新池前缀")
    path = fresh_db()
    client = web_app(path).test_client()
    # /52 pool: 256 new slots; each old /60 lease maps to exactly one slot.
    jpost(client, "/api/pool", {"cidr": "2001:db8:aaaa::/52", "deleg_len": 60})
    touched = []
    import app.logic as logic
    original = logic._reconcile_blocked

    def spy(conn, new_pool, lease_id):
        touched.append(lease_id)
        return original(conn, new_pool, lease_id)

    logic._reconcile_blocked = spy
    try:
        jpost(client, "/api/advance", {"target_seconds": 3600})
        code, body = jpost(client, "/api/advance/commit", {})
    finally:
        logic._reconcile_blocked = original
    check("本批只触碰到期租约 1/2，未扫其它在租明细",
          code == 200 and sorted(touched) == [1, 2])

    conn = connect(path)
    check("不在本批的阻挡租约 3/5/6 原样保留",
          [r["slot"] for r in conn.execute(
              "SELECT slot FROM pool_blocked_slots WHERE pool_id = 2"
              " ORDER BY slot")] == [2, 5, 6])
    conn.close()


def test_multi_slot_overlap_and_partial_unblock():
    print("[3e] 一个前缀横跨多个新池槽位：部分解挡与余量")
    path = fresh_db()
    client = web_app(path).test_client()
    # /58 pool with /60 delegation = 4 slots covering old slots 0..3.
    # Old slots 0,1 (exp 3600) and 2 (exp 7200) are active; slot 3 is free.
    code, body = jpost(client, "/api/pool",
                       {"cidr": "2001:db8:aaaa::/58", "deleg_len": 60})
    check("换池只排除相交槽位，余量=1", code == 200
          and body["blocked"] == 3 and body["free"] == 1)
    conn = connect(path)
    check("阻挡槽位 0/1/2 与空闲 [3,4)", [
        (r["slot"], r["lease_id"]) for r in conn.execute(
            "SELECT slot, lease_id FROM pool_blocked_slots ORDER BY slot")
    ] == [(0, 1), (1, 2), (2, 3)] and [
        tuple(r) for r in conn.execute(
            "SELECT start_slot, end_slot FROM free_intervals"
            " WHERE pool_id = 2")] == [(3, 4)])
    conn.close()
    # 3600: old slots 0,1 expire -> new slots 0,1 free, slot2 still blocked.
    jpost(client, "/api/advance", {"target_seconds": 3600})
    jpost(client, "/api/advance/commit", {})
    conn = connect(path)
    check("本次到期只解挡 slot0/1，slot2 仍挡", [
        tuple(r) for r in conn.execute(
            "SELECT start_slot, end_slot FROM free_intervals"
            " WHERE pool_id = 2 ORDER BY start_slot")] == [(0, 2), (3, 4)]
        and [r["slot"] for r in conn.execute(
            "SELECT slot FROM pool_blocked_slots WHERE pool_id = 2")] == [2])
    conn.close()
    code, body = jpost(client, "/api/lease", {"count": 2})
    check("解挡的两个槽位按 first-fit 发放", code == 200
          and [x["slot"] for x in body["issued"]] == [0, 1])
    # 7200: old slot2 and the two new leases expire; slot2 becomes free and
    # the whole range coalesces.
    jpost(client, "/api/advance", {"target_seconds": 7200})
    jpost(client, "/api/advance/commit", {})
    conn = connect(path)
    check("最后一笔阻挡到期提交后全部 4 槽可领", [
        tuple(r) for r in conn.execute(
            "SELECT start_slot, end_slot FROM free_intervals"
            " WHERE pool_id = 2")] == [(0, 4)])
    conn.close()


def test_slash56_boundary():
    print("[3b] /56 委派长度与严格边界")
    path = fresh_db()
    client = web_app(path).test_client()
    # /56 pool with /56 delegation: one slot, strict boundary network.
    code, body = jpost(client, "/api/pool",
                       {"cidr": "2001:db8:bbbb::/56", "deleg_len": 56})
    check("/56 池 + /56 委派合法（1 个前缀）", code == 200 and body["slots"] == 1)
    code, body = jpost(client, "/api/lease", {"count": 1})
    check("/56 委派前缀正确", code == 200
          and body["issued"][0]["prefix"] == "2001:db8:bbbb::/56")


def test_lease_and_pool_full():
    print("[4] 领址：60 分钟、避开未到期前缀、池满提示")
    path = fresh_db()
    client = web_app(path).test_client()
    # First lease on the old pool must take the first free slot = 3.
    code, body = jpost(client, "/api/lease", {"count": 1})
    check("首笔领址取空闲区间首槽 slot3（不回扫在租明细）",
          code == 200 and body["issued"][0]["slot"] == 3
          and body["issued"][0]["expires_at"] == 3600)

    # One-slot pool: /60 CIDR with /60 delegation.
    path2 = fresh_db()
    c2 = web_app(path2).test_client()
    jpost(c2, "/api/pool", {"cidr": "2001:db8:cccc::/60", "deleg_len": 60})
    code, body = jpost(c2, "/api/lease", {"count": 1})
    check("单槽池首次领址成功", code == 200 and body["issued"][0]["slot"] == 0)
    code, body = jpost(c2, "/api/lease", {"count": 1})
    check("池满返回 400 提示且不写入", code == 400 and "池满" in body["error"])
    conn = connect(path2)
    check("池满后新池在租仍为 1 条", conn.execute(
        "SELECT COUNT(*) c FROM leases l JOIN pools p ON p.id=l.pool_id"
        " WHERE p.is_new=1 AND l.status='active'").fetchone()["c"] == 1)
    conn.close()


def test_expiry_with_pause_leases():
    print("[5] 时钟推进：暂停期间新领且本次到期的租约须计入")
    path = fresh_db()
    client = web_app(path).test_client()
    gw = gateway_app(path).test_client()

    code, body = jpost(client, "/api/advance", {"target_seconds": 3600})
    check("暂停核算成功，预计到期旧租约 2 条（slot0/1）",
          code == 200 and sorted(body["expected_expiries"]) == [1, 2])

    # Leasing stays available while paused; new lease expires at 0+3600.
    code, leased = jpost(client, "/api/lease", {"count": 1})
    new_id = leased["issued"][0]["id"]
    check("暂停期间照常领址", code == 200 and leased["issued"][0]["expires_at"] == 3600)
    # Gateway keeps answering with committed state during the pause.
    check("暂停期间网关仍可查撤路",
          gw.get("/withdrawal").status_code == 200)

    code, body = jpost(client, "/api/advance/commit")
    check("提交成功", code == 200)
    check("本次到期共 3 条，暂停新领租约计入 added",
          sorted(body["settled"]) == [1, 2, new_id] and body["added"] == [new_id])
    conn = connect(path)
    check("时钟推进到 3600",
          conn.execute("SELECT now_seconds FROM meta").fetchone()[0] == 3600)
    check("到期租约状态为 expired", conn.execute(
        "SELECT COUNT(*) c FROM leases WHERE id IN (1,2,?) AND status='expired'",
        (new_id,)).fetchone()["c"] == 3)
    # Slots 0,1 and the new slot-3 lease expire together, but slot 2 is still
    # leased until t=7200, so its gap must remain. The released ranges
    # coalesce to [0,2) and [3,5); slot 4's pre-expired range stays inside.
    fr = [tuple(r) for r in conn.execute(
        "SELECT start_slot,end_slot FROM free_intervals ORDER BY start_slot")]
    check("释放槽位按占用缺口合并：[0,2),[3,5),[7,4096)",
          fr == [(0, 2), (3, 5), (7, 4096)])
    check("未到期旧租约（slot2/5/6）保持 active", conn.execute(
        "SELECT COUNT(*) c FROM leases WHERE status='active'").fetchone()["c"] == 3)
    conn.close()
    view = gw.get("/withdrawal").get_json()
    check("撤路结论随提交同时生效：最早撤路变为 7200",
          view["earliest_withdrawal_seconds"] == 7200
          and view["uncovered_count"] == 3)

    # Second batch: slots 2 and 5 expire at t=7200; the whole [0,6) run must
    # now collapse into one maximal range despite cross-batch release order.
    jpost(client, "/api/advance", {"target_seconds": 7200})
    code, body = jpost(client, "/api/advance/commit")
    check("第二批推进成功", code == 200 and sorted(body["settled"]) == [3, 5])
    conn = connect(path)
    fr2 = [tuple(r) for r in conn.execute(
        "SELECT start_slot,end_slot FROM free_intervals ORDER BY start_slot")]
    check("跨批次相邻释放塌缩为 [0,6),[7,4096)", fr2 == [(0, 6), (7, 4096)])
    conn.close()


def test_list_changed_redo():
    print("[6] 名单变化退回：提示重做，保留原时钟与已领租约")
    path = fresh_db()
    client = web_app(path).test_client()
    jpost(client, "/api/advance", {"target_seconds": 3600})
    code, leased = jpost(client, "/api/lease", {"count": 1})
    kept_id = leased["issued"][0]["id"]

    # Simulate an external change to the due list (lease 2 no longer due).
    conn = connect(path)
    conn.execute("UPDATE leases SET expires_at = 9000 WHERE id = 2")
    conn.close()

    code, body = jpost(client, "/api/advance/commit")
    check("名单变化返回 409 并要求重做", code == 409 and body.get("redo_required"))
    conn = connect(path)
    check("退回后时钟仍为 0",
          conn.execute("SELECT now_seconds FROM meta").fetchone()[0] == 0)
    check("退回后 pending 已清除",
          conn.execute("SELECT pending_id FROM meta").fetchone()[0] is None)
    check("暂停期间所领租约保留且 active", conn.execute(
        "SELECT status FROM leases WHERE id=?", (kept_id,)).fetchone()[0] == "active")
    check("被改动租约的修改也未回滚（仅推进事务退回）", conn.execute(
        "SELECT expires_at FROM leases WHERE id=2").fetchone()[0] == 9000)
    conn.close()

    # Redo at the same target: lease 2 is no longer due, while lease 1 and
    # the lease taken during the aborted pause (kept_id) still expire at 3600.
    code, body = jpost(client, "/api/advance", {"target_seconds": 3600})
    check("可重新发起推进", code == 200
          and sorted(body["expected_expiries"]) == sorted([1, kept_id]))
    code, body = jpost(client, "/api/advance/commit")
    check("重做后提交成功", code == 200
          and sorted(body["settled"]) == sorted([1, kept_id]))


def test_commit_interruption():
    print("[7] 提交中断：进程在 COMMIT 前丢失，按同一已提交状态恢复")
    path = fresh_db()
    client = web_app(path).test_client()
    gw = gateway_app(path).test_client()
    jpost(client, "/api/advance", {"target_seconds": 7200})
    code, leased = jpost(client, "/api/lease", {"count": 2})
    kept_ids = [x["id"] for x in leased["issued"]]

    resp = client.post("/api/advance/commit", json={"fault_inject": True})
    check("故障注入返回 500", resp.status_code == 500
          and resp.get_json()["restart_recovery"])

    # A new request triggers startup-style recovery of the stale pending row.
    recovered = recover_pending(connect(path))
    check("恢复流程识别并中止悬挂推进", recovered is not None)
    conn = connect(path)
    check("恢复后时钟仍为 0（无部分生效）",
          conn.execute("SELECT now_seconds FROM meta").fetchone()[0] == 0)
    check("恢复后无 pending",
          conn.execute("SELECT pending_id FROM meta").fetchone()[0] is None)
    check("已领租约全部保留 active", conn.execute(
        "SELECT COUNT(*) c FROM leases WHERE id IN (%s) AND status='active'"
        % ",".join("?" * len(kept_ids)), kept_ids).fetchone()["c"] == 2)
    check("无租约被提前到期", conn.execute(
        "SELECT COUNT(*) c FROM leases WHERE status='expired'"
    ).fetchone()["c"] == 1)  # only the pre-seeded expired one
    conn.close()
    check("网关在恢复后读到一致已提交状态",
          gw.get("/withdrawal").get_json()["earliest_withdrawal_seconds"] == 3600)


def test_restart_and_no_scan():
    print("[8] 重启不回扫在租明细；找空位开销不随连续占用段增长")
    path = fresh_db()
    env = dict(os.environ, DRILL_DB=path, PYTHONPATH=os.path.dirname(
        os.path.abspath(__file__)))
    code = ("from app.web import create_app;"
            "create_app(%r).run(port=8077, use_reloader=False)" % path)
    proc = subprocess.Popen([sys.executable, "-c", code], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        _wait_port(8077)
        def call(url, payload):
            import json
            req = urllib.request.Request(
                "http://127.0.0.1:8077" + url,
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"}, method="POST")
            return json.loads(urllib.request.urlopen(req).read())
        # /48 pool with /60 delegation -> 4096 slots; take 500 contiguous.
        call("/api/pool", {"cidr": "2001:db8:dddd::/48", "deleg_len": 60})
        call("/api/lease", {"count": 500})
    finally:
        proc.kill()
        proc.wait()

    conn = connect(path)
    frags = conn.execute(
        "SELECT COUNT(*) c FROM free_intervals WHERE pool_id=2").fetchone()["c"]
    check("重启前空闲区间仅 1 个连续段 [500,4096)", frags == 1)
    conn.close()

    # Restart: no lease scan rebuilds free space; persisted ranges are used.
    proc = subprocess.Popen([sys.executable, "-c", code], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        _wait_port(8077)
        import json
        req = urllib.request.Request(
            "http://127.0.0.1:8077/api/lease",
            data=json.dumps({"count": 1}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        t0 = time.perf_counter()
        body = json.loads(urllib.request.urlopen(req).read())
        elapsed_ms = (time.perf_counter() - t0) * 1000
        check("重启后首笔领址直接命中持久化区间首槽 slot500",
              body["issued"][0]["slot"] == 500)
        check("找空位耗时不随 500 条连续占用增长（实测 %.2fms < 200ms）" % elapsed_ms,
              elapsed_ms < 200)
    finally:
        proc.kill()
        proc.wait()


def test_legacy_volume_migration():
    print("[8b] 旧卷校正：暂停领址、重启可重试、保留已发租约")
    path = fresh_db()
    # Emulate a volume written by an older build: the swap is committed with
    # one whole-pool free range, a new-pool lease was already issued on an
    # overlapping slot, and there is no calibration/blocked-slot data.
    conn = connect(path)
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "INSERT INTO pools(cidr, prefix_len, deleg_len, slot_count, is_new)"
        " VALUES ('2001:db8:aaaa::/55', 55, 60, 32, 1)")
    conn.execute(
        "INSERT INTO free_intervals(pool_id, start_slot, end_slot)"
        " VALUES (2, 1, 32)")
    conn.execute("UPDATE meta SET new_pool_id = 2")
    conn.execute(
        "INSERT INTO leases(pool_id, slot, prefix, starts_at, expires_at, status)"
        " VALUES (2, 0, '2001:db8:aaaa::/60', 0, 7200, 'active')")
    conn.execute("COMMIT")
    conn.close()
    before = connect(path)
    history = [tuple(r) for r in before.execute(
        "SELECT id, slot, expires_at, status FROM leases ORDER BY id")]
    now_before = before.execute("SELECT now_seconds FROM meta").fetchone()[0]
    before.close()

    # Before any correction: leasing is paused (only a restart calibrates).
    from app.logic import DrillError, allocate
    conn = connect(path)
    try:
        allocate(conn, 1)
        paused = False
    except DrillError as exc:
        paused = "校正" in str(exc)
    conn.close()
    check("旧卷校正完成前暂停领址", paused)

    # Interrupted correction: pending marker committed, data not updated.
    conn = connect(path)
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "INSERT INTO pool_calibration(pool_id, state, detail, updated_at)"
        " VALUES (2, 'pending', 'interrupted', 0)")
    conn.execute("COMMIT")
    conn.close()
    conn = connect(path)
    check("校正中断后仍是 pending 且不可领址", conn.execute(
        "SELECT state FROM pool_calibration WHERE pool_id=2").fetchone()[0]
        == "pending")
    conn.close()

    # Restart retries and commits the correction in one transaction.
    web = web_app(path).test_client()
    conn = connect(path)
    check("重启重试后校正完成", conn.execute(
        "SELECT state FROM pool_calibration WHERE pool_id=2").fetchone()[0]
        == "done")
    check("校正后新池余量排除 5 个旧租约前缀", conn.execute(
        "SELECT SUM(end_slot-start_slot) s FROM free_intervals"
        " WHERE pool_id=2").fetchone()["s"] == 27)
    check("已发租约与到期时间原样保留",
          [tuple(r) for r in conn.execute(
              "SELECT id, slot, expires_at, status FROM leases ORDER BY id")]
          == history)
    check("校正不推进模拟时钟",
          conn.execute("SELECT now_seconds FROM meta").fetchone()[0]
          == now_before)
    conn.close()

    code, body = jpost(web, "/api/lease", {"count": 1})
    check("校正后首笔领址落在未被占用的槽位", code == 200
          and body["issued"][0]["slot"] == 3)
    # The legacy new-pool lease on slot 0 must stay unavailable even though it
    # predates the block rows.
    conn = connect(path)
    check("旧卷遗留的新池在租槽位未被重复发放", conn.execute(
        "SELECT COUNT(*) c FROM leases WHERE pool_id=2 AND slot=0"
    ).fetchone()["c"] == 1)
    conn.close()

    # The same old lease keeps blocking until the real expiry: at 3600 only
    # old slot0's own lease expires; but the legacy new lease (slot 0, 7200)
    # still holds it, so the slot must remain unavailable.
    jpost(web, "/api/advance", {"target_seconds": 3600})
    jpost(web, "/api/advance/commit", {})
    conn = connect(path)
    check("旧租约到期后仍被在租新租约占用的槽位不释放", conn.execute(
        "SELECT start_slot FROM free_intervals WHERE pool_id=2"
        " AND start_slot <= 0 AND end_slot > 0").fetchone() is None)
    conn.close()
    jpost(web, "/api/advance", {"target_seconds": 7200})
    jpost(web, "/api/advance/commit", {})
    conn = connect(path)
    check("该新租约自身到期提交后槽位释放", conn.execute(
        "SELECT 1 FROM free_intervals WHERE pool_id=2"
        " AND start_slot <= 0 AND end_slot > 0").fetchone() is not None)
    conn.close()

    # A second restart must not redo anything or rewrite the ranges.
    web2 = web_app(path).test_client()
    conn = connect(path)
    check("再次重启不重复校正", [tuple(r) for r in conn.execute(
        "SELECT pool_id, state FROM pool_calibration")] == [(2, "done")])
    conn.close()
    check("再次重启后仍可正常领址",
          web2.post("/api/lease", json={"count": 1}).status_code == 200)


def _wait_port(port, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen("http://127.0.0.1:%d/healthz" % port, timeout=1)
            return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("port %d not ready" % port)


def test_abort_and_non_advance():
    print("[9] 手动退回与非法推进参数")
    path = fresh_db()
    client = web_app(path).test_client()
    code, _ = jpost(client, "/api/advance", {"target_seconds": 3600})
    check("可暂停", code == 200)
    code, body = jpost(client, "/api/advance", {"target_seconds": 7200})
    check("暂停中拒绝第二次暂停", code == 400)
    code, _ = jpost(client, "/api/advance/abort")
    check("手动退回成功", code == 200)
    code, body = jpost(client, "/api/advance", {"target_seconds": 0})
    check("目标时刻不大于当前时钟被拒", code == 400)
    conn = connect(path)
    check("退回/拒绝后时钟保持 0",
          conn.execute("SELECT now_seconds FROM meta").fetchone()[0] == 0)
    conn.close()


if __name__ == "__main__":
    tests = [test_seed_and_gateway, test_invalid_config_keeps_state,
             test_swap_and_overlap, test_slash56_boundary,
             test_overlap_capacity_and_release,
             test_batch_unblocks_only_due_leases,
             test_multi_slot_overlap_and_partial_unblock,
             test_lease_and_pool_full,
             test_expiry_with_pause_leases, test_list_changed_redo,
             test_commit_interruption, test_restart_and_no_scan,
             test_legacy_volume_migration,
             test_abort_and_non_advance]
    for t in tests:
        t()
    print("\n结果：%d 通过, %d 失败" % (PASS, FAIL))
    sys.exit(1 if FAIL else 0)
