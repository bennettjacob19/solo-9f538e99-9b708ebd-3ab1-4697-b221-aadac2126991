"""Business logic: pool swap, leasing, simulated clock and withdrawal data.

Concurrency model (SQLite WAL, single writer):
  * Reads never block: the gateway process opens the database read-only.
  * Every state-changing request runs in one BEGIN IMMEDIATE transaction.
  * A prepared clock advance is staging data; leasing stays available while
    it is pending. COMMIT either publishes clock + free ranges + withdrawal
    together, or rolls everything back, so both processes always observe one
    consistent committed state.
"""
import ipaddress
import sqlite3

from .common import (LEASE_SECONDS, overlapping_slots, slot_prefix,
                     validate_pool)
from .db import connect


class DrillError(Exception):
    """Validation / state conflict reported to the operator (HTTP 4xx)."""


class ListChanged(DrillError):
    """The expiry set changed between prepare and commit: redo required."""


# --------------------------------------------------------------------------
# Reads
# --------------------------------------------------------------------------

def get_state(conn):
    meta = conn.execute("SELECT * FROM meta WHERE id = 1").fetchone()
    pools = conn.execute("SELECT * FROM pools ORDER BY id").fetchall()
    pending = None
    if meta["pending_id"]:
        pending = conn.execute(
            "SELECT * FROM clock_advance WHERE id = ?",
            (meta["pending_id"],)).fetchone()
    return {"meta": meta, "pools": pools, "pending": pending}


def active_pool_id(conn, meta=None):
    meta = meta or conn.execute("SELECT * FROM meta WHERE id = 1").fetchone()
    return meta["new_pool_id"] or conn.execute(
        "SELECT id FROM pools WHERE is_new = 0").fetchone()["id"]


def free_summary(conn, pool_id):
    rows = conn.execute(
        "SELECT start_slot, end_slot FROM free_intervals"
        " WHERE pool_id = ? ORDER BY start_slot", (pool_id,)).fetchall()
    free = sum(r["end_slot"] - r["start_slot"] for r in rows)
    total = conn.execute(
        "SELECT slot_count FROM pools WHERE id = ?", (pool_id,)).fetchone()["slot_count"]
    blocked = conn.execute(
        "SELECT COUNT(DISTINCT slot) AS c FROM pool_blocked_slots"
        " WHERE pool_id = ?", (pool_id,)).fetchone()["c"]
    return {"total": total, "free": free, "fragments": len(rows),
            "blocked": blocked}


def _block_new_pool(conn, pool_id, network, deleg_len, slot_count):
    """Exclude every new-pool slot intersecting a valid old-pool lease.

    Rows are written to `pool_blocked_slots` and carved out of
    `free_intervals` (never by enumerating candidate prefixes: only the
    lease rows that really intersect contribute). Returns the number of
    slots carved out by this call.
    """
    old = conn.execute("SELECT id FROM pools WHERE is_new = 0").fetchone()
    rows = conn.execute(
        "SELECT id, prefix FROM leases WHERE pool_id = ? AND status = 'active'"
        " ORDER BY id", (old["id"],)).fetchall()
    blocked = 0
    for lease in rows:
        for slot in overlapping_slots(network, deleg_len, lease["prefix"],
                                      slot_count):
            cur = conn.execute(
                "INSERT OR IGNORE INTO pool_blocked_slots(pool_id, slot, lease_id)"
                " VALUES (?, ?, ?)", (pool_id, slot, lease["id"]))
            if cur.rowcount and _remove_slot(conn, pool_id, slot):
                blocked += 1
    return blocked


def calibrate_pool(conn, now=None):
    """Bring a pool's overlap accounting up to date (old-volume migration).

    Runs in one transaction. If the stored free ranges still cover the whole
    pool the blocked slots are carved out from the persisted ranges; if the
    volume is already partially migrated the blocked rows are reconciled
    against the current free ranges. Idempotent, so an interrupted correction
    is simply retried on the next start. Never expands candidate prefixes.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        meta = conn.execute("SELECT * FROM meta WHERE id = 1").fetchone()
        now = meta["now_seconds"] if now is None else now
        pool_id = meta["new_pool_id"]
        if pool_id is None:
            conn.execute("COMMIT")
            return None
        row = conn.execute(
            "SELECT * FROM pools WHERE id = ?", (pool_id,)).fetchone()
        network = ipaddress.IPv6Network(row["cidr"])
        _block_new_pool(conn, pool_id, network, row["deleg_len"],
                        row["slot_count"])
        # A volume written by an older build may also carry active new-pool
        # leases whose slots are still listed as free; never offer those.
        for lease in conn.execute(
                "SELECT slot FROM leases WHERE pool_id = ? AND status = 'active'",
                (pool_id,)).fetchall():
            _remove_slot(conn, pool_id, lease["slot"])
        _defragment(conn, pool_id)
        # Report every blocked slot, including ones recorded by an earlier
        # interrupted attempt (a retried correction must still say so).
        blocked = conn.execute(
            "SELECT COUNT(DISTINCT slot) AS c FROM pool_blocked_slots"
            " WHERE pool_id = ?", (pool_id,)).fetchone()["c"]
        conn.execute(
            "INSERT INTO pool_calibration(pool_id, state, detail, updated_at)"
            " VALUES (?, 'done', ?, ?)"
            " ON CONFLICT(pool_id) DO UPDATE SET state = 'done',"
            " detail = excluded.detail, updated_at = excluded.updated_at",
            (pool_id, "校正完成：%d 个新池前缀被未到期旧租约占用，已从余量排除"
             % blocked, now))
        conn.execute(
            "INSERT INTO clock_log(advance_id, event, from_seconds, to_seconds,"
            " detail, at_seconds) VALUES (NULL, 'calibrate', ?, ?, ?, ?)",
            (now, now, "新池 %s 校正完成，阻挡槽位 %d 个" % (row["cidr"], blocked),
             now))
        conn.execute("COMMIT")
        return blocked
    except Exception:
        conn.execute("ROLLBACK")
        raise


def calibration_state(conn, pool_id):
    """'done', 'pending' or None (old volume not migrated yet)."""
    row = conn.execute(
        "SELECT state FROM pool_calibration WHERE pool_id = ?",
        (pool_id,)).fetchone()
    return row["state"] if row else None


# --------------------------------------------------------------------------
# Free-space management: persisted half-open [start, end) slot ranges
# --------------------------------------------------------------------------

def _take_first_slot(conn, pool_id):
    """First-fit allocation. Cost is one indexed lookup against free ranges;
    the number of leases in an occupied run never enters the scan."""
    row = conn.execute(
        "SELECT rowid, start_slot, end_slot FROM free_intervals"
        " WHERE pool_id = ? ORDER BY start_slot LIMIT 1", (pool_id,)).fetchone()
    if row is None:
        return None
    slot = row["start_slot"]
    if row["end_slot"] - slot == 1:
        conn.execute(
            "DELETE FROM free_intervals WHERE rowid = ?", (row["rowid"],))
    else:
        conn.execute(
            "UPDATE free_intervals SET start_slot = ? WHERE rowid = ?",
            (slot + 1, row["rowid"]))
    return slot


def _remove_slot(conn, pool_id, slot):
    """Carve one slot out of the persisted free ranges (used when a new-pool
    slot is blocked by an unexpired old lease). Cost is one indexed lookup
    plus at most one range split; leases are never scanned."""
    row = conn.execute(
        "SELECT rowid, start_slot, end_slot FROM free_intervals"
        " WHERE pool_id = ? AND start_slot <= ? AND end_slot > ?",
        (pool_id, slot, slot)).fetchone()
    if row is None:
        return False
    start, end = row["start_slot"], row["end_slot"]
    if end - start == 1:
        conn.execute(
            "DELETE FROM free_intervals WHERE rowid = ?", (row["rowid"],))
    elif start == slot:
        conn.execute(
            "UPDATE free_intervals SET start_slot = ? WHERE rowid = ?",
            (slot + 1, row["rowid"]))
    elif end == slot + 1:
        conn.execute(
            "UPDATE free_intervals SET end_slot = ? WHERE rowid = ?",
            (slot, row["rowid"]))
    else:
        conn.execute(
            "UPDATE free_intervals SET end_slot = ? WHERE rowid = ?",
            (slot, row["rowid"]))
        conn.execute(
            "INSERT INTO free_intervals(pool_id, start_slot, end_slot)"
            " VALUES (?, ?, ?)", (pool_id, slot + 1, end))
    return True


def _release_slot(conn, pool_id, slot):
    """Return a slot to the free ranges unless it is still unusable.

    A slot is not released while an active lease of the same pool still holds
    it (legacy overlap) or while a valid old-pool lease intersects it. Both
    checks are indexed point lookups, never a scan of the lease table.
    Coalescing happens once per batch in `_defragment`.
    """
    if conn.execute(
            "SELECT 1 FROM pool_blocked_slots WHERE pool_id = ? AND slot = ?"
            " LIMIT 1", (pool_id, slot)).fetchone():
        return
    if conn.execute(
            "SELECT 1 FROM leases WHERE pool_id = ? AND slot = ?"
            " AND status = 'active' LIMIT 1", (pool_id, slot)).fetchone():
        return
    if conn.execute(
            "SELECT 1 FROM free_intervals WHERE pool_id = ?"
            " AND start_slot <= ? AND end_slot > ? LIMIT 1",
            (pool_id, slot, slot)).fetchone():
        return
    conn.execute(
        "INSERT INTO free_intervals(pool_id, start_slot, end_slot)"
        " VALUES (?, ?, ?)", (pool_id, slot, slot + 1))


def _defragment(conn, pool_id):
    """Merge all mutually adjacent free ranges of one pool into maximal
    ranges. Cost is O(f) in the number of free fragments, independent of the
    number of leases. Called once after a clock-advance batch so releases in
    arbitrary order still collapse to the same final interval set."""
    rows = conn.execute(
        "SELECT rowid, start_slot, end_slot FROM free_intervals"
        " WHERE pool_id = ? ORDER BY start_slot", (pool_id,)).fetchall()
    if len(rows) <= 1:
        return
    merged_start, merged_end = rows[0]["start_slot"], rows[0]["end_slot"]
    stale = [rows[0]["rowid"]]
    for row in rows[1:]:
        if row["start_slot"] == merged_end:
            merged_end = row["end_slot"]
            stale.append(row["rowid"])
        else:
            if stale:
                conn.execute(
                    "UPDATE free_intervals SET start_slot = ?, end_slot = ?"
                    " WHERE rowid = ?",
                    (merged_start, merged_end, stale[0]))
                for rid in stale[1:]:
                    conn.execute(
                        "DELETE FROM free_intervals WHERE rowid = ?", (rid,))
            merged_start, merged_end = row["start_slot"], row["end_slot"]
            stale = [row["rowid"]]
    conn.execute(
        "UPDATE free_intervals SET start_slot = ?, end_slot = ? WHERE rowid = ?",
        (merged_start, merged_end, stale[0]))
    for rid in stale[1:]:
        conn.execute("DELETE FROM free_intervals WHERE rowid = ?", (rid,))


def _reconcile_blocked(conn, new_pool, lease_id):
    """Drop one settled lease's blocks and free only the slots nothing else
    still holds. Touches just that lease's block rows (indexed), so the cost
    is the number of slots that lease intersected, never the lease table."""
    rows = conn.execute(
        "SELECT slot FROM pool_blocked_slots WHERE lease_id = ?"
        " AND pool_id = ?", (lease_id, new_pool["id"])).fetchall()
    for row in rows:
        slot = row["slot"]
        conn.execute(
            "DELETE FROM pool_blocked_slots"
            " WHERE pool_id = ? AND lease_id = ? AND slot = ?",
            (new_pool["id"], lease_id, slot))
        still_blocked = conn.execute(
            "SELECT 1 FROM pool_blocked_slots WHERE pool_id = ? AND slot = ?"
            " LIMIT 1", (new_pool["id"], slot)).fetchone()
        if still_blocked:
            continue
        _release_slot(conn, new_pool["id"], slot)


# --------------------------------------------------------------------------
# Pool swap
# --------------------------------------------------------------------------

def change_pool(conn, cidr_text, deleg_len):
    """Validate and bring the new pool online. Invalid input raises before
    any write, so the previous state is preserved exactly."""
    try:
        network, deleg_len, slot_count = validate_pool(cidr_text, int(deleg_len))
    except (TypeError, ValueError) as exc:
        raise DrillError("新池配置无效，已保留原状：%s" % exc)
    cidr = str(network)

    conn.execute("BEGIN IMMEDIATE")
    try:
        meta = conn.execute("SELECT * FROM meta WHERE id = 1").fetchone()
        if meta["new_pool_id"] is not None:
            raise DrillError("本演练为单次换池，新池已上线，不能再次换池")
        if meta["pending_id"] is not None:
            raise DrillError("时钟推进处于暂停待提交状态，请先提交或退回后再换池")
        dup = conn.execute("SELECT 1 FROM pools WHERE cidr = ?", (cidr,)).fetchone()
        if dup:
            raise DrillError("该 CIDR 已存在，配置无效，已保留原状")
        now = meta["now_seconds"]
        cur = conn.execute(
            "INSERT INTO pools(cidr, prefix_len, deleg_len, slot_count, is_new)"
            " VALUES (?, ?, ?, ?, 1)",
            (cidr, network.prefixlen, deleg_len, slot_count))
        new_id = cur.lastrowid
        # One range covering the whole pool: no candidate prefixes expanded.
        conn.execute(
            "INSERT INTO free_intervals(pool_id, start_slot, end_slot)"
            " VALUES (?, 0, ?)", (new_id, slot_count))
        # Mark calibration pending first: if anything fails below, the whole
        # transaction rolls back and no new pool is visible at all.
        conn.execute(
            "INSERT INTO pool_calibration(pool_id, state, detail, updated_at)"
            " VALUES (?, 'pending', ?, ?)",
            (new_id, "换池后校正未完成，暂停领址", now))
        conn.execute(
            "UPDATE meta SET new_pool_id = ? WHERE id = 1", (new_id,))
        conn.execute(
            "INSERT INTO clock_log(advance_id, event, from_seconds, to_seconds,"
            " detail, at_seconds) VALUES (NULL, 'pool-swap', ?, ?, ?, ?)",
            (now, now, "新池 %s 上线，委派 /%d，共 %d 个前缀；旧租约按原期限到期"
             % (cidr, deleg_len, slot_count), now))
        # Same transaction: exclude every slot that an active old lease still
        # occupies, so neither the remaining capacity nor allocation can
        # offer a prefix that overlaps a valid lease.
        _block_new_pool(conn, new_id, network, deleg_len, slot_count)
        _defragment(conn, new_id)
        blocked = conn.execute(
            "SELECT COUNT(DISTINCT slot) AS c FROM pool_blocked_slots"
            " WHERE pool_id = ?", (new_id,)).fetchone()["c"]
        conn.execute(
            "UPDATE pool_calibration SET state = 'done', detail = ?,"
            " updated_at = ? WHERE pool_id = ?",
            (blocked, now, new_id))
        _republish_withdrawal(conn)
        conn.execute("COMMIT")
        return new_id, cidr, deleg_len, slot_count, blocked
    except Exception:
        conn.execute("ROLLBACK")
        raise


def startup_calibrate(db_path=None, max_attempts=3):
    """One-shot startup step for the writer process.

    A committed new pool without a `done` calibration (i.e. a volume written
    by an older build) is paused for leasing first, then corrected in one
    transaction. Any interruption rolls the correction back while the
    pending marker stays committed, so the next start simply retries. On an
    already-calibrated volume this is two indexed lookups and never a lease
    scan. Returns the correction result, or None when nothing was needed.
    """
    last = None
    for _ in range(max_attempts):
        conn = connect(db_path)
        try:
            meta = conn.execute("SELECT * FROM meta WHERE id = 1").fetchone()
            pool_id = meta["new_pool_id"]
            if pool_id is None or calibration_state(conn, pool_id) == "done":
                return None
            conn.execute("BEGIN IMMEDIATE")
            try:
                now = conn.execute(
                    "SELECT now_seconds FROM meta WHERE id = 1").fetchone()[
                        "now_seconds"]
                conn.execute(
                    "INSERT INTO pool_calibration(pool_id, state, detail,"
                    " updated_at) VALUES (?, 'pending', ?, ?)"
                    " ON CONFLICT(pool_id) DO UPDATE SET state = 'pending',"
                    " detail = excluded.detail, updated_at = excluded.updated_at",
                    (pool_id, "旧卷校正未完成，暂停领址", now))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        try:
            cal = connect(db_path)
            try:
                return calibrate_pool(cal)
            finally:
                cal.close()
        except sqlite3.OperationalError as exc:
            # Concurrent writer holds the lock: pending stays, try again.
            last = exc
        except Exception as exc:  # leave pending; a restart retries
            last = exc
            break
    return last


# --------------------------------------------------------------------------
# Leasing
# --------------------------------------------------------------------------

def allocate(conn, count=1):
    """Issue 60-minute lease(s) from the active pool, skipping unexpired
    prefixes via the free ranges. Available even while an advance is paused.
    """
    try:
        count = int(count)
    except (TypeError, ValueError):
        raise DrillError("领址数量必须是整数")
    if count < 1 or count > 4096:
        raise DrillError("领址数量需在 1..4096 之间")

    conn.execute("BEGIN IMMEDIATE")
    try:
        meta = conn.execute("SELECT * FROM meta WHERE id = 1").fetchone()
        pool_id = active_pool_id(conn, meta)
        pool = conn.execute(
            "SELECT * FROM pools WHERE id = ?", (pool_id,)).fetchone()
        # A volume from an older build may still hold a new pool whose
        # overlap accounting has not been corrected. Never lease from it
        # until the one-shot startup/retry calibration has committed.
        if pool["is_new"] and meta["new_pool_id"] is not None:
            cal = conn.execute(
                "SELECT state FROM pool_calibration WHERE pool_id = ?",
                (pool_id,)).fetchone()
            if cal is None or cal["state"] != "done":
                raise DrillError("旧卷校正未完成，已暂停领址；请重启服务完成校正后重试")
        network = ipaddress.IPv6Network(pool["cidr"])
        now = meta["now_seconds"]
        issued = []
        for _ in range(count):
            slot = _take_first_slot(conn, pool_id)
            if slot is None:
                raise DrillError("池 %s 已无可分配前缀（池满），本次请求未写入"
                                 % pool["cidr"])
            pfx = slot_prefix(network, pool["deleg_len"], slot)
            cur = conn.execute(
                "INSERT INTO leases(pool_id, slot, prefix, starts_at,"
                " expires_at, status) VALUES (?, ?, ?, ?, ?, 'active')",
                (pool_id, slot, pfx, now, now + LEASE_SECONDS))
            issued.append({"id": cur.lastrowid, "prefix": pfx, "slot": slot,
                           "starts_at": now, "expires_at": now + LEASE_SECONDS})
        conn.execute(
            "INSERT INTO clock_log(advance_id, event, from_seconds, to_seconds,"
            " detail, at_seconds) VALUES (NULL, 'lease', ?, ?, ?, ?)",
            (now, now, "从 %s 发放 %d 个 60 分钟租约（租约 id %s）"
             % (pool["cidr"], len(issued),
                ",".join(str(x["id"]) for x in issued)), now))
        conn.execute("COMMIT")
        return pool["cidr"], issued
    except Exception:
        conn.execute("ROLLBACK")
        raise


# --------------------------------------------------------------------------
# Simulated clock: prepare (pause) / commit / abort
# --------------------------------------------------------------------------

def _expiry_set(conn, target):
    rows = conn.execute(
        "SELECT id FROM leases WHERE status = 'active' AND expires_at <= ?"
        " ORDER BY id", (target,)).fetchall()
    return [r["id"] for r in rows]


def prepare_advance(conn, target):
    try:
        target = int(target)
    except (TypeError, ValueError):
        raise DrillError("目标时刻必须是整数秒")
    conn.execute("BEGIN IMMEDIATE")
    try:
        meta = conn.execute("SELECT * FROM meta WHERE id = 1").fetchone()
        if meta["pending_id"] is not None:
            raise DrillError("已有暂停中的时钟推进，请先提交或退回")
        now = meta["now_seconds"]
        if target <= now:
            raise DrillError("目标时刻必须晚于当前模拟时钟 %d" % now)
        ids = _expiry_set(conn, target)
        max_id = conn.execute(
            "SELECT COALESCE(MAX(id), 0) AS m FROM leases").fetchone()["m"]
        cur = conn.execute(
            "INSERT INTO clock_advance(target_seconds, snapshot_ids,"
            " snapshot_max_id, state) VALUES (?, ?, ?, 'pending')",
            (target, ",".join(map(str, ids)), max_id))
        conn.execute(
            "UPDATE meta SET pending_id = ? WHERE id = 1", (cur.lastrowid,))
        conn.execute(
            "INSERT INTO clock_log(advance_id, event, from_seconds, to_seconds,"
            " detail, at_seconds) VALUES (?, 'prepare', ?, ?, ?, ?)",
            (cur.lastrowid, now, target,
             "暂停待提交，本次预计到期租约 %d 条：%s"
             % (len(ids), ",".join(map(str, ids)) or "无"), now))
        conn.execute("COMMIT")
        return cur.lastrowid, target, ids
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _abort_pending(conn, advance_id, reason, now):
    conn.execute(
        "UPDATE clock_advance SET state = 'aborted' WHERE id = ?", (advance_id,))
    conn.execute("UPDATE meta SET pending_id = NULL WHERE id = 1")
    conn.execute(
        "INSERT INTO clock_log(advance_id, event, from_seconds, to_seconds,"
        " detail, at_seconds) VALUES (?, 'abort', ?, ?, ?, ?)",
        (advance_id, now, now, reason, now))


def commit_advance(conn, fault_inject=False):
    """Atomically expire this batch, advance the clock and republish the
    withdrawal conclusion. New leases taken during the pause that expire in
    the same batch are included. A changed expiry list aborts with the
    original clock and all issued leases intact."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        meta = conn.execute("SELECT * FROM meta WHERE id = 1").fetchone()
        if not meta["pending_id"]:
            raise DrillError("没有暂停中的时钟推进可提交")
        adv = conn.execute(
            "SELECT * FROM clock_advance WHERE id = ?",
            (meta["pending_id"],)).fetchone()
        target = adv["target_seconds"]
        snapshot = [int(x) for x in adv["snapshot_ids"].split(",") if x]
        current = _expiry_set(conn, target)
        snap_set, cur_set = set(snapshot), set(current)
        if snap_set - cur_set:
            # A lease that was due has disappeared from the due list: the
            # list changed, so the operator must redo.
            missing = sorted(snap_set - cur_set)
            _abort_pending(
                conn, adv["id"],
                "到期名单变化（缺失 %s），退回并保留原时钟与已领租约，请重做"
                % ",".join(map(str, missing)), meta["now_seconds"])
            conn.execute("COMMIT")
            raise ListChanged(
                "到期名单变化，已退回，请重做（原时钟 %d 与已领租约均保留）"
                % meta["now_seconds"])

        settled = current  # contains any leases newly taken during the pause
        added = sorted(cur_set - snap_set)
        old_pool = conn.execute(
            "SELECT id FROM pools WHERE is_new = 0").fetchone()
        new_pool = None
        if meta["new_pool_id"] is not None:
            new_pool = conn.execute(
                "SELECT * FROM pools WHERE id = ?",
                (meta["new_pool_id"],)).fetchone()
        touched_pools = set()
        unblocked = 0
        for lease_id in settled:
            row = conn.execute(
                "SELECT pool_id, slot, prefix FROM leases WHERE id = ?",
                (lease_id,)).fetchone()
            conn.execute(
                "UPDATE leases SET status = 'expired' WHERE id = ?",
                (lease_id,))
            _release_slot(conn, row["pool_id"], row["slot"])
            touched_pools.add(row["pool_id"])
            # Only leases settled by *this* batch can lift a new-pool block,
            # and only the slots they actually intersect are re-examined. No
            # other lease is scanned and no candidate prefix is enumerated.
            if new_pool is not None and row["pool_id"] == old_pool["id"]:
                held = conn.execute(
                    "SELECT 1 FROM pool_blocked_slots"
                    " WHERE pool_id = ? AND lease_id = ? LIMIT 1",
                    (new_pool["id"], lease_id)).fetchone()
                if held:
                    _reconcile_blocked(conn, new_pool, lease_id)
                    unblocked += 1
                    touched_pools.add(new_pool["id"])
        # Slots released in one batch may bridge ranges that were not yet
        # adjacent at each individual release; coalesce once at the end.
        for pool_id in touched_pools:
            _defragment(conn, pool_id)

        conn.execute(
            "UPDATE meta SET now_seconds = ?, pending_id = NULL WHERE id = 1",
            (target,))
        conn.execute(
            "UPDATE clock_advance SET state = 'committed' WHERE id = ?",
            (adv["id"],))
        conn.execute(
            "INSERT INTO clock_log(advance_id, event, from_seconds, to_seconds,"
            " detail, at_seconds) VALUES (?, 'commit', ?, ?, ?, ?)",
            (adv["id"], meta["now_seconds"], target,
             "本次到期 %d 条：%s%s%s"
             % (len(settled), ",".join(map(str, settled)) or "无",
                ("；其中暂停期间新领并计入：%s" % ",".join(map(str, added)))
                if added else "",
                ("；解除新池阻挡租约 %d 条" % unblocked) if unblocked else ""),
             target))
        _republish_withdrawal(conn)
        if fault_inject:
            # Simulate losing the process immediately before COMMIT. SQLite
            # discards the open transaction on close; recovery reopens from
            # the last committed state.
            conn.close()
            raise RuntimeError("FAULT_INJECTED: process lost before COMMIT")
        conn.execute("COMMIT")
        return {"target": target, "settled": settled, "added": added}
    except DrillError:
        raise
    except RuntimeError:
        raise  # fault injection: connection already closed
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


def abort_advance(conn):
    conn.execute("BEGIN IMMEDIATE")
    try:
        meta = conn.execute("SELECT * FROM meta WHERE id = 1").fetchone()
        if not meta["pending_id"]:
            raise DrillError("没有暂停中的时钟推进可退回")
        _abort_pending(conn, meta["pending_id"],
                       "值班员手动退回，时钟与已领租约保持不变",
                       meta["now_seconds"])
        conn.execute("COMMIT")
        return meta["pending_id"]
    except Exception:
        conn.execute("ROLLBACK")
        raise


# --------------------------------------------------------------------------
# Withdrawal publication (written here, read by the independent gateway)
# --------------------------------------------------------------------------

def _republish_withdrawal(conn):
    old = conn.execute("SELECT * FROM pools WHERE is_new = 0").fetchone()
    meta = conn.execute("SELECT * FROM meta WHERE id = 1").fetchone()
    new_net = None
    new_id = None
    if meta["new_pool_id"]:
        new_row = conn.execute(
            "SELECT * FROM pools WHERE id = ?",
            (meta["new_pool_id"],)).fetchone()
        new_id = new_row["id"]
        new_net = ipaddress.IPv6Network(new_row["cidr"])

    uncovered = []
    rows = conn.execute(
        "SELECT * FROM leases WHERE pool_id = ? AND status = 'active'"
        " ORDER BY expires_at, id", (old["id"],)).fetchall()
    for row in rows:
        if new_net is None or not _is_covered(row["prefix"], new_net):
            uncovered.append(row)
    earliest = uncovered[0]["expires_at"] if uncovered else None
    conn.execute(
        "UPDATE withdrawal_publish SET pool_id = ?, cidr = ?,"
        " earliest_seconds = ?, published_at = ? WHERE id = 1",
        (new_id, str(new_net) if new_net else None, earliest,
         meta["now_seconds"]))


def _is_covered(prefix_text, new_network):
    from .common import is_covered
    return is_covered(prefix_text, new_network)


def read_published(conn):
    """Gateway-side read: the published earliest safe withdrawal time."""
    pub = conn.execute(
        "SELECT * FROM withdrawal_publish WHERE id = 1").fetchone()
    return pub


def read_uncovered_old_leases(conn):
    """Gateway-side read: valid old leases not fully covered by the new
    aggregate. Pure SELECT against committed data only."""
    pub = read_published(conn)
    old = conn.execute("SELECT * FROM pools WHERE is_new = 0").fetchone()
    rows = conn.execute(
        "SELECT * FROM leases WHERE pool_id = ? AND status = 'active'"
        " ORDER BY expires_at, id", (old["id"],)).fetchall()
    new_net = ipaddress.IPv6Network(pub["cidr"]) if pub["cidr"] else None
    out = []
    for row in rows:
        if new_net is None or not _is_covered(row["prefix"], new_net):
            out.append(dict(row))
    return out


def recent_logs(conn, limit=20):
    return conn.execute(
        "SELECT * FROM clock_log ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
