"""Writer-side Flask process: operator console for the single-gateway drill.

Endpoints (HTML + JSON API):
  GET  /                 dashboard (clock, pools, free ranges, leases, logs)
  POST /api/pool         bring the new pool online (invalid input = no write)
  POST /api/lease        issue 60-minute lease(s) from the active pool
  POST /api/advance      prepare an advance (clock pauses, drill continues)
  POST /api/advance/commit   settle the batch and publish one new state
  POST /api/advance/abort    reject the prepared advance, state preserved
"""
from flask import Flask, jsonify, redirect, render_template, request, url_for

from .db import connect, init_db, recover_pending
from .logic import (DrillError, ListChanged, abort_advance, allocate,
                    change_pool, commit_advance, free_summary, get_state,
                    prepare_advance, recent_logs)


def create_app(db_path=None):
    app = Flask(__name__)
    app.config["DB_PATH"] = db_path

    def db():
        # Recovery runs once per process at startup, not per request: a
        # pending advance is normal operator state, not a crashed process.
        return connect(app.config.get("DB_PATH"))

    @app.get("/")
    def dashboard():
        conn = db()
        try:
            state = get_state(conn)
            meta, pending = state["meta"], state["pending"]
            pool_views = []
            for pool in state["pools"]:
                summary = free_summary(conn, pool["id"])
                active_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM leases"
                    " WHERE pool_id = ? AND status = 'active'",
                    (pool["id"],)).fetchone()["c"]
                pool_views.append({
                    "pool": pool, "summary": summary, "active_count": active_count,
                    "is_active": pool["id"] == (meta["new_pool_id"]
                                                or state["pools"][0]["id"])})
            leases = conn.execute(
                "SELECT l.*, p.cidr AS pool_cidr FROM leases l"
                " JOIN pools p ON p.id = l.pool_id"
                " ORDER BY l.id DESC LIMIT 30").fetchall()
            logs = recent_logs(conn)
            return render_template(
                "dashboard.html", meta=meta, pools=pool_views, leases=leases,
                logs=logs, pending=pending)
        finally:
            conn.close()

    @app.get("/healthz")
    def healthz():
        return jsonify(ok=True)

    # -- JSON API --------------------------------------------------------

    @app.post("/api/pool")
    def api_pool():
        cidr = request.form.get("cidr") or (request.json or {}).get("cidr")
        deleg = request.form.get("deleg_len") or (request.json or {}).get("deleg_len")
        conn = db()
        try:
            new_id, cidr, deleg_len, slots = change_pool(conn, cidr, deleg)
            payload = {"ok": True, "pool_id": new_id, "cidr": cidr,
                       "deleg_len": deleg_len, "slots": slots}
            return _respond(payload, "新池 %s 已上线" % cidr)
        except DrillError as exc:
            return _respond({"ok": False, "error": str(exc)}, str(exc), ok=False)
        finally:
            conn.close()

    @app.post("/api/lease")
    def api_lease():
        src = request.form if request.form else (request.json or {})
        count = src.get("count", 1)
        conn = db()
        try:
            cidr, issued = allocate(conn, count)
            return _respond(
                {"ok": True, "pool": cidr, "issued": issued},
                "已从 %s 发放 %d 个 60 分钟租约" % (cidr, len(issued)))
        except DrillError as exc:
            return _respond({"ok": False, "error": str(exc)}, str(exc), ok=False)
        finally:
            conn.close()

    @app.post("/api/advance")
    def api_advance():
        target = (request.form.get("target_seconds")
                  or (request.json or {}).get("target_seconds"))
        conn = db()
        try:
            advance_id, target, ids = prepare_advance(conn, target)
            return _respond(
                {"ok": True, "advance_id": advance_id, "target": target,
                 "expected_expiries": ids},
                "时钟已暂停在待提交状态，本次到期 %d 条，期间可照常领址/查撤路"
                % len(ids))
        except DrillError as exc:
            return _respond({"ok": False, "error": str(exc)}, str(exc), ok=False)
        finally:
            conn.close()

    @app.post("/api/advance/commit")
    def api_advance_commit():
        src = request.form if request.form else (request.json or {})
        fault = bool(src.get("fault_inject"))
        conn = db()
        try:
            result = commit_advance(conn, fault_inject=fault)
            return _respond(
                {"ok": True, **result},
                "时钟推进至 %d，本次到期 %d 条（暂停期间新领计入 %d 条）"
                % (result["target"], len(result["settled"]), len(result["added"])))
        except ListChanged as exc:
            return _respond({"ok": False, "error": str(exc),
                             "redo_required": True}, str(exc), ok=False, code=409)
        except DrillError as exc:
            return _respond({"ok": False, "error": str(exc)}, str(exc), ok=False)
        except RuntimeError as exc:
            # Fault injection closed the connection; restart recovery handles it.
            return jsonify({"ok": False, "error": str(exc),
                            "restart_recovery": True}), 500
        finally:
            try:
                conn.close()
            except Exception:
                pass

    @app.post("/api/advance/abort")
    def api_advance_abort():
        conn = db()
        try:
            advance_id = abort_advance(conn)
            return _respond({"ok": True, "advance_id": advance_id},
                            "已退回，原时钟与已领租约保留")
        except DrillError as exc:
            return _respond({"ok": False, "error": str(exc)}, str(exc), ok=False)
        finally:
            conn.close()

    def _respond(payload, flash_msg, ok=True, code=None):
        wants_json = request.is_json or not request.form
        if wants_json:
            return jsonify(payload), (code or (200 if ok else 400))
        # Form posts redirect back to the dashboard with a flash-style query.
        from urllib.parse import quote
        return redirect(url_for("dashboard") +
                        ("?msg=%s" % quote(flash_msg) if flash_msg else ""))

    with app.app_context():
        conn = init_db(app.config.get("DB_PATH"))
        # One-shot recovery for advances stranded by an interrupted process.
        recover_pending(conn)
        conn.close()
    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=8000)
