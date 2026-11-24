"""Independent gateway process: read-only withdrawal queries.

This process never writes. It opens SQLite in read-only mode and only reads
the published withdrawal row plus valid old leases not fully covered by the
new aggregate, so the operator knows the earliest moment the old aggregate
route can be withdrawn.
"""
from flask import Flask, jsonify

from .common import DB_PATH
from .db import connect
from .logic import read_published, read_uncovered_old_leases


def create_app(db_path=None):
    app = Flask(__name__)
    app.config["DB_PATH"] = db_path or DB_PATH

    def ro_db():
        # Read-only URI connection: no schema bootstrap, no recovery writes.
        return connect(app.config["DB_PATH"], readonly=True)

    @app.get("/healthz")
    def healthz():
        return jsonify(ok=True, role="gateway", readonly=True)

    @app.get("/withdrawal")
    def withdrawal():
        """Published earliest withdrawal time + the uncovered old leases."""
        conn = ro_db()
        try:
            pub = read_published(conn)
            uncovered = read_uncovered_old_leases(conn)
            return jsonify({
                "published_new_pool": pub["cidr"],
                "published_at_seconds": pub["published_at"],
                "earliest_withdrawal_seconds": pub["earliest_seconds"],
                "can_withdraw_now": pub["earliest_seconds"] is None,
                "uncovered_count": len(uncovered),
                "uncovered_leases": [
                    {"lease_id": row["id"], "prefix": row["prefix"],
                     "expires_at": row["expires_at"]}
                    for row in uncovered],
            })
        finally:
            conn.close()

    @app.get("/")
    def index():
        return ("<h2>Gateway withdrawal view (read-only)</h2>"
                "<p>GET <a href='/withdrawal'>/withdrawal</a></p>")

    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=8001)
