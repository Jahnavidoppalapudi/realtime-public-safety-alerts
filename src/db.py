"""Database setup: apply the schema and a read-only role for Grafana."""

from __future__ import annotations

from pathlib import Path

SCHEMA = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"


def connect(url: str):
    import psycopg

    return psycopg.connect(url)


def apply_schema(
    conn, grafana_password: str | None = None, reader_role: str = "grafana_reader"
) -> None:
    """Create or update tables and views, and the read-only dashboard login.

    Safe to run on every start.
    """
    from psycopg import sql

    with conn.transaction():
        # One processor at a time may change the schema
        conn.execute("select pg_advisory_xact_lock(4242)")
        conn.execute(SCHEMA.read_text())
        if grafana_password:
            role = sql.Identifier(reader_role)
            exists = conn.execute(
                "select 1 from pg_roles where rolname = %s", (reader_role,)
            ).fetchone()
            verb = sql.SQL("alter" if exists else "create")
            conn.execute(
                sql.SQL("{} role {} login password {}").format(
                    verb, role, sql.Literal(grafana_password)
                )
            )
            # Dashboards can read everything, including tables added later, and change nothing
            conn.execute(sql.SQL("grant usage on schema public to {}").format(role))
            conn.execute(sql.SQL("grant select on all tables in schema public to {}").format(role))
            conn.execute(
                sql.SQL(
                    "alter default privileges in schema public grant select on tables to {}"
                ).format(role)
            )
