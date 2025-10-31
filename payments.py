import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

DB_PATH = Path("payments.db")


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        # Base table with username column
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS payments (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                paid_generations INTEGER NOT NULL DEFAULT 0,
                last_payment_at TEXT,
                total_spent_cents INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        # Migration: add username if missing for existing DBs
        try:
            conn.execute("ALTER TABLE payments ADD COLUMN username TEXT")
        except sqlite3.OperationalError:
            # Column likely exists; ignore
            pass
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS trg_payments_updated_at
            AFTER UPDATE ON payments
            FOR EACH ROW BEGIN
                UPDATE payments SET updated_at = datetime('now') WHERE user_id = OLD.user_id;
            END;
            """
        )


def get_user_balance(user_id: int) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT paid_generations FROM payments WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return int(row[0]) if row else 0


def set_user_balance(user_id: int, value: int, last_payment_at: Optional[str] = None, username: Optional[str] = None) -> None:
    iso = last_payment_at or datetime.utcnow().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO payments(user_id, username, paid_generations, last_payment_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = COALESCE(excluded.username, username),
                paid_generations = excluded.paid_generations,
                last_payment_at = excluded.last_payment_at
            """,
            (user_id, username, value, iso),
        )


def increment_user_balance(user_id: int, delta: int = 1, username: Optional[str] = None) -> int:
    if delta == 0:
        return get_user_balance(user_id)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("BEGIN")
        cur = conn.execute("SELECT paid_generations, last_payment_at FROM payments WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        current = int(row[0]) if row else 0
        new_val = max(0, current + delta)
        iso = datetime.utcnow().isoformat()
        if row:
            conn.execute(
                "UPDATE payments SET paid_generations = ?, last_payment_at = ?, username = COALESCE(?, username) WHERE user_id = ?",
                (new_val, iso if delta > 0 else row[1], username, user_id),
            )
        else:
            conn.execute(
                "INSERT INTO payments(user_id, username, paid_generations, last_payment_at) VALUES(?, ?, ?, ?)",
                (user_id, username, new_val, iso if delta > 0 else None),
            )
        conn.commit()
        return new_val
