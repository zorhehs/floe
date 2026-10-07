"""
Synthetic OLTP workload against the PostgreSQL source (drives the CDC stream).

Operation mix per tick (configurable rate, default 100 ops/s):
  45% new order           20% order status/price update   10% order delete
  15% customer balance update   5% new customer   5% customer delete (GDPR-style)

Runs until stopped (or --duration seconds). Prints throughput every 10s.
"""

from __future__ import annotations

import argparse
import os
import random
import signal
import string
import time
from datetime import date, timedelta

from common.db import pg_connect

PRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
FIRST_DATE = date(1992, 1, 1)
DAYS = (date(1998, 8, 2) - FIRST_DATE).days

MIX = [
    ("insert_order", 45), ("update_order", 20), ("delete_order", 10),
    ("update_customer", 15), ("insert_customer", 5), ("delete_customer", 5),
]

running = True


def _stop(*_):
    global running
    running = False


def rand_text(n: int) -> str:
    return "".join(random.choices(string.ascii_lowercase + " ", k=n)).strip() or "x"


class Workload:
    def __init__(self, conn):
        self.conn = conn
        with conn.cursor() as cur:
            cur.execute("SELECT coalesce(min(o_orderkey), 1), coalesce(max(o_orderkey), 1) FROM shop.orders")
            self.min_order, self.max_order = cur.fetchone()
            cur.execute("SELECT coalesce(min(c_custkey), 1), coalesce(max(c_custkey), 1) FROM shop.customer")
            self.min_cust, self.max_cust = cur.fetchone()
        if self.max_order <= 1:
            raise SystemExit("shop.orders is empty - run `make load` first")

    def _existing(self, cur, table, key, lo, hi):
        cur.execute(f"SELECT {key} FROM shop.{table} WHERE {key} >= %s ORDER BY {key} LIMIT 1",
                    [random.randint(lo, hi)])
        row = cur.fetchone()
        return row[0] if row else None

    def insert_order(self, cur):
        cust = self._existing(cur, "customer", "c_custkey", self.min_cust, self.max_cust) or 1
        cur.execute(
            "INSERT INTO shop.orders VALUES (nextval('shop.order_key_seq'), %s, 'O', %s, %s, %s, %s, 0, %s, localtimestamp) "
            "RETURNING o_orderkey",
            [cust, round(random.uniform(900, 500000), 2), FIRST_DATE + timedelta(days=random.randint(0, DAYS)),
             random.choice(PRIORITIES), f"Clerk#{random.randint(1, 1000):09d}", rand_text(40)])
        self.max_order = max(self.max_order, cur.fetchone()[0])

    def update_order(self, cur):
        key = self._existing(cur, "orders", "o_orderkey", self.min_order, self.max_order)
        if key:
            cur.execute("UPDATE shop.orders SET o_orderstatus = %s, o_totalprice = round(o_totalprice * %s::numeric, 2), "
                        "updated_at = localtimestamp WHERE o_orderkey = %s",
                        [random.choice("OFP"), random.uniform(0.9, 1.1), key])

    def delete_order(self, cur):
        key = self._existing(cur, "orders", "o_orderkey", self.min_order, self.max_order)
        if key:
            cur.execute("DELETE FROM shop.orders WHERE o_orderkey = %s", [key])

    def update_customer(self, cur):
        key = self._existing(cur, "customer", "c_custkey", self.min_cust, self.max_cust)
        if key:
            cur.execute("UPDATE shop.customer SET c_acctbal = round(c_acctbal + %s::numeric, 2), "
                        "updated_at = localtimestamp WHERE c_custkey = %s",
                        [round(random.uniform(-500, 500), 2), key])

    def insert_customer(self, cur):
        cur.execute(
            "INSERT INTO shop.customer VALUES (nextval('shop.customer_key_seq'), %s, %s, %s, %s, %s, %s, %s, localtimestamp) "
            "RETURNING c_custkey",
            [f"Customer#{random.randint(1, 10**9):09d}", rand_text(30), random.randint(0, 24),
             f"{random.randint(10, 34)}-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
             round(random.uniform(-999.99, 9999.99), 2), random.choice(SEGMENTS), rand_text(60)])
        self.max_cust = max(self.max_cust, cur.fetchone()[0])

    def delete_customer(self, cur):
        key = self._existing(cur, "customer", "c_custkey", self.min_cust, self.max_cust)
        if key:
            cur.execute("DELETE FROM shop.customer WHERE c_custkey = %s", [key])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ops-per-sec", type=float, default=float(os.getenv("WORKLOAD_OPS_PER_SEC", "100")))
    p.add_argument("--duration", type=int, default=0, help="seconds, 0 = until stopped")
    args = p.parse_args()
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    while True:  # wait for the database + data
        try:
            conn = pg_connect(autocommit=False)
            wl = Workload(conn)
            break
        except SystemExit as exc:
            print(exc, flush=True)
            time.sleep(10)
        except Exception as exc:  # noqa: BLE001
            print(f"waiting for PostgreSQL: {exc}", flush=True)
            time.sleep(5)

    ops, weights = zip(*MIX, strict=True)
    tick = 0.1
    per_tick = args.ops_per_sec * tick
    carry = 0.0
    done = {o: 0 for o in ops}
    started = last_report = time.time()
    print(f"workload: {args.ops_per_sec} ops/s, mix={dict(MIX)}", flush=True)

    while running and (not args.duration or time.time() - started < args.duration):
        t0 = time.time()
        carry += per_tick
        n, carry = int(carry), carry - int(carry)
        if n:
            try:
                with conn.cursor() as cur:  # one transaction per tick
                    for op in random.choices(ops, weights, k=n):
                        getattr(wl, op)(cur)
                        done[op] += 1
                conn.commit()
            except Exception as exc:  # noqa: BLE001 - keep the workload alive
                conn.rollback()
                print(f"transaction rolled back: {exc}", flush=True)
        if time.time() - last_report >= 10:
            elapsed = time.time() - started
            print(f"{elapsed:7.0f}s total={sum(done.values())} ({sum(done.values()) / elapsed:.1f} ops/s) {done}",
                  flush=True)
            last_report = time.time()
        time.sleep(max(0.0, tick - (time.time() - t0)))

    conn.close()
    print(f"workload stopped: {done}", flush=True)


if __name__ == "__main__":
    main()
