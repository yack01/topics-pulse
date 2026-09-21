#!/usr/bin/env python3
"""Backfill template: reads a CSV of historical messages and posts each one
to POST /messages, one at a time — the same endpoint real-time sources use.
There is no separate bulk-import API by design (see project README).

Expected CSV columns (header row required):
    login,user_id,text,created_at,source

- user_id is optional; leave the cell empty if you don't have one.
- created_at must be RFC3339, e.g. 2026-09-01T12:00:00Z
- source must be one of: game, forum, telegram

Usage:
    python3 import_csv.py --file history.csv --api http://localhost:8080

No idempotency/dedup is enforced by the API (see brief decisions), so
re-running this script against the same file will create duplicate rows.
"""
import argparse
import csv
import sys
import time
import urllib.error
import urllib.request
import json


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--file", required=True, help="Path to the CSV file")
    p.add_argument("--api", default="http://localhost:8080", help="Base URL of the TopicsPulse API")
    p.add_argument("--delay-ms", type=int, default=0, help="Optional delay between requests, in milliseconds")
    p.add_argument("--dry-run", action="store_true", help="Parse and print rows without sending them")
    return p.parse_args()


def post_message(api_base, row):
    payload = {
        "login": row["login"],
        "text": row["text"],
        "created_at": row["created_at"],
        "source": row["source"],
    }
    user_id = row.get("user_id", "").strip()
    if user_id:
        payload["user_id"] = int(user_id)

    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{api_base}/messages",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status, resp.read().decode("utf-8")


def main():
    args = parse_args()

    sent, failed = 0, 0
    with open(args.file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = {"login", "text", "created_at", "source"} - set(reader.fieldnames or [])
        if missing:
            print(f"CSV is missing required columns: {sorted(missing)}", file=sys.stderr)
            sys.exit(1)

        for i, row in enumerate(reader, start=1):
            if args.dry_run:
                print(row)
                continue
            try:
                status, body = post_message(args.api, row)
                if status >= 300:
                    print(f"row {i}: unexpected status {status}: {body}", file=sys.stderr)
                    failed += 1
                else:
                    sent += 1
            except urllib.error.HTTPError as e:
                print(f"row {i}: HTTP {e.code}: {e.read().decode('utf-8', 'ignore')}", file=sys.stderr)
                failed += 1
            except Exception as e:
                print(f"row {i}: {e}", file=sys.stderr)
                failed += 1

            if args.delay_ms:
                time.sleep(args.delay_ms / 1000)

    if not args.dry_run:
        print(f"done: {sent} sent, {failed} failed")


if __name__ == "__main__":
    main()
