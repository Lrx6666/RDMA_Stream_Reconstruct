#!/usr/bin/env python3
"""Publish Aio0 write progress for live_unpack_payload_stream.py.

The SPDK iostat counters are cumulative, so the initial bytes_written value is
used as a baseline. Start this monitor before starting dd731.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time


def iostat(rpc: str, bdev: str) -> int:
    result = subprocess.run(
        [rpc, "bdev_get_iostat", "-b", bdev],
        check=True,
        capture_output=True,
        text=True,
    )
    data = json.loads(result.stdout)
    for item in data.get("bdevs", []):
        if item.get("name") == bdev:
            return int(item["bytes_written"])
    raise RuntimeError(f"bdev not found in iostat output: {bdev}")


def publish(path: str, written: int, done: bool) -> None:
    temporary = path + ".part"
    with open(temporary, "w", encoding="utf-8") as f:
        f.write(f"written_bytes={written}\n")
        f.write(f"done={1 if done else 0}\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rpc", required=True, help="path to SPDK rpc.py")
    parser.add_argument("--bdev", default="Aio0")
    parser.add_argument("--total", type=int, required=True, help="index total_size")
    parser.add_argument("--progress", required=True)
    parser.add_argument("--interval", type=float, default=0.5)
    args = parser.parse_args()
    if args.total <= 0 or args.interval <= 0:
        parser.error("total and interval must be positive")

    os.makedirs(os.path.dirname(os.path.abspath(args.progress)), exist_ok=True)
    baseline = iostat(args.rpc, args.bdev)
    publish(args.progress, 0, False)
    print(f"monitoring {args.bdev}; baseline bytes_written={baseline}", flush=True)

    while True:
        current = iostat(args.rpc, args.bdev)
        delta = max(0, current - baseline)
        written = min(delta, args.total)
        done = written >= args.total
        publish(args.progress, written, done)
        print(
            f"written_bytes={written}/{args.total} "
            f"(raw_delta={delta}) done={int(done)}",
            flush=True,
        )
        if done:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
