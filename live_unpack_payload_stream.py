#!/usr/bin/env python3
"""Restore completed files while a payload file is still being written.

The writer publishes progress through a small text file.  Recommended format:

    written_bytes=6553600
    done=0

The progress file should be replaced atomically by the writer (write a .part
file, flush/fsync it, then os.replace it).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from pathlib import PurePosixPath


@dataclass(frozen=True)
class Entry:
    name: str
    offset: int
    size: int


def parse_index(path: str) -> tuple[int | None, int | None, list[Entry]]:
    expected_count = expected_total = None
    entries: list[Entry] = []
    with open(path, "r", encoding="utf-8") as f:
        for line_no, raw in enumerate(f, 1):
            line = raw.rstrip("\r\n")
            if not line:
                continue
            if line.startswith("#"):
                if line.startswith("# file_count="):
                    expected_count = int(line.split("=", 1)[1])
                elif line.startswith("# total_size="):
                    expected_total = int(line.split("=", 1)[1])
                continue
            parts = line.split("|")
            if len(parts) != 3:
                raise ValueError(f"{path}:{line_no}: expected path|offset|size")
            name, offset_text, size_text = parts
            offset, size = int(offset_text), int(size_text)
            if not name or offset < 0 or size < 0:
                raise ValueError(f"{path}:{line_no}: invalid entry")
            entries.append(Entry(name, offset, size))

    if expected_count is not None and expected_count != len(entries):
        raise ValueError(f"index file_count={expected_count}, entries={len(entries)}")
    previous_end = 0
    for entry in entries:
        if entry.offset != previous_end:
            raise ValueError(f"non-contiguous index at {entry.name}")
        previous_end = entry.offset + entry.size
    if expected_total is not None and expected_total != previous_end:
        raise ValueError(f"index total_size={expected_total}, entries end at {previous_end}")
    return expected_count, expected_total, entries


def safe_output_path(root: str, name: str) -> str:
    p = PurePosixPath(name)
    if p.is_absolute() or ".." in p.parts or "" in p.parts or "\\" in name:
        raise ValueError(f"unsafe relative path in index: {name!r}")
    root_abs = os.path.abspath(root)
    output = os.path.abspath(os.path.join(root_abs, *p.parts))
    if os.path.commonpath((root_abs, output)) != root_abs:
        raise ValueError(f"path escapes output directory: {name!r}")
    return output


def read_progress(path: str) -> tuple[int, bool]:
    values: dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            key, sep, value = line.partition("=")
            if sep:
                values[key.strip()] = value.strip()
    if "written_bytes" not in values:
        raise ValueError(f"progress file lacks written_bytes: {path}")
    written = int(values["written_bytes"])
    done = values.get("done", "0").lower() in {"1", "true", "yes", "done"}
    if written < 0:
        raise ValueError("written_bytes cannot be negative")
    return written, done


def restore_entry(payload, output_dir: str, entry: Entry, overwrite: bool) -> None:
    destination = safe_output_path(output_dir, entry.name)
    if os.path.exists(destination) and not overwrite:
        raise FileExistsError(f"destination exists (use --overwrite): {destination}")
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    temporary = destination + ".live-part"
    with open(temporary, "wb") as out:
        payload.seek(entry.offset)
        remaining = entry.size
        while remaining:
            chunk = payload.read(min(1024 * 1024, remaining))
            if len(chunk) != min(1024 * 1024, remaining):
                raise ValueError(f"short read while reading {entry.name}")
            out.write(chunk)
            remaining -= len(chunk)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temporary, destination)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("index")
    parser.add_argument("payload")
    parser.add_argument("output")
    parser.add_argument("progress", help="progress file published by the writer")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--poll-interval", type=float, default=0.5)
    args = parser.parse_args()
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be positive")

    try:
        _, expected_total, entries = parse_index(args.index)
        if expected_total is None:
            raise ValueError("index must contain # total_size=...")
        os.makedirs(args.output, exist_ok=True)
        restored: set[int] = set()
        last_reported = -1
        with open(args.payload, "rb") as payload:
            while True:
                if not os.path.exists(args.progress):
                    time.sleep(args.poll_interval)
                    continue
                try:
                    written, done = read_progress(args.progress)
                except (OSError, ValueError):
                    time.sleep(args.poll_interval)
                    continue
                if written < last_reported:
                    raise ValueError("progress moved backwards")
                last_reported = written
                for index, entry in enumerate(entries):
                    if index in restored:
                        continue
                    if entry.offset + entry.size <= written:
                        restore_entry(payload, args.output, entry, args.overwrite)
                        restored.add(index)
                        print(f"restored {entry.name} ({entry.size} bytes)", flush=True)
                if done:
                    if written < expected_total:
                        raise ValueError(f"done=1 but written_bytes={written}, expected {expected_total}")
                    if len(restored) != len(entries):
                        raise ValueError("done=1 but not all indexed files were restored")
                    break
                time.sleep(args.poll_interval)
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"completed: {len(entries)} files restored to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
