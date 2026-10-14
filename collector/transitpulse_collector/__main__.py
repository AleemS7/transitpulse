"""Run the collector outside Azure: `python -m transitpulse_collector run-once [--static] [--local DIR]`."""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

from transitpulse_collector.settings import Settings
from transitpulse_collector.wiring import build_collector


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="transitpulse_collector")
    parser.add_argument("command", choices=["run-once", "loop"])
    parser.add_argument("--static", action="store_true", help="collect the static GTFS zip instead")
    parser.add_argument("--local", type=Path, help="write to this folder instead of Databricks")
    parser.add_argument("--interval", type=int, default=120, help="seconds between loop runs")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    collector = build_collector(Settings.from_env(), local_out=args.local)

    while True:
        started = time.monotonic()
        hb = collector.run_static() if args.static else collector.run_realtime()
        print(hb.to_json().decode())
        if args.command == "run-once":
            return 0 if all(f.status in ("ok", "unchanged") for f in hb.feeds) else 1
        time.sleep(max(0.0, args.interval - (time.monotonic() - started)))


if __name__ == "__main__":
    raise SystemExit(main())
