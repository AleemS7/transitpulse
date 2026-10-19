"""Job task: unpack newly landed GTFS schedule zips before the pipeline refresh."""

import argparse
import logging
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-root", required=True, help="folder that contains the transitpulse package")
    parser.add_argument("--zip-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args(argv)

    sys.path.insert(0, args.src_root)
    from transitpulse.gtfs.extract import extract_new_feeds

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("extract_gtfs")
    extracted = extract_new_feeds(Path(args.zip_dir), Path(args.out_dir))
    for item in extracted:
        log.info("extracted feed_version=%s table=%s", item.feed_version, item.table)
    log.info("extracted %d new file(s)", len(extracted))


# Serverless runs this file in an IPython shell, where any SystemExit (even 0) fails the task,
# so finish normally and let real errors raise.
if __name__ == "__main__":
    main()
