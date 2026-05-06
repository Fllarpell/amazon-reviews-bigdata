import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.dbutil import connect
from lib.logutil import setup_logging
from lib.schema_migrations import revert_last_migration

logger = logging.getLogger(__name__)


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="skip confirmation",
    )
    args = parser.parse_args()
    if not args.yes:
        sys.stderr.write("revert drops schema from the latest ledger entry; use --yes to confirm\n")
        sys.exit(2)
    conn = connect()
    try:
        version_id = revert_last_migration(conn)
        logger.info("reverted %s", version_id)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
