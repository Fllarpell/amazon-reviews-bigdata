"""Apply pending migrations from ``migrations/versions``."""

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.dbutil import connect
from lib.logutil import setup_logging
from lib.schema_migrations import apply_pending_migrations

logger = logging.getLogger(__name__)


def main() -> None:
    setup_logging()
    conn = connect()
    try:
        apply_pending_migrations(conn)
        logger.info("migrations up to date")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
