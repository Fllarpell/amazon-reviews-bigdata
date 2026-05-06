import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.dbutil import connect
from lib.logutil import setup_logging
from lib.schema_migrations import verify_applied_migrations

logger = logging.getLogger(__name__)


def main() -> None:
    setup_logging()
    conn = connect()
    try:
        with conn.cursor() as cur:
            verify_applied_migrations(cur)
        logger.info("all applied migrations passed verify.sql")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
