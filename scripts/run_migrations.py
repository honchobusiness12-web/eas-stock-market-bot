import asyncio
import os
import sys

import asyncpg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


async def run_migrations():
    if not DATABASE_URL:
        print("❌ DATABASE_URL is not set.")
        sys.exit(1)

    conn = await asyncpg.connect(DATABASE_URL)

    try:
        print("🔄 Running database migrations...")

        migration_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "migrations",
            "001_create_stock_tables.sql",
        )

        with open(migration_path, "r", encoding="utf-8") as f:
            sql = f.read()

        await conn.execute(sql)

        print("✅ Migrations completed successfully")
    except Exception as err:
        print(f"❌ Migration failed: {err}")
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run_migrations())
