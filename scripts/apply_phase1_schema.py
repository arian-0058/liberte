import asyncio
import asyncpg
from pathlib import Path

DSN = "postgresql://postgres:%40123%23246%21@localhost:5432/liberte"
SQL_FILE = Path("I:/liberte/sql/phase1.sql")

async def main():
    print(f"🔄 Connecting to {DSN}")
    try:
        sql_text = SQL_FILE.read_text()
    except Exception as e:
        print(f"❌ Couldn't read {SQL_FILE}: {e}")
        return

    try:
        conn = await asyncpg.connect(DSN)
        await conn.execute(sql_text)
        await conn.close()
        print("✅ Schema applied successfully to database 'liberte'.")
    except Exception as e:
        print(f"❌ Error applying schema: {e}")

if __name__ == "__main__":
    asyncio.run(main())
