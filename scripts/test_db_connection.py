import asyncio
import asyncpg

# ✅ اطلاعات اتصال به دیتابیس Postgres
DSN = "postgresql://postgres:%40123%23246%21@localhost:5432/liberte"


async def main():
    print("🔄 Testing connection to:", DSN)
    try:
        conn = await asyncpg.connect(DSN)
        db, user = await conn.fetchrow("SELECT current_database(), current_user;")
        print(f"✅ Connected successfully!\n📦 Database: {db}\n👤 User: {user}")

        # تست سریع وجود جدول‌ها
        rows = await conn.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
        )
        if rows:
            print("📊 Tables found:", [r["table_name"] for r in rows])
        else:
            print("⚠️ No tables found — you may need to run phase1.sql.")

        await conn.close()
    except Exception as e:
        print("❌ Connection failed:", str(e))


if __name__ == "__main__":
    asyncio.run(main())
