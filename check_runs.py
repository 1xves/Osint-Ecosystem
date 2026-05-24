import asyncio, asyncpg

DB = "postgresql://postgres.wuojatgaxkeqpubsvrrg:!!FLashpoint2099@aws-1-us-east-1.pooler.supabase.com:6543/postgres"

async def run():
    conn = await asyncpg.connect(DB, statement_cache_size=0)
    rows = await conn.fetch(
        "SELECT run_id, run_status, failure_reason, total_entities_found, started_at, completed_at "
        "FROM agent_runs ORDER BY started_at DESC LIMIT 5"
    )
    for r in rows:
        print(dict(r))
    await conn.close()

asyncio.run(run())
