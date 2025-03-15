import asyncpg
from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorCollection

from common.mongo import mongo_client
from common.pg_pool import postgres_pool

TEST_ACCOUNT_ID = "test_account_id"
TEST_BALANCE = 100_000


async def initialize_database() -> None:
    bank_account_ddl = """
        CREATE TABLE IF NOT EXISTS bank_account (
            account_id VARCHAR PRIMARY KEY,
            balance NUMERIC(20, 2) NOT NULL
        )
    """

    await postgres_pool.start_pool()

    conn: asyncpg.connection.Connection
    async with postgres_pool.get_pool().acquire() as conn:
        await conn.execute("DROP TABLE if exists bank_account")

        await conn.execute(bank_account_ddl)

        await conn.execute(
            "INSERT INTO bank_account (account_id, balance) VALUES ($1, $2);",
            TEST_ACCOUNT_ID,
            TEST_BALANCE,
        )


async def initialize_DDD_pg_database() -> None:
    bank_account_ddl = """
        CREATE TABLE IF NOT EXISTS bank_account (
            account_id VARCHAR PRIMARY KEY,
            balance NUMERIC(20, 2) NOT NULL,
            version INTEGER
        )
    """

    await postgres_pool.start_pool()

    conn: asyncpg.connection.Connection
    async with postgres_pool.get_pool().acquire() as conn:
        await conn.execute("DROP TABLE if exists bank_account")

        await conn.execute(bank_account_ddl)

        await conn.execute(
            "INSERT INTO bank_account (account_id, balance, version) VALUES ($1, $2, 1);",
            TEST_ACCOUNT_ID,
            TEST_BALANCE,
        )


async def initialize_DDD_mongo_database() -> None:
    async with await mongo_client.start_session() as session:
        db: AsyncIOMotorDatabase = session.client["db"]

        await db.drop_collection("bank_account")
        await db.create_collection("bank_account")

        bank_accounts: AsyncIOMotorCollection = db["bank_account"]

        await bank_accounts.insert_one(
            {"_id": TEST_ACCOUNT_ID, "balance": TEST_BALANCE, "version": 1},
        )
