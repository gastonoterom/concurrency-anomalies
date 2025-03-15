import contextlib
from typing import AsyncGenerator


from common.mongo import mongo_client
from common.pg_pool import postgres_pool
from example_4_ddd.uow import PostgresUnitOfWork, MongoUnitOfWork


@contextlib.asynccontextmanager
async def postgres_uow_factory() -> AsyncGenerator[PostgresUnitOfWork, None]:
    async with postgres_pool.get_pool().acquire() as conn:
        # We set serialization level at this stage
        transaction = conn.transaction(isolation="repeatable_read")
        await transaction.start()

        uow = PostgresUnitOfWork(
            conn,  # type: ignore
            transaction,
        )

        try:
            yield uow
            await uow.commit()

        except BaseException as e:
            await uow.rollback()
            raise e


@contextlib.asynccontextmanager
async def mongo_uow_factory() -> AsyncGenerator[MongoUnitOfWork, None]:
    async with await mongo_client.start_session() as session:
        session.start_transaction()

        uow = MongoUnitOfWork(session)

        try:
            yield uow
            await uow.commit()

        except Exception as e:
            await uow.rollback()
            raise e
