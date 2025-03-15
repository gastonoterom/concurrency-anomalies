from abc import ABC, abstractmethod

from asyncpg import Connection
from asyncpg.transaction import Transaction
from motor.motor_asyncio import AsyncIOMotorClientSession, AsyncIOMotorClient

from example_4_ddd.aggregates import Aggregate


class UnitOfWork(ABC):
    def __init__(self) -> None:
        # Aggregate,  Persistence callback
        self.__tracked_aggregates: list[tuple[Aggregate, callable]] = []

    def track(
        self,
        aggregate: Aggregate,
        persistence_callback: callable,
    ) -> None:
        self.__tracked_aggregates.append((aggregate, persistence_callback))

    async def commit(self) -> None:
        await self._persist_tracked_aggregates()
        self.__tracked_aggregates.clear()

        await self._commit()

    async def rollback(self) -> None:
        self.__tracked_aggregates.clear()
        await self._rollback()

    async def _persist_tracked_aggregates(self) -> None:
        for aggregate, callback in self.__tracked_aggregates:
            await callback()

    @abstractmethod
    async def _commit(self) -> None:
        pass

    @abstractmethod
    async def _rollback(self) -> None:
        pass


class PostgresUnitOfWork(UnitOfWork):
    def __init__(self, conn: Connection, transaction: Transaction) -> None:
        super().__init__()
        self.conn = conn
        self._transaction = transaction

    async def _commit(self) -> None:
        await self._transaction.commit()

    async def _rollback(self) -> None:
        await self._transaction.rollback()


class MongoUnitOfWork(UnitOfWork):
    def __init__(self, session: AsyncIOMotorClientSession) -> None:
        super().__init__()
        self.session = session

    async def _commit(self) -> None:
        await self.session.commit_transaction()

    async def _rollback(self) -> None:
        await self.session.abort_transaction()
