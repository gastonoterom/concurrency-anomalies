from abc import ABC, abstractmethod

from motor.motor_asyncio import AsyncIOMotorCollection

from example_4_ddd.aggregates import Aggregate, BankAccount
from example_4_ddd.uow import UnitOfWork, PostgresUnitOfWork, MongoUnitOfWork


class Repository[T: Aggregate](ABC):
    def __init__(self, uow: UnitOfWork) -> None:
        self.__uow = uow

    async def find(self, entity_id: str) -> T | None:
        aggregate = await self._find(entity_id)

        if aggregate is None:
            return None

        self.__uow.track(
            aggregate,
            persistence_callback=lambda: self._update(aggregate),
        )

        return aggregate

    @abstractmethod
    async def _find(self, entity_id: str) -> T | None:
        pass

    @abstractmethod
    async def _update(
        self,
        aggregate: Aggregate,
    ) -> None:
        pass


class BankAccountRepository(Repository[BankAccount], ABC):
    pass


class PostgresBankAccountRepository(BankAccountRepository):
    def __init__(self, uow: PostgresUnitOfWork) -> None:
        super().__init__(uow)
        self._uow = uow

    async def _find(self, entity_id: str) -> BankAccount | None:
        row = await self._uow.conn.fetchrow(
            "SELECT * FROM bank_account WHERE account_id = $1",
            entity_id,
        )

        if row is None:
            return None

        return BankAccount(**row)

    async def _update(self, aggregate: BankAccount) -> None:
        aggregate.update_version()

        await self._uow.conn.execute(
            "UPDATE bank_account SET balance = $1, version = $2 WHERE account_id = $3",
            aggregate.balance,
            aggregate.version,
            aggregate.entity_id,
        )


class MongoBankAccountRepository(BankAccountRepository):
    def __init__(self, uow: MongoUnitOfWork) -> None:
        super().__init__(uow)
        self._uow = uow
        self.bank_accounts: AsyncIOMotorCollection = uow.session.client["db"][
            "bank_account"
        ]

    async def _find(self, entity_id: str) -> BankAccount | None:
        document: dict = await self.bank_accounts.find_one({"_id": entity_id})

        if document is None:
            return None

        return BankAccount(
            account_id=document["_id"],
            balance=document["balance"],
            version=document["version"],
        )

    async def _update(
        self,
        aggregate: BankAccount,
    ) -> None:
        previous_version = aggregate.version

        aggregate.update_version()

        result = await self.bank_accounts.update_one(
            {"_id": aggregate.entity_id, "version": previous_version},
            {"$set": {"balance": aggregate.balance, "version": aggregate.version}},
        )

        if result.modified_count != 1:
            raise Exception("Serialization error due to concurrent update!")


def bank_account_repository_factory(uow: UnitOfWork) -> BankAccountRepository:
    if isinstance(uow, PostgresUnitOfWork):
        return PostgresBankAccountRepository(uow)

    elif isinstance(uow, MongoUnitOfWork):
        return MongoBankAccountRepository(uow)

    else:
        raise ValueError(f"{uow} not supported")
