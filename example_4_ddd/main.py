import asyncio
from dataclasses import dataclass


from common.ddl import (
    TEST_ACCOUNT_ID,
    initialize_DDD_pg_database,
    initialize_DDD_mongo_database,
)
from example_4_ddd.repositories import bank_account_repository_factory
from example_4_ddd.uow import UnitOfWork
from example_4_ddd.uow_factory import postgres_uow_factory, mongo_uow_factory


@dataclass(frozen=True)
class BankAccount:
    account_id: str
    balance: float


async def withdraw(
    uow: UnitOfWork,
    account_id: str,
    amount: int,
    _delay_seconds: int = 0,
) -> None:
    print(f"Performing withdrawal on account {account_id}, for ${amount}")

    repository = bank_account_repository_factory(uow)

    # Fetch the desired account
    account = await repository.find(entity_id=account_id)
    assert account is not None

    # Business logic, assert some domain rules
    assert account.balance - amount >= 0, "Insufficient balance for withdrawal!"

    # Artificial delay, this can always happen in an async environment (network trips, etc...)
    await asyncio.sleep(_delay_seconds)

    # Update the new accounts balance
    account.withdraw(amount)


async def pg_withdraw(
    account_id: str,
    amount: int,
    _delay_seconds: int = 0,
):
    async with postgres_uow_factory() as uow:
        await withdraw(uow, account_id, amount, _delay_seconds)

    # Dispatch successful transaction message after transaction
    print(f"Success! ${amount} was withdrawn from {account_id}. Using Postgres.")


async def main_pg():
    # This also creates an initial account with $100_000
    await initialize_DDD_pg_database()

    results = await asyncio.gather(
        pg_withdraw(TEST_ACCOUNT_ID, 100_000, _delay_seconds=1),
        pg_withdraw(TEST_ACCOUNT_ID, 100_000, _delay_seconds=2),
        return_exceptions=True,
    )

    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 1
    print("Error:", exceptions[0])


async def mongo_withdraw(
    account_id: str,
    amount: int,
    _delay_seconds: int = 0,
):
    async with mongo_uow_factory() as uow:
        await withdraw(uow, account_id, amount, _delay_seconds)

    # Dispatch successful transaction message after transaction
    print(f"Success! ${amount} was withdrawn from {account_id}. Using MongoDB.")


async def main_mongo():
    # This also creates an initial account with $100_000
    await initialize_DDD_mongo_database()

    results = await asyncio.gather(
        mongo_withdraw(TEST_ACCOUNT_ID, 100_000, _delay_seconds=1),
        mongo_withdraw(TEST_ACCOUNT_ID, 100_000, _delay_seconds=2),
        return_exceptions=True,
    )

    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 1
    print(exceptions[0])


if __name__ == "__main__":
    # First round, postgres:

    print("First round, postgres:")
    asyncio.run(main_pg())

    # Expected console output:

    # Performing withdrawal on account test_account_id, for $100000
    # Performing withdrawal on account test_account_id, for $100000
    # Success! $100000 was withdrawn from test_account_id. Using Postgres.
    # Error: could not serialize access due to concurrent update

    # Second round, mongodb:

    print("Second round, mongodb:")
    asyncio.run(main_mongo())

    # Expected console output:

    # Performing withdrawal on account test_account_id, for $100000
    # Performing withdrawal on account test_account_id, for $100000
    # Success! $100000 was withdrawn from test_account_id. Using MongoDB.
    # Serialization error due to concurrent update!
