import asyncio
from dataclasses import dataclass

import asyncpg

from common.ddl import initialize_database, TEST_ACCOUNT_ID
from common.pg_pool import postgres_pool


@dataclass(frozen=True)
class BankAccount:
    account_id: str
    balance: float


async def withdraw(
    account_id: str,
    amount: int,
    _delay_seconds: int = 0,
) -> None:
    conn: asyncpg.Connection
    async with postgres_pool.get_pool().acquire() as conn:
        print(f"Performing withdrawal on account {account_id}, for ${amount}")

        # Begin a default postgres database transaction
        transaction = conn.transaction()
        await transaction.start()

        # Select the bank account
        row = await conn.fetchrow(
            "SELECT * FROM bank_account WHERE account_id = $1", account_id
        )
        account = BankAccount(*row)

        # Business logic, assert some domain rules
        assert account.balance - amount >= 0, "Insufficient balance for withdrawal!"

        # Artificial delay, this can always happen in an async environment (network trips, etc...)
        await asyncio.sleep(_delay_seconds)

        # Update the new accounts balance
        new_balance = account.balance - amount
        await conn.execute("UPDATE bank_account SET balance = $1", new_balance)

        # Commit the transaction
        await transaction.commit()

    # Dispatch successful transaction message
    print(
        f"Success! ${amount} was withdrawn from {account_id}. The account's new balance is: ${new_balance}"
    )


async def main():
    # This also creates an initial account with $100_000
    await initialize_database()

    await asyncio.gather(
        withdraw(TEST_ACCOUNT_ID, 100_000, _delay_seconds=1),
        withdraw(TEST_ACCOUNT_ID, 100_000, _delay_seconds=2),
    )


if __name__ == "__main__":
    asyncio.run(main())

    # Console output:

    # Performing withdrawal on account test_id, for $100000
    # Performing withdrawal on account test_id, for $100000

    # Success! $100000 was withdrawn from test_id. The account's new balance is: $0.0
    # Success! $100000 was withdrawn from test_id. The account's new balance is: $0.0

    # The operation was processed twice... when it shouldn't
