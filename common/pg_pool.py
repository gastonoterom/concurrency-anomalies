import os

from asyncpg import Pool, create_pool


class PostgresPool:
    def __init__(
        self,
        connection_url: str,
    ) -> None:
        self._connection_url: str = connection_url
        self._pool: Pool | None = None

    def get_pool(self) -> Pool:
        assert self._pool is not None, "ERROR: Postgres connection pool not started"
        return self._pool

    async def start_pool(self) -> Pool:
        self._pool = await create_pool(dsn=self._connection_url)
        assert self._pool is not None, "ERROR: Postgres connection pool not started"
        return self._pool

    async def cleanup(self) -> None:
        assert self._pool is not None, "ERROR: Postgres connection pool not started"
        await self._pool.close()
        self._pool = None


_connection_url = os.environ.get("POSTGRES_CONNECTION_URL")
_connection_url = (
    _connection_url or "postgresql://postgres:postgres@localhost:5438/postgres"
)

postgres_pool = PostgresPool(_connection_url)
