from abc import ABC


class Aggregate(ABC):
    def __init__(self, entity_id: str, version: int) -> None:
        self.__entity_id = entity_id
        self.__version = version

    @property
    def entity_id(self) -> str:
        return self.__entity_id

    @property
    def version(self) -> int:
        return self.__version

    def update_version(self) -> None:
        self.__version += 1


class BankAccount(Aggregate):
    def __init__(
        self,
        account_id: str,
        balance: float,
        version: int = 1,
    ) -> None:
        super().__init__(account_id, version)
        self.__balance = balance

    @property
    def account_id(self) -> str:
        return self.entity_id

    @property
    def balance(self) -> float:
        return self.__balance

    def withdraw(self, amount: float) -> None:
        assert self.__balance >= amount, "Insufficient balance!"

        self.__balance -= amount
