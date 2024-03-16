"""
Модуль описывает репозиторий, работающий в оперативной памяти
"""

from itertools import count
from typing import Any

from bookkeeper.repository.abstract_repository import AbstractRepository, T


class MemoryRepository(AbstractRepository[T]):
    """
    Репозиторий, работающий в оперативной памяти. Хранит данные в словаре.
    """

    def __init__(self) -> None:
        self._container: dict[int, T] = {}
        self._counter = count(1)

    def add(self, obj: T) -> int:
        if getattr(obj, "primary_key", None) != 0:
            raise ValueError(
                f"trying to add object {obj} with filled `primary_key` attribute"
            )
        primary_key = next(self._counter)
        self._container[primary_key] = obj
        obj.primary_key = primary_key
        return primary_key

    def get(self, primary_key: int) -> T | None:
        return self._container.get(primary_key)

    def get_all(self, where: dict[str, Any] | None = None) -> list[T]:
        if where is None:
            return list(self._container.values())
        return [
            obj
            for obj in self._container.values()
            if all(getattr(obj, attr) == value for attr, value in where.items())
        ]

    def update(self, obj: T) -> None:
        if obj.primary_key == 0:
            raise ValueError("attempt to update object with unknown primary key")
        self._container[obj.primary_key] = obj

    def delete(self, primary_key: int) -> None:
        self._container.pop(primary_key)
