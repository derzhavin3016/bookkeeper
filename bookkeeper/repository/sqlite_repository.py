"""
SQlite3 repository implementation
"""

from typing import Any
from inspect import get_annotations
from pathlib import Path
import sqlite3
from contextlib import closing

from bookkeeper.repository.abstract_repository import AbstractRepository, T, PK_FIELD_NAME


class SqliteRepository(AbstractRepository[T]):
    """
    SqliteRepository class, stores models in a database
    """

    def __init__(self, db_filename: Path, cls: type) -> None:
        self._db_name = db_filename
        self._cls = cls
        self._table_name = cls.__name__.lower()
        self._fields = get_annotations(cls, eval_str=True)
        self._fields.pop(PK_FIELD_NAME)

    def add(self, obj: T) -> int:
        names = ", ".join(self._fields)
        placeholders = ", ".join("?" * len(self._fields))

        values = [getattr(obj, key) for key in self._fields]

        with closing(sqlite3.connect(self._db_name)) as conn:
            with conn as cursor:
                cursor.execute("PRAGMA foreign_keys = ON")
                cursor.execute(
                    f"INSERT INTO {self._table_name} ({names}) VALUES ({placeholders})",
                    values,
                )

                obj.primary_key = cursor.lastrowid

        return obj.primary_key

    def get(self, primary_key: int) -> T | None:
        names = ", ".join(self._fields)

        with closing(sqlite3.connect(self._db_name)) as conn:
            with conn as cursor:
                data = cursor.execute(
                    f"SELECT {names} FROM {self._table_name} WHERE id = {primary_key}"
                ).fetchone()

                if data is None:
                    return None

        return self._cls(**data)

    def get_all(self, where: dict[str, Any] | None = None) -> list[T]:
        names = ", ".join(self._fields)
        query = f"SELECT {names} FROM {self._table_name}"
        params: list[Any] = []
        if where is not None:
            query += "WHERE "
            query += f"WHERE {' AND '.join(map(lambda name: f'{name}=?', where))}"
            params.extend(where[name] for name in where)

        with closing(sqlite3.connect(self._db_name)) as conn:
            with conn as cursor:
                data_lst = cursor.execute(query, params).fetchall()

        return [self._cls(**data) for data in data_lst]

    def update(self, obj: T) -> None:
        pass

    def delete(self, primary_key: int) -> None:
        pass
