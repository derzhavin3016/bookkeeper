"""
SQlite3 repository implementation
"""

from typing import Any
from inspect import get_annotations
from pathlib import Path
import sqlite3

# from datetime import datetime
from contextlib import closing

from bookkeeper.repository.abstract_repository import (
    AbstractRepository,
    T,
    PK_FIELD_NAME,
)


class SqliteRepository(AbstractRepository[T]):
    """
    SqliteRepository class, stores models in a database
    """

    def __init__(self, db_filename: Path, cls: type) -> None:
        self._db_name = db_filename
        self._cls = cls
        self._table_name = cls.__name__.lower()
        self._fields = get_annotations(cls, eval_str=True)
        if len(self._fields) == 0:
            raise TypeError("Trying to create repository without annotated fields")
        self._fields.pop(PK_FIELD_NAME)

        self._init_database()

    @staticmethod
    def _set_pragmas(cursor: sqlite3.Cursor):
        cursor.execute("PRAGMA foreign_keys = ON")

    _type_mappings: dict[type, str] = {
        str: "TEXT",
        int: "INTEGER",
        float: "REAL",
        # datetime: "DATETIME",
    }

    @staticmethod
    def _map_to_sql(tpy: type) -> str:
        res = SqliteRepository._type_mappings.get(tpy, None)
        if res is None:
            raise ValueError(f"Type {tpy} is not supported yet")
        return res

    def _init_database(self):
        with closing(sqlite3.connect(self._db_name)) as con, con as con, closing(
            con.cursor()
        ) as cursor:
            self._set_pragmas(cursor)
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {self._table_name}"
                + f"({PK_FIELD_NAME} INTEGER PRIMARY KEY NOT NULL, "
                + ", ".join(
                    f"{name} {self._map_to_sql(tpy)}"
                    for name, tpy in self._fields.items()
                )
                + ")"
            )

    def add(self, obj: T) -> int:
        if (prim_key := getattr(obj, PK_FIELD_NAME, None)) is None:
            raise ValueError(f"Trying to add object without `{PK_FIELD_NAME}` attribute")
        if prim_key != 0:
            raise ValueError(
                f"Trying to add object with filled `{PK_FIELD_NAME}` attribute"
            )

        names = ", ".join(self._fields)
        placeholders = ", ".join("?" * len(self._fields))

        values = [getattr(obj, key) for key in self._fields]

        with closing(sqlite3.connect(self._db_name)) as con, con as con, closing(
            con.cursor()
        ) as cursor:
            self._set_pragmas(cursor)
            cursor.execute(
                f"INSERT INTO {self._table_name} ({names}) VALUES ({placeholders})",
                values,
            )

            assert cursor.lastrowid is not None
            obj.primary_key = cursor.lastrowid

        return obj.primary_key

    def _make_obj(
        self, primary_key: int, names: tuple[Any, ...], vals: tuple[Any, ...]
    ) -> T:
        obj = self._cls()
        setattr(obj, PK_FIELD_NAME, primary_key)

        for idx, col_desc in enumerate(names):
            if col_desc[0] not in self._fields:
                raise ValueError(f"Unexpected field name: {col_desc[0]}")
            setattr(obj, col_desc[0], vals[idx])

        return obj

    def get(self, primary_key: int) -> T | None:
        names = ", ".join(self._fields)

        with closing(sqlite3.connect(self._db_name)) as con, con as con, closing(
            con.cursor()
        ) as cursor:
            self._set_pragmas(cursor)
            data = cursor.execute(
                f"SELECT {names} FROM {self._table_name} "
                f"WHERE {PK_FIELD_NAME} = {primary_key}"
            ).fetchone()

            if data is None:
                return None
            names = cursor.description

        return self._make_obj(primary_key, names, data)

    def get_all(self, where: dict[str, Any] | None = None) -> list[T]:
        names = ", ".join(self._fields)
        query = f"SELECT {PK_FIELD_NAME}, {names} FROM {self._table_name}"
        params: list[Any] = []
        if where is not None:
            query += f" WHERE {' AND '.join(map(lambda name: f'{name} = ?', where))}"
            params.extend(where[name] for name in where)

        with closing(sqlite3.connect(self._db_name)) as con, con as con, closing(
            con.cursor()
        ) as cursor:
            self._set_pragmas(cursor)
            data_lst = cursor.execute(query, params).fetchall()
            names = cursor.description

        return [self._make_obj(data[0], names[1:], data[1:]) for data in data_lst]

    def update(self, obj: T) -> None:
        names = ", ".join(self._fields)
        placeholders = ", ".join("?" * len(self._fields))

        primary_key = getattr(obj, PK_FIELD_NAME)
        with closing(sqlite3.connect(self._db_name)) as con, con as con, closing(
            con.cursor()
        ) as cursor:
            self._set_pragmas(cursor)
            cursor.execute(
                f"UPDATE {self._table_name} "
                f"SET ({names}) = ({placeholders}) "
                f"WHERE {PK_FIELD_NAME}={primary_key}",
                [getattr(obj, name) for name in self._fields],
            )

            if con.total_changes == 0:
                raise ValueError(
                    f"Trying to update object with key {primary_key}, "
                    "which does not exist"
                )

    def delete(self, primary_key: int) -> None:
        with closing(sqlite3.connect(self._db_name)) as con, con as con, closing(
            con.cursor()
        ) as cursor:
            self._set_pragmas(cursor)

            cursor.execute(
                f"DELETE FROM {self._table_name} WHERE {PK_FIELD_NAME}={primary_key}"
            )

            if con.total_changes == 0:
                raise KeyError(
                    f"Trying to delete object with key {primary_key}, "
                    "which does not exist"
                )
