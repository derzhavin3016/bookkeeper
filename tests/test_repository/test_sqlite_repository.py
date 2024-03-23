import pytest
from dataclasses import dataclass

from bookkeeper.repository.sqlite_repository import SqliteRepository


@dataclass
class Custom:
    primary_key: int = 0
    data: int = 0
    data_str: str = ""


@pytest.fixture
def custom_class():
    return Custom


@pytest.fixture
def db_name(tmp_path):
    return tmp_path / "test_db.db"


@pytest.fixture
def repo(custom_class, db_name):
    return SqliteRepository(db_name, custom_class)


def test_crud(repo, custom_class):
    obj = custom_class()
    primary_key = repo.add(obj)
    assert obj.primary_key == primary_key
    assert repo.get(primary_key) == obj
    # obj2 = custom_class()
    # obj2.primary_key = primary_key
    # repo.update(obj2)
    # assert repo.get(primary_key) == obj2
    # repo.delete(primary_key)
    # assert repo.get(primary_key) is None
