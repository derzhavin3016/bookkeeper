import pytest
from dataclasses import dataclass

from bookkeeper.repository.sqlite_repository import SqliteRepository


@dataclass
class Custom:
    primary_key: int = 0
    data: int = 0
    data_str: str = ""
    data_float: float = 0.0


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
    obj2 = custom_class()
    obj2.primary_key = primary_key
    repo.update(obj2)
    assert repo.get(primary_key) == obj2
    repo.delete(primary_key)
    assert repo.get(primary_key) is None


def test_cannot_add_with_pk(repo, custom_class):
    obj = custom_class()
    obj.primary_key = 1
    with pytest.raises(ValueError):
        repo.add(obj)


def test_cannot_add_without_pk(repo):
    with pytest.raises(ValueError):
        repo.add(0)


def test_cannot_delete_unexistent(repo):
    with pytest.raises(KeyError):
        repo.delete(1)


def test_cannot_update_without_pk(repo, custom_class):
    obj = custom_class()
    with pytest.raises(ValueError):
        repo.update(obj)


def test_get_all(repo, custom_class):
    objects = [custom_class() for i in range(5)]
    for o in objects:
        repo.add(o)
    assert repo.get_all() == objects


def test_get_all_with_condition(repo, custom_class):
    objects = []
    for i in range(5):
        o = custom_class()
        o.data = i
        o.data_str = "test"
        repo.add(o)
        objects.append(o)
    assert repo.get_all({"data": "0"}) == [objects[0]]
    assert repo.get_all({"data_str": "test"}) == objects
