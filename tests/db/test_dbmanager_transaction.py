from unittest import mock

import pytest

from wbtools.db.dbmanager import WBDBManager


def _manager():
    return WBDBManager("db", "user", "pw", "host")


@mock.patch("wbtools.db.dbmanager.psycopg2.connect")
def test_transaction_commits_on_success(connect):
    db = _manager()
    with db.transaction():
        assert db.afp.curs is connect.return_value.cursor.return_value
        assert db.person.conn is connect.return_value
    connection = connect.return_value
    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()
    connection.close.assert_called_once()


@mock.patch("wbtools.db.dbmanager.psycopg2.connect")
def test_transaction_rolls_back_on_error(connect):
    db = _manager()
    with pytest.raises(ValueError):
        with db.transaction():
            raise ValueError("ABC refused the batch")
    connection = connect.return_value
    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()
    connection.close.assert_called_once()
