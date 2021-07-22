import abc
import logging

from contextlib import contextmanager
from typing import Union, List, Dict

import psycopg2


logger = logging.getLogger(__name__)


class AbstractWBDBManager(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def __init__(self, dbname, user, password, host):
        self.db_name = dbname
        self.user = user
        self.password = password
        self.host = host
        self.connection_str = "dbname='" + dbname
        if user:
            self.connection_str += "' user='" + user
        if password:
            self.connection_str += "' password='" + password
        self.connection_str += "' host='" + host + "'"
        self.conn = None
        self.curs = None

    def __enter__(self):
        self.conn = psycopg2.connect(self.connection_str)
        self.curs = self.conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.commit()
        self.curs.close()
        self.conn.close()

    @contextmanager
    def get_cursor(self):
        close_conn = False
        try:
            if not self.curs or self.curs.closed:
                logger.debug("opening a new db cursor")
                self.conn = psycopg2.connect(self.connection_str)
                self.curs = self.conn.cursor()
                close_conn = True
            else:
                logger.debug("using existing db cursor")
            yield self.curs
        finally:
            if close_conn:
                logger.debug("closing db cursor and connection")
                self.conn.commit()
                self.curs.close()
                self.conn.close()

    def get_db_manager(self, cls):
        new_dbmanager = cls(self.db_name, self.user, self.password, self.host)
        new_dbmanager.conn = self.conn
        new_dbmanager.curs = self.curs
        return new_dbmanager

    def _get_single_field(self, join_key: str, field_name: str) -> Union[str, None]:
        """
        get a specific field from a single field table. Field and table name must be equal

        Args:
            join_key (str): joinkey
            field_name (str): the field to retrieve from the related <field_name> table
        Returns:
            str: the value of the specified field
        """
        table_prefix = field_name.split("_")[0]
        with self.get_cursor() as curs:
            curs.execute("SELECT {} FROM {} WHERE joinkey = %s ORDER BY {}_timestamp DESC".format(
                field_name, field_name, table_prefix), (join_key,))
            res = curs.fetchone()
            return res[0] if res and res[0] != "NULL" and res[0] != "null" else None

    def _get_single_field_multiple_keys(self, join_keys: List[str], field_name: str) -> Union[Dict[str, str], None]:
        table_prefix = field_name.split("_")[0]
        with self.get_cursor() as curs:
            curs.execute("SELECT joinkey, {}, {}_timestamp FROM {} WHERE joinkey IN %s".format(
                field_name, table_prefix, field_name), (tuple(join_keys),))
            res = curs.fetchall()
            val_maxtime = {}
            for row in res:
                if row[0] not in val_maxtime or row[2] > val_maxtime[row[0]][1]:
                    val_maxtime[row[0]] = (row[1], row[2])
            return {joinkey: val_time[0] for joinkey, val_time in val_maxtime.items()}
