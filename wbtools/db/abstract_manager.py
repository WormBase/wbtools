import abc
from typing import Union

import psycopg2


class AbstractWBDBManager(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def __init__(self, dbname, user, password, host):
        self.connection_str = "dbname='" + dbname
        if user:
            self.connection_str += "' user='" + user
        if password:
            self.connection_str += "' password='" + password
        self.connection_str += "' host='" + host + "'"

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
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT {} FROM {} WHERE joinkey = %s ORDER BY {}_timestamp DESC".format(
                field_name, field_name, table_prefix), (join_key,))
            res = curs.fetchone()
            return res[0] if res and res[0] != "NULL" and res[0] != "null" else None
