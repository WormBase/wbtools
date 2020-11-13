import psycopg2

from wbtools.db.abstract_manager import AbstractWBDBManager


class WBExpressionDBManager(AbstractWBDBManager):

    def __init__(self, dbname, user, password, host):
        super().__init__(dbname, user, password, host)

    def get_all_expression_sentences(self, min_num_words: int = 0):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT exp_pattern FROM exp_pattern")
            res = curs.fetchall()
            return [row[0] for row in res if len(row[0].split()) > min_num_words]
