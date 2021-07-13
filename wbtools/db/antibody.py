import psycopg2

from wbtools.db.abstract_manager import AbstractWBDBManager


class WBAntibodyDBManager(AbstractWBDBManager):

    def __init__(self, dbname, user, password, host):
        super().__init__(dbname, user, password, host)

    def get_antibody_str_values(self, from_date):
        with self.get_cursor() as curs:
            curs.execute("SELECT cur_paper, cur_strdata FROM cur_strdata where cur_datatype = 'antibody' "
                         "and cur_timestamp >= %(from_date)s", {'from_date': from_date})
            return [(row[0], row[1]) for row in curs.fetchall()]

    def get_antibody_str_value(self, paper_id):
        with self.get_cursor() as curs:
            curs.execute("SELECT cur_strdata FROM cur_strdata where cur_datatype = 'antibody' "
                         "and cur_paper = %(paper_id)s ORDER BY cur_timestamp DESC", {'paper_id': paper_id})
            res = curs.fetchone()
            if res:
                return res[0]
            else:
                return None

    def save_antybody_str_values(self, paper_id, str_values):
        with self.get_cursor() as curs:
            curs.execute("INSERT INTO cur_strdata (cur_paper, cur_datatype, cur_strdata) "
                         "VALUES (%s, 'antibody', %s)", (paper_id, str_values))

    def get_paper_ids_processed_antibody(self):
        with self.get_cursor() as curs:
            curs.execute("SELECT cur_paper FROM cur_strdata where cur_datatype = 'antibody'")
            return [row[0] for row in curs.fetchall()]

