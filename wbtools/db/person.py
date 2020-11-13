import psycopg2

from wbtools.db.abstract_manager import AbstractWBDBManager


class WBPersonDBManager(AbstractWBDBManager):

    def __init__(self, dbname, user, password, host):
        super().__init__(dbname, user, password, host)

    def get_person_id_from_email_address(self, email_address):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT * FROM two_email WHERE two_email=%s", (email_address, ))
            res = curs.fetchone()
            if res:
                return res[0]
            else:
                curs.execute("SELECT * FROM two_old_email WHERE two_old_email=%s", (email_address, ))
                res = curs.fetchone()
                if res:
                    return res[0]
            return None

    def get_current_email_address_for_person(self, person_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT * FROM two_email WHERE joinkey=%s", (person_id, ))
            res = curs.fetchone()
            return res[2] if res else None

    def get_corresponding_emails(self, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT two_email.two_email "
                         "FROM pap_author_corresponding "
                         "JOIN pap_author_possible "
                         "ON pap_author_corresponding.author_id = pap_author_possible.author_id "
                         "JOIN pap_author on pap_author.pap_author = pap_author_corresponding.author_id "
                         "JOIN two_email ON two_email.joinkey = pap_author_possible.pap_author_possible "
                         "WHERE pap_author.joinkey = %s", (paper_id, ))
            res = curs.fetchone()
            return [res[0]] if res else []
