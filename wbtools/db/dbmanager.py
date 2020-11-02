import psycopg2


class WBDBManager(object):

    def __init__(self, dbname, user, password, host):
        self.connection_str = "dbname='" + dbname
        if user:
            self.connection_str += "' user='" + user
        if password:
            self.connection_str += "' password='" + password
        self.connection_str += "' host='" + host + "'"

    def get_all_paper_ids(self, added_or_modified_after: str = '1970-010-1'):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT DISTINCT joinkey from pap_electronic_path WHERE pap_timestamp > %s ORDER BY joinkey",
                         (added_or_modified_after, ))
            res = curs.fetchall()
            return [row[0] for row in res] if res else []
