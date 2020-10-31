import psycopg2


class WBDBManager(object):

    def __init__(self, dbname, user, password, host):
        connection_str = "dbname='" + dbname
        if user:
            connection_str += "' user='" + user
        if password:
            connection_str += "' password='" + password
        connection_str += "' host='" + host + "'"
        self.conn = psycopg2.connect(connection_str)
        self.cur = self.conn.cursor()

    def close(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def get_all_paper_ids(self, added_or_modified_after: str = '1970-010-1'):
        self.cur.execute("SELECT DISTINCT joinkey from pap_electronic_path WHERE pap_timestamp > '{}' ORDER BY joinkey"
                         .format(added_or_modified_after))
        rows = self.cur.fetchall()
        return [row[0] for row in rows]
