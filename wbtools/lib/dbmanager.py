from collections import defaultdict

import psycopg2


class DBManager(object):

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

    def get_wb_papers_files_paths(self, from_date: str = '1970-01-01'):
        self.cur.execute("SELECT * FROM pap_electronic_path WHERE pap_timestamp >= '{}' ORDER BY joinkey"
                         .format(from_date))
        rows = self.cur.fetchall()
        papid_files = defaultdict(list)
        for row in rows:
            papid_files[row[0]].append(row[1])
        return papid_files
