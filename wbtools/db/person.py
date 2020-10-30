from wbtools.db.dbmanager import WBDBManager


class WBPersonDBManager(WBDBManager):

    def get_person_id_from_email_address(self, email_address):
        self.cur.execute("SELECT * FROM two_email WHERE two_email='{}'".format(email_address))
        res = self.cur.fetchone()
        if res:
            return res[0]
        else:
            self.cur.execute("SELECT * FROM two_old_email WHERE two_old_email='{}'".format(email_address))
            res = self.cur.fetchone()
            if res:
                return res[0]
        return None

    def get_current_email_address_for_person(self, person_id):
        self.cur.execute("SELECT * FROM two_email WHERE joinkey='{}'".format(person_id))
        res = self.cur.fetchone()
        if res:
            return res[2]
        else:
            return None

    def get_corresponding_emails(self, paper_id):
        self.cur.execute("SELECT two_email.two_email "
                         "FROM pap_author_corresponding "
                         "JOIN pap_author_possible "
                         "ON pap_author_corresponding.author_id = pap_author_possible.author_id "
                         "JOIN pap_author on pap_author.pap_author = pap_author_corresponding.author_id "
                         "JOIN two_email ON two_email.joinkey = pap_author_possible.pap_author_possible "
                         "WHERE pap_author.joinkey = '{}'".format(paper_id))
        res = self.cur.fetchone()
        if res:
            return [res[0]]
        return []
