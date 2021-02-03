from typing import Union, List
from psycopg2 import sql

import psycopg2

from wbtools.db.abstract_manager import AbstractWBDBManager
from wbtools.db.person import WBPersonDBManager
from wbtools.literature.person import WBAuthor


class WBPaperDBManager(AbstractWBDBManager):

    def __init__(self, dbname, user, password, host):
        super().__init__(dbname, user, password, host)
        self.person_db_manager = WBPersonDBManager(dbname, user, password, host)

    def get_paper_abstract(self, paper_id):
        return self._get_single_field(join_key=paper_id, field_name="pap_abstract")

    def get_paper_title(self, paper_id):
        return self._get_single_field(join_key=paper_id, field_name="pap_title")

    def get_paper_journal(self, paper_id):
        return self._get_single_field(join_key=paper_id, field_name="pap_journal")

    def get_paper_pub_date(self, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute(
                "SELECT pap_year, pap_month, pap_day FROM pap_year JOIN pap_month ON "
                "pap_year.joinkey = pap_month.joinkey JOIN pap_day ON pap_year.joinkey = pap_day.joinkey "
                "WHERE pap_year.joinkey = %s", (paper_id,))
            res = curs.fetchone()
            return res[0] + '-' + res[1] + '-' + res[2] if res else None

    def get_paper_authors(self, paper_id) -> List[WBAuthor]:
        authors = []
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute(
                "SELECT pap_author FROM pap_author "
                "WHERE joinkey = %s", (paper_id,))
            res = curs.fetchall()
            authors_ids = [row[0] for row in res]
            for author_id in authors_ids:
                curs.execute("SELECT pap_author_possible, pap_author_verified LIKE 'YES%%' "
                             "FROM pap_author_possible LEFT OUTER JOIN pap_author_verified ON "
                             "pap_author_possible.author_id = pap_author_verified.author_id and "
                             "pap_author_possible.pap_join = pap_author_verified.pap_join "
                             "WHERE pap_author_possible.author_id = %s",
                             (author_id,))
                res = curs.fetchone()
                if res:
                    curs.execute("SELECT count(*) from pap_author_corresponding WHERE pap_author = %s", (author_id,))
                    is_corresponding = curs.fetchone()[0] == 1
                    person = self.person_db_manager.get_person(person_id=res[0])
                    author = WBAuthor.from_person(person)
                    author.verified = res[1]
                    author.corresponding = is_corresponding
                    authors.append(author)
        return authors

    def get_pmid(self, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute(
                "SELECT * FROM pap_identifier WHERE joinkey = %s AND pap_identifier LIKE 'pmid%'", (paper_id, ))
            res = curs.fetchone()
            if res and res[1].startswith("pmid"):
                return res[1].replace("pmid", "")
            else:
                return None

    def get_corresponding_emails(self, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT two_email.two_email "
                         "FROM pap_author_corresponding "
                         "JOIN pap_author_possible "
                         "ON pap_author_corresponding.author_id = pap_author_possible.author_id "
                         "JOIN pap_author on pap_author.pap_author = pap_author_corresponding.author_id "
                         "JOIN two_email ON two_email.joinkey = pap_author_possible.pap_author_possible "
                         "WHERE pap_author.joinkey = %s", (paper_id,))
            res = curs.fetchone()
            return [res[0]] if res else []

    def get_automated_classification_values(self, paper_id: str):
        """
        get all the automated classification values for the paper

        Args:
            paper_id (str): 8 digit numeric id that identifies the paper
        Returns:
            Union[str, None]: the value associated to the classification ('low', 'medium', or 'high') or None if the
                              paper has not been classified yet
        """
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT cur_datatype, cur_blackbox from cur_blackbox WHERE cur_paper = %s", (paper_id,))
            res = curs.fetchall()
            return [(row[0], row[1]) for row in res]

    def get_file_paths(self, paper_id: str):
        """
        get the list of file paths associated with the specified paper_id

        Args:
            paper_id (str): 8 digit numeric id that identifies the paper
        Returns:
            list[str]: a list of electronic paths to the files
        """
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT pap_electronic_path FROM pap_electronic_path WHERE joinkey = %s ORDER BY joinkey",
                         (paper_id, ))
            rows = curs.fetchall()
            return [row[0] for row in rows] if rows else []
