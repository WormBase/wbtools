from typing import Union
from psycopg2 import sql

import psycopg2

from wbtools.db.abstract_manager import AbstractWBDBManager


class WBPaperDBManager(AbstractWBDBManager):

    def __init__(self, dbname, user, password, host):
        super().__init__(dbname, user, password, host)

    def _get_single_field(self, paper_id: str, field_name: str) -> Union[str, None]:
        """
        get a specific field for a paper from a single field table

        Args:
            paper_id (str): 8 digit numeric id that specifies the paper
            field_name (str): the field to retrieve from the related pap_<field_name> table
        Returns:
            str: the value of the specified field for the paper
        """
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT * FROM pap_{} WHERE joinkey = %s".format(sql.Identifier(field_name)), (paper_id, ))
            res = curs.fetchone()
            return res[1] if res else None

    def get_paper_abstract(self, paper_id):
        return self._get_single_field(paper_id=paper_id, field_name="abstract")

    def get_paper_title(self, paper_id):
        return self._get_single_field(paper_id=paper_id, field_name="title")

    def get_paper_journal(self, paper_id):
        return self._get_single_field(paper_id=paper_id, field_name="journal")

    def get_pmid(self, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute(
                "SELECT * FROM pap_identifier WHERE joinkey = %s AND pap_identifier LIKE 'pmid%'", (paper_id, ))
            res = curs.fetchone()
            if res and res[1].startswith("pmid"):
                return res[1].replace("pmid", "")
            else:
                return None

    def get_svm_values(self, paper_id: str):
        """
        get all the SVM (Support Vector Machine) classifier values for the paper

        Args:
            paper_id (str): 8 digit numeric id that identifies the paper
        Returns:
            Union[str, None]: the value associated to the svm ('low', 'medium', or 'high') or None if the paper has not
                              been classified by SVMs yet
        """
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT cur_datatype, cur_svmdata from cur_svmdata WHERE cur_paper = %s", (paper_id,))
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
