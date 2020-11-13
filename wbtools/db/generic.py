import logging
import psycopg2

from wbtools.db.abstract_manager import AbstractWBDBManager

logger = logging.getLogger(__name__)


class WBGenericDBManager(AbstractWBDBManager):

    def __init__(self, dbname, user, password, host):
        super().__init__(dbname, user, password, host)

    def get_all_paper_ids(self, added_or_modified_after: str = '1970-010-1'):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT DISTINCT joinkey from pap_electronic_path WHERE pap_timestamp > %s ORDER BY joinkey",
                         (added_or_modified_after, ))
            res = curs.fetchall()
            return [row[0] for row in res] if res else []

    def get_curated_variations(self, exclude_id_used_as_name: bool = False):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT * FROM obo_data_variation WHERE obo_data_variation ~ 'name' AND "
                         "obo_data_variation ~ 'gene'")
            res = curs.fetchall()
            variations = []
            for row in res:
                for line in row[1].split('\n'):
                    if line.startswith('name:'):
                        var_name = line[7:-1]
                        if not exclude_id_used_as_name or not var_name.startswith("WBVar"):
                            variations.append(var_name)
                        break
                else:
                    logger.warning(f"Possible bogus variation entry in DB: {row[0]}")
            return variations
