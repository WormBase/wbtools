import logging
from collections import defaultdict
from typing import List

import psycopg2

from wbtools.db.abstract_manager import AbstractWBDBManager
from wbtools.lib.nlp.common import EntityType
from wbtools.lib.scraping import get_curated_papers

logger = logging.getLogger(__name__)


class WBGenericDBManager(AbstractWBDBManager):

    def __init__(self, dbname, user, password, host):
        super().__init__(dbname, user, password, host)

    def get_all_paper_ids(self, added_or_modified_after: str = '1970-01-01', exclude_ids: List[str] = None):
        if not added_or_modified_after:
            added_or_modified_after = '1970-01-01'
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT DISTINCT joinkey, pap_timestamp from pap_electronic_path WHERE pap_timestamp > %s "
                         "ORDER BY pap_timestamp DESC",
                         (added_or_modified_after, ))
            res = curs.fetchall()
            exclude_ids = set(exclude_ids) if exclude_ids else set()
            paper_ids_time = [(row[0], row[1]) for row in res if row[0] not in exclude_ids and "WBPaper" + row[0] not in
                              exclude_ids] if res else []
            paper_id_timestamps = defaultdict(list)
            for paper_id, timestamp in paper_ids_time:
                paper_id_timestamps[paper_id].append(timestamp)
            paper_id_maxtime = {paper_id: max(timestamps) for paper_id, timestamps in paper_id_timestamps.items()}
            return sorted(list(set(paper_id_maxtime.keys())), reverse=True, key=lambda x : paper_id_maxtime[x])

    def get_paper_ids_with_pap_types(self, pap_types: List[str]):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT DISTINCT pap_type.joinkey from pap_type join pap_type_index "
                         "ON pap_type.joinkey = pap_type_index.joinkey WHERE pap_type_index.pap_type_index IN %s ",
                         (tuple(pap_types),))
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

    def get_curated_strains(self, exclude_id_used_as_name: bool = False):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT obo_name_strain FROM obo_name_strain")
            res = curs.fetchall()
            return sorted(list(set([row[0] for row in res if not exclude_id_used_as_name or not
                                    row[0].startswith("WBStrain")])))

    def get_paper_ids_with_email_addresses_extracted(self):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT DISTINCT joinkey from pdf_email")
            res = curs.fetchall()
            return [row[0] for row in res] if res else []

    def get_curated_entities(self, entity_type: EntityType, exclude_id_used_as_name: bool = False):
        if entity_type == EntityType.STRAIN:
            return self.get_curated_strains(exclude_id_used_as_name=exclude_id_used_as_name)
        elif entity_type == EntityType.VARIATION:
            return self.get_curated_variations(exclude_id_used_as_name=exclude_id_used_as_name)

    def get_allele_designations(self):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT DISTINCT lab_alleledesignation from lab_alleledesignation")
            res = curs.fetchall()
            return [row[0] for row in res] if res else []

    def get_strain_designations(self):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT DISTINCT lab_straindesignation from lab_straindesignation")
            res = curs.fetchall()
            return [row[0] for row in res] if res else []

    def get_lab_designations(self, entity_type: EntityType):
        if entity_type == EntityType.STRAIN:
            return self.get_strain_designations()
        elif entity_type == EntityType.VARIATION:
            return self.get_allele_designations()

    def get_paper_ids(self, query, must_be_autclass_positive_data_types: List[str] = None,
                      must_be_positive_manual_flag_data_types: List[str] = None,
                      must_be_curation_negative_data_types: List[str] = None,
                      combine_filters: str = 'OR', count: bool = False, limit: int = None, offset: int = None):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute(query)
            res = curs.fetchall()
            paper_ids = list(set([row[0] for row in res]))
            if must_be_autclass_positive_data_types and must_be_autclass_positive_data_types[0]:
                paper_ids = list(set(paper_ids) & set(self.get_paper_ids_flagged_positive_autclass(
                    must_be_autclass_positive_data_types, combine_filters)))
            if must_be_positive_manual_flag_data_types and must_be_positive_manual_flag_data_types[0]:
                paper_ids = list(set(paper_ids) & set(self.get_paper_ids_flagged_positive_manual(
                    must_be_positive_manual_flag_data_types, combine_filters)))
            if must_be_curation_negative_data_types and must_be_curation_negative_data_types[0]:
                paper_ids = list(set(paper_ids) - set([pap_id for datatype in must_be_curation_negative_data_types for
                                                       pap_id in get_curated_papers(datatype)]))
            if count:
                return len(paper_ids)
            else:
                return sorted(paper_ids, reverse=True)[offset: offset + limit] if limit and offset and limit != offset \
                    else sorted(paper_ids, reverse=True)

    def get_paper_ids_flagged_positive_autclass(self, data_types: List[str], combine_fitlers: str = 'OR'):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT cur_paper, cur_datatype FROM cur_blackbox "
                         "WHERE cur_datatype IN %s AND UPPER(cur_blackbox) IN ('HIGH', 'MEDIUM')",
                         (tuple(data_types),))
            if combine_fitlers == 'AND':
                datatype_ids = {svm_filter: set() for svm_filter in data_types}
                for row in curs.fetchall():
                    datatype_ids[row[1]].add(row[0])
                return set.intersection(*list(datatype_ids.values()))
            else:
                return [row[0] for row in curs.fetchall()]

    def get_paper_ids_flagged_positive_manual(self, data_types: List[str], combine_filters: str = 'OR'):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT afp_email.joinkey FROM afp_email " + " ".join(
                ["JOIN afp_" + table_name + " ON afp_email.joinkey = afp_" + table_name + ".joinkey " for table_name in
                 data_types]) + " WHERE " + (combine_filters + " ").join(["afp_" + data_type + " <> ''" for
                                                                          data_type in data_types]))
        return [row[0] for row in curs.fetchall()]
