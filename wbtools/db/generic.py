import datetime
import pytz
import logging
import re
from collections import defaultdict
from typing import List

from wbtools.db.abstract_manager import AbstractWBDBManager
from wbtools.lib.nlp.common import EntityType, SPECIES_ALIASES
from wbtools.lib.scraping import get_curated_papers


utc = pytz.UTC
logger = logging.getLogger(__name__)


class WBGenericDBManager(AbstractWBDBManager):

    def __init__(self, dbname, user, password, host):
        super().__init__(dbname, user, password, host)

    def get_all_paper_ids(self, added_or_modified_after: str = '1970-01-01', exclude_ids: List[str] = None):
        if not added_or_modified_after:
            added_or_modified_after = '1970-01-01'
        with self.get_cursor() as curs:
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
            return sorted(list(set(paper_id_maxtime.keys())), reverse=True, key=lambda x: paper_id_maxtime[x])

    def get_paper_ids_with_pap_types(self, pap_types: List[str]):
        with self.get_cursor() as curs:
            curs.execute("SELECT DISTINCT pap_type.joinkey from pap_type join pap_type_index "
                         "ON pap_type.pap_type = pap_type_index.joinkey WHERE pap_type_index.pap_type_index IN %s ",
                         (tuple(pap_types),))
            res = curs.fetchall()
            return [row[0] for row in res] if res else []

    def get_curated_variations(self, exclude_id_used_as_name: bool = False):
        with self.get_cursor() as curs:
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

    def get_variation_name_id_map(self):
        with self.get_cursor() as curs:
            curs.execute("SELECT * FROM obo_name_variation WHERE joinkey != ''")
            rows = curs.fetchall()
            return {row[1]: row[0] for row in rows}

    def get_curated_strains(self, exclude_id_used_as_name: bool = False):
        with self.get_cursor() as curs:
            curs.execute("SELECT obo_name_strain FROM obo_name_strain")
            res = curs.fetchall()
            return sorted(list(set([row[0] for row in res if not exclude_id_used_as_name or not
                                    row[0].startswith("WBStrain")])))

    def get_strain_name_id_map(self):
        with self.get_cursor() as curs:
            curs.execute("SELECT * FROM obo_name_strain where joinkey != ''")
            res = curs.fetchall()
            return {row[1]: row[0] for row in res}

    def get_curated_genes(self, exclude_id_used_as_name: bool = False, include_seqname: bool = True,
                          include_synonyms: bool = True):
        with self.get_cursor() as curs:
            genes_names = set()
            curs.execute("SELECT * FROM gin_locus WHERE joinkey != ''")
            genes_names.update([row[1] for row in curs.fetchall()])
            if not exclude_id_used_as_name:
                curs.execute("SELECT * FROM gin_wbgene WHERE joinkey != ''")
                genes_names.update([row[1] for row in curs.fetchall()])
            if include_seqname:
                curs.execute("SELECT * FROM gin_seqname WHERE joinkey != ''")
                genes_names.update([row[1] for row in curs.fetchall()])
            if include_synonyms:
                curs.execute("SELECT * FROM gin_synonyms WHERE joinkey != ''")
                genes_names.update([row[1] for row in curs.fetchall()])
            return sorted(list(set([gene_name for gene_name in genes_names if not exclude_id_used_as_name or
                                    not gene_name.startswith("WBGene")])))

    def get_gene_name_id_map(self):
        with self.get_cursor() as curs:
            gene_name_id_map = {}
            curs.execute("SELECT * FROM gin_locus WHERE joinkey != ''")
            for row in curs.fetchall():
                gene_name_id_map[row[1]] = "WBGene" + row[0]
            curs.execute("SELECT * FROM gin_synonyms WHERE joinkey != ''")
            for row in curs.fetchall():
                if row[1] not in gene_name_id_map:
                    gene_name_id_map[row[1]] = "WBGene" + row[0]
            curs.execute("SELECT * FROM gin_wbgene WHERE joinkey != ''")
            for row in curs.fetchall():
                if row[1] not in gene_name_id_map:
                    gene_name_id_map[row[1]] = "WBGene" + row[0]
            curs.execute("SELECT * FROM gin_seqname WHERE joinkey != ''")
            for row in curs.fetchall():
                if row[1] not in gene_name_id_map:
                    gene_name_id_map[row[1]] = "WBGene" + row[0]
            return gene_name_id_map

    def get_curated_transgenes(self, exclude_id_used_as_name: bool = False, exclude_invalid: bool = True):
        with self.get_cursor() as curs:
            invalid_ids = set()
            if exclude_invalid:
                curs.execute("select joinkey from trp_objpap_falsepos where trp_objpap_falsepos = 'Fail'")
                invalid_ids = set([row[0] for row in curs.fetchall()])
            curs.execute("SELECT * FROM trp_publicname")
            rows = curs.fetchall()
            return sorted(list(set([row[1] for row in rows if (not exclude_invalid or row[0] not in invalid_ids) and
                                    (not exclude_id_used_as_name or not row[1].startswith("WBTransgene"))])))

    def get_transgene_name_id_map(self):
        with self.get_cursor() as curs:
            transgene_name_id_map = {}
            curs.execute("SELECT trp_name.trp_name, trp_synonym.trp_synonym "
                         "FROM trp_name, trp_synonym "
                         "WHERE trp_name.joinkey = trp_synonym.joinkey")
            rows = curs.fetchall()
            transgene_name_id_map.update({row[1]: row[0] for row in rows})
            curs.execute("SELECT trp_name.trp_name, trp_publicname.trp_publicname "
                         "FROM trp_name, trp_publicname "
                         "WHERE trp_name.joinkey = trp_publicname.joinkey")
            rows = curs.fetchall()
            transgene_name_id_map.update({row[1]: row[0] for row in rows})
            return transgene_name_id_map

    def get_taxon_id_names_map(self):
        with self.get_cursor() as curs:
            curs.execute("SELECT * FROM pap_species_index")
            rows = curs.fetchall()
            taxon_id_name_map = {row[0]: [row[1]] for row in rows}
            for taxon_id, species_alias_arr in SPECIES_ALIASES.items():
                taxon_id_name_map[taxon_id].extend(species_alias_arr)
            for species_id, regex_list in taxon_id_name_map.items():
                if len(regex_list[0].split(" ")) > 1:
                    taxon_id_name_map[species_id].append(regex_list[0][0] + ". " + " ".join(regex_list[0].split(" ")[1:]))
            return taxon_id_name_map

    def get_paper_ids_with_email_addresses_extracted(self):
        with self.get_cursor() as curs:
            curs.execute("SELECT DISTINCT joinkey from pdf_email")
            res = curs.fetchall()
            return [row[0] for row in res] if res else []

    def get_curated_entities(self, entity_type: EntityType, exclude_id_used_as_name: bool = False):
        if entity_type == EntityType.STRAIN:
            return self.get_curated_strains(exclude_id_used_as_name=exclude_id_used_as_name)
        elif entity_type == EntityType.VARIATION:
            return self.get_curated_variations(exclude_id_used_as_name=exclude_id_used_as_name)
        elif entity_type == EntityType.GENE:
            return self.get_curated_genes(exclude_id_used_as_name=exclude_id_used_as_name)
        elif entity_type == EntityType.TRANSGENE:
            return self.get_curated_transgenes(exclude_id_used_as_name=exclude_id_used_as_name)
        else:
            return []

    def get_allele_designations(self):
        with self.get_cursor() as curs:
            curs.execute("SELECT DISTINCT lab_alleledesignation from lab_alleledesignation")
            res = curs.fetchall()
            return [row[0] for row in res] if res else []

    def get_strain_designations(self):
        with self.get_cursor() as curs:
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
                      combine_filters: str = 'OR', count: bool = False, limit: int = None, offset: int = None,
                      tazendra_user: str = None, tazendra_password: str = None):
        if must_be_autclass_positive_data_types and len(must_be_autclass_positive_data_types) == 1 and \
                must_be_autclass_positive_data_types[0] == '':
            must_be_autclass_positive_data_types = None
        if must_be_positive_manual_flag_data_types and len(must_be_positive_manual_flag_data_types) == 1 and \
                must_be_positive_manual_flag_data_types[0] == '':
            must_be_positive_manual_flag_data_types = None
        if must_be_curation_negative_data_types and len(must_be_curation_negative_data_types) == 1 and \
                must_be_curation_negative_data_types[0] == '':
            must_be_curation_negative_data_types = None
        with self.get_cursor() as curs:
            if count and not must_be_autclass_positive_data_types and not must_be_positive_manual_flag_data_types and \
                    not must_be_curation_negative_data_types:
                curs.execute(re.sub(r"SELECT (.*) FROM (.*)", r"SELECT COUNT(\1) FROM \2", query))
                res = curs.fetchone()
                return res[0]
            else:
                curs.execute(query)
                res = curs.fetchall()
                paper_ids = list(set([row[0] for row in res]))
                if must_be_autclass_positive_data_types and must_be_autclass_positive_data_types[0] and \
                must_be_positive_manual_flag_data_types and must_be_positive_manual_flag_data_types[0] and \
                        combine_filters == "OR":
                    paper_ids = list(set(paper_ids) & (set(self.get_paper_ids_flagged_positive_autclass(
                        must_be_autclass_positive_data_types, combine_filters)) | set(
                        self.get_paper_ids_flagged_positive_manual(must_be_positive_manual_flag_data_types,
                                                                   combine_filters))))
                else:
                    if must_be_autclass_positive_data_types and must_be_autclass_positive_data_types[0]:
                        paper_ids = list(set(paper_ids) & set(self.get_paper_ids_flagged_positive_autclass(
                            must_be_autclass_positive_data_types, combine_filters)))
                    if must_be_positive_manual_flag_data_types and must_be_positive_manual_flag_data_types[0]:
                        paper_ids = list(set(paper_ids) & set(self.get_paper_ids_flagged_positive_manual(
                            must_be_positive_manual_flag_data_types, combine_filters)))
                if must_be_curation_negative_data_types and must_be_curation_negative_data_types[0]:
                    paper_ids = list(set(paper_ids) - set([pap_id for datatype in must_be_curation_negative_data_types
                                                           for pap_id in get_curated_papers(datatype, tazendra_user,
                                                                                            tazendra_password)]))
                if count:
                    return len(paper_ids)
                else:
                    return sorted(paper_ids, reverse=True)[offset: offset + limit] if \
                        limit is not None and offset is not None else sorted(paper_ids, reverse=True)

    def get_paper_ids_flagged_positive_autclass(self, data_types: List[str], combine_fitlers: str = 'OR'):
        with self.get_cursor() as curs:
            curs.execute("SELECT cur_paper, cur_datatype, cur_timestamp, UPPER(cur_blackbox) from cur_blackbox "
                         "WHERE cur_datatype IN %s", (tuple(data_types),))
            pap_maxdate = defaultdict(lambda: utc.localize(datetime.datetime.min))
            pap_autclass = {}
            for row in curs.fetchall():
                if row[2] >= pap_maxdate[row[0] + "_" + row[1]]:
                    pap_maxdate[row[0] + "_" + row[1]] = row[2]
                    pap_autclass[row[0] + "_" + row[1]] = row[3]
            pap_id_datatype = [(paper_id_datatype.split("_")[0], "_".join(paper_id_datatype.split("_")[1:])) for
                               paper_id_datatype, autclass in pap_autclass.items() if autclass in ['HIGH', 'MEDIUM']]
            if combine_fitlers == 'AND':
                datatype_ids = {svm_filter: set() for svm_filter in data_types}
                for pid_type in pap_id_datatype:
                    datatype_ids[pid_type[1]].add(pid_type[0])
                return list(set.intersection(*list(datatype_ids.values())))
            else:
                return list(set(pap_id for pap_id, _ in pap_id_datatype))

    def get_paper_ids_flagged_positive_manual(self, data_types: List[str], combine_filters: str = 'OR'):
        with self.get_cursor() as curs:
            curs.execute("SELECT afp_email.joinkey FROM afp_email " + " ".join(
                ["JOIN afp_" + table_name + " ON afp_email.joinkey = afp_" + table_name + ".joinkey " for table_name in
                 data_types]) + " WHERE " + (combine_filters + " ").join(["afp_" + data_type + " <> ''" for
                                                                          data_type in data_types]))
            return [row[0] for row in curs.fetchall()]

    def get_blacklisted_email_addresses(self):
        with self.get_cursor() as curs:
            curs.execute("select frm_email_skip from frm_email_skip")
            res = curs.fetchall()
            if res:
                return [row[0] for row in res]
            else:
                return []

