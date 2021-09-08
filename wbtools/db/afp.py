import time
import urllib.parse
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Tuple

from wbtools.db.abstract_manager import AbstractWBDBManager
from wbtools.db.generic import WBGenericDBManager
from wbtools.db.paper import WBPaperDBManager
from wbtools.db.person import WBPersonDBManager
from wbtools.lib.nlp.common import EntityType
from wbtools.lib.scraping import get_curated_papers

QUERY_PAPER_IDS_NO_SUBMISSION = "SELECT distinct afp_ve.joinkey FROM afp_version afp_ve FULL OUTER JOIN afp_genestudied afp_g ON afp_ve.joinkey = afp_g.joinkey FULL OUTER JOIN afp_species afp_s ON afp_ve.joinkey = afp_s.joinkey FULL OUTER JOIN afp_variation afp_v ON afp_ve.joinkey = afp_v.joinkey FULL OUTER JOIN afp_strain afp_st ON afp_ve.joinkey = afp_st.joinkey FULL OUTER JOIN afp_transgene afp_t ON afp_ve.joinkey = afp_t.joinkey FULL OUTER JOIN afp_seqchange afp_seq ON afp_ve.joinkey = afp_seq.joinkey FULL OUTER JOIN afp_geneint afp_ge ON afp_ve.joinkey = afp_ge.joinkey FULL OUTER JOIN afp_geneprod afp_gp ON afp_ve.joinkey = afp_gp.joinkey FULL OUTER JOIN afp_genereg afp_gr ON afp_ve.joinkey = afp_gr.joinkey FULL OUTER JOIN afp_newmutant afp_nm ON afp_ve.joinkey = afp_nm.joinkey FULL OUTER JOIN afp_rnai afp_rnai ON afp_ve.joinkey = afp_rnai.joinkey FULL OUTER JOIN afp_overexpr afp_ov ON afp_ve.joinkey = afp_ov.joinkey FULL OUTER JOIN afp_structcorr afp_stc ON afp_ve.joinkey = afp_stc.joinkey FULL OUTER JOIN afp_antibody ON afp_ve.joinkey = afp_antibody.joinkey FULL OUTER JOIN afp_siteaction ON afp_ve.joinkey = afp_siteaction.joinkey FULL OUTER JOIN afp_timeaction ON afp_ve.joinkey = afp_timeaction.joinkey FULL OUTER JOIN afp_rnaseq ON afp_ve.joinkey = afp_rnaseq.joinkey FULL OUTER JOIN afp_chemphen ON afp_ve.joinkey = afp_chemphen.joinkey FULL OUTER JOIN afp_envpheno ON afp_ve.joinkey = afp_envpheno.joinkey FULL OUTER JOIN afp_catalyticact ON afp_ve.joinkey = afp_catalyticact.joinkey FULL OUTER JOIN afp_humdis ON afp_ve.joinkey = afp_humdis.joinkey FULL OUTER JOIN afp_additionalexpr ON afp_ve.joinkey = afp_additionalexpr.joinkey FULL OUTER JOIN afp_comment ON afp_ve.joinkey = afp_comment.joinkey WHERE afp_ve.afp_version = '2' AND afp_g.afp_genestudied IS NULL AND afp_s.afp_species IS NULL AND afp_v.afp_variation IS NULL AND afp_st.afp_strain IS NULL AND afp_t.afp_transgene IS NULL AND afp_seq.afp_seqchange IS NULL AND afp_ge.afp_geneint IS NULL AND afp_gp.afp_geneprod IS NULL AND afp_gr.afp_genereg IS NULL AND afp_nm.afp_newmutant IS NULL AND afp_rnai.afp_rnai IS NULL AND afp_ov.afp_overexpr IS NULL AND afp_stc.afp_structcorr IS NULL AND afp_antibody.afp_antibody IS NULL AND afp_siteaction.afp_siteaction IS NULL AND afp_timeaction.afp_timeaction IS NULL AND afp_rnaseq.afp_rnaseq IS NULL AND afp_chemphen.afp_chemphen IS NULL AND afp_envpheno.afp_envpheno IS NULL AND afp_catalyticact.afp_catalyticact IS NULL AND afp_humdis.afp_humdis IS NULL AND afp_additionalexpr.afp_additionalexpr IS NULL AND afp_comment.afp_comment IS NULL "
QUERY_PAPER_IDS_FULL_SUBMISSION = "SELECT distinct afp_lasttouched.joinkey FROM afp_lasttouched JOIN afp_version ON afp_lasttouched.joinkey = afp_version.joinkey WHERE afp_version.afp_version = '2' "
QUERY_PAPER_IDS_PARTIAL_SUBMISSION = "SELECT distinct afp_ve.joinkey FROM afp_version afp_ve FULL OUTER JOIN afp_lasttouched afp_l ON afp_ve.joinkey = afp_l.joinkey FULL OUTER JOIN afp_genestudied afp_g ON afp_ve.joinkey = afp_g.joinkey FULL OUTER JOIN afp_species afp_s ON afp_ve.joinkey = afp_s.joinkey FULL OUTER JOIN afp_variation afp_v ON afp_ve.joinkey = afp_v.joinkey FULL OUTER JOIN afp_strain afp_st ON afp_ve.joinkey = afp_st.joinkey FULL OUTER JOIN afp_transgene afp_t ON afp_ve.joinkey = afp_t.joinkey FULL OUTER JOIN afp_seqchange afp_seq ON afp_ve.joinkey = afp_seq.joinkey FULL OUTER JOIN afp_geneint afp_ge ON afp_ve.joinkey = afp_ge.joinkey FULL OUTER JOIN afp_geneprod afp_gp ON afp_ve.joinkey = afp_gp.joinkey FULL OUTER JOIN afp_genereg afp_gr ON afp_ve.joinkey = afp_gr.joinkey FULL OUTER JOIN afp_newmutant afp_nm ON afp_ve.joinkey = afp_nm.joinkey FULL OUTER JOIN afp_rnai afp_rnai ON afp_ve.joinkey = afp_rnai.joinkey FULL OUTER JOIN afp_overexpr afp_ov ON afp_ve.joinkey = afp_ov.joinkey FULL OUTER JOIN afp_structcorr afp_stc ON afp_ve.joinkey = afp_stc.joinkey FULL OUTER JOIN afp_antibody ON afp_ve.joinkey = afp_antibody.joinkey FULL OUTER JOIN afp_siteaction ON afp_ve.joinkey = afp_siteaction.joinkey FULL OUTER JOIN afp_timeaction ON afp_ve.joinkey = afp_timeaction.joinkey FULL OUTER JOIN afp_rnaseq ON afp_ve.joinkey = afp_rnaseq.joinkey FULL OUTER JOIN afp_chemphen ON afp_ve.joinkey = afp_chemphen.joinkey FULL OUTER JOIN afp_envpheno ON afp_ve.joinkey = afp_envpheno.joinkey FULL OUTER JOIN afp_catalyticact ON afp_ve.joinkey = afp_catalyticact.joinkey FULL OUTER JOIN afp_humdis ON afp_ve.joinkey = afp_humdis.joinkey FULL OUTER JOIN afp_additionalexpr ON afp_ve.joinkey = afp_additionalexpr.joinkey FULL OUTER JOIN afp_comment ON afp_ve.joinkey = afp_comment.joinkey WHERE afp_ve.afp_version = '2' AND afp_l.afp_lasttouched is NULL AND (afp_g.afp_genestudied IS NOT NULL OR afp_s.afp_species IS NOT NULL OR afp_v.afp_variation IS NOT NULL OR afp_st.afp_strain IS NOT NULL OR afp_t.afp_transgene IS NOT NULL OR afp_seq.afp_seqchange IS NOT NULL OR afp_ge.afp_geneint IS NOT NULL OR afp_gp.afp_geneprod IS NOT NULL OR afp_gr.afp_genereg IS NOT NULL OR afp_nm.afp_newmutant IS NOT NULL OR afp_rnai.afp_rnai IS NOT NULL OR afp_ov.afp_overexpr IS NOT NULL OR afp_stc.afp_structcorr IS NOT NULL OR afp_antibody.afp_antibody IS NOT NULL OR afp_siteaction.afp_siteaction IS NOT NULL OR afp_timeaction.afp_timeaction IS NOT NULL OR afp_rnaseq.afp_rnaseq IS NOT NULL OR afp_chemphen.afp_chemphen IS NOT NULL OR afp_envpheno.afp_envpheno IS NOT NULL OR afp_catalyticact.afp_catalyticact IS NOT NULL OR afp_humdis.afp_humdis IS NOT NULL OR afp_additionalexpr.afp_additionalexpr IS NOT NULL OR afp_comment.afp_comment IS NOT NULL)"


AFP_ENTITIES_SEPARATOR = " | "
AFP_IDS_SEPARATOR = ";%;"
PAP_AFP_EVIDENCE_CODE = "Inferred_automatically \"from author first pass afp_genestudied\""


class WBAFPDBManager(AbstractWBDBManager):

    def __init__(self, dbname, user, password, host):
        super().__init__(dbname, user, password, host)

    def get_filtered_paper_ids(self, query, must_be_autclass_positive_data_types: List[str] = None,
                               must_be_positive_manual_flag_data_types: List[str] = None,
                               must_be_curation_negative_data_types: List[str] = None,
                               combine_filters: str = 'OR', offset: int = None, limit: int = None,
                               count: bool = False, tazendra_user: str = None, tazendra_password: str = None):
        main_paper_ids = self.get_db_manager(cls=WBGenericDBManager).get_paper_ids(
            query=query,
            must_be_autclass_positive_data_types=must_be_autclass_positive_data_types,
            must_be_positive_manual_flag_data_types=must_be_positive_manual_flag_data_types,
            must_be_curation_negative_data_types=must_be_curation_negative_data_types,
            combine_filters=combine_filters, count=count, offset=offset, limit=limit, tazendra_user=tazendra_user,
            tazendra_password=tazendra_password)
        if must_be_autclass_positive_data_types and len(must_be_autclass_positive_data_types) > 0 and \
                must_be_autclass_positive_data_types[0] != '':
            additional_paper_ids = self.get_db_manager(cls=WBGenericDBManager).get_paper_ids(
                query=query,
                must_be_autclass_positive_data_types=None,
                must_be_positive_manual_flag_data_types=must_be_autclass_positive_data_types,
                must_be_curation_negative_data_types=must_be_curation_negative_data_types,
                combine_filters=combine_filters, count=count, offset=offset, limit=limit, tazendra_user=tazendra_user,
                tazendra_password=tazendra_password)
            return list(set(main_paper_ids) | set(additional_paper_ids))
        else:
            return main_paper_ids

    def get_paper_ids_afp_processed(self, must_be_autclass_positive_data_types: List[str] = None,
                                    must_be_positive_manual_flag_data_types: List[str] = None,
                                    must_be_curation_negative_data_types: List[str] = None,
                                    combine_filters: str = 'OR', offset: int = None, limit: int = None,
                                    count: bool = False, tazendra_user: str = None, tazendra_password: str = None):
        return self.get_db_manager(cls=WBGenericDBManager).get_paper_ids(
            query="SELECT joinkey FROM afp_version WHERE afp_version = '2'",
            must_be_autclass_positive_data_types=must_be_autclass_positive_data_types,
            must_be_positive_manual_flag_data_types=must_be_positive_manual_flag_data_types,
            must_be_curation_negative_data_types=must_be_curation_negative_data_types,
            combine_filters=combine_filters, count=count, offset=offset, limit=limit, tazendra_user=tazendra_user,
            tazendra_password=tazendra_password)

    def get_paper_ids_afp_no_submission(self, must_be_autclass_positive_data_types: List[str] = None,
                                        must_be_positive_manual_flag_data_types: List[str] = None,
                                        must_be_curation_negative_data_types: List[str] = None,
                                        combine_filters: str = 'OR', offset: int = None, limit: int = None,
                                        count: bool = False, tazendra_user: str = None, tazendra_password: str = None):
        return self.get_db_manager(cls=WBGenericDBManager).get_paper_ids(
            query=QUERY_PAPER_IDS_NO_SUBMISSION,
            must_be_autclass_positive_data_types=must_be_autclass_positive_data_types,
            must_be_positive_manual_flag_data_types=must_be_positive_manual_flag_data_types,
            must_be_curation_negative_data_types=must_be_curation_negative_data_types,
            combine_filters=combine_filters, count=count, offset=offset, limit=limit, tazendra_user=tazendra_user,
            tazendra_password=tazendra_password)

    def get_paper_ids_afp_full_submission(self, must_be_autclass_positive_data_types: List[str] = None,
                                          must_be_positive_manual_flag_data_types: List[str] = None,
                                          must_be_curation_negative_data_types: List[str] = None,
                                          combine_filters: str = 'OR', offset: int = None, limit: int = None,
                                          count: bool = False, tazendra_user: str = None,
                                          tazendra_password: str = None):
        # note that the value of 'must_be_autclass_positive_data_types' are added to the manual filter, so that
        # submitted values are filtered, and not the original svm value
        combined_manual_autoclass_datatypes = None
        if must_be_autclass_positive_data_types:
            combined_manual_autoclass_datatypes = must_be_autclass_positive_data_types
        if must_be_positive_manual_flag_data_types:
            if combined_manual_autoclass_datatypes:
                combined_manual_autoclass_datatypes = [cl for cl in list(set(must_be_autclass_positive_data_types) |
                               set(must_be_positive_manual_flag_data_types)) if cl != '']
            else:
                combined_manual_autoclass_datatypes = must_be_positive_manual_flag_data_types
        return self.get_db_manager(cls=WBGenericDBManager).get_paper_ids(
            query=QUERY_PAPER_IDS_FULL_SUBMISSION,
            must_be_positive_manual_flag_data_types=combined_manual_autoclass_datatypes,
            must_be_curation_negative_data_types=must_be_curation_negative_data_types,
            combine_filters=combine_filters, count=count, offset=offset, limit=limit, tazendra_user=tazendra_user,
            tazendra_password=tazendra_password)

    def get_paper_ids_afp_partial_submission(self, must_be_autclass_positive_data_types: List[str] = None,
                                             must_be_positive_manual_flag_data_types: List[str] = None,
                                             must_be_curation_negative_data_types: List[str] = None,
                                             combine_filters: str = 'OR', offset: int = None, limit: int = None,
                                             count: bool = False, tazendra_user: str = None,
                                             tazendra_password: str = None):
        return self.get_db_manager(cls=WBGenericDBManager).get_paper_ids(
            query=QUERY_PAPER_IDS_PARTIAL_SUBMISSION,
            must_be_autclass_positive_data_types=must_be_autclass_positive_data_types,
            must_be_positive_manual_flag_data_types=must_be_positive_manual_flag_data_types,
            must_be_curation_negative_data_types=must_be_curation_negative_data_types,
            combine_filters=combine_filters, count=count, offset=offset, limit=limit, tazendra_user=tazendra_user,
            tazendra_password=tazendra_password)

    def get_num_papers_no_entities(self):
        with self.get_cursor() as curs:
            curs.execute("select count(*) FROM "
                         "afp_version JOIN tfp_genestudied ON afp_version.joinkey = tfp_genestudied.joinkey "
                         "JOIN tfp_transgene ON afp_version.joinkey = tfp_transgene.joinkey "
                         "JOIN tfp_variation ON afp_version.joinkey = tfp_variation.joinkey "
                         "JOIN tfp_strain ON afp_version.joinkey = tfp_strain.joinkey "
                         "WHERE afp_version.afp_version = '2' AND tfp_genestudied.tfp_genestudied = '' "
                         "AND tfp_transgene.tfp_transgene = '' AND tfp_variation.tfp_variation = '' "
                         "AND tfp_strain = ''")
            res = curs.fetchone()
            if res:
                return res[0]
            else:
                return 0

    def get_list_papers_no_entities(self, from_offset: int = None, count: int = None):
        with self.get_cursor() as curs:
            query = "select afp_version.joinkey FROM afp_version JOIN tfp_genestudied ON afp_version.joinkey = " \
                    "tfp_genestudied.joinkey JOIN tfp_transgene ON afp_version.joinkey = tfp_transgene.joinkey JOIN " \
                    "tfp_variation ON afp_version.joinkey = tfp_variation.joinkey JOIN tfp_strain ON " \
                    "afp_version.joinkey = tfp_strain.joinkey WHERE afp_version.afp_version = '2' AND " \
                    "tfp_genestudied.tfp_genestudied = '' AND tfp_transgene.tfp_transgene = '' AND " \
                    "tfp_variation.tfp_variation = '' AND tfp_strain = ''"
            if from_offset:
                query += " OFFSET " + str(from_offset)
            if count:
                query += " LIMIT " + str(count)
            curs.execute(query)
            res = curs.fetchall()
            if res:
                return [papid[0] for papid in res]
            else:
                return []

    def get_num_papers_old_afp_processed(self):
        with self.get_cursor() as curs:
            curs.execute("SELECT count(distinct afp_email.joinkey) FROM afp_email FULL OUTER JOIN afp_version "
                         "ON afp_email.joinkey = afp_version.joinkey WHERE afp_version.afp_version IS NULL "
                         "OR afp_version.afp_version = '1' AND afp_email.afp_email IS NOT NULL")
            res = curs.fetchone()
            if res:
                return int(res[0])
            else:
                return 0

    def get_num_papers_old_afp_author_submitted(self):
        with self.get_cursor() as curs:
            curs.execute("SELECT count(distinct afp_lasttouched.joinkey) FROM afp_lasttouched "
                         "FULL OUTER JOIN afp_version ON afp_lasttouched.joinkey = afp_version.joinkey JOIN afp_email "
                         "ON afp_lasttouched.joinkey = afp_email.joinkey WHERE afp_version.afp_version IS NULL OR "
                         "afp_version.afp_version = '1'")
            res = curs.fetchone()
            if res:
                return int(res[0])
            else:
                return 0

    def get_num_entities_per_paper(self, enetity_label):
        with self.get_cursor() as curs:
            curs.execute("SELECT tfp_{} FROM tfp_{} FULL OUTER JOIN afp_version ON "
                         "tfp_{}.joinkey = afp_version.joinkey WHERE afp_version.afp_version = '2'"
                         .format(enetity_label, enetity_label, enetity_label))
            res = curs.fetchall()
            return [len(row[0].split(" | ")) for row in res]

    def get_afp_curatable_paper_ids(self):
        """
        get the list of curatable papers (i.e., papers that can be processed by AFP - type must be 'primary' or pap_type
        equal to 1).

        Returns:
            List[str]: the set of curatable papers
        """
        with self.get_cursor() as curs:
            curs.execute("SELECT * FROM pap_primary_data JOIN pap_type "
                         "ON pap_primary_data.joinkey = pap_type.joinkey "
                         "JOIN pap_species ON pap_primary_data.joinkey = pap_species.joinkey "
                         "JOIN pap_year ON pap_primary_data.joinkey = pap_year.joinkey "
                         "WHERE pap_primary_data.pap_primary_data = 'primary' AND pap_species.pap_species = '6239' "
                         "AND CAST(REGEXP_REPLACE(COALESCE(pap_year,'0'), '[^0-9]+', '', 'g') AS INTEGER) >= {} "
                         "AND pap_primary_data.joinkey NOT IN (SELECT distinct joinkey FROM pap_type WHERE "
                         "pap_type = '14' OR pap_type = '26' OR pap_type = '15')".format(str(datetime.now().year - 2)))
            rows = curs.fetchall()
            return [row[0] for row in rows]

    def get_afp_processed_paper_ids(self):
        """
        get the set of papers that have already been processed by AFP

        Returns:
            List[str]: the set of papers that have already been processed
        """
        with self.get_cursor() as curs:
            curs.execute("SELECT * FROM afp_passwd")
            rows = curs.fetchall()
            return [row[0] for row in rows]

    def paper_is_afp_processed(self, paper_id):
        with self.get_cursor() as curs:
            curs.execute("SELECT * FROM afp_passwd WHERE joinkey = '{}'".format(paper_id))
            rows = curs.fetchone()
            return True if rows else False

    def get_processed_date(self, paper_id):
        with self.get_cursor() as curs:
            curs.execute("select afp_timestamp from afp_passwd where joinkey = '{}'".format(paper_id))
            res = curs.fetchone()
            return res[0] if res else None

    def author_has_submitted(self, paper_id):
        with self.get_cursor() as curs:
            curs.execute("SELECT count(*) from afp_lasttouched WHERE joinkey = '{}'".format(paper_id))
            row = curs.fetchone()
            return int(row[0]) == 1

    def author_has_modified(self, paper_id):
        full_submission_ids = self.get_paper_ids_afp_full_submission()
        partial_submission_ids = self.get_paper_ids_afp_partial_submission()
        modified_ids = set(full_submission_ids) | set(partial_submission_ids)
        return paper_id in modified_ids

    def get_passwd(self, paper_id):
        with self.get_cursor() as curs:
            curs.execute("SELECT * FROM afp_passwd WHERE joinkey = '{}'".format(paper_id))
            res = curs.fetchone()
            if res:
                return res[1]
            else:
                return None

    def get_paper_id_from_passwd(self, passwd):
        with self.get_cursor() as curs:
            curs.execute("SELECT * FROM afp_passwd WHERE afp_passwd = '{}'".format(passwd))
            res = curs.fetchone()
            if res:
                return res[0]
            else:
                return None

    def get_afp_form_link(self, paper_id, base_url):
        paper_dbmanager = self.get_db_manager(cls=WBPaperDBManager)
        passwd = self.get_passwd(paper_id)
        title = paper_dbmanager.get_paper_title(paper_id)
        journal = paper_dbmanager.get_paper_journal(paper_id)
        pmid = paper_dbmanager.get_pmid(paper_id)
        if not pmid:
            pmid = ''
        doi = paper_dbmanager.get_doi(paper_id)
        if not doi:
            doi = ''
        person_id = self.get_latest_contributor_id(paper_id)
        if not person_id:
            afp_emails = self.get_contact_emails(paper_id)
            if afp_emails:
                person_id = self.get_db_manager(cls=WBPersonDBManager).get_person_id_from_email_address(afp_emails[0])
        if person_id:
            url = base_url + "?paper=" + paper_id + "&passwd=" + passwd + "&title=" + urllib.parse.quote(title) + \
                  "&journal=" + urllib.parse.quote(journal) + "&pmid=" + pmid + "&doi=" + doi + "&personid=" + \
                  person_id.replace("two", "") + "&hide_genes=false&hide_alleles=false&hide_strains=false"
        else:
            url = ""
        return url

    def set_extracted_entities_in_paper(self, publication_id, entities_ids: List[str], table_name):
        with self.get_cursor() as curs:
            curs.execute("DELETE FROM {} WHERE joinkey = %s".format(table_name), (publication_id, ))
            curs.execute("INSERT INTO {} (joinkey, {}) VALUES(%s, %s)".format(table_name, table_name),
                         (publication_id, AFP_ENTITIES_SEPARATOR.join(entities_ids)))

    def set_value_with_history(self, publication_id, table_name, value):
        with self.get_cursor() as curs:
            curs.execute("DELETE FROM {} WHERE joinkey = %s".format(table_name), (publication_id, ))
            curs.execute("INSERT INTO {} (joinkey, {}) VALUES(%s, %s)".format(table_name, table_name),
                         (publication_id, value))
            curs.execute("INSERT INTO {}_hst (joinkey, {}_hst) VALUES(%s, %s)".format(table_name, table_name),
                         (publication_id, value))

    def set_extracted_antibody(self, paper_id):
        with self.get_cursor() as curs:
            curs.execute("DELETE FROM tfp_antibody WHERE joinkey = %s", (paper_id, ))
            curs.execute("INSERT INTO tfp_antibody (joinkey, tfp_antibody) VALUES(%s, 'checked')", (paper_id, ))

    def set_passwd(self, publication_id, passwd):
        self.set_value_with_history(publication_id, "afp_passwd", passwd)

    def set_contact_emails(self, publication_id, email_addr_list: List[str]):
        self.set_value_with_history(publication_id, "afp_email", " | ".join(email_addr_list))

    def get_contact_emails(self, paper_id):
        with self.get_cursor() as curs:
            curs.execute("SELECT afp_email FROM afp_email WHERE joinkey = %s", (paper_id, ))
            res = curs.fetchone()
            if res:
                return res[0].split(" | ")
            else:
                return []

    def set_submitted_gene_list(self, genes, paper_id):
        self.set_value_with_history(paper_id, "afp_genestudied", genes)

    def set_submitted_gene_model_update(self, gene_model_update, paper_id):
        self.set_value_with_history(paper_id, "afp_structcorr", gene_model_update)

    def set_submitted_species_list(self, species, paper_id):
        self.set_value_with_history(paper_id, "afp_species", species)

    def set_submitted_alleles_list(self, alleles, paper_id):
        self.set_value_with_history(paper_id, "afp_variation", alleles)

    def set_submitted_allele_seq_change(self, allele_seq_change, paper_id):
        self.set_value_with_history(paper_id, "afp_seqchange", allele_seq_change)

    def set_submitted_other_alleles(self, other_alleles, paper_id):
        self.set_value_with_history(paper_id, "afp_othervariation", other_alleles)

    def set_submitted_strains_list(self, strains, paper_id):
        self.set_value_with_history(paper_id, "afp_strain", strains)

    def set_submitted_other_strains(self, other_strains, paper_id):
        self.set_value_with_history(paper_id, "afp_otherstrain", other_strains)

    def set_submitted_transgenes_list(self, transgenes, paper_id):
        self.set_value_with_history(paper_id, "afp_transgene", transgenes)

    def set_submitted_new_transgenes(self, new_transgenes, paper_id):
        self.set_value_with_history(paper_id, "afp_othertransgene", new_transgenes)

    def set_submitted_new_antibody(self, new_antibody, paper_id):
        self.set_value_with_history(paper_id, "afp_antibody", new_antibody)

    def set_submitted_other_antibodies(self, other_antibodies, paper_id):
        self.set_value_with_history(paper_id, "afp_otherantibody", other_antibodies)

    def set_submitted_anatomic_expr(self, anatomic_expr, paper_id):
        self.set_value_with_history(paper_id, "afp_otherexpr", anatomic_expr)

    def set_submitted_site_action(self, site_action, paper_id):
        self.set_value_with_history(paper_id, "afp_siteaction", site_action)

    def set_submitted_time_action(self, time_action, paper_id):
        self.set_value_with_history(paper_id, "afp_timeaction", time_action)

    def set_submitted_rnaseq(self, rnaseq, paper_id):
        self.set_value_with_history(paper_id, "afp_rnaseq", rnaseq)

    def set_submitted_additional_expr(self, additional_expr, paper_id):
        self.set_value_with_history(paper_id, "afp_additionalexpr", additional_expr)

    def set_submitted_gene_int(self, gene_int, paper_id):
        self.set_value_with_history(paper_id, "afp_geneint", gene_int)

    def set_submitted_phys_int(self, phys_int, paper_id):
        self.set_value_with_history(paper_id, "afp_geneprod", phys_int)

    def set_submitted_gene_reg(self, gene_reg, paper_id):
        self.set_value_with_history(paper_id, "afp_genereg", gene_reg)

    def set_submitted_allele_pheno(self, allele_pheno, paper_id):
        self.set_value_with_history(paper_id, "afp_newmutant", allele_pheno)

    def set_submitted_rnai_pheno(self, rnai_pheno, paper_id):
        self.set_value_with_history(paper_id, "afp_rnai", rnai_pheno)

    def set_submitted_transover_pheno(self, transover_pheno, paper_id):
        self.set_value_with_history(paper_id, "afp_overexpr", transover_pheno)

    def set_submitted_chemical(self, chemical, paper_id):
        self.set_value_with_history(paper_id, "afp_chemphen", chemical)

    def set_submitted_env(self, env, paper_id):
        self.set_value_with_history(paper_id, "afp_envpheno", env)

    def set_submitted_protein(self, protein, paper_id):
        self.set_value_with_history(paper_id, "afp_catalyticact", protein)

    def set_submitted_othergenefunc(self, othergenefunc, paper_id):
        self.set_value_with_history(paper_id, "afp_othergenefunc", othergenefunc)

    def set_submitted_disease(self, disease, paper_id):
        self.set_value_with_history(paper_id, "afp_humdis", disease)

    def set_submitted_comments(self, comments, paper_id):
        self.set_value_with_history(paper_id, "afp_comment", comments)

    def set_version(self, paper_id):
        with self.get_cursor() as curs:
            curs.execute("DELETE FROM afp_version WHERE joinkey = %s", (paper_id,))
            curs.execute("INSERT INTO afp_version (joinkey, afp_version) VALUES(%s, '2')", (paper_id,))

    def set_gene_list(self, genes, paper_id):
        self.set_value_with_history(paper_id, "afp_genestudied", genes)

    def set_gene_model_update(self, gene_model_update, paper_id):
        self.set_value_with_history(paper_id, "afp_structcorr", gene_model_update)

    def save_extracted_data_to_db(self, paper_id: str, genes: List[str], alleles: List[str], species: List[str],
                                  strains: List[str], transgenes: List[str], author_emails: List[str]):
        passwd = self.get_passwd(paper_id=paper_id)
        passwd = time.time() if not passwd else passwd
        if self.get_db_manager(WBPaperDBManager).is_antibody_set(paper_id):
            self.set_extracted_antibody(paper_id)
        self.set_extracted_entities_in_paper(paper_id, genes, "tfp_genestudied")
        self.set_extracted_entities_in_paper(paper_id, alleles, "tfp_variation")
        self.set_extracted_entities_in_paper(paper_id, species, "tfp_species")
        self.set_extracted_entities_in_paper(paper_id, strains, "tfp_strain")
        self.set_extracted_entities_in_paper(paper_id, transgenes, "tfp_transgene")
        self.set_version(paper_id)
        self.set_passwd(paper_id, passwd)
        self.set_contact_emails(paper_id, author_emails)
        return passwd

    def set_pap_gene_list(self, paper_id, person_id):
        with self.get_cursor() as curs:
            curs.execute("SELECT * FROM pap_gene WHERE joinkey = %s AND pap_evidence = %s", (paper_id,
                                                                                             PAP_AFP_EVIDENCE_CODE))
            res_pap = curs.fetchall()
            if res_pap:
                for res in res_pap:
                    curs.execute("INSERT INTO h_pap_gene (joinkey, pap_gene, pap_order, pap_curator, pap_evidence) "
                                 "VALUES('{}', '{}', {}, '{}', '{}')".format(res[0], res[1], res[2], res[3], res[4]))
            curs.execute("DELETE FROM pap_gene WHERE joinkey = '{}' AND pap_evidence = '{}'".format(
                paper_id, PAP_AFP_EVIDENCE_CODE))
            max_order = 0
            curs.execute("SELECT MAX(pap_order) FROM pap_gene WHERE joinkey = '{}' AND pap_evidence <> '{}'"
                             .format(paper_id, PAP_AFP_EVIDENCE_CODE))
            res = curs.fetchone()
            if res and res[0]:
                max_order = int(res[0])
            curs.execute("SELECT * FROM afp_genestudied WHERE joinkey = '{}'".format(paper_id))
            res_afp = curs.fetchone()
            afp_ids = set()
            if res_afp:
                afp_ids = set([gene_str.split(AFP_IDS_SEPARATOR)[0] for gene_str in res_afp[1]
                              .split(AFP_ENTITIES_SEPARATOR)])
            for afp_id in afp_ids:
                curs.execute("INSERT INTO pap_gene (joinkey, pap_gene, pap_order, pap_curator, pap_evidence) "
                                 "VALUES('{}', '{}', {}, '{}', '{}')".format(paper_id, afp_id, max_order + 1,
                                                                             person_id, PAP_AFP_EVIDENCE_CODE))
                curs.execute("INSERT INTO h_pap_gene (joinkey, pap_gene, pap_order, pap_curator, pap_evidence) "
                                 "VALUES('{}', '{}', {}, '{}', '{}')".format(paper_id, afp_id, max_order + 1,
                                                                             person_id, PAP_AFP_EVIDENCE_CODE))
                max_order += 1

    def set_pap_species_list(self, paper_id, person_id):
        with self.get_cursor() as curs:
            curs.execute("SELECT * FROM pap_species WHERE joinkey = '{}' AND pap_evidence = '{}'".format(
                paper_id, PAP_AFP_EVIDENCE_CODE))
            res_pap = curs.fetchall()
            if res_pap:
                for res in res_pap:
                    curs.execute("INSERT INTO h_pap_species (joinkey, pap_species, pap_order, pap_curator, "
                                     "pap_evidence) VALUES('{}', '{}', {}, '{}', '{}')".format(res[0], res[1],
                                                                                               res[2], res[3], res[4]))
            curs.execute("DELETE FROM pap_species WHERE joinkey = '{}' AND pap_evidence = '{}'".format(
                paper_id, PAP_AFP_EVIDENCE_CODE))
            max_order = 0
            curs.execute("SELECT MAX(pap_order) FROM pap_species WHERE joinkey = '{}' AND pap_evidence <> '{}'"
                             .format(paper_id, PAP_AFP_EVIDENCE_CODE))
            res = curs.fetchone()
            if res and res[0]:
                max_order = int(res[0])
            curs.execute("SELECT * FROM afp_species WHERE joinkey = '{}'".format(paper_id))
            res_afp = curs.fetchone()
            afp_species = set([gene_str for gene_str in res_afp[1].split(AFP_ENTITIES_SEPARATOR)])
            for afp_sp in afp_species:
                curs.execute("SELECT * FROM pap_species_index WHERE pap_species_index = '{}'".format(afp_sp))
                res_species_index = curs.fetchone()
                if res_species_index:
                    curs.execute("INSERT INTO pap_species (joinkey, pap_species, pap_order, pap_curator, pap_evidence) "
                                     "VALUES('{}', '{}', {}, '{}', '{}')".format(paper_id, res_species_index[0],
                                                                                 max_order + 1, person_id,
                                                                                 PAP_AFP_EVIDENCE_CODE))
                    curs.execute("INSERT INTO h_pap_species (joinkey, pap_species, pap_order, pap_curator, "
                                     "pap_evidence) VALUES('{}', '{}', {}, '{}', '{}')".format(
                        paper_id, res_species_index[0], max_order + 1, person_id, PAP_AFP_EVIDENCE_CODE))
                    max_order += 1

    def set_contributor(self, paper_id, person_id):
        with self.get_cursor() as curs:
            curs.execute("INSERT INTO afp_contributor (joinkey, afp_contributor) VALUES(%s, %s)", (paper_id, person_id))
            curs.execute("INSERT INTO afp_contributor_hst (joinkey, afp_contributor_hst) VALUES(%s, %s)",
                         (paper_id, person_id))

    def get_latest_contributor_id(self, paper_id):
        with self.get_cursor() as curs:
            curs.execute("select afp_contributor from afp_contributor WHERE joinkey = '{}' ORDER BY afp_timestamp DESC"
                         .format(paper_id))
            res = curs.fetchone()
            return res[0] if res else None

    def set_last_touched(self, paper_id):
        self.set_value_with_history(paper_id, "afp_lasttouched", int(time.time()))

    def get_num_contributors(self):
        with self.get_cursor() as curs:
            curs.execute("select count(distinct afp_contributor.afp_contributor) from afp_contributor join "
                         "afp_lasttouched on afp_contributor.joinkey = afp_lasttouched.joinkey join afp_version on "
                         "afp_contributor.joinkey = afp_version.joinkey where afp_version.afp_version = '2'")
            res = curs.fetchone()
            if res:
                return int(res[0])
            else:
                return 0

    def get_list_contributors_with_numbers(self, from_offset, count):
        with self.get_cursor() as curs:
            person_db_manager = self.get_db_manager(cls=WBPersonDBManager)
            curs.execute("select afp_contributor.afp_contributor, count(DISTINCT afp_contributor.joinkey) from "
                         "afp_contributor join afp_lasttouched on afp_contributor.joinkey = afp_lasttouched.joinkey "
                         "join afp_version on afp_contributor.joinkey = afp_version.joinkey "
                         "where afp_version.afp_version = '2' "
                         "group by afp_contributor.afp_contributor order by count(DISTINCT afp_contributor.joinkey) "
                         "desc OFFSET {} LIMIT {}".format(from_offset, count))
            res = curs.fetchall()
            if res:
                return [(person_db_manager.get_email(row[0]), row[1]) for row in res]
            else:
                return []

    def get_num_unique_emailed_addresses(self):
        with self.get_cursor() as curs:
            curs.execute("select afp_email.afp_email from afp_email WHERE joinkey IN "
                         "(SELECT joinkey from afp_version WHERE afp_version.afp_version = '2')")
            return len(list(set([email_addr for row in curs.fetchall() for email_addr in row[0].split(" | ")])))

    def get_emailed_authors_with_count(self, from_offset, count):
        with self.get_cursor() as curs:
            curs.execute("select afp_email from "
                         "afp_email where joinkey in (select joinkey from afp_version where "
                         "afp_version.afp_version = '2') ")
            email_counter = defaultdict(int)
            for pap_email_addresses in curs.fetchall():
                for email_addr in pap_email_addresses[0].split(" | "):
                    email_counter[email_addr] += 1
            ret_list = sorted([(email_addr, counter) for email_addr, counter in email_counter.items()],
                              key=lambda x: x[1], reverse=True)
            if from_offset > 0 or count > 0:
                return ret_list[from_offset : from_offset + count]
            else:
                return ret_list

    def get_author_token_from_email(self, email):
        with self.get_cursor() as curs:
            curs.execute("SELECT two_timestamp FROM two_email WHERE two_email = '{}'".format(email))
            res = curs.fetchall()
            return str(res[0][0].timestamp()) + "/" + str(int(res[0][0].utcoffset().total_seconds() / 3600)) if res \
                else None

    def is_token_valid(self, token):
        return self.get_email_from_token(token) is not None

    def get_email_from_token(self, token):
        ts_tokenarr = token.split("/")
        if len(ts_tokenarr) == 2:
            ts_token = (datetime.utcfromtimestamp(float(ts_tokenarr[0])) + timedelta(hours=int(ts_tokenarr[1]))) \
                           .strftime('%Y-%m-%d %H:%M:%S.%f') + ts_tokenarr[1]
            with self.get_cursor() as curs:
                curs.execute("SELECT two_email FROM two_email WHERE two_timestamp = '{}'".format(ts_token))
                res = curs.fetchone()
                if res:
                    return res[0]
                else:
                    return None
        else:
            return None

    # author paper page (lists of paper ids by author)
    def get_papers_waiting_sub_from_auth_token(self, token, offset, count):
        author_email = self.get_email_from_token(token)
        with self.get_cursor() as curs:
            curs.execute("SELECT DISTINCT afp_email.joinkey FROM afp_email JOIN afp_version afp_ve "
                         "ON afp_email.joinkey = afp_ve.joinkey "
                         "FULL OUTER JOIN pap_author ON afp_email.joinkey = pap_author.joinkey "
                         "FULL OUTER JOIN pap_author_possible ON pap_author.pap_author = pap_author_possible.author_id "
                         "FULL OUTER JOIN pap_author_verified ON pap_author.pap_author = pap_author_verified.author_id "
                         "FULL OUTER JOIN two_email ON pap_author_possible.pap_author_possible = two_email.joinkey "
                         "FULL OUTER JOIN afp_lasttouched ON afp_email.joinkey = afp_lasttouched.joinkey "
                         "FULL OUTER JOIN afp_genestudied afp_g ON afp_ve.joinkey = afp_g.joinkey "
                         "FULL OUTER JOIN afp_species afp_s ON afp_ve.joinkey = afp_s.joinkey "
                         "FULL OUTER JOIN afp_variation afp_v ON afp_ve.joinkey = afp_v.joinkey "
                         "FULL OUTER JOIN afp_strain afp_st ON afp_ve.joinkey = afp_st.joinkey "
                         "FULL OUTER JOIN afp_transgene afp_t ON afp_ve.joinkey = afp_t.joinkey "
                         "FULL OUTER JOIN afp_seqchange afp_seq ON afp_ve.joinkey = afp_seq.joinkey "
                         "FULL OUTER JOIN afp_geneint afp_ge ON afp_ve.joinkey = afp_ge.joinkey "
                         "FULL OUTER JOIN afp_geneprod afp_gp ON afp_ve.joinkey = afp_gp.joinkey "
                         "FULL OUTER JOIN afp_genereg afp_gr ON afp_ve.joinkey = afp_gr.joinkey "
                         "FULL OUTER JOIN afp_newmutant afp_nm ON afp_ve.joinkey = afp_nm.joinkey "
                         "FULL OUTER JOIN afp_rnai afp_rnai ON afp_ve.joinkey = afp_rnai.joinkey "
                         "FULL OUTER JOIN afp_overexpr afp_ov ON afp_ve.joinkey = afp_ov.joinkey "
                         "FULL OUTER JOIN afp_structcorr afp_stc ON afp_ve.joinkey = afp_stc.joinkey "
                         "FULL OUTER JOIN afp_antibody ON afp_ve.joinkey = afp_antibody.joinkey "
                         "FULL OUTER JOIN afp_siteaction ON afp_ve.joinkey = afp_siteaction.joinkey "
                         "FULL OUTER JOIN afp_timeaction ON afp_ve.joinkey = afp_timeaction.joinkey "
                         "FULL OUTER JOIN afp_rnaseq ON afp_ve.joinkey = afp_rnaseq.joinkey "
                         "FULL OUTER JOIN afp_chemphen ON afp_ve.joinkey = afp_chemphen.joinkey "
                         "FULL OUTER JOIN afp_envpheno ON afp_ve.joinkey = afp_envpheno.joinkey "
                         "FULL OUTER JOIN afp_catalyticact ON afp_ve.joinkey = afp_catalyticact.joinkey "
                         "FULL OUTER JOIN afp_humdis ON afp_ve.joinkey = afp_humdis.joinkey "
                         "FULL OUTER JOIN afp_additionalexpr ON afp_ve.joinkey = afp_additionalexpr.joinkey "
                         "FULL OUTER JOIN afp_comment ON afp_ve.joinkey = afp_comment.joinkey "
                         "WHERE afp_lasttouched.afp_lasttouched IS NULL AND afp_ve.afp_version = '2' "
                         "AND (afp_email.afp_email LIKE '%{}%' OR two_email.two_email = '{}') "
                         "AND (pap_author_verified.pap_author_verified IS NULL OR "
                         "pap_author_verified.pap_author_verified NOT LIKE 'NO%') "
                         "AND afp_g.afp_genestudied IS NULL AND afp_s.afp_species IS NULL AND "
                         "afp_v.afp_variation IS NULL AND afp_st.afp_strain IS NULL AND "
                         "afp_t.afp_transgene IS NULL AND afp_seq.afp_seqchange IS NULL AND "
                         "afp_ge.afp_geneint IS NULL AND afp_gp.afp_geneprod IS NULL AND "
                         "afp_gr.afp_genereg IS NULL AND afp_nm.afp_newmutant IS NULL AND "
                         "afp_rnai.afp_rnai IS NULL AND afp_ov.afp_overexpr IS NULL AND "
                         "afp_stc.afp_structcorr IS NULL AND afp_antibody.afp_antibody IS NULL AND "
                         "afp_siteaction.afp_siteaction IS NULL AND afp_timeaction.afp_timeaction IS NULL AND "
                         "afp_rnaseq.afp_rnaseq IS NULL AND afp_chemphen.afp_chemphen IS NULL AND "
                         "afp_envpheno.afp_envpheno IS NULL AND afp_catalyticact.afp_catalyticact IS NULL AND "
                         "afp_humdis.afp_humdis IS NULL AND afp_additionalexpr.afp_additionalexpr IS NULL AND "
                         "afp_comment.afp_comment IS NULL "
                         "ORDER BY afp_email.joinkey DESC "
                         "OFFSET {} LIMIT {}".format(author_email, author_email, offset, count))
            res = curs.fetchall()
            return [row[0] for row in res]

    def get_num_papers_waiting_sub_from_auth_token(self, token):
        author_email = self.get_email_from_token(token)
        with self.get_cursor() as curs:
            curs.execute("SELECT count(DISTINCT afp_email.joinkey) FROM afp_email JOIN afp_version afp_ve "
                         "ON afp_email.joinkey = afp_ve.joinkey "
                         "FULL OUTER JOIN pap_author ON afp_email.joinkey = pap_author.joinkey "
                         "FULL OUTER JOIN pap_author_possible ON pap_author.pap_author = pap_author_possible.author_id "
                         "FULL OUTER JOIN pap_author_verified ON pap_author.pap_author = pap_author_verified.author_id "
                         "FULL OUTER JOIN two_email ON pap_author_possible.pap_author_possible = two_email.joinkey "
                         "FULL OUTER JOIN afp_lasttouched ON afp_email.joinkey = afp_lasttouched.joinkey "
                         "FULL OUTER JOIN afp_genestudied afp_g ON afp_ve.joinkey = afp_g.joinkey "
                         "FULL OUTER JOIN afp_species afp_s ON afp_ve.joinkey = afp_s.joinkey "
                         "FULL OUTER JOIN afp_variation afp_v ON afp_ve.joinkey = afp_v.joinkey "
                         "FULL OUTER JOIN afp_strain afp_st ON afp_ve.joinkey = afp_st.joinkey "
                         "FULL OUTER JOIN afp_transgene afp_t ON afp_ve.joinkey = afp_t.joinkey "
                         "FULL OUTER JOIN afp_seqchange afp_seq ON afp_ve.joinkey = afp_seq.joinkey "
                         "FULL OUTER JOIN afp_geneint afp_ge ON afp_ve.joinkey = afp_ge.joinkey "
                         "FULL OUTER JOIN afp_geneprod afp_gp ON afp_ve.joinkey = afp_gp.joinkey "
                         "FULL OUTER JOIN afp_genereg afp_gr ON afp_ve.joinkey = afp_gr.joinkey "
                         "FULL OUTER JOIN afp_newmutant afp_nm ON afp_ve.joinkey = afp_nm.joinkey "
                         "FULL OUTER JOIN afp_rnai afp_rnai ON afp_ve.joinkey = afp_rnai.joinkey "
                         "FULL OUTER JOIN afp_overexpr afp_ov ON afp_ve.joinkey = afp_ov.joinkey "
                         "FULL OUTER JOIN afp_structcorr afp_stc ON afp_ve.joinkey = afp_stc.joinkey "
                         "FULL OUTER JOIN afp_antibody ON afp_ve.joinkey = afp_antibody.joinkey "
                         "FULL OUTER JOIN afp_siteaction ON afp_ve.joinkey = afp_siteaction.joinkey "
                         "FULL OUTER JOIN afp_timeaction ON afp_ve.joinkey = afp_timeaction.joinkey "
                         "FULL OUTER JOIN afp_rnaseq ON afp_ve.joinkey = afp_rnaseq.joinkey "
                         "FULL OUTER JOIN afp_chemphen ON afp_ve.joinkey = afp_chemphen.joinkey "
                         "FULL OUTER JOIN afp_envpheno ON afp_ve.joinkey = afp_envpheno.joinkey "
                         "FULL OUTER JOIN afp_catalyticact ON afp_ve.joinkey = afp_catalyticact.joinkey "
                         "FULL OUTER JOIN afp_humdis ON afp_ve.joinkey = afp_humdis.joinkey "
                         "FULL OUTER JOIN afp_additionalexpr ON afp_ve.joinkey = afp_additionalexpr.joinkey "
                         "FULL OUTER JOIN afp_comment ON afp_ve.joinkey = afp_comment.joinkey "
                         "WHERE afp_lasttouched.afp_lasttouched IS NULL AND afp_ve.afp_version = '2' "
                         "AND (afp_email.afp_email LIKE '%{}%' OR two_email.two_email = '{}') "
                         "AND (pap_author_verified.pap_author_verified IS NULL OR "
                         "pap_author_verified.pap_author_verified NOT LIKE 'NO%') "
                         "AND afp_g.afp_genestudied IS NULL AND afp_s.afp_species IS NULL AND "
                         "afp_v.afp_variation IS NULL AND afp_st.afp_strain IS NULL AND "
                         "afp_t.afp_transgene IS NULL AND afp_seq.afp_seqchange IS NULL AND "
                         "afp_ge.afp_geneint IS NULL AND afp_gp.afp_geneprod IS NULL AND "
                         "afp_gr.afp_genereg IS NULL AND afp_nm.afp_newmutant IS NULL AND "
                         "afp_rnai.afp_rnai IS NULL AND afp_ov.afp_overexpr IS NULL AND "
                         "afp_stc.afp_structcorr IS NULL AND afp_antibody.afp_antibody IS NULL AND "
                         "afp_siteaction.afp_siteaction IS NULL AND afp_timeaction.afp_timeaction IS NULL AND "
                         "afp_rnaseq.afp_rnaseq IS NULL AND afp_chemphen.afp_chemphen IS NULL AND "
                         "afp_envpheno.afp_envpheno IS NULL AND afp_catalyticact.afp_catalyticact IS NULL AND "
                         "afp_humdis.afp_humdis IS NULL AND afp_additionalexpr.afp_additionalexpr IS NULL AND "
                         "afp_comment.afp_comment IS NULL ".format(author_email, author_email))
            res = curs.fetchone()
            return res[0] - self.get_num_papers_partial_from_auth_token(token)

    def get_papers_submitted_from_auth_token(self, token, offset, count):
        author_email = self.get_email_from_token(token)
        with self.get_cursor() as curs:
            curs.execute("SELECT DISTINCT afp_lasttouched.joinkey FROM afp_lasttouched JOIN afp_version ON "
                         "afp_lasttouched.joinkey = afp_version.joinkey "
                         "JOIN afp_email ON afp_lasttouched.joinkey = afp_email.joinkey "
                         "FULL OUTER JOIN pap_author ON afp_email.joinkey = pap_author.joinkey "
                         "FULL OUTER JOIN pap_author_possible ON pap_author.pap_author = pap_author_possible.author_id "
                         "FULL OUTER JOIN pap_author_verified ON pap_author.pap_author = pap_author_verified.author_id "
                         "FULL OUTER JOIN two_email ON pap_author_possible.pap_author_possible = two_email.joinkey "
                         "WHERE (afp_email.afp_email LIKE '%{}%' OR two_email.two_email = '{}') "
                         "AND (pap_author_verified.pap_author_verified IS NULL OR "
                         "pap_author_verified.pap_author_verified NOT LIKE 'NO%') "
                         "AND afp_version.afp_version = '2' "
                         "ORDER BY joinkey DESC OFFSET {} LIMIT {}".format(author_email, author_email, offset,
                                                                           count))
            res = curs.fetchall()
            return [row[0] for row in res if row[0]]

    def get_num_papers_submitted_from_auth_token(self, token):
        author_email = self.get_email_from_token(token)
        with self.get_cursor() as curs:
            curs.execute("SELECT COUNT(DISTINCT afp_lasttouched.joinkey) FROM afp_lasttouched JOIN afp_version ON "
                         "afp_lasttouched.joinkey = afp_version.joinkey "
                         "JOIN afp_email ON afp_lasttouched.joinkey = afp_email.joinkey "
                         "FULL OUTER JOIN pap_author ON afp_email.joinkey = pap_author.joinkey "
                         "FULL OUTER JOIN pap_author_possible ON pap_author.pap_author = pap_author_possible.author_id "
                         "FULL OUTER JOIN pap_author_verified ON pap_author.pap_author = pap_author_verified.author_id "
                         "FULL OUTER JOIN two_email ON pap_author_possible.pap_author_possible = two_email.joinkey "
                         "WHERE (afp_email.afp_email LIKE '%{}%' OR two_email.two_email = '{}') "
                         "AND (pap_author_verified.pap_author_verified IS NULL OR "
                         "pap_author_verified.pap_author_verified NOT LIKE 'NO%') "
                         "AND afp_version.afp_version = '2'".format(author_email, author_email))
            res = curs.fetchone()
            return res[0]

    def get_num_papers_partial_from_auth_token(self, token):
        author_email = self.get_email_from_token(token)
        with self.get_cursor() as curs:
            curs.execute("SELECT count(DISTINCT afp_ve.joinkey) FROM afp_version afp_ve "
                         "FULL OUTER JOIN afp_lasttouched afp_l ON afp_ve.joinkey = afp_l.joinkey "
                         "FULL OUTER JOIN afp_genestudied afp_g ON afp_ve.joinkey = afp_g.joinkey "
                         "FULL OUTER JOIN afp_species afp_s ON afp_ve.joinkey = afp_s.joinkey "
                         "FULL OUTER JOIN afp_variation afp_v ON afp_ve.joinkey = afp_v.joinkey "
                         "FULL OUTER JOIN afp_strain afp_st ON afp_ve.joinkey = afp_st.joinkey "
                         "FULL OUTER JOIN afp_transgene afp_t ON afp_ve.joinkey = afp_t.joinkey "
                         "FULL OUTER JOIN afp_seqchange afp_seq ON afp_ve.joinkey = afp_seq.joinkey "
                         "FULL OUTER JOIN afp_geneint afp_ge ON afp_ve.joinkey = afp_ge.joinkey "
                         "FULL OUTER JOIN afp_geneprod afp_gp ON afp_ve.joinkey = afp_gp.joinkey "
                         "FULL OUTER JOIN afp_genereg afp_gr ON afp_ve.joinkey = afp_gr.joinkey "
                         "FULL OUTER JOIN afp_newmutant afp_nm ON afp_ve.joinkey = afp_nm.joinkey "
                         "FULL OUTER JOIN afp_rnai afp_rnai ON afp_ve.joinkey = afp_rnai.joinkey "
                         "FULL OUTER JOIN afp_overexpr afp_ov ON afp_ve.joinkey = afp_ov.joinkey "
                         "FULL OUTER JOIN afp_structcorr afp_stc ON afp_ve.joinkey = afp_stc.joinkey "
                         "FULL OUTER JOIN afp_antibody ON afp_ve.joinkey = afp_antibody.joinkey "
                         "FULL OUTER JOIN afp_siteaction ON afp_ve.joinkey = afp_siteaction.joinkey "
                         "FULL OUTER JOIN afp_timeaction ON afp_ve.joinkey = afp_timeaction.joinkey "
                         "FULL OUTER JOIN afp_rnaseq ON afp_ve.joinkey = afp_rnaseq.joinkey "
                         "FULL OUTER JOIN afp_chemphen ON afp_ve.joinkey = afp_chemphen.joinkey "
                         "FULL OUTER JOIN afp_envpheno ON afp_ve.joinkey = afp_envpheno.joinkey "
                         "FULL OUTER JOIN afp_catalyticact ON afp_ve.joinkey = afp_catalyticact.joinkey "
                         "FULL OUTER JOIN afp_comment ON afp_ve.joinkey = afp_comment.joinkey "
                         "JOIN afp_email ON afp_ve.joinkey = afp_email.joinkey "
                         "FULL OUTER JOIN pap_author ON afp_email.joinkey = pap_author.joinkey "
                         "FULL OUTER JOIN pap_author_possible ON pap_author.pap_author = pap_author_possible.author_id "
                         "FULL OUTER JOIN pap_author_verified ON pap_author.pap_author = pap_author_verified.author_id "
                         "FULL OUTER JOIN two_email ON pap_author_possible.pap_author_possible = two_email.joinkey "
                         "WHERE (afp_email LIKE '%{}%' OR two_email.two_email = '{}') AND afp_ve.afp_version = '2' "
                         "AND afp_l.afp_lasttouched is NULL "
                         "AND (pap_author_verified.pap_author_verified IS NULL OR "
                         "pap_author_verified.pap_author_verified NOT LIKE 'NO%') "
                         "AND (afp_g.afp_genestudied IS NOT NULL OR afp_s.afp_species IS NOT NULL OR "
                         "afp_v.afp_variation IS NOT NULL OR afp_st.afp_strain IS NOT NULL OR "
                         "afp_t.afp_transgene IS NOT NULL OR afp_seq.afp_seqchange IS NOT NULL OR "
                         "afp_ge.afp_geneint IS NOT NULL OR afp_gp.afp_geneprod IS NOT NULL OR "
                         "afp_gr.afp_genereg IS NOT NULL OR afp_nm.afp_newmutant IS NOT NULL OR "
                         "afp_rnai.afp_rnai IS NOT NULL OR afp_ov.afp_overexpr IS NOT NULL OR "
                         "afp_stc.afp_structcorr IS NOT NULL OR afp_antibody.afp_antibody IS NOT NULL OR "
                         "afp_siteaction.afp_siteaction IS NOT NULL OR afp_timeaction.afp_timeaction IS NOT NULL OR "
                         "afp_rnaseq.afp_rnaseq IS NOT NULL OR afp_chemphen.afp_chemphen IS NOT NULL OR "
                         "afp_envpheno.afp_envpheno IS NOT NULL OR afp_catalyticact.afp_catalyticact IS NOT NULL OR "
                         "afp_comment.afp_comment IS NOT NULL) ".format(author_email, author_email))
            res = curs.fetchone()
            if res:
                return int(res[0])
            else:
                return 0

    def get_papers_partial_from_auth_token(self, token, offset, count):
        author_email = self.get_email_from_token(token)
        with self.get_cursor() as curs:
            curs.execute("SELECT DISTINCT afp_ve.joinkey FROM afp_version afp_ve "
                         "FULL OUTER JOIN afp_lasttouched afp_l ON afp_ve.joinkey = afp_l.joinkey "
                         "FULL OUTER JOIN afp_genestudied afp_g ON afp_ve.joinkey = afp_g.joinkey "
                         "FULL OUTER JOIN afp_species afp_s ON afp_ve.joinkey = afp_s.joinkey "
                         "FULL OUTER JOIN afp_variation afp_v ON afp_ve.joinkey = afp_v.joinkey "
                         "FULL OUTER JOIN afp_strain afp_st ON afp_ve.joinkey = afp_st.joinkey "
                         "FULL OUTER JOIN afp_transgene afp_t ON afp_ve.joinkey = afp_t.joinkey "
                         "FULL OUTER JOIN afp_seqchange afp_seq ON afp_ve.joinkey = afp_seq.joinkey "
                         "FULL OUTER JOIN afp_geneint afp_ge ON afp_ve.joinkey = afp_ge.joinkey "
                         "FULL OUTER JOIN afp_geneprod afp_gp ON afp_ve.joinkey = afp_gp.joinkey "
                         "FULL OUTER JOIN afp_genereg afp_gr ON afp_ve.joinkey = afp_gr.joinkey "
                         "FULL OUTER JOIN afp_newmutant afp_nm ON afp_ve.joinkey = afp_nm.joinkey "
                         "FULL OUTER JOIN afp_rnai afp_rnai ON afp_ve.joinkey = afp_rnai.joinkey "
                         "FULL OUTER JOIN afp_overexpr afp_ov ON afp_ve.joinkey = afp_ov.joinkey "
                         "FULL OUTER JOIN afp_structcorr afp_stc ON afp_ve.joinkey = afp_stc.joinkey "
                         "FULL OUTER JOIN afp_antibody ON afp_ve.joinkey = afp_antibody.joinkey "
                         "FULL OUTER JOIN afp_siteaction ON afp_ve.joinkey = afp_siteaction.joinkey "
                         "FULL OUTER JOIN afp_timeaction ON afp_ve.joinkey = afp_timeaction.joinkey "
                         "FULL OUTER JOIN afp_rnaseq ON afp_ve.joinkey = afp_rnaseq.joinkey "
                         "FULL OUTER JOIN afp_chemphen ON afp_ve.joinkey = afp_chemphen.joinkey "
                         "FULL OUTER JOIN afp_envpheno ON afp_ve.joinkey = afp_envpheno.joinkey "
                         "FULL OUTER JOIN afp_catalyticact ON afp_ve.joinkey = afp_catalyticact.joinkey "
                         "FULL OUTER JOIN afp_comment ON afp_ve.joinkey = afp_comment.joinkey "
                         "JOIN afp_email ON afp_ve.joinkey = afp_email.joinkey "
                         "FULL OUTER JOIN pap_author ON afp_email.joinkey = pap_author.joinkey "
                         "FULL OUTER JOIN pap_author_possible ON pap_author.pap_author = pap_author_possible.author_id "
                         "FULL OUTER JOIN pap_author_verified ON pap_author.pap_author = pap_author_verified.author_id "
                         "FULL OUTER JOIN two_email ON pap_author_possible.pap_author_possible = two_email.joinkey "
                         "WHERE (afp_email LIKE '%{}%' OR two_email.two_email = '{}') AND afp_ve.afp_version = '2' "
                         "AND afp_l.afp_lasttouched is NULL "
                         "AND (pap_author_verified.pap_author_verified IS NULL OR "
                         "pap_author_verified.pap_author_verified NOT LIKE 'NO%') "
                         "AND (afp_g.afp_genestudied IS NOT NULL OR afp_s.afp_species IS NOT NULL OR "
                         "afp_v.afp_variation IS NOT NULL OR afp_st.afp_strain IS NOT NULL OR "
                         "afp_t.afp_transgene IS NOT NULL OR afp_seq.afp_seqchange IS NOT NULL OR "
                         "afp_ge.afp_geneint IS NOT NULL OR afp_gp.afp_geneprod IS NOT NULL OR "
                         "afp_gr.afp_genereg IS NOT NULL OR afp_nm.afp_newmutant IS NOT NULL OR "
                         "afp_rnai.afp_rnai IS NOT NULL OR afp_ov.afp_overexpr IS NOT NULL OR "
                         "afp_stc.afp_structcorr IS NOT NULL OR afp_antibody.afp_antibody IS NOT NULL OR "
                         "afp_siteaction.afp_siteaction IS NOT NULL OR afp_timeaction.afp_timeaction IS NOT NULL OR "
                         "afp_rnaseq.afp_rnaseq IS NOT NULL OR afp_chemphen.afp_chemphen IS NOT NULL OR "
                         "afp_envpheno.afp_envpheno IS NOT NULL OR afp_catalyticact.afp_catalyticact IS NOT NULL OR "
                         "afp_comment.afp_comment IS NOT NULL) "
                         "ORDER BY afp_ve.joinkey DESC OFFSET {} LIMIT {}".format(author_email, author_email,
                                                                                  offset, count))
            res = curs.fetchall()
            return [row[0] for row in res]

    def get_papers_emails_no_submission_emailed_between(self, after_month, before_month):
        with self.get_cursor() as curs:
            curs.execute("SELECT afp_email.joinkey, afp_email.afp_email from afp_email JOIN afp_version "
                         "ON afp_email.joinkey = afp_version.joinkey FULL OUTER JOIN afp_lasttouched "
                         "ON afp_email.joinkey = afp_lasttouched.joinkey "
                         "WHERE afp_email.afp_timestamp < now() - interval '{} month' "
                         "AND afp_email.afp_timestamp > now() - interval '{} months' "
                         "AND afp_version.afp_version = '2' "
                         "AND afp_lasttouched.afp_lasttouched IS NULL AND afp_email.joinkey NOT IN "
                         "(SELECT joinkey from pap_status WHERE pap_status = 'invalid')".format(after_month, before_month))
            rows = curs.fetchall()
            return [(row[0], row[1]) for row in rows]

    def get_positive_paper_ids_sumbitted_last_month_for_data_type(self, data_type_table_name, tazendra_user,
                                                                  tazendra_password):
        curated_ids = get_curated_papers(data_type_table_name[4:], tazendra_user, tazendra_password)
        with self.get_cursor() as curs:
            curs.execute("SELECT {}.joinkey, {}.{} from {} join afp_lasttouched "
                         "ON {}.joinkey = afp_lasttouched.joinkey JOIN afp_version "
                         "ON afp_lasttouched.joinkey = afp_version.joinkey "
                         "WHERE afp_version.afp_version = '2' AND {}.afp_timestamp > now() - interval '1 week' AND "
                         "{}.{} IS NOT NULL".format(
            data_type_table_name, data_type_table_name, data_type_table_name, data_type_table_name,
            data_type_table_name, data_type_table_name, data_type_table_name, data_type_table_name))
            rows = curs.fetchall()
            return {row[0]: row[1] if row[1] != "Checked" else "" for row in rows if row[0] not in curated_ids and
                    row[1] != "" and
                    row[1] != "[{\"id\":1,\"name\":\"\"}]" and
                    row[1] != "[{\"id\":1,\"name\":\"\",\"publicationId\":\"\"}]"}

    @staticmethod
    def extract_afp_entites_from_stored_value(stored_value, prefix):
        return [(name_mod_id.split(";%;")[1], prefix + name_mod_id.split(";%;")[0]) if ";%;" in name_mod_id
                else (name_mod_id, "") for name_mod_id in stored_value.split(" | ") if stored_value]

    def get_author_modified_entities_with_counter(self, entity_type: EntityType, added: bool = True, limit: int = None,
                                                  offset: int = None) -> Tuple[List[Tuple[int, str, str]], int]:
        """
        return the list of entities most added or removed by authors wrt the entities extracted by AFP pipeline, sorted
        by the number of times they have been added or removed in all the papers processed so far.

        Args:
            entity_type (EntityType): the type of entity to consider
            added (bool): whether to count added entities (or removed ones if False)
            limit (int): return up to the specified number of entities
            offset (int): start from a certain position in the list, used for pagination

        Returns:
            Tuple[List[Tuple[int, str, str]], int]: a tuple containing the list of entities sorted by counter and the
            length of the complete list. Each element in the list contains a tuple with the counter, the entity name
            and the entity id.

        """
        table_name = "genestudied"
        prefix = "WBGene"
        if entity_type == EntityType.SPECIES:
            table_name = "species"
            prefix = ""
        elif entity_type == EntityType.STRAIN:
            table_name = "strain"
            prefix = "WBStrain"
        elif entity_type == EntityType.VARIATION:
            table_name = "variation"
            prefix = ""
        elif entity_type == EntityType.TRANSGENE:
            table_name = "transgene"
            prefix = ""
        with self.get_cursor() as curs:
            curs.execute("SELECT afp_version.joinkey, tfp_{} FROM tfp_{} join afp_version "
                         "on tfp_{}.joinkey = afp_version.joinkey "
                         "WHERE afp_version.afp_version = '2'".format(table_name, table_name, table_name))
            extracted_pap_str = {res[0]: res[1] for res in curs.fetchall()}
            curs.execute("SELECT afp_version.joinkey, afp_{} FROM afp_{} join afp_version "
                         "on afp_{}.joinkey = afp_version.joinkey "
                         "WHERE afp_version.afp_version = '2'".format(table_name, table_name, table_name))
            submitted_pap_str = {res[0]: res[1] for res in curs.fetchall()}
        entity_diff_counter = defaultdict(int)
        for pap_id, value in extracted_pap_str.items():
            if pap_id in submitted_pap_str:
                extracted_entities = self.extract_afp_entites_from_stored_value(value, prefix)
                submitted_entities = self.extract_afp_entites_from_stored_value(submitted_pap_str[pap_id], prefix)
                if added:
                    diff = set([name + ";%;" + mod_id for name, mod_id in submitted_entities]) - set(
                        [name + ";%;" + mod_id for name, mod_id in extracted_entities])
                else:
                    diff = set([name + ";%;" + mod_id for name, mod_id in extracted_entities]) - set(
                        [name + ";%;" + mod_id for name, mod_id in submitted_entities])
                for name_mod_id in diff:
                    name, mod_id = name_mod_id.split(";%;")
                    entity_diff_counter[(name, mod_id)] += 1
        sorted_list = sorted([(counter, name, mod_id) for (name, mod_id), counter in entity_diff_counter.items()],
                             key=lambda x: x[0], reverse=True)
        if offset is None:
            offset = 0
        if limit is None:
            limit = len(sorted_list) - offset
        return sorted_list[offset: offset + limit], len(sorted_list)

