import time
from datetime import datetime
from typing import List

import psycopg2

from wbtools.db.abstract_manager import AbstractWBDBManager
from wbtools.db.generic import WBGenericDBManager
from wbtools.db.paper import WBPaperDBManager

QUERY_PAPER_IDS_NO_SUBMISSION = "SELECT afp_email.joinkey AS k FROM afp_email JOIN afp_version afp_ve ON afp_email.joinkey = afp_ve.joinkey FULL OUTER JOIN afp_genestudied afp_g ON afp_ve.joinkey = afp_g.joinkey FULL OUTER JOIN afp_species afp_s ON afp_ve.joinkey = afp_s.joinkey FULL OUTER JOIN afp_variation afp_v ON afp_ve.joinkey = afp_v.joinkey FULL OUTER JOIN afp_strain afp_st ON afp_ve.joinkey = afp_st.joinkey FULL OUTER JOIN afp_transgene afp_t ON afp_ve.joinkey = afp_t.joinkey FULL OUTER JOIN afp_seqchange afp_seq ON afp_ve.joinkey = afp_seq.joinkey FULL OUTER JOIN afp_geneint afp_ge ON afp_ve.joinkey = afp_ge.joinkey FULL OUTER JOIN afp_geneprod afp_gp ON afp_ve.joinkey = afp_gp.joinkey FULL OUTER JOIN afp_genereg afp_gr ON afp_ve.joinkey = afp_gr.joinkey FULL OUTER JOIN afp_newmutant afp_nm ON afp_ve.joinkey = afp_nm.joinkey FULL OUTER JOIN afp_rnai afp_rnai ON afp_ve.joinkey = afp_rnai.joinkey FULL OUTER JOIN afp_overexpr afp_ov ON afp_ve.joinkey = afp_ov.joinkey FULL OUTER JOIN afp_structcorr afp_stc ON afp_ve.joinkey = afp_stc.joinkey FULL OUTER JOIN afp_antibody ON afp_ve.joinkey = afp_antibody.joinkey FULL OUTER JOIN afp_siteaction ON afp_ve.joinkey = afp_siteaction.joinkey FULL OUTER JOIN afp_timeaction ON afp_ve.joinkey = afp_timeaction.joinkey FULL OUTER JOIN afp_rnaseq ON afp_ve.joinkey = afp_rnaseq.joinkey FULL OUTER JOIN afp_chemphen ON afp_ve.joinkey = afp_chemphen.joinkey FULL OUTER JOIN afp_envpheno ON afp_ve.joinkey = afp_envpheno.joinkey FULL OUTER JOIN afp_catalyticact ON afp_ve.joinkey = afp_catalyticact.joinkey FULL OUTER JOIN afp_humdis ON afp_ve.joinkey = afp_humdis.joinkey FULL OUTER JOIN afp_additionalexpr ON afp_ve.joinkey = afp_additionalexpr.joinkey FULL OUTER JOIN afp_comment ON afp_ve.joinkey = afp_comment.joinkey WHERE afp_ve.afp_version = '2' AND afp_g.afp_genestudied IS NULL AND afp_s.afp_species IS NULL AND afp_v.afp_variation IS NULL AND afp_st.afp_strain IS NULL AND afp_t.afp_transgene IS NULL AND afp_seq.afp_seqchange IS NULL AND afp_ge.afp_geneint IS NULL AND afp_gp.afp_geneprod IS NULL AND afp_gr.afp_genereg IS NULL AND afp_nm.afp_newmutant IS NULL AND afp_rnai.afp_rnai IS NULL AND afp_ov.afp_overexpr IS NULL AND afp_stc.afp_structcorr IS NULL AND afp_antibody.afp_antibody IS NULL AND afp_siteaction.afp_siteaction IS NULL AND afp_timeaction.afp_timeaction IS NULL AND afp_rnaseq.afp_rnaseq IS NULL AND afp_chemphen.afp_chemphen IS NULL AND afp_envpheno.afp_envpheno IS NULL AND afp_catalyticact.afp_catalyticact IS NULL AND afp_humdis.afp_humdis IS NULL AND afp_additionalexpr.afp_additionalexpr IS NULL AND afp_comment.afp_comment IS NULL "
QUERY_PAPER_IDS_FULL_SUBMISSION = "SELECT afp_email.joinkey FROM afp_email JOIN afp_lasttouched ON afp_email.joinkey = afp_lasttouched.joinkey JOIN afp_version ON afp_lasttouched.joinkey = afp_version.joinkey WHERE afp_version.afp_version = '2' "
QUERY_PAPER_IDS_PARTIAL_SUBMISSION = "SELECT afp_e.joinkey FROM afp_email afp_e JOIN afp_version afp_ve ON afp_e.joinkey = afp_ve.joinkey FULL OUTER JOIN afp_lasttouched afp_l ON afp_ve.joinkey = afp_l.joinkey FULL OUTER JOIN afp_genestudied afp_g ON afp_ve.joinkey = afp_g.joinkey FULL OUTER JOIN afp_species afp_s ON afp_ve.joinkey = afp_s.joinkey FULL OUTER JOIN afp_variation afp_v ON afp_ve.joinkey = afp_v.joinkey FULL OUTER JOIN afp_strain afp_st ON afp_ve.joinkey = afp_st.joinkey FULL OUTER JOIN afp_transgene afp_t ON afp_ve.joinkey = afp_t.joinkey FULL OUTER JOIN afp_seqchange afp_seq ON afp_ve.joinkey = afp_seq.joinkey FULL OUTER JOIN afp_geneint afp_ge ON afp_ve.joinkey = afp_ge.joinkey FULL OUTER JOIN afp_geneprod afp_gp ON afp_ve.joinkey = afp_gp.joinkey FULL OUTER JOIN afp_genereg afp_gr ON afp_ve.joinkey = afp_gr.joinkey FULL OUTER JOIN afp_newmutant afp_nm ON afp_ve.joinkey = afp_nm.joinkey FULL OUTER JOIN afp_rnai afp_rnai ON afp_ve.joinkey = afp_rnai.joinkey FULL OUTER JOIN afp_overexpr afp_ov ON afp_ve.joinkey = afp_ov.joinkey FULL OUTER JOIN afp_structcorr afp_stc ON afp_ve.joinkey = afp_stc.joinkey FULL OUTER JOIN afp_antibody ON afp_ve.joinkey = afp_antibody.joinkey FULL OUTER JOIN afp_siteaction ON afp_ve.joinkey = afp_siteaction.joinkey FULL OUTER JOIN afp_timeaction ON afp_ve.joinkey = afp_timeaction.joinkey FULL OUTER JOIN afp_rnaseq ON afp_ve.joinkey = afp_rnaseq.joinkey FULL OUTER JOIN afp_chemphen ON afp_ve.joinkey = afp_chemphen.joinkey FULL OUTER JOIN afp_envpheno ON afp_ve.joinkey = afp_envpheno.joinkey FULL OUTER JOIN afp_catalyticact ON afp_ve.joinkey = afp_catalyticact.joinkey FULL OUTER JOIN afp_humdis ON afp_ve.joinkey = afp_humdis.joinkey FULL OUTER JOIN afp_additionalexpr ON afp_ve.joinkey = afp_additionalexpr.joinkey FULL OUTER JOIN afp_comment ON afp_ve.joinkey = afp_comment.joinkey WHERE afp_ve.afp_version = '2' AND afp_l.afp_lasttouched is NULL AND (afp_g.afp_genestudied IS NOT NULL OR afp_s.afp_species IS NOT NULL OR afp_v.afp_variation IS NOT NULL OR afp_st.afp_strain IS NOT NULL OR afp_t.afp_transgene IS NOT NULL OR afp_seq.afp_seqchange IS NOT NULL OR afp_ge.afp_geneint IS NOT NULL OR afp_gp.afp_geneprod IS NOT NULL OR afp_gr.afp_genereg IS NOT NULL OR afp_nm.afp_newmutant IS NOT NULL OR afp_rnai.afp_rnai IS NOT NULL OR afp_ov.afp_overexpr IS NOT NULL OR afp_stc.afp_structcorr IS NOT NULL OR afp_antibody.afp_antibody IS NOT NULL OR afp_siteaction.afp_siteaction IS NOT NULL OR afp_timeaction.afp_timeaction IS NOT NULL OR afp_rnaseq.afp_rnaseq IS NOT NULL OR afp_chemphen.afp_chemphen IS NOT NULL OR afp_envpheno.afp_envpheno IS NOT NULL OR afp_catalyticact.afp_catalyticact IS NOT NULL OR afp_humdis.afp_humdis IS NOT NULL OR afp_additionalexpr.afp_additionalexpr IS NOT NULL OR afp_comment.afp_comment IS NOT NULL)"


AFP_ENTITIES_SEPARATOR = " | "
AFP_IDS_SEPARATOR = ";%;"
PAP_AFP_EVIDENCE_CODE = "Inferred_automatically \"from author first pass afp_genestudied\""


class WBAFPDBManager(AbstractWBDBManager):

    def __init__(self, dbname, user, password, host):
        super().__init__(dbname, user, password, host)
        self.generic_db_manager = WBGenericDBManager(dbname=dbname, user=user, password=password, host=host)

    def get_paper_ids_afp_no_submission(self, must_be_autclass_positive_data_types: List[str] = None,
                                        must_be_positive_manual_flag_data_types: List[str] = None,
                                        must_be_curation_negative_data_types: List[str] = None,
                                        combine_filters: str = 'OR', offset: int = None, limit: int = None,
                                        count: bool = False):
        return self.generic_db_manager.get_paper_ids(
            query=QUERY_PAPER_IDS_NO_SUBMISSION,
            must_be_autclass_positive_data_types=must_be_autclass_positive_data_types,
            must_be_positive_manual_flag_data_types=must_be_positive_manual_flag_data_types,
            must_be_curation_negative_data_types=must_be_curation_negative_data_types,
            combine_filters=combine_filters, count=count, offset=offset, limit=limit)

    def get_paper_ids_afp_full_submission(self, must_be_autclass_positive_data_types: List[str] = None,
                                          must_be_positive_manual_flag_data_types: List[str] = None,
                                          must_be_curation_negative_data_types: List[str] = None,
                                          combine_filters: str = 'OR', offset: int = None, limit: int = None,
                                          count: bool = False):
        return self.generic_db_manager.get_paper_ids(
            query=QUERY_PAPER_IDS_FULL_SUBMISSION,
            must_be_autclass_positive_data_types=must_be_autclass_positive_data_types,
            must_be_positive_manual_flag_data_types=must_be_positive_manual_flag_data_types,
            must_be_curation_negative_data_types=must_be_curation_negative_data_types,
            combine_filters=combine_filters, count=count, offset=offset, limit=limit)

    def get_paper_ids_afp_partial_submission(self, must_be_autclass_positive_data_types: List[str] = None,
                                             must_be_positive_manual_flag_data_types: List[str] = None,
                                             must_be_curation_negative_data_types: List[str] = None,
                                             combine_filters: str = 'OR', offset: int = None, limit: int = None,
                                             count: bool = False):
        return self.generic_db_manager.get_paper_ids(
            query=QUERY_PAPER_IDS_PARTIAL_SUBMISSION,
            must_be_autclass_positive_data_types=must_be_autclass_positive_data_types,
            must_be_positive_manual_flag_data_types=must_be_positive_manual_flag_data_types,
            must_be_curation_negative_data_types=must_be_curation_negative_data_types,
            combine_filters=combine_filters, count=count, offset=offset, limit=limit)

    def get_afp_curatable_paper_ids(self):
        """
        get the list of curatable papers (i.e., papers that can be processed by AFP - type must be 'primary' or pap_type
        equal to 1).

        Returns:
            List[str]: the set of curatable papers
        """
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
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
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT * FROM afp_passwd")
            rows = curs.fetchall()
            return [row[0] for row in rows]

    def get_passwd(self, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT * FROM afp_passwd WHERE joinkey = '{}'".format(paper_id))
            res = curs.fetchone()
            if res:
                return res[1]
            else:
                return None

    def set_extracted_entities_in_paper(self, publication_id, entities_ids: List[str], table_name):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM {} WHERE joinkey = '{}'".format(table_name, publication_id))
            curs.execute("INSERT INTO {} (joinkey, {}) VALUES('{}', '{}')".format(
                table_name, table_name, publication_id, AFP_ENTITIES_SEPARATOR.join(entities_ids)))

    def set_antibody(self, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM tfp_antibody WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO tfp_antibody (joinkey, tfp_antibody) VALUES('{}', 'checked')".format(paper_id))

    def set_passwd(self, publication_id, passwd):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_passwd WHERE joinkey = '{}'".format(publication_id))
            curs.execute(
                "INSERT INTO afp_passwd (joinkey, afp_passwd) VALUES('{}', '{}')".format(publication_id, passwd))
            curs.execute(
                "INSERT INTO afp_passwd_hst (joinkey, afp_passwd_hst) VALUES('{}', '{}')".format(publication_id, passwd))

    def set_email(self, publication_id, email_addr_list: List[str]):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_email WHERE joinkey = '{}'".format(publication_id))
            for email_addr in email_addr_list:
                curs.execute(
                    "INSERT INTO afp_email (joinkey, afp_email) VALUES('{}', '{}')".format(publication_id, email_addr))
                curs.execute(
                    "INSERT INTO afp_email_hst (joinkey, afp_email_hst) VALUES('{}', '{}')".format(publication_id,
                                                                                                   email_addr))

    def set_gene_list(self, genes, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_genestudied WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_genestudied (joinkey, afp_genestudied) VALUES('{}', '{}')"
                         .format(paper_id, genes))
            curs.execute("INSERT INTO afp_genestudied_hst (joinkey, afp_genestudied_hst) VALUES('{}', '{}')"
                         .format(paper_id, genes))

    def set_gene_model_update(self, gene_model_update, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_structcorr WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_structcorr (joinkey, afp_structcorr) VALUES('{}', '{}')"
                         .format(paper_id, gene_model_update))
            curs.execute("INSERT INTO afp_structcorr_hst (joinkey, afp_structcorr_hst) VALUES('{}', '{}')"
                         .format(paper_id, gene_model_update))

    def set_species_list(self, species, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_species WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_species (joinkey, afp_species) VALUES('{}', '{}')"
                         .format(paper_id, species))
            curs.execute("INSERT INTO afp_species_hst (joinkey, afp_species_hst) VALUES('{}', '{}')"
                         .format(paper_id, species))

    def set_alleles_list(self, alleles, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_variation WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_variation (joinkey, afp_variation) VALUES('{}', '{}')"
                         .format(paper_id, alleles))
            curs.execute("INSERT INTO afp_variation_hst (joinkey, afp_variation_hst) VALUES('{}', '{}')"
                         .format(paper_id, alleles))

    def set_allele_seq_change(self, allele_seq_change, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_seqchange WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_seqchange (joinkey, afp_seqchange) VALUES('{}', '{}')"
                         .format(paper_id, allele_seq_change))
            curs.execute("INSERT INTO afp_seqchange_hst (joinkey, afp_seqchange_hst) VALUES('{}', '{}')"
                         .format(paper_id, allele_seq_change))

    def set_other_alleles(self, other_alleles, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_othervariation WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_othervariation (joinkey, afp_othervariation) VALUES('{}', '{}')"
                         .format(paper_id, other_alleles))
            curs.execute("INSERT INTO afp_othervariation_hst (joinkey, afp_othervariation_hst) VALUES('{}', '{}')"
                         .format(paper_id, other_alleles))

    def set_strains_list(self, strains, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_strain WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_strain (joinkey, afp_strain) VALUES('{}', '{}')"
                         .format(paper_id, strains))
            curs.execute("INSERT INTO afp_strain_hst (joinkey, afp_strain_hst) VALUES('{}', '{}')"
                         .format(paper_id, strains))

    def set_other_strains(self, other_strains, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_otherstrain WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_otherstrain (joinkey, afp_otherstrain) VALUES('{}', '{}')"
                         .format(paper_id, other_strains))
            curs.execute("INSERT INTO afp_otherstrain_hst (joinkey, afp_otherstrain_hst) VALUES('{}', '{}')"
                         .format(paper_id, other_strains))

    def set_transgenes_list(self, transgenes, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_transgene WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_transgene (joinkey, afp_transgene) VALUES('{}', '{}')"
                         .format(paper_id, transgenes))
            curs.execute("INSERT INTO afp_transgene_hst (joinkey, afp_transgene_hst) VALUES('{}', '{}')"
                         .format(paper_id, transgenes))

    def set_new_transgenes(self, new_transgenes, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_othertransgene WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_othertransgene (joinkey, afp_othertransgene) VALUES('{}', '{}')"
                         .format(paper_id, new_transgenes))
            curs.execute("INSERT INTO afp_othertransgene_hst (joinkey, afp_othertransgene_hst) VALUES('{}', '{}')"
                         .format(paper_id, new_transgenes))

    def set_new_antibody(self, new_antibody, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_antibody WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_antibody (joinkey, afp_antibody) VALUES('{}', '{}')"
                         .format(paper_id, new_antibody))
            curs.execute("INSERT INTO afp_antibody_hst (joinkey, afp_antibody_hst) VALUES('{}', '{}')"
                         .format(paper_id, new_antibody))

    def set_other_antibodies(self, other_antibodies, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_otherantibody WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_otherantibody (joinkey, afp_otherantibody) VALUES('{}', '{}')"
                         .format(paper_id, other_antibodies))
            curs.execute("INSERT INTO afp_otherantibody_hst (joinkey, afp_otherantibody_hst) VALUES('{}', '{}')"
                         .format(paper_id, other_antibodies))

    def set_anatomic_expr(self, anatomic_expr, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_otherexpr WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_otherexpr (joinkey, afp_otherexpr) VALUES('{}', '{}')"
                         .format(paper_id, anatomic_expr))
            curs.execute("INSERT INTO afp_otherexpr_hst (joinkey, afp_otherexpr_hst) VALUES('{}', '{}')"
                         .format(paper_id, anatomic_expr))

    def set_site_action(self, site_action, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_siteaction WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_siteaction (joinkey, afp_siteaction) VALUES('{}', '{}')"
                         .format(paper_id, site_action))
            curs.execute("INSERT INTO afp_siteaction_hst (joinkey, afp_siteaction_hst) VALUES('{}', '{}')"
                         .format(paper_id, site_action))

    def set_time_action(self, time_action, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_timeaction WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_timeaction (joinkey, afp_timeaction) VALUES('{}', '{}')"
                         .format(paper_id, time_action))
            curs.execute("INSERT INTO afp_timeaction_hst (joinkey, afp_timeaction_hst) VALUES('{}', '{}')"
                         .format(paper_id, time_action))

    def set_rnaseq(self, rnaseq, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_rnaseq WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_rnaseq (joinkey, afp_rnaseq) VALUES('{}', '{}')"
                         .format(paper_id, rnaseq))
            curs.execute("INSERT INTO afp_rnaseq_hst (joinkey, afp_rnaseq_hst) VALUES('{}', '{}')"
                         .format(paper_id, rnaseq))

    def set_additional_expr(self, additional_expr, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_additionalexpr WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_additionalexpr (joinkey, afp_additionalexpr) VALUES('{}', '{}')"
                         .format(paper_id, additional_expr))
            curs.execute("INSERT INTO afp_additionalexpr_hst (joinkey, afp_additionalexpr_hst) VALUES('{}', '{}')"
                         .format(paper_id, additional_expr))

    def set_gene_int(self, gene_int, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_geneint WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_geneint (joinkey, afp_geneint) VALUES('{}', '{}')"
                         .format(paper_id, gene_int))
            curs.execute("INSERT INTO afp_geneint_hst (joinkey, afp_geneint_hst) VALUES('{}', '{}')"
                         .format(paper_id, gene_int))

    def set_phys_int(self, phys_int, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_geneprod WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_geneprod (joinkey, afp_geneprod) VALUES('{}', '{}')"
                         .format(paper_id, phys_int))
            curs.execute("INSERT INTO afp_geneprod_hst (joinkey, afp_geneprod_hst) VALUES('{}', '{}')"
                         .format(paper_id, phys_int))

    def set_gene_reg(self, gene_reg, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_genereg WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_genereg (joinkey, afp_genereg) VALUES('{}', '{}')"
                         .format(paper_id, gene_reg))
            curs.execute("INSERT INTO afp_genereg_hst (joinkey, afp_genereg_hst) VALUES('{}', '{}')"
                         .format(paper_id, gene_reg))

    def set_allele_pheno(self, allele_pheno, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_newmutant WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_newmutant (joinkey, afp_newmutant) VALUES('{}', '{}')"
                         .format(paper_id, allele_pheno))
            curs.execute("INSERT INTO afp_newmutant_hst (joinkey, afp_newmutant_hst) VALUES('{}', '{}')"
                         .format(paper_id, allele_pheno))

    def set_rnai_pheno(self, rnai_pheno, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_rnai WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_rnai (joinkey, afp_rnai) VALUES('{}', '{}')"
                         .format(paper_id, rnai_pheno))
            curs.execute("INSERT INTO afp_rnai_hst (joinkey, afp_rnai_hst) VALUES('{}', '{}')"
                         .format(paper_id, rnai_pheno))

    def set_transover_pheno(self, transover_pheno, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_overexpr WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_overexpr (joinkey, afp_overexpr) VALUES('{}', '{}')"
                         .format(paper_id, transover_pheno))
            curs.execute("INSERT INTO afp_overexpr_hst (joinkey, afp_overexpr_hst) VALUES('{}', '{}')"
                         .format(paper_id, transover_pheno))

    def set_chemical(self, chemical, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_chemphen WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_chemphen (joinkey, afp_chemphen) VALUES('{}', '{}')"
                         .format(paper_id, chemical))
            curs.execute("INSERT INTO afp_chemphen_hst (joinkey, afp_chemphen_hst) VALUES('{}', '{}')"
                         .format(paper_id, chemical))

    def set_env(self, env, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_envpheno WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_envpheno (joinkey, afp_envpheno) VALUES('{}', '{}')"
                         .format(paper_id, env))
            curs.execute("INSERT INTO afp_envpheno_hst (joinkey, afp_envpheno_hst) VALUES('{}', '{}')"
                         .format(paper_id, env))

    def set_protein(self, protein, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_catalyticact WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_catalyticact (joinkey, afp_catalyticact) VALUES('{}', '{}')"
                         .format(paper_id, protein))
            curs.execute("INSERT INTO afp_catalyticact_hst (joinkey, afp_catalyticact_hst) VALUES('{}', '{}')"
                         .format(paper_id, protein))

    def set_othergenefunc(self, othergenefunc, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_othergenefunc WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_othergenefunc (joinkey, afp_othergenefunc) VALUES('{}', '{}')"
                         .format(paper_id, othergenefunc))
            curs.execute("INSERT INTO afp_othergenefunc_hst (joinkey, afp_othergenefunc_hst) VALUES('{}', '{}')"
                         .format(paper_id, othergenefunc))

    def set_disease(self, disease, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_humdis WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_humdis (joinkey, afp_humdis) VALUES('{}', '{}')"
                         .format(paper_id, disease))
            curs.execute("INSERT INTO afp_humdis_hst (joinkey, afp_humdis_hst) VALUES('{}', '{}')"
                         .format(paper_id, disease))

    def set_comments(self, comments, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_comment WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_comment (joinkey, afp_comment) VALUES('{}', '{}')"
                         .format(paper_id, comments))
            curs.execute("INSERT INTO afp_comment_hst (joinkey, afp_comment_hst) VALUES('{}', '{}')"
                         .format(paper_id, comments))

    def set_version(self, paper_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("DELETE FROM afp_version WHERE joinkey = '{}'".format(paper_id))
            curs.execute("INSERT INTO afp_version (joinkey, afp_version) VALUES('{}', '2')".format(paper_id))

    def save_extracted_data_to_db(self, paper_id: str, genes: List[str], alleles: List[str], species: List[str], 
                                  strains: List[str], transgenes: List[str], author_email: str):
        passwd = self.get_passwd(paper_id=paper_id)
        passwd = time.time() if not passwd else passwd
        if self.get_db_manager(WBPaperDBManager).is_antibody_set(paper_id):
            self.set_antibody(paper_id)
        self.set_extracted_entities_in_paper(paper_id, genes, "tfp_genestudied")
        self.set_extracted_entities_in_paper(paper_id, alleles, "tfp_variation")
        self.set_extracted_entities_in_paper(paper_id, species, "tfp_species")
        self.set_extracted_entities_in_paper(paper_id, strains, "tfp_strain")
        self.set_extracted_entities_in_paper(paper_id, transgenes, "tfp_transgene")
        self.set_version(paper_id)
        self.set_passwd(paper_id, passwd)
        self.set_email(paper_id, [author_email])
        return passwd
