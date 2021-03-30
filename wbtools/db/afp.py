from typing import List

from wbtools.db.abstract_manager import AbstractWBDBManager
from wbtools.db.generic import WBGenericDBManager

QUERY_PAPER_IDS_NO_SUBMISSION = "SELECT afp_email.joinkey AS k FROM afp_email JOIN afp_version afp_ve ON afp_email.joinkey = afp_ve.joinkey FULL OUTER JOIN afp_genestudied afp_g ON afp_ve.joinkey = afp_g.joinkey FULL OUTER JOIN afp_species afp_s ON afp_ve.joinkey = afp_s.joinkey FULL OUTER JOIN afp_variation afp_v ON afp_ve.joinkey = afp_v.joinkey FULL OUTER JOIN afp_strain afp_st ON afp_ve.joinkey = afp_st.joinkey FULL OUTER JOIN afp_transgene afp_t ON afp_ve.joinkey = afp_t.joinkey FULL OUTER JOIN afp_seqchange afp_seq ON afp_ve.joinkey = afp_seq.joinkey FULL OUTER JOIN afp_geneint afp_ge ON afp_ve.joinkey = afp_ge.joinkey FULL OUTER JOIN afp_geneprod afp_gp ON afp_ve.joinkey = afp_gp.joinkey FULL OUTER JOIN afp_genereg afp_gr ON afp_ve.joinkey = afp_gr.joinkey FULL OUTER JOIN afp_newmutant afp_nm ON afp_ve.joinkey = afp_nm.joinkey FULL OUTER JOIN afp_rnai afp_rnai ON afp_ve.joinkey = afp_rnai.joinkey FULL OUTER JOIN afp_overexpr afp_ov ON afp_ve.joinkey = afp_ov.joinkey FULL OUTER JOIN afp_structcorr afp_stc ON afp_ve.joinkey = afp_stc.joinkey FULL OUTER JOIN afp_antibody ON afp_ve.joinkey = afp_antibody.joinkey FULL OUTER JOIN afp_siteaction ON afp_ve.joinkey = afp_siteaction.joinkey FULL OUTER JOIN afp_timeaction ON afp_ve.joinkey = afp_timeaction.joinkey FULL OUTER JOIN afp_rnaseq ON afp_ve.joinkey = afp_rnaseq.joinkey FULL OUTER JOIN afp_chemphen ON afp_ve.joinkey = afp_chemphen.joinkey FULL OUTER JOIN afp_envpheno ON afp_ve.joinkey = afp_envpheno.joinkey FULL OUTER JOIN afp_catalyticact ON afp_ve.joinkey = afp_catalyticact.joinkey FULL OUTER JOIN afp_humdis ON afp_ve.joinkey = afp_humdis.joinkey FULL OUTER JOIN afp_additionalexpr ON afp_ve.joinkey = afp_additionalexpr.joinkey FULL OUTER JOIN afp_comment ON afp_ve.joinkey = afp_comment.joinkey WHERE afp_ve.afp_version = '2' AND afp_g.afp_genestudied IS NULL AND afp_s.afp_species IS NULL AND afp_v.afp_variation IS NULL AND afp_st.afp_strain IS NULL AND afp_t.afp_transgene IS NULL AND afp_seq.afp_seqchange IS NULL AND afp_ge.afp_geneint IS NULL AND afp_gp.afp_geneprod IS NULL AND afp_gr.afp_genereg IS NULL AND afp_nm.afp_newmutant IS NULL AND afp_rnai.afp_rnai IS NULL AND afp_ov.afp_overexpr IS NULL AND afp_stc.afp_structcorr IS NULL AND afp_antibody.afp_antibody IS NULL AND afp_siteaction.afp_siteaction IS NULL AND afp_timeaction.afp_timeaction IS NULL AND afp_rnaseq.afp_rnaseq IS NULL AND afp_chemphen.afp_chemphen IS NULL AND afp_envpheno.afp_envpheno IS NULL AND afp_catalyticact.afp_catalyticact IS NULL AND afp_humdis.afp_humdis IS NULL AND afp_additionalexpr.afp_additionalexpr IS NULL AND afp_comment.afp_comment IS NULL "
QUERY_PAPER_IDS_FULL_SUBMISSION = "SELECT afp_email.joinkey FROM afp_email JOIN afp_lasttouched ON afp_email.joinkey = afp_lasttouched.joinkey JOIN afp_version ON afp_lasttouched.joinkey = afp_version.joinkey WHERE afp_version.afp_version = '2' "
QUERY_PAPER_IDS_PARTIAL_SUBMISSION = "SELECT afp_e.joinkey FROM afp_email afp_e JOIN afp_version afp_ve ON afp_e.joinkey = afp_ve.joinkey FULL OUTER JOIN afp_lasttouched afp_l ON afp_ve.joinkey = afp_l.joinkey FULL OUTER JOIN afp_genestudied afp_g ON afp_ve.joinkey = afp_g.joinkey FULL OUTER JOIN afp_species afp_s ON afp_ve.joinkey = afp_s.joinkey FULL OUTER JOIN afp_variation afp_v ON afp_ve.joinkey = afp_v.joinkey FULL OUTER JOIN afp_strain afp_st ON afp_ve.joinkey = afp_st.joinkey FULL OUTER JOIN afp_transgene afp_t ON afp_ve.joinkey = afp_t.joinkey FULL OUTER JOIN afp_seqchange afp_seq ON afp_ve.joinkey = afp_seq.joinkey FULL OUTER JOIN afp_geneint afp_ge ON afp_ve.joinkey = afp_ge.joinkey FULL OUTER JOIN afp_geneprod afp_gp ON afp_ve.joinkey = afp_gp.joinkey FULL OUTER JOIN afp_genereg afp_gr ON afp_ve.joinkey = afp_gr.joinkey FULL OUTER JOIN afp_newmutant afp_nm ON afp_ve.joinkey = afp_nm.joinkey FULL OUTER JOIN afp_rnai afp_rnai ON afp_ve.joinkey = afp_rnai.joinkey FULL OUTER JOIN afp_overexpr afp_ov ON afp_ve.joinkey = afp_ov.joinkey FULL OUTER JOIN afp_structcorr afp_stc ON afp_ve.joinkey = afp_stc.joinkey FULL OUTER JOIN afp_antibody ON afp_ve.joinkey = afp_antibody.joinkey FULL OUTER JOIN afp_siteaction ON afp_ve.joinkey = afp_siteaction.joinkey FULL OUTER JOIN afp_timeaction ON afp_ve.joinkey = afp_timeaction.joinkey FULL OUTER JOIN afp_rnaseq ON afp_ve.joinkey = afp_rnaseq.joinkey FULL OUTER JOIN afp_chemphen ON afp_ve.joinkey = afp_chemphen.joinkey FULL OUTER JOIN afp_envpheno ON afp_ve.joinkey = afp_envpheno.joinkey FULL OUTER JOIN afp_catalyticact ON afp_ve.joinkey = afp_catalyticact.joinkey FULL OUTER JOIN afp_humdis ON afp_ve.joinkey = afp_humdis.joinkey FULL OUTER JOIN afp_additionalexpr ON afp_ve.joinkey = afp_additionalexpr.joinkey FULL OUTER JOIN afp_comment ON afp_ve.joinkey = afp_comment.joinkey WHERE afp_ve.afp_version = '2' AND afp_l.afp_lasttouched is NULL AND (afp_g.afp_genestudied IS NOT NULL OR afp_s.afp_species IS NOT NULL OR afp_v.afp_variation IS NOT NULL OR afp_st.afp_strain IS NOT NULL OR afp_t.afp_transgene IS NOT NULL OR afp_seq.afp_seqchange IS NOT NULL OR afp_ge.afp_geneint IS NOT NULL OR afp_gp.afp_geneprod IS NOT NULL OR afp_gr.afp_genereg IS NOT NULL OR afp_nm.afp_newmutant IS NOT NULL OR afp_rnai.afp_rnai IS NOT NULL OR afp_ov.afp_overexpr IS NOT NULL OR afp_stc.afp_structcorr IS NOT NULL OR afp_antibody.afp_antibody IS NOT NULL OR afp_siteaction.afp_siteaction IS NOT NULL OR afp_timeaction.afp_timeaction IS NOT NULL OR afp_rnaseq.afp_rnaseq IS NOT NULL OR afp_chemphen.afp_chemphen IS NOT NULL OR afp_envpheno.afp_envpheno IS NOT NULL OR afp_catalyticact.afp_catalyticact IS NOT NULL OR afp_humdis.afp_humdis IS NOT NULL OR afp_additionalexpr.afp_additionalexpr IS NOT NULL OR afp_comment.afp_comment IS NOT NULL)"


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
