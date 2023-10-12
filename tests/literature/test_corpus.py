import os
import unittest
import tempfile

from tests.config_reader import read_db_config, load_env_file
from wbtools.lib.nlp.common import PaperSections
from wbtools.literature.corpus import CorpusManager
from wbtools.literature.paper import WBPaper

TESTDATA_DIR = os.path.join(os.path.dirname(__file__), '../data', 'text_files')


@unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                 "local_config", "db.cfg")), "Test DB config file not present")
class TestCorpusManager(unittest.TestCase):

    def setUp(self) -> None:
        config = read_db_config()
        load_env_file()
        self.cm = CorpusManager()
        self.cm.load_from_wb_database(db_host=config["wb_database"]["db_host"],
                                      db_user=config["wb_database"]["db_user"],
                                      db_password=config["wb_database"]["db_password"],
                                      db_name=config["wb_database"]["db_name"], max_num_papers=2)

    def test_get_flat_corpus_list_and_idx_paperid_map(self):
        flat_list, idx_paperid_map = self.cm.get_flat_corpus_list_and_idx_paperid_map(
            tokenize=True, remove_stopwords=True, remove_alpha=True)
        self.assertEqual(len(flat_list), len(idx_paperid_map))
        self.assertTrue(len(idx_paperid_map[0]) == 8)
        self.assertEqual(len(set(idx_paperid_map.values())), self.cm.size())

    def test_split_sentences(self):
        cl1, map1 = self.cm.get_flat_corpus_list_and_idx_paperid_map(tokenize=False, remove_stopwords=False,
                                                                     remove_alpha=False, split_sentences=True)
        cl2, map2 = self.cm.get_flat_corpus_list_and_idx_paperid_map(tokenize=True, remove_stopwords=True,
                                                                     remove_alpha=True, split_sentences=True)
        self.assertEqual(len(cl1), len(cl2))

    def test_exclude_sections(self):
        test_list, _ = self.cm.get_flat_corpus_list_and_idx_paperid_map(
            split_sentences=False, remove_sections=[PaperSections.INTRODUCTION, PaperSections.REFERENCES],
            must_be_present=[PaperSections.RESULTS], lowercase=False, tokenize=False, remove_stopwords=False,
            remove_alpha=False)
        pass

    def test_load_from_wb_database(self):
        db_config = read_db_config()
        cm = CorpusManager()
        cm.load_from_wb_database(db_name=db_config["wb_database"]["db_name"], db_user=db_config["wb_database"]["db_user"],
                                 db_password=db_config["wb_database"]["db_password"],
                                 db_host=db_config["wb_database"]["db_host"], max_num_papers=2)
        self.assertTrue(cm.size() == 2)
        cm = CorpusManager()
        cm.load_from_wb_database(db_name=db_config["wb_database"]["db_name"],
                                 db_user=db_config["wb_database"]["db_user"],
                                 db_password=db_config["wb_database"]["db_password"],
                                 db_host=db_config["wb_database"]["db_host"], max_num_papers=2,
                                 exclude_temp_pdf=True)
        self.assertFalse(any([paper.is_temp() for paper in cm.get_all_papers()]))

    def test_load_supplemental(self):
        db_config = read_db_config()
        cm = CorpusManager()
        cm.load_from_wb_database(db_name=db_config["wb_database"]["db_name"],
                                 db_user=db_config["wb_database"]["db_user"],
                                 db_password=db_config["wb_database"]["db_password"],
                                 db_host=db_config["wb_database"]["db_host"],
                                 paper_ids=["00062512"])
        self.assertTrue(len(cm.get_paper("00062512").supplemental_docs) > 0)

    # def test_load_from_wb_database_afp(self):
    #     db_config = read_db_config()
    #     cm = CorpusManager()
    #     cm.load_from_wb_database(db_name=db_config["wb_database"]["db_name"],
    #                              db_user=db_config["wb_database"]["db_user"],
    #                              db_password=db_config["wb_database"]["db_password"],
    #                              db_host=db_config["wb_database"]["db_host"],
    #                              max_num_papers=2,
    #                              load_curation_info=True, load_afp_info=True,
    #                              exclude_temp_pdf=True, exclude_afp_processed=True, must_be_autclass_flagged=True)
    #     self.assertFalse(any([paper.afp_processed for paper in cm.get_all_papers()]))


if __name__ == '__main__':
    unittest.main()
