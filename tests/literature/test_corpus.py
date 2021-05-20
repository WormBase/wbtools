import os
import unittest
import tempfile

from tests.config_reader import read_db_config, read_tazendra_config
from wbtools.lib.nlp.common import PaperSections
from wbtools.literature.corpus import CorpusManager
from wbtools.literature.paper import WBPaper

TESTDATA_DIR = os.path.join(os.path.dirname(__file__), '../data', 'text_files')


class TestCorpusManager(unittest.TestCase):

    def setUp(self) -> None:
        self.cm = CorpusManager()
        self.cm.load_from_dir_with_txt_files(dir_path=TESTDATA_DIR)

    def test_load_from_dir(self):
        self.assertGreater(self.cm.size(), 0)
        self.assertGreater(len([paper for paper in self.cm.get_all_papers()]), 0)
        self.assertTrue(self.cm.get_paper('00026804').paper_id == '00026804')
        self.assertTrue(self.cm.get_paper('00026804').get_text_docs() != "")
        self.assertGreater(len(self.cm.get_paper('00026804').supplemental_docs), 0)

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

    def test_pickle(self):
        tmp_file = tempfile.NamedTemporaryFile()
        self.cm.save(tmp_file.name)
        self.assertGreater(os.path.getsize(tmp_file.name), 0)

        cm = CorpusManager()
        cm.load(tmp_file.name)
        self.assertGreater(cm.size(), 0)
        self.assertTrue(all([type(paper) == WBPaper for paper in cm.corpus.values()]))

    def test_exclude_sections(self):
        test_list, _ = self.cm.get_flat_corpus_list_and_idx_paperid_map(
            split_sentences=False, remove_sections=[PaperSections.INTRODUCTION, PaperSections.REFERENCES],
            must_be_present=[PaperSections.RESULTS], lowercase=False, tokenize=False, remove_stopwords=False,
            remove_alpha=False)
        pass

    @unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                     "local_config", "db.cfg")), "Test DB config file not present")
    def test_load_from_wb_database(self):
        db_config = read_db_config()
        tazendra_config = read_tazendra_config()
        cm = CorpusManager()
        cm.load_from_wb_database(db_name=db_config["wb_database"]["db_name"], db_user=db_config["wb_database"]["db_user"],
                                 db_password=db_config["wb_database"]["db_password"],
                                 db_host=db_config["wb_database"]["db_host"],
                                 ssh_user=tazendra_config["ssh"]["ssh_user"],
                                 ssh_passwd=tazendra_config["ssh"]["ssh_password"], max_num_papers=2)
        self.assertTrue(cm.size() == 2)
        cm.load_from_wb_database(db_name=db_config["wb_database"]["db_name"],
                                 db_user=db_config["wb_database"]["db_user"],
                                 db_password=db_config["wb_database"]["db_password"],
                                 db_host=db_config["wb_database"]["db_host"],
                                 ssh_user=tazendra_config["ssh"]["ssh_user"],
                                 ssh_passwd=tazendra_config["ssh"]["ssh_password"], max_num_papers=2,
                                 exclude_temp_pdf=True)
        self.assertFalse(any([paper.is_temp() for paper in cm.get_all_papers()]))

    @unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                     "local_config", "db.cfg")), "Test DB config file not present")
    def test_load_from_wb_database_afp(self):
        db_config = read_db_config()
        tazendra_config = read_tazendra_config()
        cm = CorpusManager()
        cm.load_from_wb_database(db_name=db_config["wb_database"]["db_name"],
                                 db_user=db_config["wb_database"]["db_user"],
                                 db_password=db_config["wb_database"]["db_password"],
                                 db_host=db_config["wb_database"]["db_host"],
                                 ssh_user=tazendra_config["ssh"]["ssh_user"],
                                 ssh_passwd=tazendra_config["ssh"]["ssh_password"], max_num_papers=2,
                                 load_curation_info=True, load_afp_info=True,
                                 exclude_temp_pdf=True, exclude_afp_processed=True, must_be_autclass_flagged=True)
        self.assertFalse(any([paper.afp_processed for paper in cm.get_all_papers()]))


if __name__ == '__main__':
    unittest.main()
