import os
import unittest

from tests.config_reader import read_db_config, read_tazendra_config
from wbtools.lib.nlp.common import PaperSections
from wbtools.lib.nlp.text_preprocessing import remove_sections_from_text, get_documents_from_text, preprocess
from wbtools.literature.corpus import CorpusManager

TESTDATA_DIR = os.path.join(os.path.dirname(__file__), '../data')


class TestTextPreprocessing(unittest.TestCase):

    def setUp(self):
        self.cm = CorpusManager()
        self.cm.load_from_dir_with_txt_files(dir_path=os.path.join(TESTDATA_DIR, 'text_files'))

    def test_sectioning(self):
        text = list(self.cm.corpus.values())[0].get_text_docs()[0]
        self.assertTrue("REFERENCES" not in remove_sections_from_text(
            text=text, sections_to_remove=[PaperSections.INTRODUCTION, PaperSections.REFERENCES],
            must_be_present=[PaperSections.RESULTS]))

    @unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                     "local_config", "db.cfg")), "Test DB config file not present")
    def test_sectioning_cell_template(self):
        config = read_db_config()
        tazendra_config = read_tazendra_config()
        cm = CorpusManager()
        cm.load_from_wb_database(db_name=config["wb_database"]["db_name"], db_user=config["wb_database"]["db_user"],
                                 db_password=config["wb_database"]["db_password"],
                                 db_host=config["wb_database"]["db_host"],
                                 ssh_user=tazendra_config["ssh"]["ssh_user"],
                                 ssh_passwd=tazendra_config["ssh"]["ssh_password"],
                                 paper_ids=['00059375'])
        fulltext = cm.get_paper('00059375').get_text_docs(remove_sections=[PaperSections.REFERENCES],
                                                          must_be_present=[PaperSections.METHOD, PaperSections.RESULTS])
        self.assertTrue(fulltext)



    def test_get_documents_from_text(self):
        docs = get_documents_from_text(list(self.cm.corpus.values())[0].main_text)
        self.assertEqual(len(docs), 1)
        docs = get_documents_from_text(list(self.cm.corpus.values())[0].main_text, split_sentences=True)
        self.assertGreater(len(docs), 10)

    def test_preprocess(self):
        doc = get_documents_from_text(list(self.cm.corpus.values())[0].main_text)[0]
        prep_text = preprocess(doc, lower=True, tokenize=True, remove_alpha=True, remove_stopwords=True)
        self.assertGreater(len(prep_text), 10)
        self.assertTrue(all([k == k.lower() for k in prep_text]))
        self.assertTrue("the" not in prep_text)


if __name__ == '__main__':
    unittest.main()