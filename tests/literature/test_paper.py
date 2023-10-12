import os
import unittest

from tests.config_reader import read_db_config, load_env_file
from wbtools.db.paper import WBPaperDBManager
from wbtools.lib.nlp.common import PaperSections
from wbtools.literature.corpus import CorpusManager
from wbtools.literature.paper import WBPaper

TESTDATA_DIR = os.path.join(os.path.dirname(__file__), '../data', 'text_files')


class TestWBPaper(unittest.TestCase):

    @unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                     "local_config", "db.cfg")), "Test DB config file not present")
    def test_load_curation_info_from_db(self):
        config = read_db_config()
        paper = WBPaper(paper_id="00004161", db_manager=WBPaperDBManager(
            dbname=config["wb_database"]["db_name"], user=config["wb_database"]["db_user"],
            password=config["wb_database"]["db_password"], host=config["wb_database"]["db_host"]))
        paper.paper_id = "00004161"
        paper.load_curation_info_from_db()
        self.assertTrue(paper.aut_class_values["seqchange"] == 'high')

    @unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                     "local_config", "db.cfg")), "Test DB config file not present")
    def test_load_bib_info_from_db(self):
        config = read_db_config()
        db_manager = WBPaperDBManager(
            dbname=config["wb_database"]["db_name"], user=config["wb_database"]["db_user"],
            password=config["wb_database"]["db_password"], host=config["wb_database"]["db_host"])
        paper = WBPaper(paper_id="00004161", db_manager=db_manager)
        with db_manager:
            paper.load_bib_info_from_db()
        self.assertGreater(len(paper.authors), 0)

    @unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                     "local_config", "db.cfg")), "Test DB config file not present")
    def test_extract_all_email_addresses_from_text(self):
        config = read_db_config()
        load_env_file()
        cm = CorpusManager()
        cm.load_from_wb_database(db_host=config["wb_database"]["db_host"],
                                 db_user=config["wb_database"]["db_user"],
                                 db_password=config["wb_database"]["db_password"],
                                 db_name=config["wb_database"]["db_name"], paper_ids=['00026804'])
        addresses = cm.get_paper('00026804').extract_all_email_addresses_from_text()
        self.assertGreater(len(addresses), 0)

    @unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                     "local_config", "db.cfg")), "Test DB config file not present")
    def test_pdf2txt_conversion(self):
        config = read_db_config()
        db_manager = WBPaperDBManager(
            dbname=config["wb_database"]["db_name"], user=config["wb_database"]["db_user"],
            password=config["wb_database"]["db_password"], host=config["wb_database"]["db_host"])
        paper = WBPaper(paper_id="00003969", db_manager=db_manager)
        paper.load_text_from_pdf_files_in_db()
        fulltext = paper.get_text_docs()
        self.assertGreater(len(fulltext), 0)
        self.assertTrue("u253, u423 deletion in exon 3" in fulltext[0])

    @unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                     "local_config", "db.cfg")), "Test DB config file not present")
    def test_pdf_table_conversion(self):
        config = read_db_config()
        db_manager = WBPaperDBManager(
            dbname=config["wb_database"]["db_name"], user=config["wb_database"]["db_user"],
            password=config["wb_database"]["db_password"], host=config["wb_database"]["db_host"])
        paper = WBPaper(paper_id="00059755", db_manager=db_manager)
        paper.load_text_from_pdf_files_in_db()
        fulltext = paper.get_text_docs()
        self.assertTrue(fulltext)

    @unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                     "local_config", "db.cfg")), "Test DB config file not present")
    def test_tokenize_sentences_with_tables(self):
        config = read_db_config()
        db_manager = WBPaperDBManager(
            dbname=config["wb_database"]["db_name"], user=config["wb_database"]["db_user"],
            password=config["wb_database"]["db_password"], host=config["wb_database"]["db_host"])
        paper = WBPaper(paper_id="00003969", db_manager=db_manager)
        paper.load_text_from_pdf_files_in_db()
        sentences = paper.get_text_docs(split_sentences=True)
        self.assertGreater(len(sentences), 0)

    @unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                     "local_config", "db.cfg")), "Test DB config file not present")
    def test_two_cols_conversion(self):
        config = read_db_config()
        db_manager = WBPaperDBManager(
            dbname=config["wb_database"]["db_name"], user=config["wb_database"]["db_user"],
            password=config["wb_database"]["db_password"], host=config["wb_database"]["db_host"])
        paper = WBPaper(paper_id="00055367", db_manager=db_manager)
        paper.load_text_from_pdf_files_in_db()
        fulltext = paper.get_text_docs()
        self.assertTrue(fulltext)
