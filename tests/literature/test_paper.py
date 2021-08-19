import os
import unittest

from tests.config_reader import read_db_config, read_tazendra_config
from wbtools.db.paper import WBPaperDBManager
from wbtools.lib.nlp.common import PaperSections
from wbtools.literature.paper import WBPaper

TESTDATA_DIR = os.path.join(os.path.dirname(__file__), '../data', 'text_files')


class TestWBPaper(unittest.TestCase):

    def test(self):
        paper = WBPaper()
        paper.add_file(dir_path=TESTDATA_DIR, filename='00026804_McCracken05.txt', remote_file=False, pdf=False)
        self.assertTrue(paper.has_same_wbpaper_id_as_filename('00026804_McCracken05.txt'))
        paper.add_file(dir_path=TESTDATA_DIR, filename='00026804_McCracken05_supp.txt', remote_file=False, pdf=False)
        self.assertEqual(len(paper.get_text_docs(remove_sections=None, split_sentences=False, lowercase=False,
                                                 tokenize=False, remove_stopwords=False, remove_alpha=False)), 2)
        self.assertTrue("REFERENCES" not in "\n\n".join(
            paper.get_text_docs(remove_sections=[PaperSections.REFERENCES], split_sentences=False, lowercase=False,
                                tokenize=False, remove_stopwords=False, remove_alpha=False)))
        self.assertGreater(len(paper.get_text_docs(split_sentences=True, lowercase=False,
                                                   tokenize=False, remove_stopwords=False, remove_alpha=False)), 2)
        self.assertTrue("\n\n".join(
            paper.get_text_docs(split_sentences=False, lowercase=True, tokenize=False,
                                remove_stopwords=False, remove_alpha=False)).islower())
        self.assertTrue(type(paper.get_text_docs(split_sentences=False, lowercase=False,
                                                 tokenize=True, remove_stopwords=False, remove_alpha=False)[0]) is list)
        self.assertTrue("." in paper.get_text_docs(split_sentences=False, lowercase=False,
                                                   tokenize=True, remove_stopwords=False, remove_alpha=False)[0])
        self.assertTrue("we" not in paper.get_text_docs(split_sentences=False,
                                                        lowercase=False, tokenize=True, remove_stopwords=True,
                                                        remove_alpha=False)[0])
        self.assertTrue("." not in paper.get_text_docs(split_sentences=False, lowercase=False,
                                                       tokenize=True, remove_stopwords=True, remove_alpha=True)[0])

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

    def test_extract_all_email_addresses_from_text(self):
        paper = WBPaper()
        paper.add_file(dir_path=TESTDATA_DIR, filename='00026804_McCracken05.txt', remote_file=False, pdf=False)
        self.assertTrue(paper.has_same_wbpaper_id_as_filename('00026804_McCracken05.txt'))
        paper.add_file(dir_path=TESTDATA_DIR, filename='00026804_McCracken05_supp.txt', remote_file=False, pdf=False)
        addresses = paper.extract_all_email_addresses_from_text()
        self.assertGreater(len(addresses), 0)

    def test_pdf2txt_conversion(self):
        config = read_db_config()
        ssh_config = read_tazendra_config()
        db_manager = WBPaperDBManager(
            dbname=config["wb_database"]["db_name"], user=config["wb_database"]["db_user"],
            password=config["wb_database"]["db_password"], host=config["wb_database"]["db_host"])
        paper = WBPaper(paper_id="00003969", db_manager=db_manager, ssh_user=ssh_config["ssh"]["ssh_user"],
                        ssh_passwd=ssh_config["ssh"]["ssh_password"])
        paper.load_text_from_pdf_files_in_db()
        fulltext = paper.get_text_docs()
        self.assertGreater(len(fulltext), 0)
        self.assertTrue("u253, u423 deletion in exon 3" in fulltext[0])

    def test_pdf_table_conversion(self):
        config = read_db_config()
        ssh_config = read_tazendra_config()
        db_manager = WBPaperDBManager(
            dbname=config["wb_database"]["db_name"], user=config["wb_database"]["db_user"],
            password=config["wb_database"]["db_password"], host=config["wb_database"]["db_host"])
        paper = WBPaper(paper_id="00059755", db_manager=db_manager, ssh_user=ssh_config["ssh"]["ssh_user"],
                        ssh_passwd=ssh_config["ssh"]["ssh_password"])
        paper.load_text_from_pdf_files_in_db()
        fulltext = paper.get_text_docs()
        self.assertTrue(fulltext)

    def test_tokenize_sentences_with_tables(self):
        config = read_db_config()
        ssh_config = read_tazendra_config()
        db_manager = WBPaperDBManager(
            dbname=config["wb_database"]["db_name"], user=config["wb_database"]["db_user"],
            password=config["wb_database"]["db_password"], host=config["wb_database"]["db_host"])
        paper = WBPaper(paper_id="00003969", db_manager=db_manager, ssh_user=ssh_config["ssh"]["ssh_user"],
                        ssh_passwd=ssh_config["ssh"]["ssh_password"])
        paper.load_text_from_pdf_files_in_db()
        sentences = paper.get_text_docs(split_sentences=True)
        self.assertGreater(len(sentences), 0)

    def test_two_cols_conversion(self):
        config = read_db_config()
        ssh_config = read_tazendra_config()
        db_manager = WBPaperDBManager(
            dbname=config["wb_database"]["db_name"], user=config["wb_database"]["db_user"],
            password=config["wb_database"]["db_password"], host=config["wb_database"]["db_host"])
        paper = WBPaper(paper_id="00055367", db_manager=db_manager, ssh_user=ssh_config["ssh"]["ssh_user"],
                        ssh_passwd=ssh_config["ssh"]["ssh_password"])
        paper.load_text_from_pdf_files_in_db()
        fulltext = paper.get_text_docs()
        self.assertTrue(fulltext)
