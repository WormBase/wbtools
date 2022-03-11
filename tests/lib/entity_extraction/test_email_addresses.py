import unittest
import os

from tests.config_reader import read_db_config, read_tazendra_config
from wbtools.lib.nlp.entity_extraction.email_addresses import get_email_addresses_from_text
from wbtools.literature.corpus import CorpusManager


class TestEmailAddresses(unittest.TestCase):

    def test_get_email_addresses_from_text(self):
        text = "test@email.com is a valid email address. @email is not. test@ is not. Other valid email addresses are:" \
               " t.test@stat.email.org"
        addr = get_email_addresses_from_text(text)
        self.assertEqual(len(addr), 2)

    @unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../..", "data",
                                                     "local_config", "db.cfg")), "Test DB config file not present")
    def test_get_email_addresses_from_paper(self):
        config = read_db_config()
        tazendra_config = read_tazendra_config()
        cm = CorpusManager()
        cm.load_from_wb_database(db_name=config["wb_database"]["db_name"], db_user=config["wb_database"]["db_user"],
                                 db_password=config["wb_database"]["db_password"],
                                 db_host=config["wb_database"]["db_host"],
                                 file_server_user=tazendra_config["file_server"]["user"],
                                 file_server_passwd=tazendra_config["file_server"]["password"],
                                 paper_ids=['00062455'])
        email_addresses = get_email_addresses_from_text(cm.get_paper('00062455').get_text_docs(
            include_supplemental=False, return_concatenated=True))
        email_addresses_in_wb = cm.get_paper('00062455').get_authors_with_email_address_in_wb()
        self.assertEqual(len(email_addresses), 3)
        self.assertGreaterEqual(len(email_addresses_in_wb), 2)


if __name__ == '__main__':
    unittest.main()
