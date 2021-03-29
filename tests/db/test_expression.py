import unittest
import os

from tests.config_reader import read_db_config
from wbtools.db.expression import WBExpressionDBManager


@unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                 "local_config", "db.cfg")), "Test DB config file not present")
class TestWBExpressionDBManager(unittest.TestCase):

    def setUp(self) -> None:
        config = read_db_config()
        self.db_manager = WBExpressionDBManager(dbname=config["wb_database"]["db_name"],
                                                user=config["wb_database"]["db_user"],
                                                password=config["wb_database"]["db_password"],
                                                host=config["wb_database"]["db_host"])

    def test_get_all_expression_sentences(self):
        sentences = self.db_manager.get_all_expression_sentences()
        self.assertTrue(sentences)


if __name__ == '__main__':
    unittest.main()