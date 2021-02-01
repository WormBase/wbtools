import re
import unittest
import configparser
import os

from wbtools.db.generic import WBGenericDBManager
from wbtools.lib.nlp.entity_extraction.generic import ALL_VAR_REGEX


@unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                 "local_config", "db.cfg")), "Test DB config file not present")
class TestWBDBManager(unittest.TestCase):

    @staticmethod
    def read_db_config():
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "local_config", "db.cfg"))
        return config

    def setUp(self) -> None:
        config = self.read_db_config()
        self.db_manager = WBGenericDBManager(dbname=config["wb_database"]["db_name"],
                                             user=config["wb_database"]["db_user"],
                                             password=config["wb_database"]["db_password"],
                                             host=config["wb_database"]["db_host"])

    def test_get_curated_variations(self):
        curated_variations = self.db_manager.get_curated_variations()
        self.assertGreater(len(curated_variations), 0)
        curated_variations = self.db_manager.get_curated_variations(exclude_id_used_as_name=True)
        for variation in curated_variations:
            self.assertTrue(re.match(ALL_VAR_REGEX, variation))


if __name__ == '__main__':
    unittest.main()