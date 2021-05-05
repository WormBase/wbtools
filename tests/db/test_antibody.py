import datetime
import os
import unittest

from tests.config_reader import read_db_config
from wbtools.db.antibody import WBAntibodyDBManager


@unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                 "local_config", "db.cfg")), "Test DB config file not present")
class TestWBDBManager(unittest.TestCase):

    def setUp(self) -> None:
        config = read_db_config()
        self.db_manager = WBAntibodyDBManager(dbname=config["wb_database"]["db_name"],
                                              user=config["wb_database"]["db_user"],
                                              password=config["wb_database"]["db_password"],
                                              host=config["wb_database"]["db_host"])

    def test_get_antibody_str_values(self):
        antib_str_values = self.db_manager.get_antibody_str_values(
            from_date=datetime.datetime.now() - datetime.timedelta(days=365))
        self.assertTrue(len(antib_str_values) > 0)
