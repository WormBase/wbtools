import os
import unittest

from tests.config_reader import read_db_config
from wbtools.db.afp import WBAFPDBManager


@unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                 "local_config", "db.cfg")), "Test DB config file not present")
class TestWBAFPDBManager(unittest.TestCase):

    def setUp(self) -> None:
        config = read_db_config()
        self.db_manager = WBAFPDBManager(dbname=config["wb_database"]["db_name"],
                                         user=config["wb_database"]["db_user"],
                                         password=config["wb_database"]["db_password"],
                                         host=config["wb_database"]["db_host"])

    def test_get_stats_timeseries(self):
        monthly_stats = self.db_manager.get_stats_timeseries(bin_period='m')
        self.assertTrue(len(monthly_stats) > 0)
        self.assertTrue(len(monthly_stats[0]) == 2)
        self.assertTrue(len(monthly_stats[0][1]) == 5)
