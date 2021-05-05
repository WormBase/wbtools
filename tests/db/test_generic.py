import re
import unittest
import os

from tests.config_reader import read_db_config
from wbtools.db.generic import WBGenericDBManager
from wbtools.lib.nlp.entity_extraction.ntt_extractor import ALL_VAR_REGEX


@unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                 "local_config", "db.cfg")), "Test DB config file not present")
class TestWBDBManager(unittest.TestCase):

    def setUp(self) -> None:
        config = read_db_config()
        self.db_manager = WBGenericDBManager(dbname=config["wb_database"]["db_name"],
                                             user=config["wb_database"]["db_user"],
                                             password=config["wb_database"]["db_password"],
                                             host=config["wb_database"]["db_host"])

    def test_get_curated_variations(self):
        curated_variations = self.db_manager.get_curated_variations()
        self.assertGreater(len(curated_variations), 0)
        curated_variations = self.db_manager.get_curated_variations(exclude_id_used_as_name=True)
        allele_regex = ALL_VAR_REGEX.format(designations=self.db_manager.get_allele_designations())
        for variation in curated_variations:
            self.assertTrue(re.match(allele_regex, variation))

    def test_entity_name_id_maps(self):
        gene_name_id_map = self.db_manager.get_gene_name_id_map()
        self.assertTrue(len(gene_name_id_map) > 0)

    def test_get_curated_transgenes(self):
        all_curated = self.db_manager.get_curated_transgenes(exclude_id_used_as_name=False, exclude_invalid=False)
        exclude_invalid = self.db_manager.get_curated_transgenes(exclude_id_used_as_name=False, exclude_invalid=True)
        self.assertGreater(len(all_curated), len(exclude_invalid))


if __name__ == '__main__':
    unittest.main()
