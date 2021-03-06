import os
import unittest

from tests.db.test_generic import TestWBDBManager
from wbtools.db.generic import WBGenericDBManager
from wbtools.lib.nlp.common import EntityType
from wbtools.lib.nlp.entity_extraction.ntt_extractor import NttExtractor


@unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../..", "data",
                                                     "local_config", "db.cfg")), "Test DB config file not present")
class TestGenericNLP(unittest.TestCase):

    def setUp(self) -> None:
        config = TestWBDBManager.read_db_config()
        db_manager = WBGenericDBManager(
            dbname=config["wb_database"]["db_name"], user=config["wb_database"]["db_user"],
            password=config["wb_database"]["db_password"], host=config["wb_database"]["db_host"])
        self.ntt_extractor = NttExtractor(db_manager=db_manager)

    def test_extract_entities(self):
        text = "  g2 is a new variant"
        variations = self.ntt_extractor.extract_entities(text, entity_type=EntityType.VARIATION)
        self.assertEqual(len(variations), 1)
        self.assertTrue("g2" in variations)

    def test_get_strains_from_text(self):
        text = "  AB4 is a strain, (N2) is another one, but AAA23 is not a strain "
        strains = self.ntt_extractor.extract_entities(text, EntityType.STRAIN, match_curated=True)
        self.assertEqual(len(strains), 2)


if __name__ == '__main__':
    unittest.main()
