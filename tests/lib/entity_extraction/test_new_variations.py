import os
import unittest

from tests.config_reader import read_db_config
from wbtools.db.generic import WBGenericDBManager
from wbtools.lib.nlp.common import EntityType
from wbtools.lib.nlp.entity_extraction.ntt_extractor import NttExtractor
from wbtools.lib.nlp.entity_extraction.variations import is_variation_suspicious


@unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../..", "data",
                                                     "local_config", "db.cfg")), "Test DB config file not present")
class TestNLPNewVariations(unittest.TestCase):

    def setUp(self) -> None:
        self.text = "  g2 is a new variant"
        config = read_db_config()
        db_manager = WBGenericDBManager(
            dbname=config["wb_database"]["db_name"], user=config["wb_database"]["db_user"],
            password=config["wb_database"]["db_password"], host=config["wb_database"]["db_host"])
        self.ntt_extractor = NttExtractor(db_manager=db_manager)

    def test_get_new_variations_from_text(self):
        variations = self.ntt_extractor.extract_all_entities_by_type(self.text, EntityType.VARIATION,
                                                                     match_curated=False,
                                                                     exclude_curated=True)
        self.assertGreater(len(variations), 0)
        text = "The result- ing ez72syb1455 animals show the exact same gfp expression pattern as that of the " \
               "parental ez72 strain and also display no mutant phenotype (Figures S2A–S2C), thereby supporting past " \
               "evidence [8] that the tra-1b isoform bears no relevant Tra function."
        variations = self.ntt_extractor.extract_all_entities_by_type(text, EntityType.VARIATION, match_curated=False,
                                                                     exclude_curated=True)
        self.assertTrue("ez72syb1455" in variations)

    def test_is_variation_suspicious(self):
        variations = self.ntt_extractor.extract_all_entities_by_type(self.text, EntityType.VARIATION,
                                                                     match_curated=False,
                                                                     exclude_curated=True)
        is_susp = is_variation_suspicious(variations[0])
        self.assertEqual(is_susp, "potentially new")


if __name__ == '__main__':
    unittest.main()
