import os
import unittest

from tests.config_reader import read_tpc_config
from wbtools.lib.nlp.literature_index.textpresso import TextpressoLiteratureIndex


@unittest.skipIf(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                                                 "local_config", "textpresso.cfg")),
                 "Textpresso test config file not present")
class TestTextpressoLiteratureIndex(unittest.TestCase):

    def setUp(self) -> None:
        config = read_tpc_config()
        self.lit_index = TextpressoLiteratureIndex(api_url=config["tpc"]["api_base_url"],
                                                   api_token=config["tpc"]["api_token"], use_cache=True,
                                                   corpora=[config["tpc"]["corpora"]])

    def test_num_documents(self):
        self.assertGreater(self.lit_index.num_documents(), 0)
        self.assertGreater(self.lit_index.num_documents(), 0)

    def test_count_matching_documents(self):
        count = self.lit_index.count_matching_documents("DREAM complex")
        self.assertGreater(count, 0)


if __name__ == '__main__':
    unittest.main()
