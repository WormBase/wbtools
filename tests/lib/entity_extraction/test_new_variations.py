import unittest

from wbtools.lib.nlp.entity_extraction.new_variations import get_new_variations_from_text, is_variation_suspicious


class TestNLPNewVariations(unittest.TestCase):

    def setUp(self) -> None:
        self.text = "  g2 is a new variant"

    def test_get_new_variations_from_text(self):
        variations = get_new_variations_from_text(self.text)
        self.assertGreater(len(variations), 0)

    def test_is_variation_suspicious(self):
        variations = get_new_variations_from_text(self.text)
        is_susp = is_variation_suspicious(variations[0][0])
        self.assertEqual(is_susp, "potentially new")


if __name__ == '__main__':
    unittest.main()
