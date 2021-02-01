import unittest

from wbtools.lib.nlp.entity_extraction.generic import get_entities_from_text_regex
from wbtools.lib.nlp.entity_extraction.new_variations import NEW_VAR_REGEX


class TestGenericNLP(unittest.TestCase):

    def test_get_entities_from_text(self):
        text = "  g2 is a new variant"
        variations = get_entities_from_text_regex(text, NEW_VAR_REGEX)
        self.assertTrue(variations)


if __name__ == '__main__':
    unittest.main()
