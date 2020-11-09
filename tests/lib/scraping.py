import unittest

from wbtools.lib.scraping import get_cgc_allele_designations


class TestScraping(unittest.TestCase):

    def test_get_cgc_allele_designations(self):
        cgc_designations = get_cgc_allele_designations()
        self.assertTrue(len(cgc_designations) > 0)
