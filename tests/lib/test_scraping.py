import unittest

from wbtools.lib.scraping import get_cgc_allele_designations, get_cgc_lab_designations


class TestScraping(unittest.TestCase):

    def test_get_cgc_allele_designations(self):
        cgc_designations = get_cgc_allele_designations()
        self.assertTrue(len(cgc_designations) > 0)

    def test_get_cgc_lab_designations(self):
        cgc_lab_designations = get_cgc_lab_designations()
        self.assertTrue(len(cgc_lab_designations) > 0)


if __name__ == '__main__':
    unittest.main()
