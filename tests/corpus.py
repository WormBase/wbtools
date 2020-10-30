import os
import unittest

from wbtools.corpus.corpus import CorpusManager

TESTDATA_DIR = os.path.join(os.path.dirname(__file__), 'data', 'text_files')


class TestCorpusManager(unittest.TestCase):

    def test_load(self):
        cm = CorpusManager()
        cm.load_from_dir_with_txt_files(dir_path=TESTDATA_DIR)
        self.assertGreater(cm.size(), 0)
        self.assertGreater(len([paper for paper in cm.get_all_papers()]), 0)
        self.assertTrue(cm.get_paper('00026804').paper_id == '00026804')
        self.assertTrue(cm.get_paper('00026804').get_text_docs() != "")
        self.assertGreater(len(cm.get_paper('00026804').supplemental_docs), 0)


if __name__ == '__main__':
    unittest.main()