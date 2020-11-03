import os
import unittest

from wbtools.literature.corpus import CorpusManager

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

    def test_get_flat_corpus_list_and_idx_paperid_map(self):
        cm = CorpusManager()
        cm.load_from_dir_with_txt_files(dir_path=TESTDATA_DIR)
        flat_list, idx_paperid_map = cm.get_flat_corpus_list_and_idx_paperid_map(tokenize=True, remove_stopwords=True,
                                                                                 remove_alpha=True)
        self.assertEqual(len(flat_list), len(idx_paperid_map))
        self.assertTrue(len(idx_paperid_map[0]) == 8)
        self.assertEqual(len(set(idx_paperid_map.values())), cm.size())


if __name__ == '__main__':
    unittest.main()
