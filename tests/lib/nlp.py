import os
import unittest

from gensim.models import Word2Vec
from gensim.test.utils import common_texts

from wbtools.lib.nlp import get_softcosine_index, get_similar_documents
from wbtools.literature.corpus import CorpusManager

TESTDATA_DIR = os.path.join(os.path.dirname(__file__), '../data')


class TestNLP(unittest.TestCase):

    def test_soft_cosine_similarity(self):
        cm = CorpusManager()
        cm.load_from_dir_with_txt_files(dir_path=os.path.join(TESTDATA_DIR, 'text_files'))
        corpus_list_token, idx_paperid_map = cm.get_flat_corpus_list_and_idx_paperid_map(
            split_sentences=False, remove_ref_section=True, lowercase=True, tokenize=True, remove_stopwords=True,
            remove_alpha=True)
        model = Word2Vec(common_texts, min_count=1)
        docsim_index, dictionary = get_softcosine_index(model=model, corpus_list_token=corpus_list_token)
        query_docs = [corpus_list_token[0], corpus_list_token[1]]
        sim_docs = get_similar_documents(docsim_index, dictionary, query_docs, idx_paperid_map)
        self.assertTrue(idx_paperid_map[0] in [sim_doc[1] for sim_doc in sim_docs[0:2]])
        self.assertTrue(idx_paperid_map[1] in [sim_doc[1] for sim_doc in sim_docs[0:2]])


if __name__ == '__main__':
    unittest.main()
