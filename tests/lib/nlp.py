import os
import unittest

from gensim.models import Word2Vec
from gensim.test.utils import common_texts

from wbtools.lib.nlp import get_softcosine_index, get_similar_documents, get_entities_from_text, ALL_VAR_REGEX, \
    NEW_VAR_REGEX, remove_sections_from_text, PaperSections
from wbtools.literature.corpus import CorpusManager

TESTDATA_DIR = os.path.join(os.path.dirname(__file__), '../data')


class TestNLP(unittest.TestCase):

    def setUp(self) -> None:
        self.cm = CorpusManager()
        self.cm.load_from_dir_with_txt_files(dir_path=os.path.join(TESTDATA_DIR, 'text_files'))

    def test_soft_cosine_similarity(self):
        corpus_list_token, idx_paperid_map = self.cm.get_flat_corpus_list_and_idx_paperid_map(
            split_sentences=False, remove_sections=[PaperSections.REFERENCES], lowercase=True, tokenize=True,
            remove_stopwords=True, remove_alpha=True)
        model = Word2Vec(common_texts, min_count=1)
        docsim_index, dictionary = get_softcosine_index(model=model, corpus_list_token=corpus_list_token)
        query_docs = [corpus_list_token[0], corpus_list_token[1]]
        sim_docs = get_similar_documents(docsim_index, dictionary, query_docs, idx_paperid_map)
        self.assertTrue(idx_paperid_map[0] in [sim_doc.paper_id for sim_doc in sim_docs[0:2]])
        self.assertTrue(idx_paperid_map[1] in [sim_doc.paper_id for sim_doc in sim_docs[0:2]])

        corpus_list_token, idx_paperid_map = self.cm.get_flat_corpus_list_and_idx_paperid_map(
            split_sentences=True, remove_sections=[PaperSections.REFERENCES], lowercase=True, tokenize=True,
            remove_stopwords=True, remove_alpha=True)
        docsim_index, dictionary = get_softcosine_index(model=model, corpus_list_token=corpus_list_token)
        query_docs = [corpus_list_token[0], corpus_list_token[1]]
        sim_docs = get_similar_documents(docsim_index, dictionary, query_docs, idx_paperid_map)
        self.assertTrue(idx_paperid_map[0] in [sim_doc.paper_id for sim_doc in sim_docs[0:2]])
        self.assertTrue(idx_paperid_map[1] in [sim_doc.paper_id for sim_doc in sim_docs[0:2]])

    def test_get_entities_from_text(self):
        text = "  g2 is a new variant"
        variations = get_entities_from_text(text, NEW_VAR_REGEX)
        self.assertTrue(variations)

    def test_sectioning(self):
        text = list(self.cm.corpus.values())[0].get_text_docs()[0]
        self.assertTrue("REFERENCES" not in remove_sections_from_text(
            text=text, sections_to_remove=[PaperSections.INTRODUCTION, PaperSections.REFERENCES],
            must_be_present=[PaperSections.RESULTS]))


if __name__ == '__main__':
    unittest.main()
