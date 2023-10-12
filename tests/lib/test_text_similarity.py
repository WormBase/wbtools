import os
import unittest

from gensim.models import Word2Vec
from gensim.test.utils import common_texts

from tests.config_reader import read_db_config, load_env_file
from wbtools.lib.nlp.common import PaperSections
from wbtools.lib.nlp.text_similarity import get_softcosine_index, get_similar_documents
from wbtools.literature.corpus import CorpusManager

TESTDATA_DIR = os.path.join(os.path.dirname(__file__), '../data')


class TestTextSimilarity(unittest.TestCase):

    def setUp(self) -> None:
        config = read_db_config()
        load_env_file()
        self.cm = CorpusManager()
        self.cm.load_from_wb_database(db_name=config["wb_database"]["db_name"],
                                      db_user=config["wb_database"]["db_user"],
                                      db_password=config["wb_database"]["db_password"],
                                      db_host=config["wb_database"]["db_host"],
                                      paper_ids=['00026804', '00026805', '00026806', '00026807', '00026808',
                                                 '00026809', '00026810', '00026811', '00026812', '00026813',
                                                 '00026814', '00026815', '00026816', '00026817', '00026818',
                                                 '00026819', '00026820', '00026821', '00026822', '00026823'])

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


if __name__ == '__main__':
    unittest.main()
