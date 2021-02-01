import itertools
from gensim.models import KeyedVectors
from gensim.corpora import Dictionary
from gensim.models import WordEmbeddingSimilarityIndex
from gensim.similarities import SoftCosineSimilarity, SparseTermSimilarityMatrix

from wbtools.lib.nlp.common import SimilarityResult


def get_softcosine_index(model_path: str = '', model=None, corpus_list_token: list = None, num_best: int = 10):
    if model_path:
        model = KeyedVectors.load_word2vec_format(model_path, binary=True)
    elif model:
        model = model.wv
    else:
        raise Exception('no model or model path provided')
    termsim_index = WordEmbeddingSimilarityIndex(model)
    dictionary = Dictionary(corpus_list_token)
    bow_corpus = [dictionary.doc2bow(doc) for doc in corpus_list_token]
    similarity_matrix = SparseTermSimilarityMatrix(termsim_index, dictionary)
    return SoftCosineSimilarity(bow_corpus, similarity_matrix, num_best=num_best), dictionary


def get_similar_documents(similarity_index, dictionary, query_documents, idx_filename_map, average_match: bool = True):
    if average_match:
        res = [SimilarityResult(score=sim[1], paper_id=idx_filename_map[sim[0]], match_idx=sim[0], query_idx=-1)
               for sim in similarity_index[dictionary.doc2bow(list(itertools.chain.from_iterable(query_documents)))]]
    else:
        res = [SimilarityResult(score=sim[1], paper_id=idx_filename_map[sim[0]], match_idx=sim[0], query_idx=q_idx) for
               q_idx, query_doc in enumerate(query_documents) for sim in similarity_index[dictionary.doc2bow(
                query_doc)]]
    return sorted(res, key=lambda x: x.score, reverse=True)
