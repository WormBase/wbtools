import itertools

from gensim.models import KeyedVectors
from nltk import word_tokenize
from nltk.corpus import stopwords
from gensim.corpora import Dictionary
from gensim.models import WordEmbeddingSimilarityIndex
from gensim.similarities import SoftCosineSimilarity, SparseTermSimilarityMatrix
from nltk import sent_tokenize, word_tokenize
from nltk.corpus import stopwords


stop_words = set(stopwords.words('english'))


def remove_references(document):
    try:
        document = document[0:document.rindex("References")]
    except:
        pass
    try:
        document = document[0:document.rindex("REFERENCES")]
    except:
        pass
    return document


def get_documents_from_text(text: str, split_sentences: bool = False, remove_ref_section: bool = False) -> list:
    if remove_ref_section:
        text = remove_references(text)
    return sent_tokenize(text) if split_sentences else [text]


def preprocess(doc, lower: bool = False, tokenize: bool = False, remove_stopwords: bool = False,
               remove_alpha: bool = False):
    if lower:
        doc = doc.lower()
    if tokenize:
        doc = word_tokenize(doc)
    if remove_stopwords:
        doc = [w for w in doc if w not in stop_words]  # Remove stopwords.
    if remove_alpha:
        doc = [w for w in doc if w.isalpha()]  # Remove numbers and special characters
    return doc


def get_softcosine_index(model_path: str = '', model=None, corpus_list_token: list = None):
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
    return SoftCosineSimilarity(bow_corpus, similarity_matrix, num_best=10), dictionary


def get_similar_documents(similarity_index, dictionary, query_documents, idx_filename_map):
    sims = similarity_index[dictionary.doc2bow(list(itertools.chain.from_iterable(query_documents)))]
    result_list = [idx_filename_map[i] for i in [a[0] for a in sims]]
    score_list = [a[1] for a in sims]
    return [(score, result) for score, result in zip(score_list, result_list)]