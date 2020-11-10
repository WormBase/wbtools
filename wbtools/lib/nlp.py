import itertools
import re

from gensim.models import KeyedVectors
from nltk import word_tokenize
from nltk.corpus import stopwords
from gensim.corpora import Dictionary
from gensim.models import WordEmbeddingSimilarityIndex
from gensim.similarities import SoftCosineSimilarity, SparseTermSimilarityMatrix
from nltk import sent_tokenize, word_tokenize
from nltk.corpus import stopwords

from wbtools.lib.scraping import get_cgc_allele_designations


stop_words = set(stopwords.words('english'))

ALL_VAR_REGEX = r'(' + '|'.join(get_cgc_allele_designations()) + '|m|p|It)(_)?([A-z]+)?([0-9]+)([a-zA-Z]{1,4}[0-9]*)?' \
                                                                 '(\[[0-9]+\])?([a-zA-Z]{1,4}[0-9]*)?(\[.+\])?'

NEW_VAR_REGEX = r'(' + '|'.join(get_cgc_allele_designations()) + '|m|p)([0-9]+)((' + \
                '|'.join(get_cgc_allele_designations()) + '|m|p|ts|gf|lf|d|sd|am|cs)[0-9]+)?'


VAR_EXCLUSION_LIST = ["p53", "p35", "p21", "p38", "p68", "p120", "p130", "p107", "uv1", "w1", "u1"]


VAR_FALSE_POSITIVE_REGEX = r'(^(and|www|ab)\d+)|(al\d+$)|(^yk\d+)'


VAR_FROM_UNWANTED_PRJ_REGEX = r'^(ok|gk|tm|cx|tt)\d+$'


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


def get_entities_from_text(text, regex):
    res = re.findall(regex, text)
    return ["".join(entity_arr) for entity_arr in res]


def get_new_variations_from_text(text):
    variations = get_entities_from_text(text, r"[\( ]" + NEW_VAR_REGEX + r"[\) ]")
    variations = [var for var in variations if var not in VAR_EXCLUSION_LIST and not re.match(
        VAR_FALSE_POSITIVE_REGEX, var) and not re.match(VAR_FROM_UNWANTED_PRJ_REGEX, var)]
    return variations


