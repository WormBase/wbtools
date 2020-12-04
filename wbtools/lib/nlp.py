import itertools
import re
from dataclasses import dataclass
from enum import Enum
from typing import List

import regex
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

NEW_VAR_REGEX = r'[\(\s](' + '|'.join(get_cgc_allele_designations()) + r'|m|p)([0-9]+)((' + \
                '|'.join(get_cgc_allele_designations()) + r'|m|p|ts|gf|lf|d|sd|am|cs)[0-9]+)?[\)\s\[]'


VAR_EXCLUSION_LIST = ["p53", "p35", "p21", "p38", "p68", "p120", "p130", "p107", "uv1", "w1", "u1"]


VAR_FALSE_POSITIVE_REGEX = r'(^(and|www|ab)\d+)|(al\d+$)|(^yk\d+)'


VAR_FROM_UNWANTED_PRJ_REGEX = r'^(ok|gk|tm|cx|tt)\d+$'


@dataclass
class SimilarityResult:
    score: int = 0
    paper_id: str = ''
    match_idx: int = 0
    query_idx: int = 0
    match: str = ''
    query: str = ''


class PaperSections(str, Enum):
    INTRODUCTION = 1
    METHOD = 2
    RELATED_WORK = 3
    RESULTS = 4
    DISCUSSION = 5
    CONCLUSION = 6
    ACKNOWLEDGEMENTS = 7
    REFERENCES = 8


PAPER_SECTIONS = {
    PaperSections.INTRODUCTION: [["introduction"], ["the", "section"], ["section"], 0.2],
    PaperSections.METHOD: [["method", "methods"], ["section"], ["section"], 0.3],
    PaperSections.RELATED_WORK: [["related work", "related works"], ["the", "previous", "section"], ["section"], 0.3],
    PaperSections.RESULTS: [["results"], ["section"], ["section"], 0.5],
    PaperSections.DISCUSSION: [["discussion", "discussions"], ["section"], ["section"], 0.7],
    PaperSections.CONCLUSION: [["conclusion", "conclusions"], ["the", "section"], ["section"], 0.8],
    PaperSections.ACKNOWLEDGEMENTS: [["acknowledgements"], ["the", "section"], ["section"], 0.8],
    PaperSections.REFERENCES: [["references", "literature"], ["the"], [], 0.9]
}


def get_documents_from_text(text: str, split_sentences: bool = False, remove_sections: List[PaperSections] = None,
                            must_be_present: List[PaperSections] = None) -> \
        list:
    if remove_sections:
        text = remove_sections_from_text(text=text, sections_to_remove=remove_sections, must_be_present=must_be_present)
    return sent_tokenize(text) if split_sentences else [text]


def remove_sections_from_text(text: str, sections_to_remove: List[PaperSections] = None,
                              must_be_present: List[PaperSections] = None):
    if sections_to_remove:
        sections_idx = {}
        for section, (section_matches, prefix_to_exclude, postfix_to_exclude, expected_position) in \
                PAPER_SECTIONS.items():
            sections_idx[section] = -1
            pre_match_regex = r"(?<!" + r"\s*|".join(prefix_to_exclude) + r"\s*)" if prefix_to_exclude else ""
            post_match_regex = r"(?!\s*" + r"\s*|".join(postfix_to_exclude) + ")" if postfix_to_exclude else ""
            for string_to_match in section_matches:
                section_match_regex = pre_match_regex + "(" + string_to_match.title() + "|" + \
                                      string_to_match.upper() + ")" + post_match_regex
                matches_idx = [match.start() for match in regex.finditer(section_match_regex, text)]
                if matches_idx:
                    # take the index closest to the expected position, if it's les than 50% off the expected position
                    idx_sorted = sorted([(match_id, match_id / len(text) - expected_position) for match_id in
                                         matches_idx if match_id / len(text) - expected_position < 0.5],
                                        key=lambda x: abs(x[1]))
                    if idx_sorted:
                        sections_idx[section] = idx_sorted[0][0]
                        if sections_idx[section] != -1:
                            break
        if not must_be_present or all([sections_idx[must_sec] != -1 for must_sec in must_be_present]):
            sec_idx_arr = sorted([(section, idx) for section, idx in sections_idx.items() if idx > -1],
                                 key=lambda x: x[1])
            # remove sections too close to each other (e.g., RESULTS AND DISCUSSION)
            sec_idx_arr = [s_i for i, s_i in enumerate(sec_idx_arr) if i == 0 or s_i[1] - sec_idx_arr[i - 1][1] > 20]
            # get first and last position of each section based on the next one
            slices_idx = [(s_i[1], (sec_idx_arr[i + 1][1] - 1) if i < len(sec_idx_arr) - 1 else len(text)) for
                          i, s_i in enumerate(sec_idx_arr) if s_i[0] in sections_to_remove]
            text = "".join([c for i, c in enumerate(text) if not any([slice_idx[0] <= i <= slice_idx[1] for slice_idx in
                                                                      slices_idx])])
    return text


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


def get_entities_from_text(text, regex):
    res = re.findall(regex, " " + text + " ")
    return ["".join(entity_arr) for entity_arr in res]


def is_variation_suspicious(variation):
    return "suspicious" if re.match(r"^p\d+$", variation) or re.match(r"^(c|m|n|k)?m[1-3]$", variation) or \
            re.match(r"^ws\d+$", variation) or re.match(r"^aa\d+", variation) or \
            re.match(r"^x(1|2|3|100|200|500|1000)$", variation) else "potentially new"


def get_new_variations_from_text(text):
    variations = set(get_entities_from_text(text, NEW_VAR_REGEX))
    variations = [var for var in variations if var not in VAR_EXCLUSION_LIST and not re.match(
        VAR_FALSE_POSITIVE_REGEX, var) and not re.match(VAR_FROM_UNWANTED_PRJ_REGEX, var)]
    return [(variation, is_variation_suspicious(variation)) for variation in variations]


