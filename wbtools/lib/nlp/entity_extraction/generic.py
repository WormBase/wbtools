import math
import re
from typing import List

from wbtools.lib.nlp.literature_index.abstract_index import AbstractLiteratureIndex
from wbtools.lib.scraping import get_cgc_allele_designations


ALL_VAR_REGEX = r'(' + '|'.join(get_cgc_allele_designations()) + '|m|p|It)(_)?([A-z]+)?([0-9]+)([a-zA-Z]{1,4}[0-9]*)?' \
                                                                 '(\[[0-9]+\])?([a-zA-Z]{1,4}[0-9]*)?(\[.+\])?'

OPENING_REGEX_STR = "[\\.\\n\\t\\'\\/\\(\\)\\[\\]\\{\\}:;\\,\\!\\?> ]"
CLOSING_REGEX_STR = "[\\.\\n\\t\\'\\/\\(\\)\\[\\]\\{\\}:;\\,\\!\\?> ]"


def get_entities_from_text_regex(text, regex):
    res = re.findall(regex, " " + text + " ")
    return ["".join(entity_arr) for entity_arr in res]


def count_keyword_matches_regex(keyword, text, case_sensitive: bool = True, match_uppercase: bool = False) -> int:
    keyword = keyword if case_sensitive else keyword.upper()
    text = text if case_sensitive else text.upper()
    match_uppercase = False if keyword.upper() == keyword else match_uppercase
    if keyword in text or match_uppercase and keyword.upper() in text:
        try:
            match_count = len(re.findall(OPENING_REGEX_STR + re.escape(keyword) + CLOSING_REGEX_STR, text))
            if match_uppercase:
                match_count += len(re.findall(OPENING_REGEX_STR + re.escape(keyword.upper()) +
                                              CLOSING_REGEX_STR, text))
            return match_count
        except:
            pass
    return 0


def extract_entity_string_matching(entity_keywords: List[str], text, lit_index: AbstractLiteratureIndex,
                                   match_uppercase: bool = False, min_num_occurrences: int = 1,
                                   tfidf_threshold: float = 0.0) -> bool:
    min_num_occurrences = 1 if min_num_occurrences < 1 else min_num_occurrences
    raw_count = sum(count_keyword_matches_regex(keyword=keyword, text=text, match_uppercase=match_uppercase) for
                    keyword in entity_keywords)
    return True if raw_count >= min_num_occurrences and (
            tfidf_threshold <= 0 or 0 < tfidf_threshold < tfidf(entity_keywords, raw_count, lit_index)) else \
        False


def tfidf(entity_keywords: List[str], raw_count, lit_index: AbstractLiteratureIndex) -> float:
    doc_counter = sum(lit_index.count_matching_documents(keyword) for keyword in entity_keywords)
    idf = math.log(float(lit_index.num_documents()) / (doc_counter if doc_counter > 0 else 0.5))
    return raw_count * idf


def extract_keywords_regex(keywords: List[str], text: str, lit_index: AbstractLiteratureIndex,
                           match_uppercase: bool = False, min_matches: int = 1, tfidf_threshold: float = 0.0,
                           blacklist: List[str] = None) -> List[str]:
    blacklist = set(blacklist) if blacklist else set()
    return [keyword for keyword in set(keywords) if keyword not in blacklist and extract_entity_string_matching(
        entity_keywords=[keyword], text=text, match_uppercase=match_uppercase, min_num_occurrences=min_matches,
        tfidf_threshold=tfidf_threshold, lit_index=lit_index)]


def extract_species_regex(self, text: str, blacklist: List[str] = None, whitelist: List[str] = None,
                          min_matches: int = 1, tfidf_threshold: float = 0.0):
    blacklist = set(blacklist) if blacklist else set()
    whitelist = set(whitelist) if whitelist else set()
    return [regex_list[0].replace("\\", "") for taxon_id, regex_list in self.taxonid_name_map.items() if
            taxon_id not in blacklist and (taxon_id in whitelist or
                                           self.is_entity_meaningful(entity_keywords=regex_list, text=text,
                                                                     match_uppercase=False,
                                                                     min_num_occurrences=min_matches,
                                                                     tfidf_threshold=tfidf_threshold))]
