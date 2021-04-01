import math
import re
from typing import List, Dict

from wbtools.db.generic import WBGenericDBManager
from wbtools.lib.nlp.common import EntityType
from wbtools.lib.nlp.literature_index.abstract_index import AbstractLiteratureIndex

ALL_VAR_REGEX = r'({designations}|m|p|It)(_)?([A-z]+)?([0-9]+)([a-zA-Z]{{1,4}}[0-9]*)?(\[[0-9]+\])?([a-zA-Z]{{1,4}}' \
                r'[0-9]*)?(\[.+\])?'

NEW_VAR_REGEX = r'[\(\s]({designations}|m|p)([0-9]+)((?:{designations}|m|p|ts|gf|lf|d|sd|am|cs)[0-9]+)?[\)\s\[]'

STRAIN_REGEX = r'[\(\s,\.:;\'\"]({designations})([0-9]+)[\)\s\,\.:;\'\"]'


OPENING_REGEX_STR = "[\\.\\n\\t\\'\\/\\(\\)\\[\\]\\{\\}:;\\,\\!\\?> ]"
CLOSING_REGEX_STR = "[\\.\\n\\t\\'\\/\\(\\)\\[\\]\\{\\}:;\\,\\!\\?> ]"


OPENING_CLOSING_REGEXES = {
    EntityType.VARIATION: [r'[\(\s](', r')[\)\s\[]'],
    EntityType.STRAIN: [r'[\(\s,\.:;\'\"](', r')[\)\s,\.:;\'\"]']
}


class NttExtractor:

    def __init__(self, db_manager: WBGenericDBManager = None):
        self.db_manager = db_manager
        self.curated_entities = {}
        for entity_type in EntityType:
            self.curated_entities[entity_type] = None
        allele_designations = self.db_manager.get_allele_designations()
        new_var_regex = NEW_VAR_REGEX.format(designations="|".join(allele_designations))
        strain_regex = STRAIN_REGEX.format(designations="|".join(self.db_manager.get_strain_designations()))
        self.entity_type_regex_map = {
            EntityType.VARIATION: new_var_regex,
            EntityType.STRAIN: strain_regex
        }

    def get_curated_entities(self, entity_type: EntityType, exclude_id_used_as_name: bool = True):
        if not self.curated_entities[entity_type]:
            self.curated_entities[entity_type] = self.db_manager.get_curated_entities(
                entity_type=entity_type, exclude_id_used_as_name=exclude_id_used_as_name)
        return self.curated_entities[entity_type]

    @staticmethod
    def match_entities_regex(text, regex):
        res = re.findall(regex, " " + text + " ")
        return ["".join(entity_arr) for entity_arr in res]

    @staticmethod
    def count_keyword_matches_regex(keyword, text, case_sensitive: bool = True,
                                    match_uppercase: bool = False) -> int:
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

    @staticmethod
    def is_entity_meaningful(entity_keywords: List[str], text, lit_index: AbstractLiteratureIndex,
                             match_uppercase: bool = False, min_num_occurrences: int = 1,
                             tfidf_threshold: float = 0.0) -> bool:
        min_num_occurrences = 1 if min_num_occurrences < 1 else min_num_occurrences
        raw_count = sum(NttExtractor.count_keyword_matches_regex(keyword=keyword, text=text,
                                                                 match_uppercase=match_uppercase) for
                        keyword in entity_keywords)
        return True if raw_count >= min_num_occurrences and (
                tfidf_threshold <= 0 or 0 < tfidf_threshold < NttExtractor.tfidf(entity_keywords=entity_keywords,
                                                                                 raw_count=raw_count,
                                                                                 lit_index=lit_index)) else False

    @staticmethod
    def tfidf(entity_keywords: List[str], raw_count, lit_index: AbstractLiteratureIndex) -> float:
        doc_counter = sum(lit_index.count_matching_documents(keyword) for keyword in entity_keywords)
        idf = math.log(float(lit_index.num_documents()) / (doc_counter if doc_counter > 0 else 0.5))
        return raw_count * idf

    @staticmethod
    def extract_meaningful_entities_by_keywords(keywords: List[str], text: str,
                                                lit_index: AbstractLiteratureIndex = None,
                                                match_uppercase: bool = False, min_matches: int = 1,
                                                tfidf_threshold: float = 0.0,
                                                blacklist: List[str] = None) -> List[str]:
        blacklist = set(blacklist) if blacklist else set()
        return [keyword for keyword in set(keywords) if keyword not in blacklist and
                NttExtractor.is_entity_meaningful(
            entity_keywords=[keyword], text=text, match_uppercase=match_uppercase, min_num_occurrences=min_matches,
            tfidf_threshold=tfidf_threshold, lit_index=lit_index)]

    def extract_species_regex(self, text: str, taxon_id_name_map: Dict[str, List[str]] = None,
                              blacklist: List[str] = None,
                              whitelist: List[str] = None, min_matches: int = 1, tfidf_threshold: float = 0.0,
                              lit_index: AbstractLiteratureIndex = None):
        blacklist = set(blacklist) if blacklist else set()
        whitelist = set(whitelist) if whitelist else set()
        if taxon_id_name_map is None:
            taxon_id_name_map = self.db_manager.get_taxon_id_names_map()
        return [regex_list[0].replace("\\", "") for taxon_id, regex_list in taxon_id_name_map.items() if
                taxon_id not in blacklist and (taxon_id in whitelist or
                                               NttExtractor.is_entity_meaningful(entity_keywords=regex_list, text=text,
                                                                                 match_uppercase=False,
                                                                                 lit_index=lit_index,
                                                                                 min_num_occurrences=min_matches,
                                                                                 tfidf_threshold=tfidf_threshold))]

    @staticmethod
    def get_entity_ids_from_names(entity_names: List[str], entity_name_id_map: Dict[str, str]):
        return list(set([(entity_name_id_map[entity_name], entity_name) for entity_name in entity_names]))

    def extract_all_entities_by_type(self, text: str, entity_type: EntityType, include_new: bool = True,
                                     match_curated: bool = False, exclude_curated: bool = False,
                                     match_entities: List[str] = None, exclude_entities: List[str] = None,
                                     exclude_id_used_as_name: bool = True):
        """
        extract entities mentioned in text

        Args:
            text (str): the input text
            entity_type (EntityType): the type of entities to extract
            include_new (bool): whether to include possibly new entities not yet in the curation database
            match_curated (bool): whether to extract curated entities obtained from the provided DB manager
            exclude_curated (bool): whether to remove curated entities obtained from the provided DB manager from the
                                    extracted ones
            match_entities (List[str]): match the provided entities
            exclude_entities (List[str]): exclude the provided entities from the results
            exclude_id_used_as_name (bool): do not extract entity ids when used as names in the DB

        Returns:
            list: the list of entities extracted from text

        """
        entities = set()
        if include_new:
            entities.update(NttExtractor.match_entities_regex(text, self.entity_type_regex_map[entity_type]))
        if match_curated:
            entities.update(NttExtractor.match_entities_regex(
                text, OPENING_CLOSING_REGEXES[entity_type][0] + '|'.join(self.db_manager.get_curated_entities(
                    entity_type=entity_type, exclude_id_used_as_name=exclude_id_used_as_name)) +
                OPENING_CLOSING_REGEXES[entity_type][1]))
        if exclude_curated:
            entities -= set(self.get_curated_entities(entity_type=entity_type, exclude_id_used_as_name=exclude_id_used_as_name))
        if match_entities:
            entities.update(NttExtractor.match_entities_regex(
                text, OPENING_CLOSING_REGEXES[entity_type][0] + '|'.join(match_entities) +
                OPENING_CLOSING_REGEXES[entity_type][1]))
        if exclude_entities:
            entities -= set(exclude_entities)
        return sorted(list(entities))
