from dataclasses import dataclass
from enum import Enum


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
    PaperSections.INTRODUCTION: [["introduction", "summary"], ["the", "section"], ["section"], 0.2],
    PaperSections.METHOD: [["method", "methods"], ["section"], ["section"], 0.3],
    PaperSections.RELATED_WORK: [["related work", "related works"], ["the", "previous", "section"], ["section"], 0.3],
    PaperSections.RESULTS: [["results"], ["section"], ["section"], 0.5],
    PaperSections.DISCUSSION: [["discussion", "discussions"], ["section"], ["section"], 0.7],
    PaperSections.CONCLUSION: [["conclusion", "conclusions"], ["the", "section"], ["section"], 0.8],
    PaperSections.ACKNOWLEDGEMENTS: [["acknowledgements"], ["the", "section"], ["section"], 0.8],
    PaperSections.REFERENCES: [["references", "literature"], ["the"], [], 0.9]
}


class EntityType(Enum):
    GENE = 1
    VARIATION = 2
    SPECIES = 3
    STRAIN = 4
    ANTIBODY = 5
    TRANSGENE = 6


class EntityExtractionType(Enum):
    TFIDF = 1
    OGER = 2


@dataclass
class ExtractedEntity:
    type: EntityType
    entity_extraction_type: EntityExtractionType
    text: str
    mod_id: str


SPECIES_ALIASES = {"9913": ["cow", "bovine", "calf"],
                   "7955": ["zebrafish"],
                   "7227": ["fruitfly", "fruitflies"],
                   "9606": ["human"],
                   "10090": ["mouse", "mice", "murine"],
                   "10116": ["rat"],
                   "559292": ["budding yeast"],
                   "4896": ["fission yeast"]}
