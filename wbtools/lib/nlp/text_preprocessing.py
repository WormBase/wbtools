import regex

from typing import List
from nltk import sent_tokenize, word_tokenize
from nltk.corpus import stopwords

from wbtools.lib.nlp.common import PaperSections, PAPER_SECTIONS


stop_words = set(stopwords.words('english'))


def get_documents_from_text(text: str, split_sentences: bool = False, remove_sections: List[PaperSections] = None,
                            must_be_present: List[PaperSections] = None) -> list:
    if remove_sections:
        text = remove_sections_from_text(text=text, sections_to_remove=remove_sections, must_be_present=must_be_present)
    if split_sentences:
        text = text.replace("Fig.", "Fig")
        text = text.replace("et al.", "et al")
        text = text.replace('.\n\n', '. ')
        text = text.replace('\n\n', '. ')
        text = text.replace('-\n', '')
        return sent_tokenize(text)
    else:
        text = text.replace('-\n', '')
        return [text]


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
                    # take the index closest to the expected position, if it's less than 50% off the expected position
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
