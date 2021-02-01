import re

from wbtools.lib.nlp.entity_extraction.generic import get_entities_from_text_regex
from wbtools.lib.scraping import get_cgc_allele_designations


NEW_VAR_REGEX = r'[\(\s](' + '|'.join(get_cgc_allele_designations()) + r'|m|p)([0-9]+)((' + \
                '|'.join(get_cgc_allele_designations()) + r'|m|p|ts|gf|lf|d|sd|am|cs)[0-9]+)?[\)\s\[]'


VAR_EXCLUSION_LIST = ["p53", "p35", "p21", "p38", "p68", "p120", "p130", "p107", "uv1", "w1", "u1"]


VAR_FALSE_POSITIVE_REGEX = r'(^(and|www|ab)\d+)|(al\d+$)|(^yk\d+)'


VAR_FROM_UNWANTED_PRJ_REGEX = r'^(ok|gk|tm|cx|tt)\d+$'


def is_variation_suspicious(variation):
    return "suspicious" if re.match(r"^p\d+$", variation) or re.match(r"^(c|m|n|k)?m[1-3]$", variation) or \
            re.match(r"^ws\d+$", variation) or re.match(r"^aa\d+", variation) or \
            re.match(r"^x(1|2|3|100|200|500|1000)$", variation) else "potentially new"


def get_new_variations_from_text(text):
    variations = set(get_entities_from_text_regex(text, NEW_VAR_REGEX))
    variations = [var for var in variations if var not in VAR_EXCLUSION_LIST and not re.match(
        VAR_FALSE_POSITIVE_REGEX, var) and not re.match(VAR_FROM_UNWANTED_PRJ_REGEX, var)]
    return [(variation, is_variation_suspicious(variation)) for variation in variations]