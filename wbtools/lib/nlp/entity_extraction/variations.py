import re


VAR_EXCLUSION_LIST = ["p53", "p35", "p21", "p38", "p68", "p120", "p130", "p107", "uv1", "w1", "u1"]


VAR_FALSE_POSITIVE_REGEX = r'(^(and|www|ab)\d+)|(al\d+$)|(^yk\d+)'


VAR_FROM_UNWANTED_PRJ_REGEX = r'^(ok|gk|tm|cx|tt)\d+$'


def is_variation_suspicious(variation):
    return "suspicious" if re.match(r"^p\d+$", variation) or re.match(r"^(c|m|n|k)?m[1-3]$", variation) or \
            re.match(r"^ws\d+$", variation) or re.match(r"^aa\d+", variation) or \
            re.match(r"^x(1|2|3|100|200|500|1000)$", variation) else "potentially new"


def is_new_variation_to_exclude(variation: str):
    return variation in VAR_EXCLUSION_LIST or re.match(VAR_FALSE_POSITIVE_REGEX, variation) or \
        re.match(VAR_FROM_UNWANTED_PRJ_REGEX, variation)
