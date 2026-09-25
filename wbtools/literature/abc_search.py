import logging
from typing import Dict, Iterator, List, Tuple

import requests

from wbtools.literature.paper import ABC_API, ABCRequestError
from wbtools.utils.auth_utils import get_authentication_token, generate_headers

logger = logging.getLogger(__name__)

SEARCH_PAGE_SIZE = 100
WB_PAPER_XREF_PREFIX = "WB:WBPaper"


def get_wb_paper_ids_from_abc(required_workflow_tags: Dict[str, List[str]], date_created_from: str,
                              date_created_to: str) -> Iterator[Tuple[str, str]]:
    """get the WB papers in the ABC corpus created in the given window that have all the required workflow tags,
    newest first

    Args:
        required_workflow_tags (Dict[str, List[str]]): ABC search facet name (e.g. "file_workflow") -> ATP ids that
                                                       must all be present on the paper
        date_created_from (str): first creation date to include, YYYY-MM-DD
        date_created_to (str): last creation date to include, YYYY-MM-DD

    Returns:
        Iterator[Tuple[str, str]]: WBPaper id (without the WBPaper prefix) and AGRKB curie of each paper

    Raises:
        ABCRequestError: if the ABC search fails
    """
    facets_values = {"mods_in_corpus.keyword": ["WB"]}
    facets_values.update(required_workflow_tags)
    page = 1
    while True:
        hits = _search_references({"facets_values": facets_values,
                                   "date_created": [date_created_from, date_created_to],
                                   "sort": [{"date_created": {"order": "desc"}}],
                                   "size_result_count": SEARCH_PAGE_SIZE, "page": page})
        for hit in hits:
            wb_paper_id = _get_wb_paper_id(hit)
            if wb_paper_id is None:
                logger.warning(f"Skipping ABC reference {hit.get('curie')}: no WBPaper cross-reference")
                continue
            yield wb_paper_id, hit["curie"]
        if len(hits) < SEARCH_PAGE_SIZE:
            return
        page += 1


def _search_references(body: dict) -> list:
    headers = generate_headers(get_authentication_token())
    try:
        response = requests.post(f"https://{ABC_API}/search/references/", json=body, headers=headers, timeout=300)
        response.raise_for_status()
        result = response.json()
    except (requests.exceptions.RequestException, ValueError) as e:
        raise ABCRequestError(f"ABC search failed: {e}") from e
    if not isinstance(result, dict) or result.get("error") or "hits" not in result:
        raise ABCRequestError(f"ABC search failed: {result}")
    return result["hits"]


def _get_wb_paper_id(hit: dict):
    for xref in hit.get("cross_references") or []:
        if xref.get("curie", "").startswith(WB_PAPER_XREF_PREFIX) and \
                str(xref.get("is_obsolete")).lower() != "true":
            return xref["curie"][len(WB_PAPER_XREF_PREFIX):]
    return None
