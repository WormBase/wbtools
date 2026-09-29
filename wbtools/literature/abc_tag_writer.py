import logging
from typing import List, Optional

import requests

from wbtools.literature.paper import ABC_API, ABCRequestError, DEFAULT_REQUEST_TIMEOUT
from wbtools.utils.auth_utils import get_authentication_token, generate_headers

logger = logging.getLogger(__name__)

# 409 reasons meaning that the ABC already has the tag
ALREADY_STORED_REASONS = ("duplicate", "different_creator")


def create_topic_entity_tags(agr_curie: str, tags: List[dict], timeout: int = DEFAULT_REQUEST_TIMEOUT) -> int:
    """create topic entity tags for a reference, with one POST /topic_entity_tag/ per tag

    Tags that the ABC already has (409 duplicate or different_creator) are skipped, so posting the same tags again
    completes a partial earlier write.

    Args:
        agr_curie (str): the AGRKB curie of the reference
        tags (List[dict]): the tag payloads, without reference_curie
        timeout (int): request timeout in seconds

    Returns:
        int: the number of tags created

    Raises:
        ABCRequestError: if a tag can't be created; the tags posted before it stay in the ABC
    """
    if not tags:
        return 0
    headers = generate_headers(get_authentication_token())
    created = 0
    for tag in tags:
        payload = dict(tag, reference_curie=agr_curie)
        try:
            response = requests.post(f"https://{ABC_API}/topic_entity_tag/", json=payload, headers=headers,
                                     timeout=timeout)
        except requests.exceptions.RequestException as e:
            raise ABCRequestError(f"ABC tag creation failed for {agr_curie} {_describe(tag)}: {e}") from e
        if response.status_code in (200, 201):
            created += 1
        elif response.status_code != 409 or _conflict_reason(response) not in ALREADY_STORED_REASONS:
            raise ABCRequestError(f"ABC tag creation failed for {agr_curie} {_describe(tag)}: "
                                  f"HTTP {response.status_code} {response.text[:500]}")
    return created


def _conflict_reason(response) -> Optional[str]:
    try:
        body = response.json()
    except ValueError:
        return None
    detail = body.get("detail") if isinstance(body, dict) else None
    return detail.get("reason") if isinstance(detail, dict) else None


def _describe(tag: dict) -> str:
    return f"(topic {tag.get('topic')}, entity {tag.get('entity')}, negated {tag.get('negated')})"
