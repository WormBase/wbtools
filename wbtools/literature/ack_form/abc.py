"""The ABC calls of an ACKnowledge save: read a paper's ACKnowledge_form tags, send the save's changes as one batch
(POST /topic_entity_tag/batch, SCRUM-4141)."""
from typing import Dict, List, Optional

import requests

from wbtools.literature.ack_form.changes import TagBatch
from wbtools.literature.ack_form.constants import ACK_FORM, SOURCES
from wbtools.literature.paper import ABC_API, ABCRequestError
from wbtools.utils.auth_utils import generate_headers, get_authentication_token

# the ABC refuses larger batches; refusing here saves the call
MAX_BATCH_CHANGES = 5000
_SOURCE_IDS: Dict[str, int] = {}
# ABC person curie -> the person's bare WB person id (None: the person has no WB cross reference)
_WB_PERSON_IDS: Dict[str, Optional[str]] = {}
WB_PERSON_XREF_PREFIX = "WB:WBPerson"


def _post(path: str, body: dict, timeout: int) -> requests.Response:
    headers = generate_headers(get_authentication_token())
    try:
        return requests.post(f"https://{ABC_API}{path}", json=body, headers=headers, timeout=timeout)
    except requests.exceptions.RequestException as e:
        raise ABCRequestError(f"ABC {path} failed: {e}") from e


def get_ack_form_source_id(timeout: int) -> int:
    """the id of the WB ACKnowledge_form tag source, looked up once per process

    Raises:
        ABCRequestError: if the ABC can't be read
    """
    if ACK_FORM not in _SOURCE_IDS:
        path = f"/tag_source/{SOURCES[ACK_FORM]}/{ACK_FORM}/WB/WB"
        try:
            response = requests.get(f"https://{ABC_API}{path}",
                                    headers=generate_headers(get_authentication_token()), timeout=timeout)
        except requests.exceptions.RequestException as e:
            raise ABCRequestError(f"ABC {path} failed: {e}") from e
        if response.status_code != 200:
            raise ABCRequestError(f"ABC {path} failed: HTTP {response.status_code} {response.text[:200]}")
        _SOURCE_IDS[ACK_FORM] = response.json()["tag_source_id"]
    return _SOURCE_IDS[ACK_FORM]


def wb_person_id(person_curie: str, timeout: int) -> Optional[str]:
    """the bare WB person id (WBPerson1843) of an ABC person, from the person's WB cross reference; None if they have
    none. Looked up once per process.

    Raises:
        ABCRequestError: if the ABC can't be read
    """
    if person_curie not in _WB_PERSON_IDS:
        path = f"/person_cross_reference/person/{person_curie}"
        try:
            response = requests.get(f"https://{ABC_API}{path}",
                                    headers=generate_headers(get_authentication_token()), timeout=timeout)
        except requests.exceptions.RequestException as e:
            raise ABCRequestError(f"ABC {path} failed: {e}") from e
        if response.status_code == 404:
            _WB_PERSON_IDS[person_curie] = None
            return None
        if response.status_code != 200:
            raise ABCRequestError(f"ABC {path} failed: HTTP {response.status_code} {response.text[:200]}")
        try:
            xrefs = response.json()
            wb_ids = sorted(xref["curie"].split(":", 1)[1] for xref in xrefs
                            if xref["curie"].startswith(WB_PERSON_XREF_PREFIX) and not xref.get("is_obsolete"))
        except (ValueError, KeyError, TypeError, AttributeError) as e:
            raise ABCRequestError(f"ABC {path} gave an unexpected answer: {e}") from e
        _WB_PERSON_IDS[person_curie] = wb_ids[0] if wb_ids else None
    return _WB_PERSON_IDS[person_curie]


def _owner_as_wb_person(tag: dict, timeout: int) -> dict:
    """the tag with its owner as the WB person id ACKnowledge uses, when the ABC gives the person's ABC curie

    The ABC records a WB person it knows under their ABC person curie (SCRUM-6729), whatever form of the id it was
    sent. ACKnowledge tells contributors apart by WB person id, so the owner is mapped back; the ABC's own value is
    kept in abc_created_by_user_id.
    """
    raw = tag.get("created_by_user_id") or tag.get("created_by")
    if not isinstance(raw, str) or not raw.startswith("AGRKB:"):
        return tag
    wb_id = wb_person_id(raw, timeout)
    if wb_id is None:
        return tag
    return dict(tag, created_by_user_id=wb_id, abc_created_by_user_id=raw)


def get_ack_form_tags(agr_curie: str, timeout: int) -> List[dict]:
    """the paper's ACKnowledge_form tags, every contributor's copy, owned by WB person ids (see _owner_as_wb_person)

    Raises:
        ABCRequestError: if the ABC can't be read
    """
    response = _post("/topic_entity_tag/by_references", {
        "curies_or_reference_ids": [agr_curie], "filters": {"source_methods": [ACK_FORM]}}, timeout)
    if response.status_code != 200:
        raise ABCRequestError(f"ABC by_references failed for {agr_curie}: HTTP {response.status_code} "
                              f"{response.text[:500]}")
    try:
        tags = response.json()["tags"]
    except (ValueError, KeyError, TypeError) as e:
        raise ABCRequestError(f"ABC by_references gave an unexpected answer for {agr_curie}: {e}") from e
    return [_owner_as_wb_person(tag, timeout) for tag in tags.get(agr_curie) or []]


def send_tag_batch(agr_curie: str, batch: TagBatch, timeout: int) -> List[int]:
    """apply a save's tag changes in one ABC transaction; returns the ids of the created tags

    Raises:
        ABCRequestError: if the batch is over the cap or the ABC refuses it or can't be reached (the ABC then stored
            nothing, unless the answer was lost after the ABC committed)
    """
    if batch.size() > MAX_BATCH_CHANGES:
        raise ABCRequestError(f"{batch.size()} tag changes for {agr_curie}; the ABC accepts at most "
                              f"{MAX_BATCH_CHANGES} in one batch")
    body = {"reference_curie": agr_curie,
            "create": [dict(tag, reference_curie=agr_curie) for tag in batch.create],
            "update": batch.update, "delete": batch.delete}
    response = _post("/topic_entity_tag/batch", body, timeout)
    if response.status_code != 200:
        raise ABCRequestError(f"ABC tag batch refused for {agr_curie}: HTTP {response.status_code} "
                              f"{response.text[:1000]}")
    try:
        return response.json()["created"]
    except (ValueError, KeyError, TypeError) as e:
        raise ABCRequestError(f"ABC tag batch gave an unexpected answer for {agr_curie}: {e}") from e
