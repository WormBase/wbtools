import logging
from typing import Dict, List, Optional

import requests

from wbtools.literature.abc_entities import (BATCH_SIZE, ENTITY_EXTRACTOR_SOURCE_METHOD, ENTITY_TYPES, MOD,
                                             TOPIC_ENTITY_TYPES, entity_from_tag, fetch_topic_entity_tags)
from wbtools.literature.paper import ABC_API, ABCRequestError, DEFAULT_REQUEST_TIMEOUT
from wbtools.utils.auth_utils import get_authentication_token, generate_headers

logger = logging.getLogger(__name__)

ACK_PIPELINE_SOURCE_METHOD = "ACKnowledge_pipeline"
DOCUMENT_CLASSIFIER_SOURCE_METHOD = "abc_document_classifier"
ENTITY_SOURCE_METHODS = (ENTITY_EXTRACTOR_SOURCE_METHOD, ACK_PIPELINE_SOURCE_METHOD)
CLASSIFICATION_SOURCE_METHODS = (DOCUMENT_CLASSIFIER_SOURCE_METHOD, ACK_PIPELINE_SOURCE_METHOD)
CLASSIFICATION_TOPICS = (
    "ATP:0000041",  # expression
    "ATP:0000061",  # catalytic activity
    "ATP:0000068",  # genetic interaction
    "ATP:0000069",  # physical interaction
    "ATP:0000070",  # regulatory interaction
    "ATP:0000082",  # RNAi phenotype
    "ATP:0000083",  # allele phenotype
    "ATP:0000084",  # overexpression phenotype
)
# the fields of an entity tag and of a classification tag that are needed to write tags derived from them
ENTITY_TAG_FIELDS = ("topic", "entity_type", "species", "entity_id_validation", "data_novelty", "data_context")
CLASSIFICATION_FIELDS = ("confidence_level", "confidence_score", "data_novelty", "data_context", "ml_model_version")


def empty_paper_tags() -> dict:
    """the value get_abc_paper_tags returns for a reference without tags"""
    return {"entities": {source: {entity_type: [] for entity_type in ENTITY_TYPES}
                         for source in ENTITY_SOURCE_METHODS},
            "entity_tag_fields": {source: {} for source in ENTITY_SOURCE_METHODS},
            "has_ack_pipeline_entity_tags": False,
            "classifications": {source: {} for source in CLASSIFICATION_SOURCE_METHODS}}


def get_abc_paper_tags(agr_curies: List[str], timeout: int = DEFAULT_REQUEST_TIMEOUT) -> Dict[str, dict]:
    """get the WB entities and classifications of the given references with one ABC call per batch

    Args:
        agr_curies (List[str]): AGRKB curies of the references
        timeout (int): request timeout in seconds

    Returns:
        Dict[str, dict]: for each curie:
            "entities": the (entity curie, entity name) pairs by entity type, for each entity source method;
            "entity_tag_fields": the fields of the first tag of each entity, for each entity source method;
            "has_ack_pipeline_entity_tags": whether the reference has any ACKnowledge_pipeline entity tag, negated
                                            ones included;
            "classifications": the latest tag of each classification topic, for each classification source method
                               (highest ml_model_version, then newest; a tag without a version is the oldest).

    Raises:
        ABCRequestError: if the ABC request fails
    """
    paper_tags = {curie: empty_paper_tags() for curie in agr_curies}
    source_methods = list(dict.fromkeys(ENTITY_SOURCE_METHODS + CLASSIFICATION_SOURCE_METHODS))
    for start in range(0, len(agr_curies), BATCH_SIZE):
        batch = agr_curies[start:start + BATCH_SIZE]
        tags_by_curie = fetch_topic_entity_tags(batch, {
            "source_methods": source_methods, "topics": list(TOPIC_ENTITY_TYPES) + list(CLASSIFICATION_TOPICS),
            "mods": [MOD]}, timeout=timeout)
        for curie in batch:
            for tag in tags_by_curie.get(curie) or []:
                _add_tag(paper_tags[curie], tag)
    return paper_tags


def _add_tag(paper_tags: dict, tag: dict):
    tag_source = tag.get("tag_source") or {}
    source = tag_source.get("source_method")
    topic = tag.get("topic")
    if tag_source.get("secondary_data_provider_abbreviation", MOD) != MOD:
        return
    if topic in TOPIC_ENTITY_TYPES and source in ENTITY_SOURCE_METHODS:
        if source == ACK_PIPELINE_SOURCE_METHOD:
            # negated "no entity found" tags count too: they record that ACKnowledge processed the paper
            paper_tags["has_ack_pipeline_entity_tags"] = True
        entity = entity_from_tag(tag)
        if entity:
            entity_type, entity_curie, entity_name = entity
            paper_tags["entities"][source][entity_type].append((entity_curie, entity_name))
            paper_tags["entity_tag_fields"][source].setdefault(
                entity_curie, {field: tag.get(field) for field in ENTITY_TAG_FIELDS})
    elif topic in CLASSIFICATION_TOPICS and source in CLASSIFICATION_SOURCE_METHODS and not tag.get("entity"):
        classification = {"negated": bool(tag.get("negated")), "date_created": tag.get("date_created") or ""}
        classification.update({field: tag.get(field) for field in CLASSIFICATION_FIELDS})
        classifications = paper_tags["classifications"][source]
        current = classifications.get(topic)
        # a paper can be classified more than once: the latest model wins, then the newest tag
        if current is None or _recency(classification) > _recency(current):
            classifications[topic] = classification


def _recency(classification: dict) -> tuple:
    version = classification["ml_model_version"]
    return (-1 if version is None else version), classification["date_created"]


def get_abc_curie(wb_paper_id: str, timeout: int = DEFAULT_REQUEST_TIMEOUT) -> Optional[str]:
    """get the AGRKB curie of a WB paper

    Args:
        wb_paper_id (str): the WB paper id without prefix (e.g. "00069459")
        timeout (int): request timeout in seconds

    Returns:
        Optional[str]: the curie, or None if the paper is not in the ABC

    Raises:
        ABCRequestError: if the ABC request fails
    """
    headers = generate_headers(get_authentication_token())
    url = f"https://{ABC_API}/reference/by_cross_reference/WB:WBPaper{wb_paper_id}"
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()["curie"]
    except (requests.exceptions.RequestException, ValueError, KeyError, TypeError) as e:
        raise ABCRequestError(f"ABC cross reference request failed for WBPaper{wb_paper_id}: {e}") from e
