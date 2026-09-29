import logging
from typing import Dict, List, Optional, Tuple

import requests

from wbtools.literature.paper import ABC_API, ABCRequestError, DEFAULT_REQUEST_TIMEOUT
from wbtools.utils.auth_utils import get_authentication_token, generate_headers

logger = logging.getLogger(__name__)

ENTITY_EXTRACTOR_SOURCE_METHOD = "abc_entity_extractor"
MOD = "WB"
BATCH_SIZE = 100
ENTITY_TYPES = ("gene", "allele", "strain", "transgene", "species")
TOPIC_ENTITY_TYPES = {
    "ATP:0000005": "gene",
    "ATP:0000285": "allele",  # classical allele, the topic the WB extractor reports alleles with
    "ATP:0000006": "allele",
    "ATP:0000027": "strain",
    "ATP:0000110": "transgene",  # transgenic allele
    "ATP:0000123": "species",
}
# WB entities only: other MODs' entities matched in a WB paper are not pre-populated
ENTITY_PREFIXES = {"gene": "WB:WBGene", "allele": "WB:WBVar", "strain": "WB:WBStrain",
                   "transgene": "WB:WBTransgene", "species": "NCBITaxon:"}


def entity_from_tag(tag: dict) -> Optional[Tuple[str, str, Optional[str]]]:
    """get the WB entity named by a topic entity tag, whatever its source method

    Args:
        tag (dict): a tag returned by /topic_entity_tag/by_references

    Returns:
        Optional[Tuple[str, str, Optional[str]]]: (entity type, entity curie, entity name), or None for negated tags,
                                                  tags of other MODs, non-entity topics and non-WB entities
    """
    entity_type = TOPIC_ENTITY_TYPES.get(tag.get("topic"))
    entity = tag.get("entity")
    tag_source = tag.get("tag_source") or {}
    if entity_type is None or not entity or tag.get("negated") or \
            tag_source.get("secondary_data_provider_abbreviation", MOD) != MOD or \
            not entity.startswith(ENTITY_PREFIXES[entity_type]):
        return None
    return entity_type, entity, tag.get("entity_name")


def fetch_topic_entity_tags(agr_curies: List[str], filters: dict, timeout: int = DEFAULT_REQUEST_TIMEOUT) -> dict:
    """get the topic entity tags of up to BATCH_SIZE references

    Args:
        agr_curies (List[str]): AGRKB curies of the references
        filters (dict): the by_references filters (source_methods, topics, mods)
        timeout (int): request timeout in seconds

    Returns:
        dict: the tags by curie

    Raises:
        ABCRequestError: if the ABC request fails
    """
    headers = generate_headers(get_authentication_token())
    body = {"curies_or_reference_ids": agr_curies, "filters": filters}
    try:
        response = requests.post(f"https://{ABC_API}/topic_entity_tag/by_references", json=body, headers=headers,
                                 timeout=timeout)
        response.raise_for_status()
        result = response.json()
    except (requests.exceptions.RequestException, ValueError) as e:
        raise ABCRequestError(f"ABC topic entity tag request failed: {e}") from e
    if not isinstance(result, dict) or not isinstance(result.get("tags"), dict):
        raise ABCRequestError(f"ABC topic entity tag request failed: {result}")
    return result["tags"]


def get_abc_extracted_entities(agr_curies: List[str]) -> Dict[str, Dict[str, List[Tuple[str, str]]]]:
    """get the entities extracted by the ABC entity extractor for the given references

    Args:
        agr_curies (List[str]): AGRKB curies of the references

    Returns:
        Dict[str, Dict[str, List[Tuple[str, str]]]]: for each curie, the (entity curie, entity name) pairs by entity
                                                    type (gene, allele, strain, transgene, species)

    Raises:
        ABCRequestError: if the ABC request fails
    """
    entities = {curie: {entity_type: [] for entity_type in ENTITY_TYPES} for curie in agr_curies}
    for start in range(0, len(agr_curies), BATCH_SIZE):
        batch = agr_curies[start:start + BATCH_SIZE]
        # scoped to WB: another MOD's extractor can tag the same reference (e.g. with its own species)
        tags_by_curie = fetch_topic_entity_tags(batch, {"source_methods": [ENTITY_EXTRACTOR_SOURCE_METHOD],
                                                        "topics": list(TOPIC_ENTITY_TYPES), "mods": [MOD]})
        for curie in batch:
            tags = tags_by_curie.get(curie) or []
            if not tags:
                logger.warning(f"No entity extraction tags in ABC for {curie}")
            for tag in tags:
                if (tag.get("tag_source") or {}).get("source_method") != ENTITY_EXTRACTOR_SOURCE_METHOD:
                    continue
                entity = entity_from_tag(tag)
                if entity:
                    entity_type, entity_curie, entity_name = entity
                    entities[curie][entity_type].append((entity_curie, entity_name))
    return entities
