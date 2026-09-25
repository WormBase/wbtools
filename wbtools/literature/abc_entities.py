import logging
from typing import Dict, List, Tuple

import requests

from wbtools.literature.paper import ABC_API, ABCRequestError, DEFAULT_REQUEST_TIMEOUT
from wbtools.utils.auth_utils import get_authentication_token, generate_headers

logger = logging.getLogger(__name__)

ENTITY_EXTRACTOR_SOURCE_METHOD = "abc_entity_extractor"
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
        tags_by_curie = _get_entity_tags(batch)
        for curie in batch:
            tags = tags_by_curie.get(curie) or []
            if not tags:
                logger.warning(f"No entity extraction tags in ABC for {curie}")
            for tag in tags:
                entity_type = TOPIC_ENTITY_TYPES.get(tag.get("topic"))
                entity = tag.get("entity")
                source_method = (tag.get("tag_source") or {}).get("source_method")
                if entity_type is None or not entity or tag.get("negated") or \
                        source_method != ENTITY_EXTRACTOR_SOURCE_METHOD:
                    continue
                if entity.startswith(ENTITY_PREFIXES[entity_type]):
                    entities[curie][entity_type].append((entity, tag.get("entity_name")))
    return entities


def _get_entity_tags(agr_curies: List[str]) -> dict:
    headers = generate_headers(get_authentication_token())
    body = {"curies_or_reference_ids": agr_curies,
            "filters": {"source_methods": [ENTITY_EXTRACTOR_SOURCE_METHOD], "topics": list(TOPIC_ENTITY_TYPES)}}
    try:
        response = requests.post(f"https://{ABC_API}/topic_entity_tag/by_references", json=body, headers=headers,
                                 timeout=DEFAULT_REQUEST_TIMEOUT)
        response.raise_for_status()
        result = response.json()
    except (requests.exceptions.RequestException, ValueError) as e:
        raise ABCRequestError(f"ABC topic entity tag request failed: {e}") from e
    if not isinstance(result, dict) or not isinstance(result.get("tags"), dict):
        raise ABCRequestError(f"ABC topic entity tag request failed: {result}")
    return result["tags"]
