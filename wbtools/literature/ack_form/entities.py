"""Parse tfp_*/afp_* values into entities with their curie, published name and species."""
import json
import re
from typing import List, NamedTuple, Optional, Tuple

from wbtools.literature.ack_form.caltech import CaltechData
from wbtools.literature.ack_form.constants import DEFAULT_TRANSGENE_TAXON


class Entity(NamedTuple):
    curie: str
    published_as: Optional[str]
    species: Optional[str]  # never set on species entities: the ABC clears it on species tags


def _split_id(item: str) -> Tuple[str, Optional[str]]:
    """'ID;%;name' -> (ID, name); a plain item -> (item, None)"""
    if ";%;" in item:
        entity_id, name = item.split(";%;", 1)
        return entity_id.strip(), name.strip() or None
    return item.strip(), None


def _resolve(entity_type: str, item: str, data: CaltechData) -> Tuple[Optional[str], Optional[str]]:
    """(curie, published name) of one item, or (None, None)"""
    entity_id, name = _split_id(item)
    if entity_type == "gene":
        return ("WB:WBGene" + entity_id, name) if re.fullmatch(r"\d{8}", entity_id) else (None, None)
    if entity_type == "species":
        taxon = data.species_taxon.get(entity_id)
        return (taxon, entity_id) if taxon else (None, None)
    prefix = {"variation": "WBVar", "strain": "WBStrain", "transgene": "WBTransgene"}[entity_type]
    if entity_id.startswith(prefix):
        return "WB:" + entity_id, name
    by_name = data.strain_by_name if entity_type == "strain" else data.transgene_by_name
    for candidate in (entity_id, re.sub(r"\s+", "", entity_id)):
        if candidate in by_name:  # the name as the author entered it, for the note
            return "WB:" + by_name[candidate], entity_id
    return None, None


def _taxon(entity_type: str, curie: str, data: CaltechData) -> Optional[str]:
    if entity_type == "transgene":
        return data.transgene_taxon.get(curie, DEFAULT_TRANSGENE_TAXON)
    taxa = {"gene": data.gene_taxon, "variation": data.variation_taxon, "strain": data.strain_taxon}[entity_type]
    return taxa.get(curie)


def parse_entities(entity_type: str, value: str, data: CaltechData) -> Tuple[List[Entity], List[str]]:
    """the entities of a tfp_*/afp_* value, in input order without duplicates, and the problems found"""
    entities: List[Entity] = []
    problems: List[str] = []
    seen = set()
    for item in (value or "").split(" | "):
        if not item.strip():
            continue
        curie, name = _resolve(entity_type, item, data)
        if curie is None:
            problems.append(f"unresolved {entity_type} name {item.strip()}")
            continue
        if curie in seen:
            continue
        seen.add(curie)
        if entity_type == "species":
            entities.append(Entity(curie, name, None))
            continue
        taxon = _taxon(entity_type, curie, data)
        if taxon is None:
            problems.append(f"no taxon {curie}")
            continue
        entities.append(Entity(curie, name, taxon))
    return entities, problems


def other_names(value: str) -> List[str]:
    """the non-empty names of an afp_other* JSON list ("other" free text); free text that is not JSON as is"""
    if not value or not value.strip():
        return []
    try:
        entries = json.loads(value)
    except ValueError:
        return [value.strip()]
    if not isinstance(entries, list):
        return [value.strip()]
    return [str(e.get("name")).strip() for e in entries
            if isinstance(e, dict) and e.get("name") is not None and str(e.get("name")).strip()]


def other_note(value: str) -> str:
    """the note of an "other" answer tag as the Perl scripts wrote it: the raw "name" values of the JSON text
    (escapes and spaces kept) joined by newlines, so unchanged answers compare equal"""
    return "\n".join(re.findall(r'"name":"(.*?)"', value or ""))
