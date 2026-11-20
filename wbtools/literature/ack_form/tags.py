"""Tag payloads and the keys used to compare expected tags with ABC tags."""
from typing import Optional, Tuple

# the fields of the ABC TopicEntityTagSchemaPost (extra="forbid": any other field is rejected with 422)
ALLOWED_CREATE_FIELDS = {
    "reference_curie", "force_insertion", "index_wft", "topic", "entity_type", "entity", "entity_id_validation",
    "species", "display_tag", "tag_source_id", "negated", "data_novelty", "data_context", "confidence_level",
    "confidence_score", "note", "validation_by_author", "validation_by_professional_biocurator", "ml_model_id",
    "date_created", "date_updated", "created_by", "updated_by"}

PIPELINE_KEY_FIELDS = ("topic", "entity_type", "entity", "species", "negated", "data_novelty", "confidence_level")


def make_tag(**fields) -> dict:
    """an ABC tag payload: None values dropped, updated_by/date_updated mirror created_by/date_created"""
    tag = {field: value for field, value in fields.items() if value is not None}
    tag["force_insertion"] = True
    if "created_by" in tag:
        tag["updated_by"] = tag["created_by"]
    if "date_created" in tag:
        tag["date_updated"] = tag["date_created"]
    return tag


def source_method(tag: dict) -> str:
    """the source method of an ABC tag (tag_source) or of an expected tag (source_method)"""
    if "source_method" in tag:
        return tag["source_method"]
    return (tag.get("tag_source") or {}).get("source_method", "")


def _value(tag: dict, field: str):
    value = tag.get(field)
    return None if value == "" else value


def pipeline_key(tag: dict) -> Tuple[Optional[object], ...]:
    return tuple(_value(tag, field) for field in PIPELINE_KEY_FIELDS)


def author_key(tag: dict) -> Tuple[Optional[object], ...]:
    """the note counts for topic-only tags (flags, "other" answers), so an edited answer is synced; entity tags
    are matched without it, so a tag the Perl scripts created keeps its id and validations, and its note is
    updated in place (diff._entity_note_updates)"""
    note = None if _value(tag, "entity") else _value(tag, "note")
    return pipeline_key(tag)[:-1] + (source_method(tag), _value(tag, "created_by"), note)
