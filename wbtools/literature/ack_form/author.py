"""The ACKnowledge_form tags of a paper's ACKnowledge submission, final or partial (spec amendment 12)."""
import json
import re
from typing import List, Optional, Tuple

from wbtools.literature.ack_form.caltech import CaltechData
from wbtools.literature.ack_form.constants import (ACK_FORM, ACK_START, DATATYPE_FLAGS, EMPTY_OTHER,
                                                   ENTITY_TYPES, NOVELTY_EXISTING, NOVELTY_NEW_TO_DB)
from wbtools.literature.ack_form.entities import other_names, other_note, parse_entities
from wbtools.literature.ack_form.tags import author_key, make_tag

AUTHOR_TABLES = [table for entity_type in ENTITY_TYPES.values()
                 for table in (entity_type.afp_table, entity_type.other_table) if table] + \
                [flag.table for flag in DATATYPE_FLAGS]


def _latest_submission(joinkey: str, data: CaltechData) -> str:
    timestamps = [data.tables.get(table, {})[joinkey].timestamp for table in AUTHOR_TABLES
                  if joinkey in data.tables.get(table, {})]
    timestamps.append(data.lasttouched.get(joinkey, ""))
    return max(timestamps)


def in_author_scope(joinkey: str, data: CaltechData) -> bool:
    """an ACKnowledge submission, final or partial (old author first pass submissions are out of scope)"""
    return joinkey in data.ack_papers and _latest_submission(joinkey, data)[:10] >= ACK_START


def authors(joinkey: str, data: CaltechData) -> List[Tuple[str, Optional[str]]]:
    """(WBPerson, contribution timestamp), sorted; unknown_author when the paper has no contributor"""
    contributors = data.contributors.get(joinkey) or {}
    return sorted(contributors.items()) or [("unknown_author", None)]


def _date(contributed: Optional[str], row_ts: Optional[str], joinkey: str, data: CaltechData) -> Optional[str]:
    return contributed or row_ts or data.lasttouched.get(joinkey) or None


def _note(entities) -> str:
    return "\n".join(f"{e.published_as} ( {e.curie} )" if e.published_as else e.curie for e in entities)


def author_entity_tags(joinkey: str, curie: str, data: CaltechData) -> Tuple[List[dict], List[Tuple[str, str]]]:
    """the expected ACKnowledge_form entity tags of a paper, and the problems found as (topic, message)"""
    if not in_author_scope(joinkey, data):
        return [], []
    tags: List[dict] = []
    problems: List[Tuple[str, str]] = []
    paper_authors = authors(joinkey, data)
    for entity_type in ENTITY_TYPES.values():
        afp = data.tables.get(entity_type.afp_table, {}).get(joinkey)
        other = data.tables.get(entity_type.other_table, {}).get(joinkey) if entity_type.other_table else None
        tfp = data.tables.get(entity_type.tfp_table, {}).get(joinkey)
        afp_entities, removed = [], []
        if afp is not None and afp.data:
            afp_entities, afp_problems = parse_entities(entity_type.name, afp.data, data)
            problems.extend((entity_type.topic, f"{entity_type.afp_table}: {p}") for p in afp_problems)
            # an unresolved author item could be any of the pipeline entities: no "removed" negations then
            if tfp is not None and tfp.data and not afp_problems:
                kept = {e.curie for e in afp_entities}
                removed = [e for e in parse_entities(entity_type.name, tfp.data, data)[0] if e.curie not in kept]
        names = other_names(other.data) if other is not None else []
        for who, contributed in paper_authors:
            def tag(row_ts, **fields):
                tags.append(make_tag(reference_curie=curie, source_method=ACK_FORM, topic=entity_type.topic,
                                     created_by=who, date_created=_date(contributed, row_ts, joinkey, data),
                                     **fields))

            for entity in afp_entities:
                tag(afp.timestamp, entity_type=entity_type.topic, entity=entity.curie, species=entity.species,
                    entity_id_validation="alliance", negated=False, data_novelty=NOVELTY_EXISTING,
                    note=_note(afp_entities))
            for entity in removed:
                tag(afp.timestamp, entity_type=entity_type.topic, entity=entity.curie, species=entity.species,
                    entity_id_validation="alliance", negated=True, data_novelty=NOVELTY_EXISTING)
            if afp is not None and afp.data == "":
                tag(afp.timestamp, negated=True, data_novelty=NOVELTY_EXISTING)
            if names:
                tag(other.timestamp, negated=False, data_novelty=NOVELTY_NEW_TO_DB,
                    note=other_note(other.data) or "\n".join(names))
            elif other is not None and other.data == EMPTY_OTHER:
                tag(other.timestamp, negated=True, data_novelty=NOVELTY_NEW_TO_DB)
    return tags, problems


def _otherantibody(value: str) -> Tuple[bool, str]:
    try:
        entries = json.loads(value or "")
    except ValueError:
        return True, ""
    if not isinstance(entries, list):
        return True, ""
    parts = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        fields = [str(entry[key]) for key in ("name", "publicationId") if entry.get(key) not in (None, "")]
        if fields:
            parts.append("; ".join(fields))
    note = " | ".join(parts)
    return note == "", note


def _humandisease(value: Optional[str]) -> Tuple[bool, str]:
    if not value:
        return True, ""
    try:
        parsed = json.loads(value)
    except ValueError:
        return False, _free_text(value)
    if not isinstance(parsed, dict):
        return False, _free_text(value)
    # SCRUM-4141: the first line is the comment (empty when there is none), each further line a disease, so the
    # answer can be rebuilt from the note (reverse.py). The author's own newlines become spaces: a newline in the
    # note is only ever a separator.
    comment = parsed.get("comment")
    comment = "" if comment in (None, "", "checked") else _one_line(str(comment))
    diseases = []
    for disease in parsed.get("diseases") or []:
        disease = re.sub(r"\s+\)", ")", re.sub(r"\(\s+", "(", _one_line(str(disease))))
        if disease:
            diseases.append(disease)
    return not parsed.get("checked"), "\n".join([comment] + diseases) if diseases else comment


def _free_text(value: str) -> str:
    """a disease answer that is not the form's JSON, on one line as every other flag note"""
    note = value.replace("\n", " ")
    return note[:-1] if note.endswith(" ") else note


def _one_line(text: str) -> str:
    return text.replace("\r", " ").replace("\n", " ").rstrip()


def convert_flag(datatype: str, value: Optional[str]) -> Tuple[bool, str]:
    """(negated, note) of an afp_<datatype> value, as convertAckToNote in populate_classifier_topic_entity.pl"""
    if datatype == "otherantibody":
        negated, note = _otherantibody(value or "")
    elif datatype == "humandisease":
        return _humandisease(value)  # its newlines are separators; its text is already on one line
    else:
        negated = value in ("", None, "0")
        note = "" if negated else value
    note = note.replace("\n", " ")
    return negated, note[:-1] if note.endswith(" ") else note


def author_flag_tags(joinkey: str, curie: str, data: CaltechData) -> List[dict]:
    """the expected ACKnowledge_form data-type flag tags of a paper: rows dated from ACK_START on, as
    populate_classifier_topic_entity.pl decides (early ACKnowledge submissions have no entity sections, so the
    ACKnowledge section test of the entity tags does not apply here)"""
    tags = []
    for flag in DATATYPE_FLAGS:
        row = data.tables.get(flag.table, {}).get(joinkey)
        if row is None or row.timestamp[:10] < ACK_START:
            continue
        negated, note = convert_flag(flag.datatype, row.data)
        for who, contributed in authors(joinkey, data):
            tags.append(make_tag(reference_curie=curie, source_method=ACK_FORM, topic=flag.topic, negated=negated,
                                 data_novelty=flag.novelty, note=note or None, created_by=who,
                                 date_created=_date(contributed, row.timestamp, joinkey, data)))
    return _merge_notes(tags)


def _merge_notes(tags: List[dict]) -> List[dict]:
    """two flags of one topic (marker and otherexpr) give tags that differ only in their note; the ABC stores those
    as one tag, the first one created, with the notes joined by " | " (create_tag appends the note), so the
    expected tags are merged the same way"""
    merged: dict = {}
    for tag in tags:
        first = merged.setdefault(author_key({**tag, "note": None}), tag)
        if first is not tag and tag.get("note"):
            notes = first["note"].split(" | ") if first.get("note") else []
            if tag["note"] not in notes:
                first["note"] = " | ".join(notes + [tag["note"]])
    return list(merged.values())


def author_tags(joinkey: str, curie: str, data: CaltechData) -> Tuple[List[dict], List[Tuple[str, str]]]:
    """all the expected ACKnowledge_form tags of a paper (entities, then data-type flags), and the problems found"""
    entity_tags, problems = author_entity_tags(joinkey, curie, data)
    return entity_tags + author_flag_tags(joinkey, curie, data), problems
