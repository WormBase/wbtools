"""Rebuild the afp_* values the ACKnowledge form and dashboard parse from a paper's ACKnowledge_form tags
(SCRUM-4141): the inverse of author.py, for the tables whose round trip is exact.

Not rebuilt (read from Caltech): afp_marker, which shares its tag with afp_otherexpr (the form only writes
afp_otherexpr).
"""
import json
import re
from typing import Dict, FrozenSet, List, Optional

from wbtools.literature.ack_form.changes import owner
from wbtools.literature.ack_form.constants import (DATATYPE_FLAGS, EMPTY_OTHER, ENTITY_TYPES, NOVELTY_EXISTING,
                                                   NOVELTY_NEW_TO_DB)

NOT_REBUILT = frozenset({"afp_marker"})
REBUILT_FLAGS = [flag for flag in DATATYPE_FLAGS if flag.table not in NOT_REBUILT]
READ_FROM_ABC: FrozenSet[str] = frozenset(
    [entity_type.afp_table for entity_type in ENTITY_TYPES.values()] +
    [entity_type.other_table for entity_type in ENTITY_TYPES.values() if entity_type.other_table] +
    [flag.table for flag in REBUILT_FLAGS])
EMPTY_OTHER_ANTIBODY = '[{"id":1,"name":"","publicationId":""}]'
ITEM_SEPARATOR = " | "
_NOTE_LINE = re.compile(r"^(.*) \( (\S+) \)$")


def _one_copy(tags: List[dict]) -> List[dict]:
    """the tags of one contributor (the lowest owner): every contributor's copy is identical"""
    owners = sorted({owner(tag) or "" for tag in tags})
    return [tag for tag in tags if (owner(tag) or "") == owners[0]] if owners else []


def _blank(value) -> bool:
    return value is None or value == ""


def _form_item(entity_type: str, curie: str, name: Optional[str]) -> str:
    """an entity as the form stores it: "<id>;%;<name>" (genes without WBGene), species by name"""
    if entity_type == "species":
        return name or curie
    short_id = curie.split(":", 1)[1]
    if entity_type == "gene":
        short_id = short_id.replace("WBGene", "", 1)
    return f"{short_id};%;{name}" if name else short_id


def _entities(entity_type: str, tags: List[dict]) -> Optional[str]:
    topic = ENTITY_TYPES[entity_type].topic
    section = [tag for tag in tags if tag.get("topic") == topic and tag.get("data_novelty") == NOVELTY_EXISTING]
    positive = [tag for tag in section if not _blank(tag.get("entity")) and not tag.get("negated")]
    if not positive:
        return "" if any(_blank(tag.get("entity")) and tag.get("negated") for tag in section) else None
    names: Dict[str, Optional[str]] = {}
    for line in (positive[0].get("note") or "").split("\n"):
        match = _NOTE_LINE.match(line.strip())
        if match:
            names[match.group(2)] = match.group(1)
        elif line.strip():
            names[line.strip()] = None
    curies = [curie for curie in names if any(tag["entity"] == curie for tag in positive)]
    curies += sorted({tag["entity"] for tag in positive} - set(curies))
    return ITEM_SEPARATOR.join(_form_item(entity_type, curie, names.get(curie)) for curie in curies)


def _other(entity_type: str, tags: List[dict]) -> Optional[str]:
    topic = ENTITY_TYPES[entity_type].topic
    section = [tag for tag in tags if tag.get("topic") == topic and tag.get("data_novelty") == NOVELTY_NEW_TO_DB
               and _blank(tag.get("entity"))]
    if not section:
        return None
    tag = section[0]
    if tag.get("negated"):
        return EMPTY_OTHER
    # the note holds the raw "name" values of the form's JSON (author.other_note): decode each one and encode the
    # list again, so the value is always valid JSON, even for a name the note holds cut at an escaped quote
    names = [_decoded(raw) for raw in (tag.get("note") or "").split("\n")]
    return json.dumps([{"id": i, "name": name} for i, name in enumerate(names, 1)], ensure_ascii=False,
                      separators=(",", ":"))


def _decoded(raw: str) -> str:
    try:
        return json.loads('"' + raw + '"')
    except ValueError:
        return raw


def _flag_tag(tags: List[dict], topic: str, novelty: str) -> Optional[dict]:
    for tag in tags:
        if tag.get("topic") == topic and tag.get("data_novelty") == novelty and _blank(tag.get("entity")):
            return tag
    return None


def _otherantibody(tag: dict) -> str:
    if tag.get("negated") or _blank(tag.get("note")):
        return EMPTY_OTHER_ANTIBODY
    entries = []
    for i, part in enumerate(tag["note"].split(ITEM_SEPARATOR), 1):
        fields = part.split("; ", 1)
        entries.append({"id": i, "name": fields[0], "publicationId": fields[1] if len(fields) > 1 else ""})
    return json.dumps(entries, separators=(",", ":"))


def _humandisease(tag: dict) -> str:
    """the form's disease JSON: the note's first line is the comment, each further line a disease (author.py)"""
    lines = (tag.get("note") or "").split("\n")
    if tag.get("negated") and lines == [""]:
        return ""
    return json.dumps({"checked": not tag.get("negated"), "comment": lines[0], "diseases": lines[1:]})


def _flag(datatype: str, tag: Optional[dict]) -> Optional[str]:
    if tag is None:
        return None
    if datatype == "otherantibody":
        return _otherantibody(tag)
    if datatype == "humandisease":
        return _humandisease(tag)
    if tag.get("negated"):
        return ""
    return tag.get("note") or "checked"


def afp_values_from_tags(tags: List[dict]) -> Dict[str, Optional[str]]:
    """the afp_* values of the tables in READ_FROM_ABC, from a paper's ACKnowledge_form tags; None = unanswered"""
    copy = _one_copy(tags)
    values: Dict[str, Optional[str]] = {}
    for name, entity_type in ENTITY_TYPES.items():
        values[entity_type.afp_table] = _entities(name, copy)
        if entity_type.other_table:
            values[entity_type.other_table] = _other(name, copy)
    for flag in REBUILT_FLAGS:
        values[flag.table] = _flag(flag.datatype, _flag_tag(copy, flag.topic, flag.novelty))
    return values
