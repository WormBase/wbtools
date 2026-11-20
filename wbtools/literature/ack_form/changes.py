"""Compare a paper's ACKnowledge answer set with every contributor's ACKnowledge_form tags in the ABC and list the
changes as one batch (SCRUM-4141).

Every contributor holds an identical copy of the paper's answer set. A save compares the sections it saved with each
copy: an answer only in the answer set is created, a tag no answer matches is deleted, and a changed yes/no or text is
updated in place (negated, note), so unchanged tags keep their ids and validations. A contributor without a copy yet
gets a full one.
"""
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set, Tuple

from wbtools.literature.ack_form.author import author_tags
from wbtools.literature.ack_form.caltech import CaltechData
from wbtools.literature.ack_form.constants import (DATATYPE_FLAGS, ENTITY_TYPES, NOVELTY_EXISTING,
                                                   NOVELTY_NEW_TO_DB)

UNKNOWN_AUTHOR = "unknown_author"
# validated by other tags; validated_right_self (the tag's own source) is not a validation to report
VALIDATED = {"validated_right", "validated_wrong", "validation_conflict"}

Key = Tuple[Optional[str], Optional[str], Optional[str]]


@dataclass
class TagBatch:
    """the changes of one save: create payloads (without reference_curie), updates and deletes by tag id"""
    create: List[dict] = field(default_factory=list)
    update: List[dict] = field(default_factory=list)
    delete: List[int] = field(default_factory=list)
    deleted_validated: List[dict] = field(default_factory=list)

    def size(self) -> int:
        return len(self.create) + len(self.update) + len(self.delete)

    def is_empty(self) -> bool:
        return self.size() == 0


def _value(tag: dict, name: str):
    value = tag.get(name)
    return None if value == "" else value


def tag_key(tag: dict) -> Key:
    """a tag within one contributor's copy: negated and note are answers, not identity"""
    return _value(tag, "topic"), _value(tag, "entity"), _value(tag, "data_novelty")


def owner(tag: dict) -> Optional[str]:
    """the raw created_by of an ABC tag (created_by may carry a display name, SCRUM-6852)"""
    return tag.get("created_by_user_id") or tag.get("created_by")


def answer_set(joinkey: str, curie: str, data: CaltechData) -> Tuple[List[dict], List[Tuple[str, str]]]:
    """the paper's answers as one copy of ACKnowledge_form tags (created_by unknown_author), and the problems found

    afp_marker is left out: it shares its tag (ATP:0000041) with afp_otherexpr, the only one of the two the form
    writes, so a legacy marker value must not decide the answer to the form's expression question.
    """
    single = deepcopy(data)
    single.contributors = {}
    single.tables.pop("afp_marker", None)
    return author_tags(joinkey, curie, single)


def section_keys(afp_tables: Iterable[str]) -> Set[Tuple[str, str]]:
    """the (topic, data_novelty) of the tags the given afp_* tables produce"""
    tables = set(afp_tables)
    keys = set()
    for entity_type in ENTITY_TYPES.values():
        if entity_type.afp_table in tables:
            keys.add((entity_type.topic, NOVELTY_EXISTING))
        if entity_type.other_table and entity_type.other_table in tables:
            keys.add((entity_type.topic, NOVELTY_NEW_TO_DB))
    for flag in DATATYPE_FLAGS:
        if flag.table in tables:
            keys.add((flag.topic, flag.novelty))
    return keys


def contributors(data: CaltechData, joinkey: str, abc_tags: List[dict], saver: str) -> List[str]:
    """afp_contributor persons, the owners of the paper's ACKnowledge_form tags and the person saving now"""
    people = set((data.contributors.get(joinkey) or {}).keys())
    people.update(owner(tag) for tag in abc_tags if owner(tag))
    people.add(saver)
    return sorted(people)


def _in_sections(tag: dict, sections: Optional[Set[Tuple[str, str]]]) -> bool:
    return sections is None or (_value(tag, "topic"), _value(tag, "data_novelty")) in sections


def _create(tag: dict, person: str, saver: str, tag_source_id: int) -> dict:
    payload = {k: v for k, v in tag.items() if k != "source_method"}
    payload.update(created_by=person, updated_by=saver, tag_source_id=tag_source_id, force_insertion=True)
    return payload


def _changes(expected: dict, existing: dict, saver: str) -> Dict[str, object]:
    changes: Dict[str, object] = {}
    if bool(expected.get("negated")) != bool(existing.get("negated")):
        changes["negated"] = bool(expected.get("negated"))
    if _value(expected, "note") != _value(existing, "note"):
        changes["note"] = _value(expected, "note")
    if changes:
        changes["updated_by"] = saver
    return changes


def plan_changes(template: List[dict], abc_tags: List[dict], people: List[str], sections: Set[Tuple[str, str]],
                 saver: str, tag_source_id: int, protect: Iterable[str] = frozenset()) -> TagBatch:
    """the creates, updates and deletes that make every person's copy equal to the template in the saved sections
    (every section for a person without tags); entity tags of a protected topic are never deleted"""
    protect = set(protect)
    expected: Dict[Key, dict] = {}
    for tag in template:
        expected.setdefault(tag_key(tag), tag)
    batch = TagBatch()
    for person in people:
        own = [tag for tag in abc_tags if owner(tag) == person]
        compared = sections if own else None
        existing: Dict[Key, List[dict]] = {}
        for tag in sorted(own, key=lambda t: t.get("topic_entity_tag_id") or 0):
            if _in_sections(tag, compared):
                existing.setdefault(tag_key(tag), []).append(tag)
        for key, tag in expected.items():
            if not _in_sections(tag, compared):
                continue
            if key not in existing:
                batch.create.append(_create(tag, person, saver, tag_source_id))
                continue
            for current in existing[key]:
                changes = _changes(tag, current, saver)
                if changes:
                    batch.update.append({"topic_entity_tag_id": current["topic_entity_tag_id"], "changes": changes})
        for key, tags in existing.items():
            if key in expected:
                continue
            for tag in tags:
                if _value(tag, "entity") and _value(tag, "topic") in protect:
                    continue
                batch.delete.append(tag["topic_entity_tag_id"])
                if any(tag.get(name) in VALIDATED
                       for name in ("validation_by_author", "validation_by_professional_biocurator")):
                    batch.deleted_validated.append(tag)
    return batch
