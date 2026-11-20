from wbtools.literature.ack_form.author import author_tags
from wbtools.literature.ack_form.caltech import Row
from wbtools.literature.ack_form.changes import (UNKNOWN_AUTHOR, answer_set, contributors, plan_changes,
                                                 section_keys)
from tests.literature.ack_form.conftest import make_data

PAPER, CURIE, TS = "00000001", "AGRKB:101000000000001", "2024-01-01T00:00:00"
SOURCE = 77
GENE, RNAI, GENEINT, STRAIN = "ATP:0000005", "ATP:0000082", "ATP:0000068", "ATP:0000027"
EXISTING, CLASSIFICATION = "ATP:0000334", "ATP:0000335"
ELEGANS = "NCBITaxon:6239"


def gene_tag(gene, note, **fields):
    return {"topic": GENE, "entity_type": GENE, "entity": gene, "species": ELEGANS, "entity_id_validation": "alliance",
            "negated": False, "data_novelty": EXISTING, "note": note, "source_method": "ACKnowledge_form",
            "created_by": UNKNOWN_AUTHOR, "updated_by": UNKNOWN_AUTHOR, **fields}


def flag_tag(topic, negated, note=None, **fields):
    return {"topic": topic, "negated": negated, "data_novelty": CLASSIFICATION, "note": note,
            "source_method": "ACKnowledge_form", "created_by": UNKNOWN_AUTHOR, "updated_by": UNKNOWN_AUTHOR,
            **fields}


def abc(tag, tag_id, who):
    """a tag as the ABC returns it: id, owner, no source_method"""
    shown = {k: v for k, v in tag.items() if k != "source_method"}
    shown.update(topic_entity_tag_id=tag_id, created_by=who, created_by_user_id=who, updated_by=who)
    return shown


TEMPLATE = [gene_tag("WB:WBGene00000830", "ctl-1 ( WB:WBGene00000830 )"), flag_tag(RNAI, False, "in L4")]
ALL = section_keys(["afp_genestudied", "afp_rnai", "afp_geneint"])


def plan(template, abc_tags, people, sections=ALL, saver="WBPerson2", protect=frozenset()):
    return plan_changes(template, abc_tags, people, sections, saver, SOURCE, protect)


def test_first_partial_save_creates_full_copy():
    a_tags = [abc(TEMPLATE[0], 1, "WBPerson1"), abc(TEMPLATE[1], 2, "WBPerson1")]
    batch = plan(TEMPLATE, a_tags, ["WBPerson1", "WBPerson2"], sections=section_keys(["afp_rnai"]))
    assert batch.update == [] and batch.delete == []
    assert sorted((t["topic"], t["created_by"]) for t in batch.create) == [(GENE, "WBPerson2"), (RNAI, "WBPerson2")]
    for tag in batch.create:
        assert tag["updated_by"] == "WBPerson2" and tag["tag_source_id"] == SOURCE
        assert tag["force_insertion"] is True and "source_method" not in tag


def test_flip_yes_no_updates_every_copy():
    abc_tags = [abc(TEMPLATE[1], 2, "WBPerson1"), abc(TEMPLATE[1], 3, "WBPerson2")]
    batch = plan([flag_tag(RNAI, True)], abc_tags, ["WBPerson1", "WBPerson2"], sections=section_keys(["afp_rnai"]))
    assert batch.create == [] and batch.delete == []
    assert sorted(u["topic_entity_tag_id"] for u in batch.update) == [2, 3]
    for update in batch.update:
        assert update["changes"] == {"negated": True, "note": None, "updated_by": "WBPerson2"}


def test_cleared_details_sends_null_note():
    abc_tags = [abc(TEMPLATE[1], 2, "WBPerson1")]
    batch = plan([flag_tag(RNAI, False, None)], abc_tags, ["WBPerson1"], saver="WBPerson1")
    assert batch.update == [{"topic_entity_tag_id": 2, "changes": {"note": None, "updated_by": "WBPerson1"}}]


def test_empty_note_equals_none():
    abc_tags = [abc(flag_tag(RNAI, False, ""), 2, "WBPerson1")]
    assert plan([flag_tag(RNAI, False, None)], abc_tags, ["WBPerson1"]).is_empty()


def test_removed_entity_is_deleted_for_every_contributor():
    old_gene = gene_tag("WB:WBGene00000002", "aap-1 ( WB:WBGene00000002 )")
    abc_tags = [abc(old_gene, 5, "WBPerson1"), abc(old_gene, 6, "WBPerson2")]
    batch = plan([], abc_tags, ["WBPerson1", "WBPerson2"], sections=section_keys(["afp_genestudied"]))
    assert sorted(batch.delete) == [5, 6] and batch.create == [] and batch.update == []


def test_added_entity_creates_and_updates_section_notes():
    both = "ctl-1 ( WB:WBGene00000830 )\naap-1 ( WB:WBGene00000002 )"
    template = [gene_tag("WB:WBGene00000830", both), gene_tag("WB:WBGene00000002", both)]
    abc_tags = [abc(TEMPLATE[0], 1, "WBPerson1"), abc(TEMPLATE[0], 2, "WBPerson2")]
    batch = plan(template, abc_tags, ["WBPerson1", "WBPerson2"], sections=section_keys(["afp_genestudied"]))
    assert sorted((t["entity"], t["created_by"]) for t in batch.create) == [
        ("WB:WBGene00000002", "WBPerson1"), ("WB:WBGene00000002", "WBPerson2")]
    assert sorted(u["topic_entity_tag_id"] for u in batch.update) == [1, 2]
    assert all(u["changes"] == {"note": both, "updated_by": "WBPerson2"} for u in batch.update)


def test_unsaved_sections_are_left_alone():
    abc_tags = [abc(TEMPLATE[1], 2, "WBPerson1"), abc(flag_tag(GENEINT, False, "old"), 3, "WBPerson1")]
    template = [TEMPLATE[1], flag_tag(GENEINT, True)]
    batch = plan(template, abc_tags, ["WBPerson1"], sections=section_keys(["afp_rnai"]))
    assert batch.is_empty()


def test_unresolved_entity_topic_is_protected():
    strain = {"topic": STRAIN, "entity_type": STRAIN, "entity": "WB:WBStrain00000001", "negated": False,
              "data_novelty": EXISTING, "note": "N2"}
    batch = plan([], [abc(strain, 9, "WBPerson1")], ["WBPerson1"], sections=section_keys(["afp_strain"]),
                 protect={STRAIN})
    assert batch.delete == []


def test_no_change_is_empty_batch():
    abc_tags = [abc(TEMPLATE[0], 1, "WBPerson1"), abc(TEMPLATE[1], 2, "WBPerson1")]
    batch = plan(TEMPLATE, abc_tags, ["WBPerson1"], saver="WBPerson1")
    assert batch.is_empty() and batch.size() == 0


def test_validated_deletes_are_reported():
    validated = dict(abc(flag_tag(RNAI, False), 2, "WBPerson1"),
                     validation_by_professional_biocurator="validated_wrong")
    batch = plan([], [validated], ["WBPerson1"], sections=section_keys(["afp_rnai"]))
    assert batch.delete == [2] and batch.deleted_validated == [validated]


def test_owner_read_from_created_by_user_id():
    # the ABC may show a display name in created_by; the raw id is in created_by_user_id (SCRUM-6852)
    tag = dict(abc(TEMPLATE[1], 2, "WBPerson1"), created_by="Jane Doe")
    assert plan([TEMPLATE[1]], [tag], ["WBPerson1"], saver="WBPerson1").is_empty()


def test_contributors_union():
    data = make_data(contributors={PAPER: {"WBPerson1": TS}})
    abc_tags = [abc(TEMPLATE[1], 2, UNKNOWN_AUTHOR)]
    assert contributors(data, PAPER, abc_tags, "WBPerson5") == ["WBPerson1", "WBPerson5", UNKNOWN_AUTHOR]


def test_unknown_author_is_a_contributor_and_kept_identical():
    abc_tags = [abc(TEMPLATE[1], 2, UNKNOWN_AUTHOR)]
    people = contributors(make_data(), PAPER, abc_tags, "WBPerson5")
    batch = plan([flag_tag(RNAI, True)], abc_tags, people, sections=section_keys(["afp_rnai"]), saver="WBPerson5")
    assert [u["topic_entity_tag_id"] for u in batch.update] == [2]
    assert [t["created_by"] for t in batch.create] == ["WBPerson5"]


def test_answer_set_matches_sync_output_for_one_contributor():
    tables = {"afp_rnai": {PAPER: Row("in L4", TS)}, "afp_strain": {PAPER: Row("", TS)}}
    data = make_data(tables=tables, ack_papers={PAPER}, contributors={PAPER: {"WBPerson1": TS}})
    template, problems = answer_set(PAPER, CURIE, data)
    expected, _ = author_tags(PAPER, CURIE, make_data(tables=tables, ack_papers={PAPER}))
    assert template == expected and problems == []
    assert data.contributors == {PAPER: {"WBPerson1": TS}}  # the caller's data is not changed


def test_section_keys_antibody_and_otherantibody_differ():
    assert section_keys(["afp_antibody"]) != section_keys(["afp_otherantibody"])
    assert section_keys(["afp_otherstrain"]) == {(STRAIN, "ATP:0000228")}
    assert section_keys(["afp_strain"]) == {(STRAIN, EXISTING)}
    assert section_keys(["afp_comment"]) == set()


def test_legacy_marker_does_not_override_expression():
    # afp_marker and afp_otherexpr share one tag (ATP:0000041): the form only writes otherexpr, so a legacy positive
    # marker must not keep the tag positive when the author unchecks expression (review I3)
    tables = {"afp_marker": {PAPER: Row("GFP marker", TS)}, "afp_otherexpr": {PAPER: Row("", TS)},
              "afp_strain": {PAPER: Row("", TS)}}
    template, _ = answer_set(PAPER, CURIE, make_data(tables=tables, ack_papers={PAPER}))
    expression = [tag for tag in template if tag["topic"] == "ATP:0000041"]
    assert len(expression) == 1 and expression[0]["negated"] is True
    abc_tags = [abc(flag_tag("ATP:0000041", False, "GFP marker | in neurons"), 4, "WBPerson1")]
    batch = plan(template, abc_tags, ["WBPerson1"], sections=section_keys(["afp_otherexpr"]), saver="WBPerson1")
    assert batch.update == [{"topic_entity_tag_id": 4,
                             "changes": {"negated": True, "note": None, "updated_by": "WBPerson1"}}]
