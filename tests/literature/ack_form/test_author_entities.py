from wbtools.literature.ack_form.caltech import Row
from wbtools.literature.ack_form.constants import EMPTY_OTHER
from wbtools.literature.ack_form.author import author_entity_tags, authors, in_author_scope
from tests.literature.ack_form.conftest import make_data

ELEGANS = "NCBITaxon:6239"
PAPER, CURIE = "00000001", "AGRKB:101000000000001"
TS = "2024-01-01T00:00:00"
STRAINS = {"strain_by_name": {}, "strain_taxon": {"WB:WBStrain1": ELEGANS, "WB:WBStrain2": ELEGANS}}


def ack(tables, **extra):
    return make_data(tables=tables, ack_papers={PAPER}, **extra)


def test_old_form_and_non_ack_papers_are_out_of_scope():
    old = ack({"afp_strain": {PAPER: Row("", "2018-12-01T00:00:00")}})
    assert in_author_scope(PAPER, old) is False
    not_ack = make_data(tables={"afp_rnai": {PAPER: Row("x", TS)}}, lasttouched={PAPER: TS})
    assert in_author_scope(PAPER, not_ack) is False
    assert author_entity_tags(PAPER, CURIE, not_ack) == ([], [])


def test_partial_submission_is_in_scope():
    data = ack({"afp_strain": {PAPER: Row("", TS)}})
    assert in_author_scope(PAPER, data) is True
    tags, _ = author_entity_tags(PAPER, CURIE, data)
    assert len(tags) == 1 and tags[0]["created_by"] == "unknown_author"


def test_empty_lists_give_topic_tags_and_no_entity_negations():
    data = ack({"afp_strain": {PAPER: Row("", TS)}, "afp_otherstrain": {PAPER: Row(EMPTY_OTHER, TS)},
                "tfp_strain": {PAPER: Row("WBStrain1;%;N2", TS)}}, **STRAINS)
    tags, _ = author_entity_tags(PAPER, CURIE, data)
    assert sorted((t["negated"], t["data_novelty"], "entity" in t) for t in tags) == [
        (True, "ATP:0000228", False), (True, "ATP:0000334", False)]
    assert all(t["topic"] == "ATP:0000027" and t["source_method"] == "ACKnowledge_form" for t in tags)


def test_removed_pipeline_entity_is_negated():
    data = ack({"afp_strain": {PAPER: Row("WBStrain1;%;N2", TS)},
                "tfp_strain": {PAPER: Row("WBStrain1;%;N2 | WBStrain2;%;CB4856", TS)}}, **STRAINS)
    tags, _ = author_entity_tags(PAPER, CURIE, data)
    assert sorted((t["entity"], t["negated"]) for t in tags) == [("WB:WBStrain1", False), ("WB:WBStrain2", True)]
    positive = [t for t in tags if not t["negated"]][0]
    assert positive["note"] == "N2 ( WB:WBStrain1 )" and positive["species"] == ELEGANS


def test_one_tag_set_per_contributor():
    data = ack({"afp_genestudied": {PAPER: Row("00000239;%;bas-1", TS)}},
               gene_taxon={"WB:WBGene00000239": ELEGANS},
               contributors={PAPER: {"WBPerson6": "2024-02-01T00:00:00", "WBPerson5": "2024-03-01T00:00:00"}})
    assert authors(PAPER, data) == [("WBPerson5", "2024-03-01T00:00:00"), ("WBPerson6", "2024-02-01T00:00:00")]
    tags, _ = author_entity_tags(PAPER, CURIE, data)
    assert [(t["created_by"], t["date_created"]) for t in tags] == [
        ("WBPerson5", "2024-03-01T00:00:00"), ("WBPerson6", "2024-02-01T00:00:00")]
    assert all(t["entity"] == "WB:WBGene00000239" for t in tags)


def test_other_species_and_empty_species():
    data = ack({"afp_species": {PAPER: Row("", TS)},
                "afp_otherspecies": {PAPER: Row('[{"id":1,"name":"Rhizobium sp."},{"id":2,"name":"X y"}]', TS)}})
    tags, _ = author_entity_tags(PAPER, CURIE, data)
    by_novelty = {t["data_novelty"]: t for t in tags}
    assert by_novelty["ATP:0000228"]["negated"] is False
    assert by_novelty["ATP:0000228"]["note"] == "Rhizobium sp.\nX y"
    assert by_novelty["ATP:0000334"]["negated"] is True and by_novelty["ATP:0000334"]["topic"] == "ATP:0000123"


def test_positive_species_tags_have_no_species_field():
    data = ack({"afp_species": {PAPER: Row("Caenorhabditis elegans", TS)}},
               species_taxon={"Caenorhabditis elegans": ELEGANS})
    tags, _ = author_entity_tags(PAPER, CURIE, data)
    assert tags[0]["entity"] == ELEGANS and "species" not in tags[0]


def test_date_falls_back_to_lasttouched():
    data = ack({"afp_strain": {PAPER: Row("", "")}}, lasttouched={PAPER: "2024-05-05T00:00:00"})
    tags, _ = author_entity_tags(PAPER, CURIE, data)
    assert tags[0]["date_created"] == "2024-05-05T00:00:00"
