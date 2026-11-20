import pytest

from wbtools.literature.ack_form.author import author_tags
from wbtools.literature.ack_form.caltech import Row
from wbtools.literature.ack_form.constants import EMPTY_OTHER
from wbtools.literature.ack_form.reverse import READ_FROM_ABC, afp_values_from_tags
from tests.literature.ack_form.conftest import make_data

PAPER, CURIE, TS = "00000001", "AGRKB:101000000000001", "2024-01-01T00:00:00"
ELEGANS = "NCBITaxon:6239"
LOOKUPS = dict(
    species_taxon={"Caenorhabditis elegans": ELEGANS, "Caenorhabditis briggsae": "NCBITaxon:6238"},
    gene_taxon={"WB:WBGene00000830": ELEGANS, "WB:WBGene00000002": ELEGANS, "WB:WBGene00000003": ELEGANS},
    variation_taxon={"WB:WBVar00000001": ELEGANS},
    strain_by_name={"N2": "WBStrain00000001"}, strain_taxon={"WB:WBStrain00000001": ELEGANS},
    transgene_by_name={}, transgene_taxon={"WB:WBTransgene00000001": ELEGANS},
)

ROUND_TRIP_CASES = [
    ("afp_genestudied", "00000830;%;ctl-1 | 00000002;%;aap-1 | 00000003"),
    ("afp_genestudied", ""),
    ("afp_species", "Caenorhabditis elegans | Caenorhabditis briggsae"),
    ("afp_variation", "WBVar00000001;%;e1370"),
    ("afp_strain", "WBStrain00000001;%;N2"),
    ("afp_strain", "N2"),
    ("afp_transgene", "WBTransgene00000001;%;syIs1"),
    ("afp_otherstrain", '[{"id":1,"name":"my strain"},{"id":2,"name":"CB4856 (Hawaii)"}]'),
    ("afp_otherstrain", EMPTY_OTHER),
    ("afp_othervariation", '[{"id":1,"name":"new allele"}]'),
    ("afp_othertransgene", '[{"id":1,"name":"new tg"}]'),
    ("afp_otherspecies", '[{"id":1,"name":"Pristionchus"}]'),
    ("afp_rnai", "checked"),
    ("afp_rnai", "RNAi in L4"),
    ("afp_rnai", ""),
    ("afp_structcorr", "the gene model is wrong"),
    ("afp_antibody", "checked"),
    ("afp_otherantibody", '[{"id":1,"name":"anti-GFP","publicationId":"PMID:1"},{"id":2,"name":"anti-X"}]'),
    ("afp_otherantibody", '[{"id":1,"name":"","publicationId":""}]'),
    ("afp_seqchange", "checked"),
    ("afp_otherexpr", "expressed in neurons"),
    ("afp_humdis",
     '{"checked": true, "comment": "seen, in mutants", "diseases": ["ALS (DOID:332)", "PD (DOID:14330)"]}'),
    ("afp_humdis", '{"checked": true, "comment": "", "diseases": ["ALS (DOID:332)"]}'),
    ("afp_humdis", '{"checked": false, "comment": "model only", "diseases": []}'),
    ("afp_humdis", '{"checked": true, "comment": "", "diseases": []}'),
    ("afp_humdis", ""),
]


def _tags(table, value):
    data = make_data(tables={table: {PAPER: Row(value, TS)}}, ack_papers={PAPER}, **LOOKUPS)
    return author_tags(PAPER, CURIE, data)[0]


JSON_TABLES = {"afp_otherstrain", "afp_othervariation", "afp_othertransgene", "afp_otherspecies", "afp_otherantibody"}


def form_value(table, value):
    """what the form shows for a value (AFPValues.js)"""
    if value is None:
        return None
    if table == "afp_humdis":
        import json
        if value == "":
            return None
        parsed = json.loads(value)
        return bool(parsed.get("checked")), parsed.get("comment") or "", parsed.get("diseases") or []
    if table in JSON_TABLES:
        import json
        if table == "afp_otherantibody":
            return [(e.get("name", ""), e.get("publicationId", "")) for e in json.loads(value)
                    if e.get("name") or e.get("publicationId")]
        return [e["name"] for e in json.loads(value) if e["name"]]
    return value != "", ("" if value == "checked" else value)


@pytest.mark.parametrize("table,value", ROUND_TRIP_CASES)
def test_round_trip(table, value):
    tags = _tags(table, value)
    rebuilt = afp_values_from_tags(tags)[table]
    assert table in READ_FROM_ABC
    assert _tags(table, rebuilt) == tags
    if not table.endswith(("genestudied", "species", "variation", "strain", "transgene")) or table in JSON_TABLES:
        assert form_value(table, rebuilt) == form_value(table, value)


def test_entities_keep_the_author_order_and_names():
    rebuilt = afp_values_from_tags(_tags("afp_genestudied", "00000830;%;ctl-1 | 00000002;%;aap-1"))
    assert rebuilt["afp_genestudied"] == "00000830;%;ctl-1 | 00000002;%;aap-1"
    assert afp_values_from_tags(_tags("afp_strain", "N2"))["afp_strain"] == "WBStrain00000001;%;N2"


def test_removed_pipeline_entities_are_not_author_entities():
    data = make_data(tables={"afp_genestudied": {PAPER: Row("00000830;%;ctl-1", TS)},
                             "tfp_genestudied": {PAPER: Row("00000830;%;ctl-1 | 00000002;%;aap-1", TS)}},
                     ack_papers={PAPER}, **LOOKUPS)
    tags = author_tags(PAPER, CURIE, data)[0]
    assert any(t.get("negated") and t.get("entity") for t in tags)
    assert afp_values_from_tags(tags)["afp_genestudied"] == "00000830;%;ctl-1"


def test_no_tags_means_unanswered():
    values = afp_values_from_tags([])
    assert set(values) == READ_FROM_ABC
    assert all(value is None for value in values.values())


def test_one_copy_is_read():
    tags = _tags("afp_rnai", "in L4")
    other_copy = [dict(tag, created_by="WBPerson9", note="stale") for tag in tags]
    assert afp_values_from_tags(other_copy + tags)["afp_rnai"] == "stale"  # the lowest owner, WBPerson9 < unknown


def test_read_from_abc_lists_every_mapped_table_except_marker():
    assert "afp_marker" not in READ_FROM_ABC
    assert "afp_comment" not in READ_FROM_ABC
    assert {"afp_genestudied", "afp_species", "afp_otherspecies", "afp_rnai", "afp_otherantibody",
            "afp_humdis"} <= READ_FROM_ABC


def test_humdis_free_text_reloads_as_comment():
    import json
    rebuilt = afp_values_from_tags(_tags("afp_humdis", "legacy free text"))["afp_humdis"]
    assert json.loads(rebuilt) == {"checked": True, "comment": "legacy free text", "diseases": []}


@pytest.mark.parametrize("value", ['[{"id":1,"name":"foo \\"bar\\" baz"}]', '[{"id":1,"name":"a\\\\b"}]',
                                   '[{"id":1,"name":"café α-strain"}]', 'free text with "quotes"'])
def test_other_answers_always_rebuild_valid_json(value):
    # the form JSON.parses the value: invalid JSON blocks the whole form (review I2)
    import json
    rebuilt = afp_values_from_tags(_tags("afp_otherstrain", value))["afp_otherstrain"]
    assert isinstance(json.loads(rebuilt), list)


def test_unicode_other_names_round_trip():
    value = '[{"id":1,"name":"café α-strain"}]'
    tags = _tags("afp_otherstrain", value)
    rebuilt = afp_values_from_tags(tags)["afp_otherstrain"]
    assert _tags("afp_otherstrain", rebuilt) == tags
    assert form_value("afp_otherstrain", rebuilt) == form_value("afp_otherstrain", value)
