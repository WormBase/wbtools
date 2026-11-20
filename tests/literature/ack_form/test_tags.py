from wbtools.literature.ack_form.constants import DATATYPE_FLAGS, ENTITY_TYPES
from wbtools.literature.ack_form.tags import author_key, make_tag, pipeline_key, source_method


def test_make_tag_drops_none_and_mirrors_audit_fields():
    tag = make_tag(topic="ATP:0000005", entity=None, created_by="WBPerson1", date_created="2020-01-01T00:00:00")
    assert "entity" not in tag
    assert tag["force_insertion"] is True
    assert tag["updated_by"] == "WBPerson1"
    assert tag["date_updated"] == "2020-01-01T00:00:00"


def test_pipeline_key_treats_empty_and_missing_alike():
    abc_tag = {"topic": "ATP:0000005", "entity_type": "", "negated": True, "data_novelty": "ATP:0000334",
               "tag_source": {"source_method": "ACKnowledge_pipeline"}}
    expected = make_tag(topic="ATP:0000005", negated=True, data_novelty="ATP:0000334",
                        source_method="ACKnowledge_pipeline")
    assert pipeline_key(abc_tag) == pipeline_key(expected)


def test_author_key_differs_on_created_by():
    one = make_tag(topic="ATP:0000005", negated=True, source_method="ACKnowledge_form", created_by="WBPerson1")
    two = make_tag(topic="ATP:0000005", negated=True, source_method="ACKnowledge_form", created_by="WBPerson2")
    assert author_key(one) != author_key(two)
    assert pipeline_key(one) == pipeline_key(two)


def test_source_method_of_abc_and_expected_tags():
    assert source_method({"tag_source": {"source_method": "ACKnowledge_form"}}) == "ACKnowledge_form"
    assert source_method({"source_method": "ACKnowledge_pipeline"}) == "ACKnowledge_pipeline"


def test_datatype_flags_are_the_25_ack_flags():
    assert len(DATATYPE_FLAGS) == 25
    assert len({flag.datatype for flag in DATATYPE_FLAGS}) == 25
    flags = {flag.datatype: flag for flag in DATATYPE_FLAGS}
    assert flags["antibody"].novelty == "ATP:0000229"
    assert flags["otherantibody"].novelty == "ATP:0000334"
    assert flags["humandisease"].table == "afp_humdis"
    assert not {"extvariation", "newstrains", "exprmosaic", "transporter"} & set(flags)


def test_entity_types():
    assert ENTITY_TYPES["gene"].other_table is None
    assert ENTITY_TYPES["species"].other_table == "afp_otherspecies"
    assert ENTITY_TYPES["strain"].topic == "ATP:0000027"
