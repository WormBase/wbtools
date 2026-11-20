from wbtools.literature.ack_form.caltech import Row
from wbtools.literature.ack_form.author import author_flag_tags, author_tags, convert_flag
from tests.literature.ack_form.conftest import make_data

PAPER, CURIE = "00000001", "AGRKB:101000000000001"
TS = "2024-01-01T00:00:00"


def test_otherantibody():
    assert convert_flag("otherantibody", '[{"name":"anti-X","publicationId":"PMID:1"},{"name":""}]') == (
        False, "anti-X; PMID:1")
    assert convert_flag("otherantibody", '[{"name":"a"},{"name":"b"}]') == (False, "a | b")
    assert convert_flag("otherantibody", "not json") == (True, "")
    assert convert_flag("otherantibody", '[{"id":1,"name":""}]') == (True, "")


def test_humandisease():
    # SCRUM-4141: the first note line is the comment (empty when there is none), each further line a disease; the
    # author's own newlines become spaces, so a newline in the note is only ever a separator
    assert convert_flag("humandisease", "") == (True, "")
    assert convert_flag("humandisease", None) == (True, "")
    assert convert_flag("humandisease", '{"checked":true,"comment":"checked","diseases":["Parkinson disease ( PD ) "]}'
                        ) == (False, "\nParkinson disease (PD)")
    assert convert_flag("humandisease", '{"checked":false,"comment":"model","diseases":[]}') == (True, "model")
    assert convert_flag("humandisease", "free text") == (False, "free text")
    assert convert_flag("humandisease", '{"checked": true, "comment": "seen, in\\nmutants", '
                                        '"diseases": ["ALS (DOID:332)", "PD\\n(DOID:14330)"]}'
                        ) == (False, "seen, in mutants\nALS (DOID:332)\nPD (DOID:14330)")
    assert convert_flag("humandisease", '{"checked": true, "comment": "", "diseases": []}') == (False, "")


def test_plain_flags():
    assert convert_flag("rnai", "0") == (True, "")
    assert convert_flag("rnai", "") == (True, "")
    assert convert_flag("rnai", None) == (True, "")
    assert convert_flag("rnai", "line1\nline2 ") == (False, "line1 line2")
    assert convert_flag("rnai", "checked") == (False, "checked")


def test_flag_tags_per_author():
    data = make_data(tables={"afp_rnai": {PAPER: Row("yes", TS)}, "afp_antibody": {PAPER: Row("", TS)},
                             "afp_strain": {PAPER: Row("", TS)}},
                     ack_papers={PAPER}, contributors={PAPER: {"WBPerson5": "2024-02-01T00:00:00"}})
    tags = author_flag_tags(PAPER, CURIE, data)
    by_topic = {t["topic"]: t for t in tags}
    assert by_topic["ATP:0000082"]["negated"] is False and by_topic["ATP:0000082"]["note"] == "yes"
    assert by_topic["ATP:0000082"]["data_novelty"] == "ATP:0000335"
    assert by_topic["ATP:0000096"]["negated"] is True and "note" not in by_topic["ATP:0000096"]
    assert by_topic["ATP:0000096"]["data_novelty"] == "ATP:0000229"
    assert all(t["created_by"] == "WBPerson5" and t["date_created"] == "2024-02-01T00:00:00" for t in tags)


def test_partial_submission_keeps_its_flags():
    data = make_data(tables={"afp_rnai": {PAPER: Row("yes", TS)}, "afp_strain": {PAPER: Row("", TS)}},
                     ack_papers={PAPER})
    tags, _ = author_tags(PAPER, CURIE, data)
    assert {t["topic"] for t in tags} == {"ATP:0000082", "ATP:0000027"}
    assert all(t["created_by"] == "unknown_author" for t in tags)


def test_flags_without_ack_sections_follow_the_date_rule():
    # early ACKnowledge submissions (2019) have flag rows but no entity sections
    data = make_data(tables={"afp_rnai": {PAPER: Row("yes", "2019-04-01T00:00:00")}})
    tags, _ = author_tags(PAPER, CURIE, data)
    assert [(t["topic"], t["created_by"]) for t in tags] == [("ATP:0000082", "unknown_author")]


def test_old_author_first_pass_flags_are_out_of_scope():
    data = make_data(tables={"afp_rnai": {PAPER: Row("yes", "2018-12-01T00:00:00")}})
    assert author_tags(PAPER, CURIE, data) == ([], [])


def test_flags_of_one_topic_merge_their_notes():
    # marker and otherexpr are both ATP:0000041: the ABC keeps one tag with the notes joined, in creation order
    data = make_data(tables={"afp_marker": {PAPER: Row("checked", TS)}, "afp_otherexpr": {PAPER: Row("GFP", TS)},
                             "afp_strain": {PAPER: Row("", TS)}},
                     ack_papers={PAPER}, contributors={PAPER: {"WBPerson5": "2024-02-01T00:00:00"}})
    expression = [t for t in author_flag_tags(PAPER, CURIE, data) if t["topic"] == "ATP:0000041"]
    assert [t["note"] for t in expression] == ["checked | GFP"]


def test_flags_of_one_topic_with_different_negation_stay_apart():
    data = make_data(tables={"afp_marker": {PAPER: Row("", TS)}, "afp_otherexpr": {PAPER: Row("GFP", TS)},
                             "afp_strain": {PAPER: Row("", TS)}},
                     ack_papers={PAPER}, contributors={PAPER: {"WBPerson5": "2024-02-01T00:00:00"}})
    expression = [t for t in author_flag_tags(PAPER, CURIE, data) if t["topic"] == "ATP:0000041"]
    assert sorted((t["negated"], t.get("note")) for t in expression) == [(False, "GFP"), (True, None)]


def test_humandisease_free_text_has_no_newline():
    assert convert_flag("humandisease", "line one\nline two") == (False, "line one line two")
