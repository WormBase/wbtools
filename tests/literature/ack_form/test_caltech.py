from datetime import datetime, timedelta, timezone

from wbtools.literature.ack_form.caltech import Removal, build_data, valid_paper
from tests.literature.ack_form.conftest import make_data

T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
T1 = datetime(2024, 6, 1, tzinfo=timezone.utc)


def rows(**extra):
    base = {"valid": [("00000001",), ("00000003",)], "merges": [("00000001", "00000002")],
            "agrkb": [("00000001", "AGRKB:101000000000001")]}
    base.update(extra)
    return base


def test_valid_paper_follows_merges_and_stops_on_cycles():
    data = make_data(valid={"00000001"}, merged_into={"00000002": "00000001", "00000005": "00000006",
                                                      "00000006": "00000005"})
    assert valid_paper(data, "00000002") == "00000001"
    assert valid_paper(data, "00000001") == "00000001"
    assert valid_paper(data, "00000005") is None
    assert valid_paper(data, "00000009") is None


def test_own_row_wins_over_a_later_merged_row():
    data = build_data(rows(tfp_variation=[("00000001", "WBVar2;%;b", T0), ("00000002", "WBVar1;%;a", T1)]))
    assert data.tables["tfp_variation"]["00000001"].data == "WBVar2;%;b"


def test_latest_own_row_wins():
    data = build_data(rows(tfp_variation=[("00000001", "WBVar2;%;b", T0), ("00000001", "", T1)]))
    row = data.tables["tfp_variation"]["00000001"]
    assert row.data == "" and row.timestamp == "2024-06-01T00:00:00"


def test_merged_row_used_when_the_survivor_has_none():
    data = build_data(rows(tfp_variation=[("00000002", "WBVar1;%;a", T1)]))
    assert data.tables["tfp_variation"]["00000001"].data == "WBVar1;%;a"


def test_rows_of_invalid_papers_are_dropped():
    data = build_data(rows(tfp_variation=[("00000009", "WBVar1;%;a", T1)]))
    assert data.tables["tfp_variation"] == {}


def test_contributors_of_a_merged_paper_go_to_the_survivor():
    data = build_data(rows(contributors_afp=[("00000002", "two123", T0)]))
    assert data.contributors == {"00000001": {"WBPerson123": "2024-01-01T00:00:00"}}


def test_blackbox_keeps_the_latest_level_upper_cased():
    data = build_data(rows(blackbox=[("00000001", "rnai", "high", T0), ("00000001", "rnai", "neg", T1)]))
    assert data.blackbox == {"00000001": {"rnai": "NEG"}}


def test_ack_papers_have_an_ack_section_row():
    data = build_data(rows(afp_othertransgene=[("00000003", '[{"id":1,"name":""}]', T0)],
                           afp_rnai=[("00000001", "yes", T0)]))
    assert data.ack_papers == {"00000003"}


def test_lookups():
    data = build_data(rows(
        species_index=[("6239", "Caenorhabditis elegans", T0)],
        genes=[("00000239", "Caenorhabditis elegans")],
        variations=[("WBVar1", 'id: WBVar1\nname: "tm1"\nspecies: "Caenorhabditis elegans"')],
        strains=[("WBStrain1", 'name: "N2"\nspecies: "Caenorhabditis elegans"')],
        transgenes=[("WBTransgene1", "ltSi560", "Caenorhabditis elegans")]))
    assert data.species_taxon == {"Caenorhabditis elegans": "NCBITaxon:6239"}
    assert data.gene_taxon == {"WB:WBGene00000239": "NCBITaxon:6239"}
    assert data.variation_taxon == {"WB:WBVar1": "NCBITaxon:6239"}
    assert data.strain_by_name == {"N2": "WBStrain1"}
    assert data.strain_taxon == {"WB:WBStrain1": "NCBITaxon:6239"}
    assert data.transgene_by_name == {"ltSi560": "WBTransgene1"}
    assert data.transgene_taxon == {"WB:WBTransgene1": "NCBITaxon:6239"}
    assert data.agrkb == {"00000001": "AGRKB:101000000000001"}


AUTHOR = 'Inferred_automatically "from author first pass afp_genestudied"'


def history(*entries):
    """h_pap_gene rows (value, pap_order, curator, evidence), an hour apart"""
    return [("00000001", value, order, curator, evidence, T0 + timedelta(hours=n))
            for n, (value, order, curator, evidence) in enumerate(entries)]


def test_contributors_come_from_afp_contributor_only():
    data = build_data(rows(contributors_afp=[("00000001", "two5", T0)],
                           contributors_pap_gene=[("00000001", "two9", T0)]))
    assert data.contributors == {"00000001": {"WBPerson5": "2024-01-01T00:00:00"}}


def test_undefined_contributor_is_dropped():
    assert build_data(rows(contributors_afp=[("00000001", "twoundefined", T0)])).contributors == {}
    data = build_data(rows(contributors_afp=[("00000001", "twoundefined", T0), ("00000001", "two7", T0)]))
    assert data.contributors == {"00000001": {"WBPerson7": "2024-01-01T00:00:00"}}


def test_curator_removal_of_an_author_entry():
    data = build_data(rows(history_gene=history(
        ("00000830;%;ctl-1", 1, "two5", AUTHOR),
        ("00000830;%;ctl-1", 1, "two5", "2024-01-01 00:00:00-08:00"),  # resubmission: same value, other evidence
        (None, 1, "two1843", None))))
    assert data.removals["gene"] == {"00000001": [Removal("00000830", "WBPerson1843", "2024-01-01T02:00:00")]}


def test_removal_of_a_gene_written_as_name_and_id():
    data = build_data(rows(history_gene=history(("daf-16 (WBGene00000912)", 1, "two5", AUTHOR),
                                                (None, 1, "two1843", None)),
                           current_gene=[("00000001", "CELE_F49C12.2 ( WBGene00009872 ) [CELE_F49C12.2]")]))
    assert [r.value for r in data.removals["gene"]["00000001"]] == ["00000912"]


def test_entries_emptied_by_a_merge_are_not_removals():
    merged = [("00000002",) + row[1:] for row in history(("00000912", 1, "two5", AUTHOR), (None, 1, "two1843", None))]
    assert build_data(rows(history_gene=merged)).removals["gene"] == {}


def test_removals_that_are_not_curator_removals_of_author_entries():
    data = build_data(rows(
        history_gene=history(
            ("00000001", 1, "two5", AUTHOR), (None, 1, "two1843", None),  # back in pap_gene
            ("00000002", 2, "two1843", "Manually_connected"), (None, 2, "two1843", None),  # curator's own entry
            ("00000003", 3, "two5", AUTHOR), ("00000004", 3, "two1843", "Manually_connected"),
            (None, 3, "two1843", None)),  # the author's value was replaced before the removal
        current_gene=[("00000001", "00000001")]))
    assert data.removals["gene"] == {}
