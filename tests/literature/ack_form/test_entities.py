from wbtools.literature.ack_form.constants import EMPTY_OTHER
from wbtools.literature.ack_form.entities import Entity, other_names, parse_entities
from tests.literature.ack_form.conftest import make_data

ELEGANS = "NCBITaxon:6239"


def test_genes_with_names_and_taxa():
    data = make_data(gene_taxon={"WB:WBGene00000239": ELEGANS, "WB:WBGene00000403": ELEGANS})
    entities, problems = parse_entities("gene", "00000239;%;bas-1 | 00000403;%;casy-1", data)
    assert entities == [Entity("WB:WBGene00000239", "bas-1", ELEGANS), Entity("WB:WBGene00000403", "casy-1", ELEGANS)]
    assert problems == []


def test_gene_without_taxon_is_dropped_and_reported():
    entities, problems = parse_entities("gene", "00000239;%;bas-1", make_data())
    assert entities == []
    assert problems == ["no taxon WB:WBGene00000239"]


def test_strain_by_id_name_and_stripped_name():
    data = make_data(strain_by_name={"N2": "WBStrain00000001", "CB4856": "WBStrain00000002"},
                     strain_taxon={"WB:WBStrain00000001": ELEGANS, "WB:WBStrain00000002": ELEGANS,
                                   "WB:WBStrain00000003": ELEGANS})
    entities, problems = parse_entities("strain", "WBStrain00000003;%;JK1 | N2 | CB 4856 | nonsense", data)
    assert [e.curie for e in entities] == ["WB:WBStrain00000003", "WB:WBStrain00000001", "WB:WBStrain00000002"]
    assert [e.published_as for e in entities] == ["JK1", "N2", "CB 4856"]
    assert problems == ["unresolved strain name nonsense"]


def test_transgene_defaults_to_elegans_and_resolves_names():
    data = make_data(transgene_by_name={"oxEx1578": "WBTransgene00014794"})
    entities, _ = parse_entities("transgene", "WBTransgene00020977;%;ltSi560 | oxEx1578", data)
    assert entities == [Entity("WB:WBTransgene00020977", "ltSi560", ELEGANS),
                        Entity("WB:WBTransgene00014794", "oxEx1578", ELEGANS)]


def test_variation_needs_a_taxon():
    data = make_data(variation_taxon={"WB:WBVar00145096": ELEGANS})
    entities, problems = parse_entities("variation", "WBVar00145096;%;e1370 | WBVar00275119;%;tm1", data)
    assert entities == [Entity("WB:WBVar00145096", "e1370", ELEGANS)]
    assert problems == ["no taxon WB:WBVar00275119"]


def test_species_names_to_taxa():
    data = make_data(species_taxon={"Caenorhabditis elegans": ELEGANS})
    entities, problems = parse_entities("species", "Caenorhabditis elegans | Escherichia coli", data)
    assert entities == [Entity(ELEGANS, "Caenorhabditis elegans", None)]
    assert problems == ["unresolved species name Escherichia coli"]


def test_duplicates_and_empty_values():
    data = make_data(gene_taxon={"WB:WBGene00000239": ELEGANS})
    entities, _ = parse_entities("gene", "00000239;%;bas-1 | 00000239;%;bas-1", data)
    assert len(entities) == 1
    assert parse_entities("gene", "", data) == ([], [])


def test_other_names():
    assert other_names('[{"id":1,"name":"a "},{"id":2,"name":""}]') == ["a"]
    assert other_names(EMPTY_OTHER) == []
    assert other_names("") == []
    assert other_names("free text") == ["free text"]
