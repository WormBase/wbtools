import re

from wbtools.literature.ack_form.author import author_tags
from wbtools.literature.ack_form.caltech import build_data
from wbtools.literature.ack_form.paper_data import load_paper_data

PAPER, OTHER, CURIE = "00000001", "00000002", "AGRKB:101000000000001"
TS = "2024-01-01T00:00:00"

# the rows of the Caltech tables, by table name (first column is the key the queries filter on)
TABLE_ROWS = {
    "pap_status": [(PAPER,), (OTHER,)],
    "afp_lasttouched": [(PAPER, TS), (OTHER, TS)],
    "afp_contributor": [(PAPER, "two100", TS), (OTHER, "two200", TS)],
    "afp_genestudied": [(PAPER, "00000830;%;ctl-1 | 00000002;%;aap-1", TS), (OTHER, "00000830;%;ctl-1", TS)],
    "tfp_genestudied": [(PAPER, "00000830;%;ctl-1 | 00000003", TS)],
    "afp_species": [(PAPER, "Caenorhabditis elegans", TS), (OTHER, "", TS)],
    "afp_strain": [(PAPER, "N2 | WBStrain00000002", TS)],
    "afp_otherstrain": [(PAPER, '[{"id":1,"name":"my strain"}]', TS)],
    "afp_variation": [(PAPER, "WBVar00000001;%;e1370", TS)],
    "afp_transgene": [(PAPER, "WBTransgene00000001;%;syIs1", TS)],
    "afp_rnai": [(PAPER, "RNAi in L4", TS), (OTHER, "x", TS)],
    "afp_antibody": [(PAPER, "", TS)],
    "pap_species_index": [("6239", "Caenorhabditis elegans", TS), ("6238", "Caenorhabditis briggsae", TS)],
    "gin_species": [("00000830", "Caenorhabditis elegans"), ("00000002", "Caenorhabditis elegans"),
                    ("00000003", "Caenorhabditis elegans"), ("00009999", "Caenorhabditis briggsae")],
    "obo_data_variation": [("WBVar00000001", 'species: "Caenorhabditis elegans"')],
    "obo_data_strain": [("WBStrain00000001", 'name: "N2"\nspecies: "Caenorhabditis elegans"'),
                        ("WBStrain00000002", 'name: "CB4856"\nspecies: "Caenorhabditis elegans"')],
    "trp_name": [("WBTransgene00000001", "syIs1", "Caenorhabditis elegans")],
}


class FakeCursor:
    """answers SELECTs from TABLE_ROWS, applying "joinkey = %s" and "IN %s" filters on the first column"""

    def __init__(self):
        self.executed = []
        self._result = []
        self.connection = FakeConnection()

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        table = re.search(r"FROM (\w+)", sql).group(1)
        rows = TABLE_ROWS.get(table, [])
        if "= %s" in sql:
            rows = [row for row in rows if row[0] == params[0]]
        elif "IN %s" in sql:
            rows = [row for row in rows if row[0] in params[0]]
        self._result = rows

    def fetchall(self):
        return list(self._result)


class FakeConnection:
    def set_session(self, *args, **kwargs):
        raise AssertionError("load_paper_data must not change the caller's session")


def all_rows():
    """the rows the sync's load() would read, keyed as QUERIES"""
    from wbtools.literature.ack_form.caltech import QUERIES
    cursor = FakeCursor()
    rows = {}
    for key, sql in QUERIES.items():
        cursor.execute(sql)
        rows[key] = cursor.fetchall()
    return rows


def test_load_paper_data_matches_build_data_for_one_paper():
    expected = author_tags(PAPER, CURIE, build_data(all_rows()))
    actual = author_tags(PAPER, CURIE, load_paper_data(FakeCursor(), PAPER))
    assert actual == expected
    assert len(actual[0]) > 5


def test_load_paper_data_reads_only_the_paper():
    cursor = FakeCursor()
    data = load_paper_data(cursor, PAPER)
    assert data.valid == {PAPER}
    assert set(data.contributors) == {PAPER}
    assert all(set(rows) <= {PAPER} for rows in data.tables.values())
    for sql, params in cursor.executed:
        if re.search(r"FROM (afp|tfp)_", sql):
            assert params == (PAPER,)


def test_load_paper_data_restricts_gene_lookup():
    cursor = FakeCursor()
    data = load_paper_data(cursor, PAPER)
    assert "WB:WBGene00009999" not in data.gene_taxon
    assert data.gene_taxon["WB:WBGene00000830"] == "NCBITaxon:6239"


def test_load_paper_data_does_not_change_the_session():
    load_paper_data(FakeCursor(), PAPER)  # FakeConnection.set_session raises
