"""The Caltech data of one paper, read on the caller's cursor (SCRUM-4141).

The ACKnowledge form saves a paper's answers and then compares them with the paper's ABC tags in the same, still
uncommitted, transaction: the data is read on that transaction's cursor so it sees the answers just written, and is
built with the sync's own build_data, so the tags are exactly those the sync would compute for the paper.
"""
import re
from typing import Dict, List, Set

from wbtools.literature.ack_form.caltech import ENTITY_TABLES, FLAG_TABLES, QUERIES, CaltechData, build_data
from wbtools.literature.ack_form.constants import ENTITY_TYPES

ITEM_SEPARATOR = " | "


def _paper_query(table: str) -> str:
    prefix = table.split("_")[0]
    return (f"SELECT joinkey, {table}, {prefix}_timestamp FROM {table} WHERE joinkey = %s "  # noqa: S608
            f"ORDER BY {prefix}_timestamp")


def _ids(rows: Dict[str, List[tuple]], entity_type: str) -> List[str]:
    """the leading ids of the items of the paper's tfp/afp values of an entity type"""
    entity = ENTITY_TYPES[entity_type]
    ids = []
    for table in (entity.tfp_table, entity.afp_table):
        for _joinkey, value, _ts in rows.get(table, []):
            for item in (value or "").split(ITEM_SEPARATOR):
                item_id = item.split(";%;", 1)[0].strip()
                if item_id:
                    ids.append(item_id)
    return ids


def _lookup(curs, keys: Set[str], restricted_sql: str) -> List[tuple]:
    """the lookup rows of the given keys only, or none without keys"""
    if not keys:
        return []
    curs.execute(restricted_sql, (tuple(sorted(keys)),))
    return curs.fetchall()


def _fetch(curs, sql: str, params=None) -> List[tuple]:
    curs.execute(sql, params)
    return curs.fetchall()


def load_paper_data(curs, joinkey: str) -> CaltechData:
    """the CaltechData of one paper, read on the given cursor (the caller's session is left as it is)

    Only the paper's own rows are read: merged papers' rows are not (the form writes the paper's own rows). Gene and
    variation lookups are restricted to the ids the paper's values contain; strains and transgenes too, unless the
    author entered a name instead of an id, which needs the whole name lookup.
    """
    rows: Dict[str, List[tuple]] = {"valid": [(joinkey,)]}
    for table in ENTITY_TABLES + FLAG_TABLES:
        rows[table] = _fetch(curs, _paper_query(table), (joinkey,))
    rows["lasttouched"] = _fetch(curs, "SELECT joinkey, afp_timestamp FROM afp_lasttouched WHERE joinkey = %s "
                                       "ORDER BY afp_timestamp", (joinkey,))
    rows["contributors_afp"] = _fetch(curs, "SELECT joinkey, afp_contributor, afp_timestamp FROM afp_contributor "
                                            "WHERE joinkey = %s ORDER BY afp_timestamp", (joinkey,))
    rows["species_index"] = _fetch(curs, QUERIES["species_index"])

    genes = {gene_id for gene_id in _ids(rows, "gene") if re.fullmatch(r"\d{8}", gene_id)}
    rows["genes"] = _lookup(curs, genes,
                            "SELECT joinkey, gin_species FROM gin_species WHERE joinkey IN %s")
    variations = {var_id for var_id in _ids(rows, "variation") if var_id.startswith("WBVar")}
    rows["variations"] = _lookup(curs, variations,
                                 "SELECT joinkey, obo_data_variation FROM obo_data_variation WHERE joinkey IN %s")
    for key, entity_type, prefix, restricted_sql in (
            ("strains", "strain", "WBStrain",
             "SELECT joinkey, obo_data_strain FROM obo_data_strain WHERE joinkey IN %s"),
            ("transgenes", "transgene", "WBTransgene",
             QUERIES["transgenes"] + " WHERE n.trp_name IN %s")):
        items = set(_ids(rows, entity_type))
        if any(not item.startswith(prefix) for item in items):
            rows[key] = _fetch(curs, QUERIES[key])
        else:
            rows[key] = _lookup(curs, items, restricted_sql)
    return build_data(rows)
