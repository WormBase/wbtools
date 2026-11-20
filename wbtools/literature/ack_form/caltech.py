"""Read the Caltech tables the sync needs into a CaltechData (read only)."""
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, NamedTuple, Optional, Set

from wbtools.literature.ack_form.constants import (ACK_SECTION_TABLES, AUTHOR_EVIDENCE, DATATYPE_FLAGS,
                                                   ENTITY_TYPES, PAPER_EDITOR_TABLES)


class Row(NamedTuple):
    data: str
    timestamp: str


class Removal(NamedTuple):
    """an author's paper editor entry (pap_gene/pap_species "from author first pass") a curator removed"""
    value: str  # the pap_* value: an 8-digit gene number or an NCBI taxon id
    curator: str  # WBPerson
    timestamp: str


@dataclass
class CaltechData:
    valid: Set[str] = field(default_factory=set)
    merged_into: Dict[str, str] = field(default_factory=dict)  # merged paper -> paper it was merged into
    agrkb: Dict[str, str] = field(default_factory=dict)
    lasttouched: Dict[str, str] = field(default_factory=dict)  # final ACKnowledge submissions
    ack_papers: Set[str] = field(default_factory=set)
    contributors: Dict[str, Dict[str, str]] = field(default_factory=dict)  # paper -> WBPerson -> timestamp
    tables: Dict[str, Dict[str, Row]] = field(default_factory=dict)  # tfp_*/afp_* table -> paper -> row
    blackbox: Dict[str, Dict[str, str]] = field(default_factory=dict)  # paper -> datatype -> level
    gene_taxon: Dict[str, str] = field(default_factory=dict)
    variation_taxon: Dict[str, str] = field(default_factory=dict)
    strain_by_name: Dict[str, str] = field(default_factory=dict)
    strain_taxon: Dict[str, str] = field(default_factory=dict)
    transgene_by_name: Dict[str, str] = field(default_factory=dict)
    transgene_taxon: Dict[str, str] = field(default_factory=dict)
    species_taxon: Dict[str, str] = field(default_factory=dict)  # species name -> NCBITaxon curie
    removals: Dict[str, Dict[str, List[Removal]]] = field(default_factory=dict)  # gene/species -> paper -> removals


ENTITY_TABLES = [table for entity_type in ENTITY_TYPES.values()
                 for table in (entity_type.tfp_table, entity_type.afp_table, entity_type.other_table) if table]
FLAG_TABLES = [flag.table for flag in DATATYPE_FLAGS]


def _table_query(table: str) -> str:
    prefix = table.split("_")[0]
    return f"SELECT joinkey, {table}, {prefix}_timestamp FROM {table} ORDER BY {prefix}_timestamp"  # noqa: S608


QUERIES: Dict[str, str] = {
    "valid": "SELECT joinkey FROM pap_status WHERE pap_status = 'valid'",
    "merges": "SELECT joinkey, pap_identifier FROM pap_identifier WHERE pap_identifier ~ '^[0-9]{8}$'",
    "agrkb": "SELECT joinkey, pap_identifier FROM pap_identifier WHERE pap_identifier ~ 'AGRKB'",
    "lasttouched": "SELECT joinkey, afp_timestamp FROM afp_lasttouched ORDER BY afp_timestamp",
    # the people who contributed to the submission (not pap_curator, which names only the one who submitted)
    "contributors_afp": "SELECT joinkey, afp_contributor, afp_timestamp FROM afp_contributor ORDER BY afp_timestamp",
    "blackbox": "SELECT cur_paper, cur_datatype, cur_blackbox, cur_timestamp FROM cur_blackbox ORDER BY cur_timestamp",
    "species_index": "SELECT joinkey, pap_species_index, pap_timestamp FROM pap_species_index ORDER BY pap_timestamp",
    "genes": "SELECT joinkey, gin_species FROM gin_species",
    "variations": "SELECT joinkey, obo_data_variation FROM obo_data_variation",
    "strains": "SELECT joinkey, obo_data_strain FROM obo_data_strain",
    "transgenes": "SELECT n.trp_name, p.trp_publicname, s.trp_species FROM trp_name n "
                  "LEFT JOIN trp_publicname p USING (joinkey) LEFT JOIN trp_species s USING (joinkey)",
}
QUERIES.update({table: _table_query(table) for table in ENTITY_TABLES + FLAG_TABLES})
for _name, _table in PAPER_EDITOR_TABLES.items():
    QUERIES[f"history_{_name}"] = (f"SELECT joinkey, {_table}, pap_order, pap_curator, pap_evidence, pap_timestamp "
                                   f"FROM h_{_table} ORDER BY pap_timestamp")  # noqa: S608
    QUERIES[f"current_{_name}"] = f"SELECT joinkey, {_table} FROM {_table}"  # noqa: S608


def utc_iso(ts) -> str:
    """a timestamp as naive UTC ISO, the way the ABC stores tag dates"""
    if isinstance(ts, datetime):
        if ts.tzinfo is not None:
            ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
        return ts.isoformat()
    return str(ts) if ts is not None else ""


def valid_paper(data: CaltechData, joinkey: str) -> Optional[str]:
    """the valid paper a paper id resolves to through merges, or None"""
    seen = set()
    while joinkey not in data.valid:
        if joinkey in seen or joinkey not in data.merged_into:
            return None
        seen.add(joinkey)
        joinkey = data.merged_into[joinkey]
    return joinkey


def _keep_rows(data: CaltechData, table_rows: List[tuple]) -> Dict[str, Row]:
    """rows by valid paper: the paper's own row wins over a merged paper's row, then the latest wins
    (e.g. the pipeline rerun of a merged duplicate must not replace the run the author saw)"""
    kept: Dict[str, Row] = {}
    own: Set[str] = set()
    for joinkey, value, ts in table_rows:
        valid = valid_paper(data, joinkey)
        if valid is None:
            continue
        if joinkey == valid:
            own.add(valid)
        elif valid in own:
            continue
        kept[valid] = Row(value or "", utc_iso(ts))
    return kept


def _person(who: str) -> str:
    return who.replace("two", "WBPerson", 1)


def _pap_value(value: Optional[str]) -> str:
    """the id of a pap_gene/pap_species value: '00000830;%;ctl-1' and 'ctl-1 (WBGene00000830)' -> '00000830'"""
    match = re.search(r"WBGene(\d{8})", value or "")
    return match.group(1) if match else (value or "").split(";%;", 1)[0].strip()


def _removals(data: CaltechData, history: List[tuple], current: List[tuple]) -> Dict[str, List[Removal]]:
    """the author entries a curator removed in the paper editor and that are not back in the pap_* table, by paper.
    The paper editor logs a removal as an h_pap_* row with no value at the entry's pap_order; the entry is the
    author's when an author row set its value (rows that only repeat the value keep it the author's). The latest
    removal of a value wins. Papers that are no longer valid are skipped: their entries were emptied by a merge."""
    present: Dict[str, Set[str]] = {}
    for joinkey, value in current:
        valid = valid_paper(data, joinkey)
        if valid and _pap_value(value):
            present.setdefault(valid, set()).add(_pap_value(value))
    slots: Dict[tuple, tuple] = {}  # (joinkey, pap_order) -> (value, set by an author row)
    removed: Dict[str, Dict[str, Removal]] = {}
    for joinkey, value, order, curator, evidence, ts in history:
        value = _pap_value(value)
        if value:
            author = AUTHOR_EVIDENCE in (evidence or "")
            old = slots.get((joinkey, order))
            slots[(joinkey, order)] = (value, author or (old is not None and old == (value, True)))
            continue
        old = slots.pop((joinkey, order), None)
        if old is None or not old[1] or joinkey not in data.valid or not curator \
                or old[0] in present.get(joinkey, set()):
            continue
        removed.setdefault(joinkey, {})[old[0]] = Removal(old[0], _person(curator), utc_iso(ts))
    return {paper: list(values.values()) for paper, values in removed.items()}


def _species_name(obo_text: str) -> Optional[str]:
    match = re.search(r'species: "(.*?)"', obo_text or "")
    return match.group(1) if match else None


def build_data(rows: Dict[str, List[tuple]]) -> CaltechData:
    """CaltechData from the rows of QUERIES (keys missing from rows count as empty)"""
    data = CaltechData()
    data.valid = {r[0] for r in rows.get("valid", [])}
    data.merged_into = {r[1]: r[0] for r in rows.get("merges", [])}
    data.agrkb = {r[0]: r[1] for r in rows.get("agrkb", []) if r[0] in data.valid}
    for joinkey, ts in rows.get("lasttouched", []):
        valid = valid_paper(data, joinkey)
        if valid and ts:
            data.lasttouched[valid] = utc_iso(ts)
    for joinkey, who, ts in rows.get("contributors_afp", []):
        valid = valid_paper(data, joinkey)
        # ACKnowledge saved "twoundefined" for a few submissions: no known person, so such a paper's tags go to
        # unknown_author like those of a paper without a contributor
        if who and valid and re.fullmatch(r"two\d+", who.strip()):
            data.contributors.setdefault(valid, {})[_person(who)] = utc_iso(ts)
    for table in ENTITY_TABLES + FLAG_TABLES:
        data.tables[table] = _keep_rows(data, rows.get(table, []))
    data.ack_papers = {joinkey for table in ACK_SECTION_TABLES for joinkey in data.tables.get(table, {})}
    own_levels: Set[tuple] = set()
    for paper, datatype, level, _ts in rows.get("blackbox", []):  # own rows first, then the latest, as tables
        valid = valid_paper(data, paper)
        if not valid or not level:
            continue
        if paper == valid:
            own_levels.add((valid, datatype))
        elif (valid, datatype) in own_levels:
            continue
        data.blackbox.setdefault(valid, {})[datatype] = level.upper()

    data.species_taxon = {name: "NCBITaxon:" + taxon_id for taxon_id, name, _ts in rows.get("species_index", [])}
    data.gene_taxon = {"WB:WBGene" + gene: data.species_taxon[species] for gene, species in rows.get("genes", [])
                       if species in data.species_taxon}
    for wbvar, obo_text in rows.get("variations", []):
        species = _species_name(obo_text)
        if species in data.species_taxon:
            data.variation_taxon["WB:" + wbvar] = data.species_taxon[species]
    for wbstrain, obo_text in rows.get("strains", []):
        if not obo_text:
            continue
        names = re.findall(r'name: "(.*?)"', obo_text)
        data.strain_by_name[names[0] if names else ""] = wbstrain
        species = _species_name(obo_text)
        if species in data.species_taxon:
            data.strain_taxon["WB:" + wbstrain] = data.species_taxon[species]
    data.strain_by_name.pop("", None)
    for wbtransgene, public_name, species in rows.get("transgenes", []):
        if public_name:
            data.transgene_by_name[public_name] = wbtransgene
        if species in data.species_taxon:
            data.transgene_taxon["WB:" + wbtransgene] = data.species_taxon[species]
    for name in PAPER_EDITOR_TABLES:
        data.removals[name] = _removals(data, rows.get(f"history_{name}", []), rows.get(f"current_{name}", []))
    return data


def load(conn) -> CaltechData:
    """run QUERIES on the Caltech database in one read-only snapshot (a lookup table rebuilt during the run would
    otherwise make entities unresolvable halfway)"""
    conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
    cur = conn.cursor()
    rows: Dict[str, List[tuple]] = {}
    for key, sql in QUERIES.items():
        cur.execute(sql)
        rows[key] = cur.fetchall()
    cur.close()
    return build_data(rows)
