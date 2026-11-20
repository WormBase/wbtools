"""Constants of the ACKnowledge -> ABC tag sync (SCRUM-6598)."""
from typing import Dict, List, NamedTuple, Optional, Set

ACK_START = "2019-03-22"  # the first ACKnowledge submission; earlier ones are old author first pass
EMPTY_OTHER = '[{"id":1,"name":""}]'

ACK_FORM = "ACKnowledge_form"
ACK_PIPELINE = "ACKnowledge_pipeline"
PAPER_EDITOR_GENE = "paper_editor_genes_curator"
PAPER_EDITOR_SPECIES = "paper_editor_species"
SOURCES = {ACK_FORM: "ATP:0000035", ACK_PIPELINE: "ECO:0008021",  # source_method -> evidence assertion
           PAPER_EDITOR_GENE: "ATP:0000036", PAPER_EDITOR_SPECIES: "ATP:0000036"}

# a curator's removal of an author's gene or species in the paper editor: the paper editor table, its tag source and
# the note that marks the sync's own negated tags of that source (the paper editor's own tags are never touched)
PAPER_EDITOR_SOURCES = {"gene": PAPER_EDITOR_GENE, "species": PAPER_EDITOR_SPECIES}
PAPER_EDITOR_TABLES = {"gene": "pap_gene", "species": "pap_species"}
REMOVAL_NOTE = "author entry removed by a curator in the paper editor"
MERGE_REMOVAL_NOTE = "As a result of a merge, author entry removed by a curator in the paper editor"
REMOVAL_NOTES = (REMOVAL_NOTE, MERGE_REMOVAL_NOTE)
# the papers whose removals a curator confirmed as the result of a gene or NCBI taxon merge (SCRUM-6598, 2026-11-12);
# every other removal keeps REMOVAL_NOTE
MERGE_REMOVALS: Dict[str, Set[str]] = {
    "gene": {"00056896", "00058832", "00059330", "00059863", "00060275", "00061198", "00061615", "00064325",
             "00065106", "00065859", "00066528"},
    "species": {"00059443"},
}
AUTHOR_EVIDENCE = "from author first pass"

NOVELTY_EXISTING = "ATP:0000334"
NOVELTY_NEW_TO_DB = "ATP:0000228"
NOVELTY_CLASSIFICATION = "ATP:0000335"
NOVELTY_ANTIBODY = "ATP:0000229"
CONTEXT_ENTITY = "ATP:0000325"
CONTEXT_CLASSIFICATION = "ATP:0000323"

SPECIES_TOPIC = "ATP:0000123"
DEFAULT_TRANSGENE_TAXON = "NCBITaxon:6239"


class EntityType(NamedTuple):
    name: str
    topic: str
    tfp_table: str
    afp_table: str
    other_table: Optional[str]


ENTITY_TYPES: Dict[str, EntityType] = {e.name: e for e in (
    EntityType("gene", "ATP:0000005", "tfp_genestudied", "afp_genestudied", None),
    EntityType("variation", "ATP:0000285", "tfp_variation", "afp_variation", "afp_othervariation"),
    EntityType("strain", "ATP:0000027", "tfp_strain", "afp_strain", "afp_otherstrain"),
    EntityType("transgene", "ATP:0000110", "tfp_transgene", "afp_transgene", "afp_othertransgene"),
    EntityType("species", SPECIES_TOPIC, "tfp_species", "afp_species", "afp_otherspecies"),
)}


class DatatypeFlag(NamedTuple):
    datatype: str
    table: str
    topic: str
    novelty: str


def _flag(datatype: str, topic: str, novelty: str = NOVELTY_CLASSIFICATION, table: Optional[str] = None):
    return DatatypeFlag(datatype, "afp_" + (table or datatype), topic, novelty)


# the ACKnowledge data-type flags of populate_classifier_topic_entity.pl, topics from its topic_to_atp file;
# extvariation and newstrains (genetics_g3_linking_curator), exprmosaic (no topic), expression_cluster and
# transporter are not ACKnowledge_form flags
DATATYPE_FLAGS: List[DatatypeFlag] = [
    _flag("antibody", "ATP:0000096", NOVELTY_ANTIBODY),
    _flag("otherantibody", "ATP:0000096", NOVELTY_EXISTING),
    _flag("catalyticact", "ATP:0000061"),
    _flag("chemicals", "ATP:0000278"),
    _flag("chemphen", "ATP:0000350"),
    _flag("domanal", "ATP:0000089"),
    _flag("envpheno", "ATP:0000351"),
    _flag("funccomp", "ATP:0000071"),
    _flag("geneint", "ATP:0000068"),
    _flag("geneprod", "ATP:0000069"),
    _flag("genereg", "ATP:0000070"),
    _flag("genesymbol", "ATP:0000048"),
    _flag("humandisease", "ATP:0000152", table="humdis"),
    _flag("lsrnai", "ATP:0000352"),
    _flag("marker", "ATP:0000041"),
    _flag("newmutant", "ATP:0000083"),
    _flag("otherexpr", "ATP:0000041"),
    _flag("othergenefunc", "ATP:0000060"),
    _flag("overexpr", "ATP:0000084"),
    _flag("rnai", "ATP:0000082"),
    _flag("seqchange", "ATP:0000056"),
    _flag("seqfeat", "ATP:0000055"),
    _flag("siteaction", "ATP:0000033"),
    _flag("structcorr", "ATP:0000054"),
    _flag("timeaction", "ATP:0000349"),
]

# ACKnowledge config.yml abc_classifications: the datatypes the pipeline pre-checks, and the levels that pre-check
CLASSIFIER_TOPICS: Dict[str, str] = {
    "otherexpr": "ATP:0000041", "catalyticact": "ATP:0000061", "geneint": "ATP:0000068", "geneprod": "ATP:0000069",
    "genereg": "ATP:0000070", "rnai": "ATP:0000082", "newmutant": "ATP:0000083", "overexpr": "ATP:0000084"}
PRECHECK_LEVELS = {"HIGH", "MEDIUM"}

# ACKnowledge writes a row in each of these at every save, empty or not; the old author first pass form never does
ACK_SECTION_TABLES = ("afp_species", "afp_strain", "afp_otherstrain", "afp_variation", "afp_othervariation",
                      "afp_transgene", "afp_othertransgene")
