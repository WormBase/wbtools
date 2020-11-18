# WBtools
> Interface to WormBase curation database and Text Mining functions

Access WormBase paper corpus information by loading pdf files (converted to txt) and curation info from the WormBase 
database. The package also exposes text mining functions on papers' fulltext.

## Installation

```pip install wbtools```

## Usage example

### Get sentences from a WormBase paper

```python
from wbtools.literature.corpus import CorpusManager

paper_id = "000050564"
cm = CorpusManager()
cm.load_from_wb_database(db_name="wb_dbname", db_user="wb_dbuser", db_password="wb_dbpasswd", db_host="wb_dbhost",
                         paper_ids=[paper_id])
sentences = cm.get_paper(paper_id).get_text_docs(split_sentences=True)
```