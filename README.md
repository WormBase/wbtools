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

### Get the latest papers (up to 50) added to WormBase or modified in the last month  

```python
from wbtools.literature.corpus import CorpusManager
import datetime

cm = CorpusManager()
cm.load_from_wb_database(db_name="wb_dbname", db_user="wb_dbuser", db_password="wb_dbpasswd", db_host="wb_dbhost",
                         from_date=datetime.datetime.now(), max_num_papers=50)
paper_ids = [paper.paper_id for paper in cm.get_all_papers()]
```

### Get the latest 50 papers added to WormBase or modified that have a final pdf version and have been flagged by WB paper classification pipeline, excluding reviews and papers with temp files only (proofs) 

```python
from wbtools.literature.corpus import CorpusManager
import datetime

cm = CorpusManager()
cm.load_from_wb_database(db_name="wb_dbname", db_user="wb_dbuser", db_password="wb_dbpasswd", db_host="wb_dbhost",
                         from_date=datetime.datetime.now(), max_num_papers=50, must_have_automated_classification=True, 
                         exclude_pap_types=['Review'], exclude_temp_pdf=True)
paper_ids = [paper.paper_id for paper in cm.get_all_papers()]
```