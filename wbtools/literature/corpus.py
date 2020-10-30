import logging
import os
from typing import Generator

from wbtools.literature.paper import WBPaper, PaperFileReader
from wbtools.db.paper import WBPaperDBManager

logger = logging.getLogger(__name__)


class CorpusManager(object):

    def __init__(self):
        self.corpus = {}

    def add_or_update_wb_paper(self, wb_paper: WBPaper):
        self.corpus[wb_paper.paper_id] = wb_paper

    def rem_wb_paper(self, wb_paper: WBPaper):
        del self.corpus[wb_paper.paper_id]

    def load_from_dir_with_txt_files(self, dir_path: str):
        """
        load literature from a set of text files with file name in the following format:
        <WBPaperID>_<Author><Year>_<additional_options>.txt

        Only files with .txt extension are loaded. Paper ID is derived from the file name and additional options are
        used to understand the type of file (e.g., main article, ocr scanned article, supplementary material etc.)

        Args:
            dir_path (str): path to the input directory containing text files
        """
        paper_reader = PaperFileReader()
        paper = WBPaper()
        for f in sorted(os.listdir(dir_path)):
            if os.path.isfile(os.path.join(dir_path, f)) and f.endswith(".txt"):
                if paper.paper_id and not paper.has_same_wbpaper_id_as_filename(f):
                    self.add_or_update_wb_paper(paper)
                    paper = WBPaper()
                paper.add_file(dir_path=dir_path, filename=f, paper_reader=paper_reader, remote_file=False, pdf=False)

    def load_from_wb_database(self, wb_paper_db_manager: WBPaperDBManager, tazendra_ssh_user, tazendra_ssh_passwd,
                              from_date):
        paper_reader = PaperFileReader(tazendra_ssh_user=tazendra_ssh_user, tazendra_ssh_passwd=tazendra_ssh_passwd)
        for paper_id in wb_paper_db_manager.get_all_paper_ids(added_or_modified_after=from_date):
            paper = WBPaper(paper_id=paper_id)
            paper.load_text_from_pdf_files_in_db(wb_paper_db_manager, paper_reader)
            self.add_or_update_wb_paper(paper)

    def size(self):
        return len(self.corpus)

    def get_flat_corpus_list_and_idx_paperid_map(self, split_sentences, remove_ref_section, lowercase, tokenize,
                                                 remove_stopwords, remove_alpha):
        flat_list = [doc for paper in self.corpus.values() for doc in paper.get_docs_from_merged_text(
            remove_ref_section=remove_ref_section, split_sentences=split_sentences,
            lowercase=lowercase, tokenize=tokenize, remove_stopwords=remove_stopwords,
            remove_alpha=remove_alpha)]
        return flat_list, {idx: paper_id for idx, paper_id in enumerate(flat_list)}

    def get_paper(self, paper_id) -> WBPaper:
        return self.corpus[paper_id]

    def get_all_papers(self) -> Generator[WBPaper, None, None]:
        for paper in self.corpus.values():
            yield paper
