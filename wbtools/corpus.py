import logging
import os
import re
import numpy as np
from io import StringIO
from dataclasses import dataclass, field
from collections import defaultdict
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from fabric.connection import Connection

from wbtools.lib.dbmanager import DBManager
from wbtools.lib.nlp import get_documents_from_text, preprocess
from wbtools.lib.timeout import timeout


TAZENDRA_SSH_HOST = "tazendra.caltech.edu"


logger = logging.getLogger(__name__)


class PaperFileReader(object):

    def __init__(self, tazendra_ssh_user: str = '', tazendra_ssh_passwd: str = ''):
        self.tazendra_ssh_user = tazendra_ssh_user
        self.tazendra_ssh_passwd = tazendra_ssh_passwd

    @staticmethod
    @timeout(3600)
    def convert_pdf_to_txt(file_stream):
        logger.info("Started pdf to text conversion")
        text = ""
        try:
            rsrcmgr = PDFResourceManager()
            retstr = StringIO()
            laparams = LAParams()
            device = TextConverter(rsrcmgr, retstr, laparams=laparams)
            fp = file_stream
            interpreter = PDFPageInterpreter(rsrcmgr, device)
            password = ""
            maxpages = 0
            caching = True
            pagenos = set()
            for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password, caching=caching,
                                          check_extractable=True):
                interpreter.process_page(page)
            text = retstr.getvalue()
            fp.close()
            device.close()
            retstr.close()
        except:
            logger.error("Cannot convert pdf file")
        return text

    def get_supplemental_file_names(self, supp_dir_path):
        with Connection(TAZENDRA_SSH_HOST, self.tazendra_ssh_user,
                        connect_kwargs={"password": self.tazendra_ssh_passwd}) as c, c.sftp() as sftp:
            return [filename for filename in sftp.listdir(supp_dir_path) if filename.endswith(".pdf")]

    def download_paper_and_extract_txt(self, file_url, pdf: bool = False):
        with Connection(TAZENDRA_SSH_HOST, self.tazendra_ssh_user, connect_kwargs={"password": self.tazendra_ssh_passwd}) as \
                c, c.sftp() as sftp, sftp.open(file_url) as file_stream:
            if pdf:
                return self.convert_pdf_to_txt(file_stream)
            else:
                return file_stream.read()

    def get_text_from_file(self, dir_path, filename, remote_file: bool = False, pdf: bool = False):
        if remote_file:
            text = self.download_paper_and_extract_txt(dir_path + filename, pdf)
        else:
            text = open(os.path.join(dir_path, filename), 'r').read()
        if np.average([len(w) for w in preprocess(text)]) < 1.001:
            text = text.replace("\n", "")
        return text


@dataclass
class WBPaper:
    wb_paperid: str = ''
    main_text: str = ''
    ocr_text: str = ''
    temp_text: str = ''
    aut_text: str = ''
    html_text: str = ''
    supplemental_docs: list = field(default_factory=list)

    def get_docs_from_merged_text(self, remove_ref_section: bool = False, split_sentences: bool = False):
        merged_text_arr = [self.main_text if self.main_text else self.html_text if self.html_text else self.ocr_text if
                           self.ocr_text else self.aut_text if self.aut_text else self.temp_text,
                           *self.supplemental_docs]
        return [d for doc in merged_text_arr for d in get_documents_from_text(doc, split_sentences, remove_ref_section)]

    def add_file(self, dir_path, filename, paper_reader: PaperFileReader, remote_file: bool = False, pdf: bool = False):
        if dir_path.endswith("supplemental/") and re.match(r'^[0-9]+$', filename):
            filenames = paper_reader.get_supplemental_file_names(dir_path + filename)
            dir_path = dir_path + filename + "/"
        else:
            filenames = [filename]
        for filename in filenames:
            logger.info("Adding file " + dir_path + filename)
            if "_lib" not in filename and "Fig" not in filename and "Figure" not in filename:
                wb_paperid, author_year, additional_options = self.get_matches_from_filename(filename)
                if not self.wb_paperid:
                    self.wb_paperid = wb_paperid
                if author_year and ("_supp" in author_year or "_Supp" in author_year or "_Table" in author_year or
                                    "_table" in author_year or "_mmc" in author_year or "_Stable" in author_year or
                                    "_Movie" in author_year or "_movie" in author_year or "supplementary" in author_year
                                    or "Supplementary" in author_year or
                                    re.match(r'[_-][Ss][0-9]+', author_year)):
                    self.supplemental_docs.append(paper_reader.get_text_from_file(dir_path, filename, remote_file, pdf))
                    return
                if not additional_options:
                    self.main_text = paper_reader.get_text_from_file(dir_path, filename, remote_file, pdf)
                elif "Supp" in additional_options or "supp" in additional_options or "Table" in additional_options or \
                        "table" in additional_options or "Movie" in additional_options or "movie" in additional_options:
                    self.supplemental_docs.append(paper_reader.get_text_from_file(dir_path, filename, remote_file, pdf))
                elif "ocr" in additional_options:
                    self.ocr_text = paper_reader.get_text_from_file(dir_path, filename, remote_file, pdf)
                elif "temp" in additional_options:
                    self.temp_text = paper_reader.get_text_from_file(dir_path, filename, remote_file, pdf)
                elif "aut" in additional_options:
                    self.aut_text = paper_reader.get_text_from_file(dir_path, filename, remote_file, pdf)
                elif "html" in additional_options:
                    self.html_text = paper_reader.get_text_from_file(dir_path, filename, remote_file, pdf)
                else:
                    logger.warning("No rule to read filename: " + filename)

    def has_same_wbpaper_id_as_filename(self, filename):
        return self.get_matches_from_filename(filename)[0] == self.wb_paperid

    @staticmethod
    def get_matches_from_filename(filename):
        match = re.match(r'^([0-9]+[-_])?([^_]+)?(_.*)?\..*$', filename)
        if match:
            return match.group(1)[:-1] if match.group(1) else None, match.group(2), match.group(3)
        else:
            raise Exception("Can't extract WBPaperID from filename: " + filename)


class CorpusManager(object):

    def __init__(self, split_sentences: bool = False, remove_references: bool = False, tokenize: bool = False,
                 remove_stopwords: bool = False, remove_alpha: bool = False):
        self.corpus = defaultdict(list)

        self.split_sentences = split_sentences
        self.remove_ref_section = remove_references
        self.tokenize = tokenize
        self.remove_stopwords = remove_stopwords
        self.remove_alpha = remove_alpha

    def add_wb_paper(self, wb_paper: WBPaper):
        docs = wb_paper.get_docs_from_merged_text(remove_ref_section=self.remove_ref_section,
                                                  split_sentences=self.split_sentences)
        for doc in docs:
            preprocessed_doc = preprocess(doc, tokenize=self.tokenize, remove_stopwords=self.remove_stopwords,
                                          remove_alpha=self.remove_alpha)
            self.corpus[wb_paper.wb_paperid].append(preprocessed_doc)

    def rem_wb_paper(self, wb_paper: WBPaper):
        del self.corpus[wb_paper.wb_paperid]

    def add_or_update_wb_paper(self, wb_paper: WBPaper):
        if wb_paper.wb_paperid in self.corpus:
            self.rem_wb_paper(wb_paper)
        self.add_wb_paper(wb_paper)

    def load_from_dir_with_txt_files(self, dir_path):
        paper_reader = PaperFileReader()
        paper = WBPaper()
        for f in sorted(os.listdir(dir_path)):
            if os.path.isfile(os.path.join(dir_path, f)) and f.endswith(".txt"):
                if paper.wb_paperid and not paper.has_same_wbpaper_id_as_filename(f):
                    self.add_or_update_wb_paper(paper)
                    paper = WBPaper()
                paper.add_file(dir_path=dir_path, filename=f, paper_reader=paper_reader, remote_file=False, pdf=False)

    def load_from_wb_database(self, dbname, user, password, host, tazendra_ssh_user, tazendra_ssh_passwd, from_date):
        dbmanager = DBManager(dbname, user, password, host)
        paper_reader = PaperFileReader(tazendra_ssh_user=tazendra_ssh_user, tazendra_ssh_passwd=tazendra_ssh_passwd)
        for paper_id, file_paths in dbmanager.get_wb_papers_files_paths(from_date=from_date).items():
            logger.info("Processing paper " + paper_id)
            paper = WBPaper(wb_paperid=paper_id)
            for file_path in file_paths:
                filename = file_path.split("/")[-1]
                dir_path = file_path.rstrip(filename)
                paper.add_file(dir_path=dir_path, filename=filename, paper_reader=paper_reader, remote_file=True,
                               pdf=True)
            self.add_or_update_wb_paper(paper)

    def size(self):
        return len(self.corpus)

    def get_flat_corpus_list(self):
        return [doc for paper_id, doc_list in self.corpus for doc in doc_list]

    def get_idx_paperid_map(self):
        return {idx: paper_id for idx, paper_id in enumerate([paper_id for paper_id, doc_list in self.corpus for doc in
                                                              doc_list])}

    def get_docs_from_paper_id(self, paper_id):
        return self.corpus[paper_id]
