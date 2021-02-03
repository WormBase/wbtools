import logging
import os
import re
import tempfile
from typing import List, Tuple

import numpy as np

from collections import defaultdict
import fitz
from fabric.connection import Connection

from wbtools.db.paper import WBPaperDBManager
from wbtools.lib.nlp.common import EntityType, EntityExtractionType, ExtractedEntity
from wbtools.lib.nlp.entity_extraction.email_addresses import get_email_addresses_from_text
from wbtools.lib.nlp.text_preprocessing import preprocess, get_documents_from_text, PaperSections
from wbtools.lib.timeout import timeout
from wbtools.literature.person import WBAuthor

logger = logging.getLogger(__name__)


TAZENDRA_SSH_HOST = "tazendra.caltech.edu"


class PaperFileReader(object):

    def __init__(self, tazendra_ssh_user: str = '', tazendra_ssh_passwd: str = ''):
        self.tazendra_ssh_user = tazendra_ssh_user
        self.tazendra_ssh_passwd = tazendra_ssh_passwd

    @staticmethod
    @timeout(3600)
    def convert_pdf_to_txt(file_path):
        logger.info("Started pdf to text conversion")
        doc = fitz.Document(file_path)
        text = " ".join([doc.loadPage(page_num).getText("text") for page_num in range(doc.pageCount)])
        return text if text is not None else ""

    def get_supplemental_file_names(self, supp_dir_path):
        with Connection(TAZENDRA_SSH_HOST, self.tazendra_ssh_user,
                        connect_kwargs={"password": self.tazendra_ssh_passwd}) as c, c.sftp() as sftp:
            try:
                return [filename for filename in sftp.listdir(supp_dir_path) if filename.endswith(".pdf")]
            except UnicodeDecodeError:
                logger.error("Cannot read non-unicode chars in filenames due to bug in ssh library")
                return []

    def download_paper_and_extract_txt(self, file_url, pdf: bool = False):
        with Connection(TAZENDRA_SSH_HOST, self.tazendra_ssh_user, connect_kwargs={"password": self.tazendra_ssh_passwd}) as \
                c, c.sftp() as sftp, sftp.open(file_url) as file_stream:
            tmp_file = tempfile.NamedTemporaryFile()
            with open(tmp_file.name, 'wb') as tmp_file_stream:
                tmp_file_stream.write(file_stream.read())
        if pdf:
            return self.convert_pdf_to_txt(tmp_file.name)
        else:
            return open(tmp_file.name).read()

    def get_text_from_file(self, dir_path, filename, remote_file: bool = False, pdf: bool = False):
        if remote_file:
            text = self.download_paper_and_extract_txt(dir_path + filename, pdf)
        else:
            with open(os.path.join(dir_path, filename), 'r') as file:
                text = file.read()
        if np.average([len(w) for w in preprocess(text).split("\n")]) < 1.001:
            text = text.replace("\n\n", " ")
            text = text.replace("\n", "")
        else:
            text = text.replace("\n", " ")
        text = text.replace("Fig.", "Fig")
        text = text.replace("et al.", "et al")
        return text


class WBPaper(object):
    """WormBase paper information"""

    def __init__(self, paper_id: str = '', main_text: str = '', ocr_text: str = '', temp_text: str = '',
                 aut_text: str = '', html_text: str = '', supplemental_docs: list = None, tazendra_ssh_user: str = '',
                 tazendra_ssh_passwd: str = '', title: str = '', journal: str = '', pub_date: str = '',
                 authors: List[WBAuthor] = None, abstract: str = ''):
        self.paper_id = paper_id
        self.title = title
        self.journal = journal
        self.pub_date = pub_date
        self.authors = authors
        self.abstract = abstract
        self.main_text = main_text
        self.ocr_text = ocr_text
        self.temp_text = temp_text
        self.aut_text = aut_text
        self.html_text = html_text
        self.supplemental_docs = supplemental_docs if supplemental_docs else []
        self.aut_class_values = defaultdict(str)
        self.paper_file_reader = PaperFileReader(tazendra_ssh_user=tazendra_ssh_user,
                                                 tazendra_ssh_passwd=tazendra_ssh_passwd)

    def get_corresponding_author(self):
        for author in self.authors:
            if author.corresponding:
                return author
        return None

    def get_text_docs(self, include_supplemental: bool = True, remove_sections: List[PaperSections] = None,
                      must_be_present: List[PaperSections] = None, split_sentences: bool = False,
                      lowercase: bool = False, tokenize: bool = False, remove_stopwords: bool = False,
                      remove_alpha: bool = False) -> list:
        """get text documents for the paper

        Args:
            include_supplemental (bool): include supplemental material
            remove_sections (List[PaperSections]): sections to be removed
            must_be_present (List[PaperSections]): sections that must be present in the text to be able to remove
                                                   undesired sections
            split_sentences (bool): split documents into sections
            lowercase (bool): transform text to lowercase
            tokenize (bool): tokenize text into words
            remove_stopwords (bool): remove common stopwords
            remove_alpha (bool): remove special characters and punctuation

        Returns:
            list: a list of documents, which can be strings or lists of strings or list of lists of strings according to
                  the combination of tokenization arguments passed
        """
        docs = [self.main_text if self.main_text else self.html_text if self.html_text else self.ocr_text if
                self.ocr_text else self.aut_text if self.aut_text else self.temp_text]
        if include_supplemental:
            docs.extend(self.supplemental_docs)
        docs = [d for doc in docs for d in get_documents_from_text(
            text=doc, split_sentences=split_sentences, must_be_present=must_be_present,
            remove_sections=remove_sections)]
        return [preprocess(doc, lower=lowercase, tokenize=tokenize, remove_stopwords=remove_stopwords,
                           remove_alpha=remove_alpha) for doc in docs]

    def add_file(self, dir_path, filename, remote_file: bool = False, pdf: bool = False):
        """add one or more files to the paper. Information about the type of file is derived from file name. If the file
           path points to a directory, all supplementary files in it are loaded.

        Args:
            dir_path (str): path to the base directory
            filename (str): name of the file to load
            remote_file (bool): whether the file is on a remote location
            pdf (bool): whether the file is in pdf format
        """
        all_supp = False
        if not self.paper_file_reader and remote_file:
            raise Exception("a paper reader must be provided to access remote files")
        if dir_path.endswith("supplemental/") and re.match(r'^[0-9]+$', filename):
            filenames = self.paper_file_reader.get_supplemental_file_names(dir_path + filename)
            dir_path = dir_path + filename + "/"
            all_supp = True
        else:
            filenames = [filename]
        for filename in filenames:
            logger.info("Adding file " + dir_path + filename)
            if "_lib" not in filename and "Fig" not in filename and "Figure" not in filename:
                wb_paperid, author_year, additional_options = self._get_matches_from_filename(filename)
                if not self.paper_id:
                    self.paper_id = wb_paperid
                if all_supp or (author_year and ("_supp" in author_year or "_Supp" in author_year or "_Table" in
                                                 author_year or "_table" in author_year or "_mmc" in author_year or
                                                 "_Stable" in author_year or "_Movie" in author_year or "_movie" in
                                                 author_year or "supplementary" in author_year or "Supplementary" in
                                                 author_year or re.match(r'[_-][Ss][0-9]+', author_year))):
                    self.supplemental_docs.append(self.paper_file_reader.get_text_from_file(
                        dir_path, filename, remote_file, pdf))
                    return
                if not additional_options:
                    self.main_text = self.paper_file_reader.get_text_from_file(dir_path, filename, remote_file, pdf)
                elif "Supp" in additional_options or "supp" in additional_options or "Table" in additional_options or \
                        "table" in additional_options or "Movie" in additional_options or "movie" in additional_options:
                    self.supplemental_docs.append(self.paper_file_reader.get_text_from_file(
                        dir_path, filename, remote_file, pdf))
                elif "ocr" in additional_options:
                    self.ocr_text = self.paper_file_reader.get_text_from_file(dir_path, filename, remote_file, pdf)
                elif "temp" in additional_options:
                    self.temp_text = self.paper_file_reader.get_text_from_file(dir_path, filename, remote_file, pdf)
                elif "aut" in additional_options:
                    self.aut_text = self.paper_file_reader.get_text_from_file(dir_path, filename, remote_file, pdf)
                elif "html" in additional_options:
                    self.html_text = self.paper_file_reader.get_text_from_file(dir_path, filename, remote_file, pdf)
                else:
                    logger.warning("No rule to read filename: " + filename)

    def load_text_from_pdf_files_in_db(self, db_name, db_user, db_password, db_host):
        """load text from pdf files in the WormBase database

        Args:
            db_name (str): database name
            db_user (str): database user
            db_password (str): database password
            db_host (str): database host
        """
        wb_paper_db_manager = WBPaperDBManager(db_name, db_user, db_password, db_host)
        file_paths = wb_paper_db_manager.get_file_paths(self.paper_id)
        for file_path in file_paths:
            filename = file_path.split("/")[-1]
            dir_path = file_path.rstrip(filename)
            self.add_file(dir_path=dir_path, filename=filename, remote_file=True, pdf=True)

    def load_curation_info_from_db(self, db_name, db_user, db_password, db_host):
        """load curation data from WormBase database

        Args:
            db_name (str): database name
            db_user (str): database user
            db_password (str): database password
            db_host (str): database host
        """
        wb_paper_db_manager = WBPaperDBManager(db_name, db_user, db_password, db_host)
        aut_class_values = wb_paper_db_manager.get_automated_classification_values(paper_id=self.paper_id)
        for class_type, class_value in aut_class_values:
            self.aut_class_values[class_type] = class_value

    def load_bib_info_from_db(self, db_name, db_user, db_password, db_host):
        """load curation data from WormBase database

        Args:
            db_name (str): database name
            db_user (str): database user
            db_password (str): database password
            db_host (str): database host
        """
        wb_paper_db_manager = WBPaperDBManager(db_name, db_user, db_password, db_host)
        self.abstract = wb_paper_db_manager.get_paper_abstract(self.paper_id)
        self.title = wb_paper_db_manager.get_paper_title(self.paper_id)
        self.journal = wb_paper_db_manager.get_paper_journal(self.paper_id)
        self.abstract = wb_paper_db_manager.get_paper_abstract(self.paper_id)
        self.pub_date = wb_paper_db_manager.get_paper_pub_date(self.paper_id)
        self.authors = wb_paper_db_manager.get_paper_authors(self.paper_id)

    def has_same_wbpaper_id_as_filename(self, filename):
        return self._get_matches_from_filename(filename)[0] == self.paper_id

    @staticmethod
    def _get_matches_from_filename(filename):
        match = re.match(r'^([0-9]+[-_])?([^_]+)?(_.*)?\..*$', filename)
        if match:
            return match.group(1)[:-1] if match.group(1) else None, match.group(2), match.group(3)
        else:
            raise Exception("Can't extract WBPaperID from filename: " + filename)

    def is_temp(self):
        """determine if the paper has a temporary pdf

        Returns:
            bool: whether the paper has a temporary pdf file
        """
        return not self.main_text

    def has_supplementary_material(self):
        """determine if the paper has supplemental material associated to it

        Returns:
            bool: whether the paper has supplemental material
        """
        return self.supplemental_docs

    def extract_all_email_addresses(self):
        return get_email_addresses_from_text(self.get_text_docs())

    def get_aut_class_value_for_datatype(self, datatype: str):
        return self.aut_class_values[datatype] if self.aut_class_values[datatype] else None

    def extract_entities(self, type_method: List[Tuple[EntityType, EntityExtractionType]],
                         include_supplemental: bool = True, remove_sections: List[PaperSections] = None,
                         must_be_present: List[PaperSections] = None) -> List[ExtractedEntity]:
        """extract biological entities from the full text

        Args:
            type_method (List[Tuple[EntityType, EntityExtractionType]]): list containing entity type and extraction type
                                                                         for each of the entity types to be extracted
            include_supplemental (bool): whether to extract entities from the supplemental material
            remove_sections (List[PaperSections]): list of sections to remove
            must_be_present (List[PaperSections]): list of sections that must be present
        Returns:
            List[ExtractedEntity]: the list of extracted entites
        """
        full_text = self.get_text_docs(
            include_supplemental=include_supplemental, remove_sections=remove_sections, must_be_present=must_be_present,
            split_sentences=False, lowercase=False, tokenize=False, remove_stopwords=False, remove_alpha=False)
        for entity_type, extraction_method in type_method:
            pass
        return

