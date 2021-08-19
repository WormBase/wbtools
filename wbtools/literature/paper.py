import logging
import os
import re
import tempfile
from typing import List, Tuple, Union

import numpy as np

from collections import defaultdict
from pdfminer.high_level import extract_text
from fabric.connection import Connection
from pdfminer.layout import LAParams

from wbtools.db.afp import WBAFPDBManager
from wbtools.db.paper import WBPaperDBManager
from wbtools.db.person import WBPersonDBManager
from wbtools.lib.nlp.entity_extraction.email_addresses import get_email_addresses_from_text
from wbtools.lib.nlp.text_preprocessing import preprocess, get_documents_from_text, PaperSections
from wbtools.lib.timeout import timeout
from wbtools.literature.person import WBAuthor

logger = logging.getLogger(__name__)

logging.getLogger("pdfminer").setLevel(logging.WARNING)


class PaperFileReader(object):

    def __init__(self, ssh_host: str = '', ssh_user: str = '', ssh_passwd: str = ''):
        self.ssh_host = ssh_host
        self.ssh_user = ssh_user
        self.ssh_passwd = ssh_passwd

    @staticmethod
    @timeout(3600)
    def convert_pdf_to_txt(file_path):
        try:
            logger.info("Started pdf to text conversion")
            laparams = LAParams(char_margin=100)
            text = extract_text(file_path, laparams=laparams)
            return text if text is not None else ""
        except:
            return ""

    def get_supplemental_file_names(self, supp_dir_path):
        with Connection(self.ssh_host, self.ssh_user,
                        connect_kwargs={"password": self.ssh_passwd}) as c, c.sftp() as sftp:
            try:
                return [filename for filename in sftp.listdir(supp_dir_path) if filename.endswith(".pdf")]
            except UnicodeDecodeError:
                logger.error("Cannot read non-unicode chars in filenames due to bug in ssh library")
                return []

    def download_paper_and_extract_txt(self, file_url, pdf: bool = False):
        try:
            with Connection(self.ssh_host, self.ssh_user, connect_kwargs={"password": self.ssh_passwd}) as \
                    c, c.sftp() as sftp, sftp.open(file_url) as file_stream:
                tmp_file = tempfile.NamedTemporaryFile()
                with open(tmp_file.name, 'wb') as tmp_file_stream:
                    tmp_file_stream.write(file_stream.read())
            if pdf:
                return self.convert_pdf_to_txt(tmp_file.name)
            else:
                return open(tmp_file.name).read()
        except FileNotFoundError:
            logger.warning("File not found: " + file_url)
            return ""

    def get_text_from_file(self, dir_path, filename, remote_file: bool = False, pdf: bool = False):
        if remote_file:
            text = self.download_paper_and_extract_txt(dir_path + filename, pdf)
        else:
            with open(os.path.join(dir_path, filename), 'r') as file:
                text = file.read()
        if np.average([len(w) for w in preprocess(text).split("\n")]) < 1.001:
            text = text.replace("\n\n", " ")
        return text


class WBPaper(object):
    """WormBase paper information"""

    def __init__(self, paper_id: str = '', main_text: str = '', ocr_text: str = '', temp_text: str = '',
                 aut_text: str = '', html_text: str = '', proof_text: str = '', supplemental_docs: list = None,
                 ssh_host: str = 'tazendra.caltech.edu', ssh_user: str = '', ssh_passwd: str = '', title: str = '',
                 journal: str = '',
                 pub_date: str = '', authors: List[WBAuthor] = None, abstract: str = '', doi: str = '', pmid: str = '',
                 db_manager: WBPaperDBManager = None):
        self.paper_id = paper_id
        self.title = title
        self.journal = journal
        self.pub_date = pub_date
        self.authors = authors
        self.abstract = abstract
        self.doi = doi
        self.pmid = pmid
        self.main_text = main_text
        self.ocr_text = ocr_text
        self.temp_text = temp_text
        self.aut_text = aut_text
        self.html_text = html_text
        self.proof_text = proof_text
        self.supplemental_docs = supplemental_docs if supplemental_docs else []
        self.aut_class_values = defaultdict(str)
        self.paper_file_reader = PaperFileReader(ssh_host=ssh_host, ssh_user=ssh_user, ssh_passwd=ssh_passwd)
        self.afp_final_submission = False
        self.afp_processed = False
        self.afp_partial_submission = False
        self.afp_contact_emails = []
        self.db_manager = db_manager

    def get_corresponding_author(self) -> Union[WBAuthor, None]:
        """
        Get the corresponding author of the paper, as recorded in WB DB

        Returns:
            Union[WBAuthor, None]: the corresponding author, if available in the DB, otherwise None
        """
        for author in self.authors:
            if author.corresponding:
                return author
        return None

    def get_text_docs(self, include_supplemental: bool = True, remove_sections: List[PaperSections] = None,
                      must_be_present: List[PaperSections] = None, split_sentences: bool = False,
                      lowercase: bool = False, tokenize: bool = False, remove_stopwords: bool = False,
                      remove_alpha: bool = False, return_concatenated: bool = False) -> Union[List[str], str]:
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
            return_concatenated (bool): whether to concatenate the text documents and return a single string containing
                                        the full text

        Returns:
            list: a list of documents, which can be strings or lists of strings or list of lists of strings according to
                  the provided combination of tokenization arguments
        """
        docs = [self.main_text if self.main_text else self.html_text if self.html_text else self.ocr_text if
                self.ocr_text else self.aut_text if self.aut_text else self.temp_text if self.temp_text else
                self.proof_text]
        if include_supplemental:
            docs.extend(self.supplemental_docs)
        docs = [d for doc in docs for d in get_documents_from_text(
            text=doc, split_sentences=split_sentences, must_be_present=must_be_present,
            remove_sections=remove_sections)]
        docs = [preprocess(doc, lower=lowercase, tokenize=tokenize, remove_stopwords=remove_stopwords,
                           remove_alpha=remove_alpha) for doc in docs]
        if return_concatenated:
            return " ".join(docs)
        else:
            return docs

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
                elif "_proof" in additional_options:
                    self.proof_text = self.paper_file_reader.get_text_from_file(dir_path, filename, remote_file, pdf)
                elif "temp" in additional_options:
                    self.temp_text = self.paper_file_reader.get_text_from_file(dir_path, filename, remote_file, pdf)
                elif "aut" in additional_options:
                    self.aut_text = self.paper_file_reader.get_text_from_file(dir_path, filename, remote_file, pdf)
                elif "html" in additional_options:
                    self.html_text = self.paper_file_reader.get_text_from_file(dir_path, filename, remote_file, pdf)
                else:
                    logger.warning("No rule to read filename: " + filename)

    def load_text_from_pdf_files_in_db(self):
        """load text from pdf files in the WormBase database"""
        if self.db_manager:
            file_paths = self.db_manager.get_file_paths(self.paper_id)
            for file_path in file_paths:
                filename = file_path.split("/")[-1]
                if filename.lower().endswith(".pdf"):
                    dir_path = file_path.rstrip(filename)
                    self.add_file(dir_path=dir_path, filename=filename, remote_file=True, pdf=True)
        else:
            raise Exception("PaperDBManager not set")

    def load_curation_info_from_db(self):
        """load curation data from WormBase database"""
        if self.db_manager:
            aut_class_values = self.db_manager.get_automated_classification_values(paper_id=self.paper_id)
            for class_type, class_value in aut_class_values:
                self.aut_class_values[class_type] = class_value
        else:
            raise Exception("PaperDBManager not set")

    def load_bib_info_from_db(self):
        """load curation data from WormBase database"""
        if self.db_manager:
            self.abstract = self.db_manager.get_paper_abstract(self.paper_id)
            self.title = self.db_manager.get_paper_title(self.paper_id)
            self.journal = self.db_manager.get_paper_journal(self.paper_id)
            self.pub_date = self.db_manager.get_paper_pub_date(self.paper_id)
            self.authors = self.db_manager.get_paper_authors(self.paper_id)
            self.doi = self.db_manager.get_doi(self.paper_id)
            self.pmid = self.db_manager.get_pmid(self.paper_id)
        else:
            raise Exception("PaperDBManager not set")

    def load_afp_info_from_db(self, paper_ids_no_submission: List[str] = None,
                              paper_ids_full_submission: List[str] = None,
                              paper_ids_partial_submission: List[str] = None):
        """load AFP data from WormBase database"""
        if self.db_manager:
            afp_db_manager = self.db_manager.get_db_manager(cls=WBAFPDBManager)
        else:
            raise Exception("PaperDBManager not set")
        if not paper_ids_no_submission or not paper_ids_full_submission or not paper_ids_partial_submission:
            afp_db_manager = self.db_manager.get_db_manager(cls=WBAFPDBManager)
            if not paper_ids_no_submission:
                paper_ids_no_submission = afp_db_manager.get_paper_ids_afp_no_submission()
            if not paper_ids_full_submission:
                paper_ids_full_submission = afp_db_manager.get_paper_ids_afp_full_submission()
            if not paper_ids_partial_submission:
                paper_ids_partial_submission = afp_db_manager.get_paper_ids_afp_partial_submission()
        self.afp_processed = self.paper_id in (set(paper_ids_no_submission) | set(paper_ids_full_submission) |
                                               set(paper_ids_full_submission))
        self.afp_partial_submission = self.paper_id in paper_ids_partial_submission
        self.afp_final_submission = self.paper_id in paper_ids_full_submission
        self.afp_contact_emails = afp_db_manager.get_contact_emails(paper_id=self.paper_id)

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
        return self.main_text is None and self.html_text is None and self.ocr_text is None \
            and self.aut_text is None and self.temp_text is None

    def has_supplementary_material(self):
        """determine if the paper has supplemental material associated to it

        Returns:
            bool: whether the paper has supplemental material
        """
        return self.supplemental_docs

    def has_main_text(self):
        return self.main_text is not None or self.html_text is not None or self.ocr_text is not None or self.aut_text \
               is not None or self.temp_text is not None or self.proof_text is not None

    def extract_all_email_addresses_from_text(self, text: str = None):
        """get all the email addresses mentioned in any of the documents associated with this paper"""
        if not text:
            text = self.get_text_docs(return_concatenated=True)
        return get_email_addresses_from_text(text)

    def write_email_addresses_to_db(self, email_addresses: List[str]):
        """write a list of email addresses associated with the paper to DB

        Args:
            email_addresses (List[str]): list of email addresses to write
        """
        if self.db_manager:
            self.db_manager.write_email_addresses_extracted_from_paper(self.paper_id, email_addresses)
        else:
            raise Exception("PaperDBManager not set")

    def extract_all_email_addresses_from_text_and_write_to_db(self):
        self.write_email_addresses_to_db(self.extract_all_email_addresses_from_text())

    def get_aut_class_value_for_datatype(self, datatype: str):
        return self.aut_class_values[datatype] if self.aut_class_values[datatype] else None

    def get_authors_with_email_address_in_wb(self, blacklisted_email_addresses: List[str] = None,
                                             first_only: bool = False) -> Union[List[Tuple[WBAuthor, str]], None]:
        """
        Get the first email address in the paper with a corresponding person entry in WB and return the person object
        and the email address found in the paper, which may be more recent than the one in WB

        Args:
            blacklisted_email_addresses (List[str]): a list of email addresses to be excluded from the search
            first_only (bool): whether to return only the first available author

        Returns:
            Union[List[Tuple[WBPerson, str]], None]: a tuple containing the WBPerson and the email address found in the paper.
                                               If no email is found with a corresponding person in WB, then the function
                                               will return the corresponding author associated with the paper in WB and
                                               its email address, if any, otherwise None.
        """
        result = []
        all_addresses = self.extract_all_email_addresses_from_text()
        if not all_addresses:
            all_addresses = self.extract_all_email_addresses_from_text(self.get_text_docs(
                include_supplemental=False, return_concatenated=True).replace(". ", "."))
        if all_addresses:
            for address in all_addresses:
                if "'" not in address and (not blacklisted_email_addresses or address not in
                                           set(blacklisted_email_addresses)):
                    person_id = self.db_manager.get_db_manager(
                        WBPersonDBManager).get_person_id_from_email_address(address)
                    if person_id:
                        # curr_address = db_manager.get_current_email_address_for_person(person_id)
                        if first_only:
                            result = (self.db_manager.get_db_manager(WBPersonDBManager).get_person(person_id=person_id),
                                      address)
                        else:
                            result.append((self.db_manager.get_db_manager(WBPersonDBManager).get_person(
                                person_id=person_id), address))
        if not result:
            if first_only:
                corresponding_author = self.get_corresponding_author()
                if corresponding_author:
                    result = corresponding_author, corresponding_author.email
            else:
                for author in self.authors:
                    if author.email:
                        result.append((author, author.email))
        if result:
            return result
        else:
            return None
