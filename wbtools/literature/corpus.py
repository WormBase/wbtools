import logging
import os
import pickle

from typing import Generator, List, Tuple, Dict
from gensim.models import Word2Vec
from gensim.test.utils import common_texts

from wbtools.db.dbmanager import WBDBManager
from wbtools.lib.nlp.common import PaperSections
from wbtools.lib.nlp.text_preprocessing import preprocess
from wbtools.lib.nlp.text_similarity import get_softcosine_index, get_similar_documents, SimilarityResult
from wbtools.literature.paper import WBPaper


logger = logging.getLogger(__name__)


class CorpusManager(object):
    """manage a list of WBPaper objects by populating their data from database or local directory"""

    def __init__(self):
        self.corpus = {}

    def add_or_update_wb_paper(self, wb_paper: WBPaper):
        """add a paper

        Args:
            wb_paper(WBPaper): the paper to add to the corpus
        """
        self.corpus[wb_paper.paper_id] = wb_paper

    def remove_wb_paper(self, wb_paper: WBPaper):
        """remove a paper

        Args:
            wb_paper(WBPaper): the paper to remove from the corpus
        """
        del self.corpus[wb_paper.paper_id]

    def load_from_dir_with_txt_files(self, dir_path: str):
        """
        load papers from a directory containing text files with file name in the following format:
        <WBPaperID>_<Author><Year>_<additional_options>.txt

        Only files with .txt extension are loaded. Paper ID is derived from the file name and additional options are
        used to understand the type of file (e.g., main article, ocr scanned article, supplementary material etc.)

        Args:
            dir_path (str): path to the input directory containing text files
        """
        paper = WBPaper()
        for f in sorted(os.listdir(dir_path)):
            if os.path.isfile(os.path.join(dir_path, f)) and f.endswith(".txt"):
                if paper.paper_id and not paper.has_same_wbpaper_id_as_filename(f):
                    self.add_or_update_wb_paper(paper)
                    paper = WBPaper()
                paper.add_file(dir_path=dir_path, filename=f, remote_file=False, pdf=False)

    def load_from_wb_database(self, db_name: str, db_user: str, db_password: str, db_host: str,
                              tazendra_ssh_user: str = None, tazendra_ssh_passwd: str = None, paper_ids: list = None,
                              from_date: str = None, load_pdf_files: bool = True, load_bib_info: bool = True,
                              load_curation_info: bool = True, max_num_papers: int = None,
                              exclude_ids: List[str] = None, must_have_automated_classification: bool = False,
                              exclude_pap_types: List[str] = None) -> None:
        """load papers from WormBase database

        Args:
            db_name (str): database name
            db_user (str): database user
            db_password (str): database password
            db_host (str): database host
            tazendra_ssh_user (str): ssh user to fetch pdf files
            tazendra_ssh_passwd (str): ssh password to fetch pdf files
            paper_ids (list): optional list of paper ids to be fetched
            from_date (str): load papers added or modified from the specified date (only if paper_ids is not provided)
            load_pdf_files (bool): load pdf files using ssh credentials
            load_bib_info (bool): load bibliographic info of the papers
            load_curation_info (bool): load curation info of the papers
            max_num_papers (int): limit number of papers to be loaded
            exclude_ids (List[str]): list of paper ids to exclude
            must_have_automated_classification (bool): whether to load only papers that have been flagged by WB
                                                       classifiers
            exclude_pap_types (List[str]): list of pap_types (string value, not numeric) to exclude
        """
        main_db_manager = WBDBManager(db_name, db_user, db_password, db_host)
        if not paper_ids:
            paper_ids = main_db_manager.generic.get_all_paper_ids(added_or_modified_after=from_date,
                                                                  exclude_ids=exclude_ids)
        if exclude_pap_types:
            ids_to_exclude = main_db_manager.generic.get_paper_ids_with_pap_types(exclude_pap_types)
            paper_ids = list(set(paper_ids) - set(ids_to_exclude))
        for paper_id in paper_ids:
            paper = WBPaper(paper_id=paper_id, tazendra_ssh_user=tazendra_ssh_user,
                            tazendra_ssh_passwd=tazendra_ssh_passwd, db_manager=main_db_manager.paper)
            if load_curation_info:
                paper.load_curation_info_from_db()
                if must_have_automated_classification and not paper.aut_class_values:
                    continue
            if load_pdf_files:
                paper.load_text_from_pdf_files_in_db()
            if load_bib_info:
                paper.load_bib_info_from_db()
            self.add_or_update_wb_paper(paper)
            if max_num_papers and self.size() >= max_num_papers:
                break

    def size(self) -> int:
        """number of papers in the corpus manager

        Returns:
            int: the number of papers
        """
        return len(self.corpus)

    def get_flat_corpus_list_and_idx_paperid_map(self, split_sentences: bool = False,
                                                 remove_sections: List[PaperSections] = None,
                                                 must_be_present: List[PaperSections] = None,
                                                 lowercase: bool = False, tokenize: bool = False,
                                                 remove_stopwords: bool = False,
                                                 remove_alpha: bool = False) -> Tuple[List[str], Dict[int, str]]:
        """get a flat list of text documents from the papers in the corpus and a map to link the index in the resulting
           list and the id of the related paper

        Args:
            split_sentences (bool): split sentences into separate documents
            remove_sections (List[PaperSections]): list of sections to remove
            must_be_present (List[PaperSections]): list of sections that must be present
            lowercase (bool): transform text to lowercase
            tokenize (bool): tokenize text into words
            remove_stopwords (bool): remove common stopwords from text
            remove_alpha (bool): remove special characters and punctuation from text

        Returns:
            Tuple[List[str], Dict[int, str]]: the flat list and the related index to paper id map
        """
        flat_list_with_ids = [(doc, paper.paper_id) for paper in self.corpus.values() for doc in paper.get_text_docs(
            include_supplemental=True, remove_sections=remove_sections, must_be_present=must_be_present,
            split_sentences=split_sentences, lowercase=lowercase, tokenize=tokenize, remove_stopwords=remove_stopwords,
            remove_alpha=remove_alpha)]
        return [d[0] for d in flat_list_with_ids], {idx: d[1] for idx, d in enumerate(flat_list_with_ids)}

    def get_paper(self, paper_id) -> WBPaper:
        """get a paper from the corpus by paper id

        Args:
            paper_id (str): paper id to retrieve

        Returns:
            WBPaper: the paper
        """
        return self.corpus[paper_id]

    def get_all_papers(self) -> Generator[WBPaper, None, None]:
        """get all the papers in the corpus

        Returns:
            Generator[WBPaper, None, None]: a generator to the papers in the corpus
        """
        for paper in self.corpus.values():
            yield paper

    def save(self, file_path: str) -> None:
        """save corpus to file

        Args:
            file_path (str): path to file to save
        """
        with open(file_path, 'wb') as out_file:
            pickle.dump(self, out_file)

    def load(self, file_path: str) -> None:
        """load corpus from previously saved file

        Args:
            file_path (str): path to file to load
        """
        with open(file_path, 'rb') as in_file:
            tmp_self = pickle.load(in_file)
            self.__dict__ = tmp_self.__dict__

    def query_papers_by_doc_similarity(self, query_docs: List[str], sentence_search: bool = False,
                                       remove_sections: List[PaperSections] = None,
                                       must_be_present: List[PaperSections] = None, path_to_model: str = None,
                                       average_match: bool = True, num_best: int = 10) -> List[SimilarityResult]:
        """query papers in the corpus by similarity with the provided query documents, which can be fulltext documents
           or sentences

        Args:
            query_docs (List[str]): list of query documents
            sentence_search (bool): perform sentence level similarity search
            remove_sections (List[PaperSections]): sections to be ignored from corpus papers
            must_be_present (List[PaperSections]): sections that must be present in corpus papers before removing
                                                   sections
            path_to_model (str): path to word2vec model
            average_match (bool): merge query documents and calculate average similarity to them
            num_best (int): limit to the first n results by similarity score

        Returns:
            List[SimilarityResult]: list of papers most similar to the provided query documents
        """
        model = Word2Vec(common_texts, min_count=1) if not path_to_model else None
        corpus_list_token, idx_paperid_map = self.get_flat_corpus_list_and_idx_paperid_map(
            split_sentences=sentence_search, remove_sections=remove_sections, must_be_present=must_be_present,
            lowercase=True, tokenize=True, remove_stopwords=True, remove_alpha=True)
        corpus_list_token_orig, _ = self.get_flat_corpus_list_and_idx_paperid_map(
            split_sentences=sentence_search, remove_sections=remove_sections, must_be_present=must_be_present,
            lowercase=False, tokenize=False, remove_stopwords=False, remove_alpha=False)
        docsim_index, dictionary = get_softcosine_index(model=model, model_path=path_to_model,
                                                        corpus_list_token=corpus_list_token, num_best=num_best)
        query_docs_preprocessed = [preprocess(doc=sentence, lower=True, tokenize=True, remove_stopwords=True,
                                              remove_alpha=True) for sentence in query_docs]
        sims = get_similar_documents(docsim_index, dictionary, query_docs_preprocessed, idx_paperid_map,
                                     average_match=average_match)
        results = [SimilarityResult(score=sim.score, paper_id=sim.paper_id, match_idx=sim.match_idx,
                                    query_idx=sim.query_idx, match="\"" + corpus_list_token_orig[sim.match_idx] + "\"",
                                    query="\"" + (" ".join(query_docs) if average_match else query_docs[sim.query_idx]
                                                  ) + "\"") for sim in sims]
        return results[0:num_best] if len(results) > num_best else results
