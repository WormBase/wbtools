import json
import logging
import os
import ssl
import urllib.request
from typing import List

from wbtools.lib.nlp.literature_index.abstract_index import AbstractLiteratureIndex


logger = logging.getLogger(__name__)


class TextpressoLiteratureIndex(AbstractLiteratureIndex):

    def __init__(self, api_url: str, api_token: str, use_cache: bool = False, corpora: List[str] = None):
        super().__init__()
        self.corpora = ["C. elegans"] if corpora is None else corpora
        self.api_base_url = api_url
        self.api_token = api_token
        if not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
            ssl._create_default_https_context = ssl._create_unverified_context
        self.cache = None if not use_cache else {'keyword_counters': {}, 'total_counter': -1}

    def _query_api(self, endpoint: str, keywords: str, query_type: str, ret_type):
        data = json.dumps({"token": self.api_token, "query": {"keywords": keywords, "type": query_type,
                                                              "corpora": self.corpora}})
        data = data.encode('utf-8')
        req = urllib.request.Request(self.api_base_url + endpoint, data,
                                     headers={'Content-type': 'application/json', 'Accept': 'application/json'})
        res = urllib.request.urlopen(req)
        logger.debug("Sending request to Textpresso Central API")
        return ret_type(json.loads(res.read().decode('utf-8')))

    def num_documents(self) -> int:
        if self.cache:
            if self.cache['total_counter'] == -1:
                self.cache['total_counter'] = self._query_api(endpoint="get_documents_count", keywords=",",
                                                              query_type="document", ret_type=int)
            return self.cache['total_counter']
        else:
            return self._query_api(endpoint="get_documents_count", keywords="a", query_type="document", ret_type=int)

    def count_matching_documents(self, keyword: str) -> int:
        if self.cache:
            if keyword not in self.cache['keyword_counters']:
                logger.debug("Keyword not found in TPC cache")
                self.cache['keyword_counters'][keyword] = self._query_api(
                    endpoint="get_documents_count", keywords=keyword, query_type="document", ret_type=int)
            return self.cache['keyword_counters'][keyword]
        else:
            return self._query_api(endpoint="get_documents_count", keywords=keyword, query_type="document",
                                   ret_type=int)
