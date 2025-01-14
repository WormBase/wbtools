import json
import logging
import urllib.request

from wbtools.utils.okta_utils import get_authentication_token


logger = logging.getLogger(__name__)


class CurationDataManager:
    def __init__(self):
        self.curation_api_base_url = "https://curation.alliancegenome.org/api/"
        self.page_limit = 2000

    def get_all_curated_entities(self, entity_type: str, mod_abbreviation: str):
        if entity_type == "gene":
            return self.get_all_curated_genes(mod_abbreviation)

    @staticmethod
    def get_entity_name(entity_type, a_team_api_search_result_obj, mod_abbreviation: str = None):
        if entity_type == 'gene':
            gene_name = a_team_api_search_result_obj['geneSymbol']['formatText']
            if mod_abbreviation and mod_abbreviation == 'WB':
                return gene_name.lower()
            return gene_name
        elif entity_type == 'protein':
            # currently just for WB
            gene_name = a_team_api_search_result_obj['geneSymbol']['formatText']
            return gene_name.upper()
        elif entity_type == 'allele':
            return a_team_api_search_result_obj['alleleSymbol']['formatText']
        elif entity_type == 'fish':
            if a_team_api_search_result_obj['subtype']['name'] != 'fish':
                return None
            return a_team_api_search_result_obj['name']

    def get_all_curated_genes(self, mod_abbreviation: str):
        all_curated_gene_names = []
        params = {
            "searchFilters": {
                "dataProviderFilter": {
                    "dataProvider.sourceOrganization.abbreviation": {
                        "queryString": mod_abbreviation,
                        "tokenOperator": "OR"
                    }
                }
            },
            "sortOrders": [],
            "aggregations": [],
            "nonNullFieldsTable": []
        }
        current_page = 0
        while True:
            logger.info(f"Fetching page {current_page} of entities from A-team API")
            url = f"{self.curation_api_base_url}gene/search?limit={self.page_limit}&page={current_page}"
            request_data_encoded = json.dumps(params).encode('utf-8')
            request = urllib.request.Request(url, data=request_data_encoded)
            request.add_header("Authorization", f"Bearer {get_authentication_token()}")
            request.add_header("Content-type", "application/json")
            request.add_header("Accept", "application/json")

            with urllib.request.urlopen(request) as response:
                resp_obj = json.loads(response.read().decode("utf8"))

            if resp_obj['returnedRecords'] < 1:
                break

            for result in resp_obj['results']:
                if result['obsolete'] or result['internal']:
                    continue
                entity_name = self.get_entity_name("gene", result, mod_abbreviation)
                if entity_name:
                    all_curated_gene_names.append(entity_name)
            current_page += 1
        return all_curated_gene_names


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    cdm = CurationDataManager()
    print(cdm.get_all_curated_entities("gene", "SGD"))
