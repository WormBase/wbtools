import unittest
from unittest import mock

import requests

from wbtools.literature.abc_entities import get_abc_extracted_entities, BATCH_SIZE
from wbtools.literature.paper import ABCRequestError


def tag(topic, entity=None, name=None, negated=False, source="abc_entity_extractor"):
    return {"topic": topic, "entity": entity, "entity_name": name, "negated": negated,
            "tag_source": {"source_method": source}}


def response(json_body, status=200):
    resp = mock.Mock()
    resp.json.return_value = json_body
    if status >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(f"{status} error")
    else:
        resp.raise_for_status.return_value = None
    return resp


@mock.patch("wbtools.literature.abc_entities.generate_headers", return_value={})
@mock.patch("wbtools.literature.abc_entities.get_authentication_token", return_value="token")
class TestGetAbcExtractedEntities(unittest.TestCase):

    def fetch(self, tags_by_curie, curies=("AGRKB:1",)):
        with mock.patch("wbtools.literature.abc_entities.requests.post",
                        return_value=response({"tags": tags_by_curie})) as post:
            result = get_abc_extracted_entities(list(curies))
        return result, post

    def test_request_filters_on_extractor_source_and_entity_topics(self, *_):
        _, post = self.fetch({"AGRKB:1": []})
        self.assertTrue(post.call_args[0][0].endswith("/topic_entity_tag/by_references"))
        body = post.call_args[1]["json"]
        self.assertEqual(body["curies_or_reference_ids"], ["AGRKB:1"])
        self.assertEqual(body["filters"]["source_methods"], ["abc_entity_extractor"])
        self.assertEqual(set(body["filters"]["topics"]),
                         {"ATP:0000005", "ATP:0000285", "ATP:0000006", "ATP:0000027", "ATP:0000110", "ATP:0000123"})

    def test_entities_are_grouped_by_type(self, *_):
        result, _ = self.fetch({"AGRKB:1": [
            tag("ATP:0000005", "WB:WBGene00002974", "lev-1"),
            tag("ATP:0000285", "WB:WBVar00088809", "md176"),
            tag("ATP:0000006", "WB:WBVar00089452", "n363"),
            tag("ATP:0000027", "WB:WBStrain00043982", "N2"),
            tag("ATP:0000110", "WB:WBTransgene00016465", "leEx2996"),
            tag("ATP:0000123", "NCBITaxon:6239", "Caenorhabditis elegans")]})
        self.assertEqual(result["AGRKB:1"], {
            "gene": [("WB:WBGene00002974", "lev-1")],
            "allele": [("WB:WBVar00088809", "md176"), ("WB:WBVar00089452", "n363")],
            "strain": [("WB:WBStrain00043982", "N2")],
            "transgene": [("WB:WBTransgene00016465", "leEx2996")],
            "species": [("NCBITaxon:6239", "Caenorhabditis elegans")]})

    def test_negated_entity_tags_are_skipped(self, *_):
        result, _ = self.fetch({"AGRKB:1": [tag("ATP:0000005", "WB:WBGene00002974", "lev-1", negated=True),
                                            tag("ATP:0000110", negated=True)]})
        self.assertEqual(result["AGRKB:1"]["gene"], [])
        self.assertEqual(result["AGRKB:1"]["transgene"], [])

    def test_other_sources_are_ignored(self, *_):
        result, _ = self.fetch({"AGRKB:1": [tag("ATP:0000005", "WB:WBGene00001", "a-1", source="ACKnowledge_form"),
                                            tag("ATP:0000005", "WB:WBGene00002", "b-2")]})
        self.assertEqual(result["AGRKB:1"]["gene"], [("WB:WBGene00002", "b-2")])

    def test_non_wb_entities_are_dropped(self, *_):
        result, _ = self.fetch({"AGRKB:1": [tag("ATP:0000005", "FB:FBgn0028430", "He"),
                                            tag("ATP:0000123", "6239", "Caenorhabditis elegans"),
                                            tag("ATP:0000123", "NCBITaxon:10090", "Mus musculus")]})
        self.assertEqual(result["AGRKB:1"]["gene"], [])
        self.assertEqual(result["AGRKB:1"]["species"], [("NCBITaxon:10090", "Mus musculus")])

    def test_papers_missing_from_the_response_are_empty(self, *_):
        result, _ = self.fetch({}, curies=("AGRKB:1",))
        self.assertEqual(result["AGRKB:1"],
                         {"gene": [], "allele": [], "strain": [], "transgene": [], "species": []})

    def test_curies_are_requested_in_batches(self, *_):
        curies = [f"AGRKB:{i}" for i in range(BATCH_SIZE + 5)]
        with mock.patch("wbtools.literature.abc_entities.requests.post",
                        return_value=response({"tags": {}})) as post:
            result = get_abc_extracted_entities(curies)
        self.assertEqual([len(c[1]["json"]["curies_or_reference_ids"]) for c in post.call_args_list],
                         [BATCH_SIZE, 5])
        self.assertEqual(len(result), BATCH_SIZE + 5)

    def test_http_error_raises(self, *_):
        with mock.patch("wbtools.literature.abc_entities.requests.post", return_value=response({}, status=500)):
            with self.assertRaises(ABCRequestError):
                get_abc_extracted_entities(["AGRKB:1"])

    def test_connection_error_raises(self, *_):
        with mock.patch("wbtools.literature.abc_entities.requests.post",
                        side_effect=requests.exceptions.ConnectionError("down")):
            with self.assertRaises(ABCRequestError):
                get_abc_extracted_entities(["AGRKB:1"])

    def test_response_without_tags_raises(self, *_):
        with mock.patch("wbtools.literature.abc_entities.requests.post",
                        return_value=response({"error": "boom"})):
            with self.assertRaises(ABCRequestError):
                get_abc_extracted_entities(["AGRKB:1"])


if __name__ == '__main__':
    unittest.main()
