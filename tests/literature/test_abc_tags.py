import unittest
from unittest import mock

import requests

from wbtools.literature.abc_entities import BATCH_SIZE
from wbtools.literature.abc_tags import get_abc_curie, get_abc_paper_tags
from wbtools.literature.paper import ABCRequestError

EXTRACTOR = "abc_entity_extractor"
ACK = "ACKnowledge_pipeline"
CLASSIFIER = "abc_document_classifier"


def tag(topic, entity=None, name=None, negated=False, source=EXTRACTOR, mod="WB", level=None, score=None,
        created="2026-01-01T00:00:00", model_version=None, **fields):
    result = {"topic": topic, "entity": entity, "entity_name": name, "negated": negated, "confidence_level": level,
              "confidence_score": score, "date_created": created, "ml_model_version": model_version,
              "tag_source": {"source_method": source, "secondary_data_provider_abbreviation": mod}}
    result.update(fields)
    return result


def response(json_body, status=200):
    resp = mock.Mock()
    resp.status_code = status
    resp.json.return_value = json_body
    if status >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(f"{status} error")
    else:
        resp.raise_for_status.return_value = None
    return resp


@mock.patch("wbtools.literature.abc_entities.generate_headers", return_value={})
@mock.patch("wbtools.literature.abc_entities.get_authentication_token", return_value="token")
class TestGetAbcPaperTags(unittest.TestCase):

    def fetch(self, tags_by_curie, curies=("AGRKB:1",), **kwargs):
        with mock.patch("wbtools.literature.abc_entities.requests.post",
                        return_value=response({"tags": tags_by_curie})) as post:
            result = get_abc_paper_tags(list(curies), **kwargs)
        return result, post

    def test_request_filters_on_three_sources_entity_and_classifier_topics_and_wb(self, *_):
        _, post = self.fetch({"AGRKB:1": []})
        self.assertTrue(post.call_args[0][0].endswith("/topic_entity_tag/by_references"))
        filters = post.call_args[1]["json"]["filters"]
        self.assertEqual(set(filters["source_methods"]), {EXTRACTOR, ACK, CLASSIFIER})
        self.assertEqual(filters["mods"], ["WB"])
        self.assertEqual(set(filters["topics"]), {
            "ATP:0000005", "ATP:0000285", "ATP:0000006", "ATP:0000027", "ATP:0000110", "ATP:0000123",
            "ATP:0000041", "ATP:0000061", "ATP:0000068", "ATP:0000069", "ATP:0000070", "ATP:0000082",
            "ATP:0000083", "ATP:0000084"})

    def test_timeout_is_passed_to_the_request(self, *_):
        _, post = self.fetch({"AGRKB:1": []}, timeout=30)
        self.assertEqual(post.call_args[1]["timeout"], 30)

    def test_entities_and_their_tag_fields_by_source(self, *_):
        result, _ = self.fetch({"AGRKB:1": [
            tag("ATP:0000005", "WB:WBGene00002974", "lev-1", entity_type="ATP:0000005", species="NCBITaxon:6239",
                entity_id_validation="alliance", data_novelty="ATP:0000334", data_context="ATP:0000325"),
            tag("ATP:0000005", "WB:WBGene00000001", "aap-1", source=ACK)]})
        paper = result["AGRKB:1"]
        self.assertEqual(paper["entities"][EXTRACTOR]["gene"], [("WB:WBGene00002974", "lev-1")])
        self.assertEqual(paper["entities"][ACK]["gene"], [("WB:WBGene00000001", "aap-1")])
        self.assertEqual(paper["entity_tag_fields"][EXTRACTOR]["WB:WBGene00002974"],
                         {"topic": "ATP:0000005", "entity_type": "ATP:0000005", "species": "NCBITaxon:6239",
                          "entity_id_validation": "alliance", "data_novelty": "ATP:0000334",
                          "data_context": "ATP:0000325"})
        self.assertTrue(paper["has_ack_pipeline_entity_tags"])

    def test_first_tag_of_an_entity_gives_its_fields(self, *_):
        result, _ = self.fetch({"AGRKB:1": [
            tag("ATP:0000285", "WB:WBVar00088809", "md176", species="NCBITaxon:6239"),
            tag("ATP:0000006", "WB:WBVar00088809", "md176", species="NCBITaxon:6238")]})
        fields = result["AGRKB:1"]["entity_tag_fields"][EXTRACTOR]["WB:WBVar00088809"]
        self.assertEqual((fields["topic"], fields["species"]), ("ATP:0000285", "NCBITaxon:6239"))

    def test_negated_only_ack_pipeline_entity_tags_count(self, *_):
        result, _ = self.fetch({"AGRKB:1": [tag("ATP:0000110", negated=True, source=ACK)]})
        self.assertTrue(result["AGRKB:1"]["has_ack_pipeline_entity_tags"])
        self.assertEqual(result["AGRKB:1"]["entities"][ACK]["transgene"], [])

    def test_ack_pipeline_classification_tags_are_not_entity_tags(self, *_):
        result, _ = self.fetch({"AGRKB:1": [tag("ATP:0000082", source=ACK, level="HIGH")]})
        self.assertFalse(result["AGRKB:1"]["has_ack_pipeline_entity_tags"])
        self.assertEqual(result["AGRKB:1"]["classifications"][ACK]["ATP:0000082"]["confidence_level"], "HIGH")

    def test_negated_and_non_wb_entities_are_skipped(self, *_):
        result, _ = self.fetch({"AGRKB:1": [
            tag("ATP:0000005", "WB:WBGene00002974", "lev-1", negated=True),
            tag("ATP:0000005", "FB:FBgn0028430", "He")]})
        self.assertEqual(result["AGRKB:1"]["entities"][EXTRACTOR]["gene"], [])
        self.assertEqual(result["AGRKB:1"]["entity_tag_fields"][EXTRACTOR], {})

    def test_classification_fields_are_kept(self, *_):
        result, _ = self.fetch({"AGRKB:1": [
            tag("ATP:0000082", source=CLASSIFIER, level="LOW", score=0.62, model_version=2,
                data_novelty="ATP:0000335", data_context="ATP:0000323")]})
        self.assertEqual(result["AGRKB:1"]["classifications"][CLASSIFIER]["ATP:0000082"],
                         {"negated": False, "confidence_level": "LOW", "confidence_score": 0.62,
                          "data_novelty": "ATP:0000335", "data_context": "ATP:0000323", "ml_model_version": 2,
                          "date_created": "2026-01-01T00:00:00"})

    def test_latest_model_wins(self, *_):
        # tags from before model versions were recorded have none: any versioned tag beats them
        result, _ = self.fetch({"AGRKB:1": [
            tag("ATP:0000041", source=CLASSIFIER, level="NEG", negated=True, model_version=2,
                created="2025-10-01T00:00:00"),
            tag("ATP:0000041", source=CLASSIFIER, level="HIGH", model_version=None, created="2026-05-07T00:00:00"),
            tag("ATP:0000041", source=CLASSIFIER, level="LOW", model_version=1, created="2026-05-08T00:00:00")]})
        topic = result["AGRKB:1"]["classifications"][CLASSIFIER]["ATP:0000041"]
        self.assertEqual((topic["negated"], topic["ml_model_version"]), (True, 2))

    def test_newest_tag_of_the_same_model_wins(self, *_):
        # the same model re-run on a paper (SCRUM-6602): reference 980853 went from NEG to LOW
        result, _ = self.fetch({"AGRKB:1": [
            tag("ATP:0000084", source=CLASSIFIER, level="LOW", model_version=2, created="2026-04-24T04:52:10.699369"),
            tag("ATP:0000084", source=CLASSIFIER, level="NEG", negated=True, model_version=2,
                created="2025-10-12T19:04:35.576091")]})
        topic = result["AGRKB:1"]["classifications"][CLASSIFIER]["ATP:0000084"]
        self.assertEqual((topic["negated"], topic["confidence_level"]), (False, "LOW"))

    def test_other_mods_and_sources_are_ignored(self, *_):
        result, _ = self.fetch({"AGRKB:1": [
            tag("ATP:0000082", source=CLASSIFIER, level="HIGH", mod="FB"),
            tag("ATP:0000082", source="ACKnowledge_form", level="HIGH"),
            tag("ATP:0000005", "WB:WBGene00000001", "aap-1", source=ACK, mod="FB")]})
        self.assertEqual(result["AGRKB:1"]["classifications"][CLASSIFIER], {})
        self.assertFalse(result["AGRKB:1"]["has_ack_pipeline_entity_tags"])

    def test_classifier_tags_with_an_entity_are_ignored(self, *_):
        result, _ = self.fetch({"AGRKB:1": [
            tag("ATP:0000082", "WB:WBGene00000001", "aap-1", source=CLASSIFIER, level="HIGH")]})
        self.assertEqual(result["AGRKB:1"]["classifications"][CLASSIFIER], {})

    def test_papers_missing_from_the_response_are_empty(self, *_):
        result, _ = self.fetch({})
        paper = result["AGRKB:1"]
        self.assertFalse(paper["has_ack_pipeline_entity_tags"])
        self.assertEqual(paper["classifications"], {CLASSIFIER: {}, ACK: {}})
        self.assertEqual(paper["entities"][EXTRACTOR],
                         {"gene": [], "allele": [], "strain": [], "transgene": [], "species": []})

    def test_curies_are_requested_in_batches(self, *_):
        curies = [f"AGRKB:{i}" for i in range(BATCH_SIZE + 5)]
        with mock.patch("wbtools.literature.abc_entities.requests.post",
                        return_value=response({"tags": {}})) as post:
            result = get_abc_paper_tags(curies)
        self.assertEqual([len(c[1]["json"]["curies_or_reference_ids"]) for c in post.call_args_list],
                         [BATCH_SIZE, 5])
        self.assertEqual(len(result), BATCH_SIZE + 5)

    def test_http_error_raises(self, *_):
        with mock.patch("wbtools.literature.abc_entities.requests.post", return_value=response({}, status=500)):
            with self.assertRaises(ABCRequestError):
                get_abc_paper_tags(["AGRKB:1"])


@mock.patch("wbtools.literature.abc_tags.generate_headers", return_value={})
@mock.patch("wbtools.literature.abc_tags.get_authentication_token", return_value="token")
class TestGetAbcCurie(unittest.TestCase):

    def test_curie_of_a_wb_paper(self, *_):
        with mock.patch("wbtools.literature.abc_tags.requests.get",
                        return_value=response({"curie": "AGRKB:101000000000001"})) as get:
            self.assertEqual(get_abc_curie("00069459", timeout=30), "AGRKB:101000000000001")
        self.assertTrue(get.call_args[0][0].endswith("/reference/by_cross_reference/WB:WBPaper00069459"))
        self.assertEqual(get.call_args[1]["timeout"], 30)

    def test_paper_not_in_the_abc(self, *_):
        with mock.patch("wbtools.literature.abc_tags.requests.get", return_value=response({}, status=404)):
            self.assertIsNone(get_abc_curie("00069459"))

    def test_server_error_raises(self, *_):
        with mock.patch("wbtools.literature.abc_tags.requests.get", return_value=response({}, status=500)):
            with self.assertRaises(ABCRequestError):
                get_abc_curie("00069459")

    def test_connection_error_raises(self, *_):
        with mock.patch("wbtools.literature.abc_tags.requests.get",
                        side_effect=requests.exceptions.ConnectionError("down")):
            with self.assertRaises(ABCRequestError):
                get_abc_curie("00069459")


if __name__ == '__main__':
    unittest.main()
