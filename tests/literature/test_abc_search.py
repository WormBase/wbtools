import unittest
from unittest import mock

import requests

from wbtools.literature.abc_search import get_wb_paper_ids_from_abc, SEARCH_PAGE_SIZE
from wbtools.literature.paper import ABCRequestError

REQUIRED_TAGS = {"file_workflow": ["ATP:0000163"], "email_extraction": ["ATP:0000355"]}


def hit(number, xrefs=None):
    curie = f"AGRKB:1010000000{number:05d}"
    if xrefs is None:
        xrefs = [{"curie": f"WB:WBPaper{number:08d}", "is_obsolete": "false"}]
    return {"curie": curie, "cross_references": xrefs}


def response(json_body, status=200):
    resp = mock.Mock()
    resp.status_code = status
    resp.json.return_value = json_body
    if status >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(f"{status} error")
    else:
        resp.raise_for_status.return_value = None
    return resp


@mock.patch("wbtools.literature.abc_search.generate_headers", return_value={})
@mock.patch("wbtools.literature.abc_search.get_authentication_token", return_value="token")
class TestGetWbPaperIdsFromAbc(unittest.TestCase):

    def run_search(self, pages):
        with mock.patch("wbtools.literature.abc_search.requests.post",
                        side_effect=[response(page) for page in pages]) as post:
            results = list(get_wb_paper_ids_from_abc(REQUIRED_TAGS, "2024-09-24", "2026-09-24"))
        return results, post

    def test_request_body(self, *_):
        _, post = self.run_search([{"hits": [hit(1)], "return_count": 1}])
        self.assertTrue(post.call_args[0][0].endswith("/search/references/"))
        body = post.call_args[1]["json"]
        self.assertEqual(body["facets_values"], {"mods_in_corpus.keyword": ["WB"], **REQUIRED_TAGS})
        self.assertEqual(body["date_created"], ["2024-09-24", "2026-09-24"])
        self.assertEqual(body["sort"], [{"date_created": {"order": "desc"}}])
        self.assertEqual(body["size_result_count"], SEARCH_PAGE_SIZE)
        self.assertEqual(body["page"], 1)

    def test_required_tags_are_not_modified(self, *_):
        tags = {"file_workflow": ["ATP:0000163"]}
        with mock.patch("wbtools.literature.abc_search.requests.post", return_value=response({"hits": []})):
            list(get_wb_paper_ids_from_abc(tags, "2024-09-24", "2026-09-24"))
        self.assertEqual(tags, {"file_workflow": ["ATP:0000163"]})

    def test_yields_wb_paper_id_and_curie(self, *_):
        results, _ = self.run_search([{"hits": [hit(70170)]}])
        self.assertEqual(results, [("00070170", "AGRKB:101000000070170")])

    def test_pages_until_a_short_page(self, *_):
        full_page = {"hits": [hit(i) for i in range(SEARCH_PAGE_SIZE)]}
        results, post = self.run_search([full_page, {"hits": [hit(500)]}])
        self.assertEqual(len(results), SEARCH_PAGE_SIZE + 1)
        self.assertEqual([c[1]["json"]["page"] for c in post.call_args_list], [1, 2])

    def test_stops_on_an_empty_page_after_a_full_one(self, *_):
        full_page = {"hits": [hit(i) for i in range(SEARCH_PAGE_SIZE)]}
        results, post = self.run_search([full_page, {"hits": []}])
        self.assertEqual(len(results), SEARCH_PAGE_SIZE)
        self.assertEqual(post.call_count, 2)

    def test_hit_without_wb_paper_xref_is_skipped(self, *_):
        results, _ = self.run_search([{"hits": [hit(1, xrefs=[{"curie": "PMID:123", "is_obsolete": "false"}]),
                                                hit(2)]}])
        self.assertEqual(results, [("00000002", "AGRKB:101000000000002")])

    def test_obsolete_wb_xref_is_ignored_string_or_bool(self, *_):
        results, _ = self.run_search([{"hits": [
            hit(1, xrefs=[{"curie": "WB:WBPaper00000001", "is_obsolete": "true"}]),
            hit(2, xrefs=[{"curie": "WB:WBPaper00000002", "is_obsolete": True}]),
            hit(3, xrefs=[{"curie": "WB:WBPaper00000099", "is_obsolete": "true"},
                          {"curie": "WB:WBPaper00000003", "is_obsolete": False}])]}])
        self.assertEqual(results, [("00000003", "AGRKB:101000000000003")])

    def test_http_error_raises(self, *_):
        with mock.patch("wbtools.literature.abc_search.requests.post", return_value=response({}, status=500)):
            with self.assertRaises(ABCRequestError):
                list(get_wb_paper_ids_from_abc(REQUIRED_TAGS, "2024-09-24", "2026-09-24"))

    def test_error_in_response_body_raises(self, *_):
        with mock.patch("wbtools.literature.abc_search.requests.post",
                        return_value=response({"error": "Elasticsearch unavailable"})):
            with self.assertRaises(ABCRequestError):
                list(get_wb_paper_ids_from_abc(REQUIRED_TAGS, "2024-09-24", "2026-09-24"))

    def test_connection_error_raises(self, *_):
        with mock.patch("wbtools.literature.abc_search.requests.post",
                        side_effect=requests.exceptions.ConnectionError("down")):
            with self.assertRaises(ABCRequestError):
                list(get_wb_paper_ids_from_abc(REQUIRED_TAGS, "2024-09-24", "2026-09-24"))


if __name__ == '__main__':
    unittest.main()
