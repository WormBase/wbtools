import unittest
from unittest import mock

import requests

from wbtools.literature.abc_tag_writer import create_topic_entity_tags
from wbtools.literature.paper import ABCRequestError

TAGS = [{"topic": "ATP:0000005", "entity": "WB:WBGene00002974", "negated": False, "tag_source_id": 159},
        {"topic": "ATP:0000082", "negated": True, "tag_source_id": 159},
        {"topic": "ATP:0000027", "negated": True, "tag_source_id": 159}]


def response(status, json_body=None, text=""):
    resp = mock.Mock()
    resp.status_code = status
    resp.text = text
    if json_body is None:
        resp.json.side_effect = ValueError("no json")
    else:
        resp.json.return_value = json_body
    return resp


def conflict(reason):
    return response(409, {"detail": {"reason": reason, "message": "exists"}})


@mock.patch("wbtools.literature.abc_tag_writer.generate_headers", return_value={})
@mock.patch("wbtools.literature.abc_tag_writer.get_authentication_token", return_value="token")
class TestCreateTopicEntityTags(unittest.TestCase):

    def post(self, responses, tags=TAGS, **kwargs):
        with mock.patch("wbtools.literature.abc_tag_writer.requests.post", side_effect=responses) as post:
            result = create_topic_entity_tags("AGRKB:1", tags, **kwargs)
        return result, post

    def test_each_tag_is_posted_with_the_reference(self, *_):
        created, post = self.post([response(201, {}), response(201, {}), response(200, {})], timeout=60)
        self.assertEqual(created, 3)
        self.assertEqual(post.call_count, 3)
        self.assertTrue(post.call_args_list[0][0][0].endswith("/topic_entity_tag/"))
        self.assertEqual(post.call_args_list[0][1]["json"], dict(TAGS[0], reference_curie="AGRKB:1"))
        self.assertEqual(post.call_args_list[0][1]["timeout"], 60)
        self.assertNotIn("reference_curie", TAGS[0])

    def test_already_stored_tags_are_not_errors(self, *_):
        created, _ = self.post([conflict("duplicate"), conflict("different_creator"), response(201, {})])
        self.assertEqual(created, 1)

    def test_other_conflicts_raise(self, *_):
        for bad in (conflict("opposite_negation"), response(409, None, text="<html>conflict</html>"),
                    response(409, ["not", "a", "dict"])):
            with self.assertRaises(ABCRequestError):
                self.post([bad], tags=TAGS[:1])

    def test_server_error_raises_naming_the_tag(self, *_):
        with self.assertRaises(ABCRequestError) as raised:
            self.post([response(201, {}), response(500, None, text="boom")])
        self.assertIn("ATP:0000082", str(raised.exception))
        self.assertIn("500", str(raised.exception))

    def test_connection_error_raises(self, *_):
        with self.assertRaises(ABCRequestError):
            self.post(requests.exceptions.ConnectionError("down"), tags=TAGS[:1])

    def test_failure_keeps_earlier_posts_and_retry_completes(self, *_):
        with self.assertRaises(ABCRequestError):
            _, first = self.post([response(201, {}), response(502, None, text="bad gateway")])
        # the retry finds the first tag already there and creates the other two
        created, post = self.post([conflict("duplicate"), response(201, {}), response(201, {})])
        self.assertEqual(created, 2)
        self.assertEqual(post.call_count, 3)

    def test_no_tags_makes_no_request(self, *_):
        created, post = self.post([], tags=[])
        self.assertEqual(created, 0)
        post.assert_not_called()


if __name__ == '__main__':
    unittest.main()
