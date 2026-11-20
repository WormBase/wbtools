from unittest import mock

import pytest
import requests

from wbtools.literature.ack_form import abc
from wbtools.literature.ack_form.changes import TagBatch
from wbtools.literature.paper import ABCRequestError

CURIE = "AGRKB:101000000000001"


def response(status, body=None, text=""):
    resp = mock.Mock(status_code=status, text=text)
    resp.json.return_value = body
    return resp


@pytest.fixture(autouse=True)
def no_auth():
    with mock.patch.object(abc, "get_authentication_token", return_value="token"), \
            mock.patch.object(abc, "generate_headers", return_value={"Authorization": "Bearer token"}):
        yield


def test_send_tag_batch_posts_one_request():
    batch = TagBatch(create=[{"topic": "ATP:0000082", "negated": False}],
                     update=[{"topic_entity_tag_id": 5, "changes": {"note": None}}], delete=[7])
    with mock.patch.object(abc.requests, "post", return_value=response(
            200, {"reference_curie": CURIE, "created": [11], "updated": 1, "deleted": 1})) as post:
        assert abc.send_tag_batch(CURIE, batch, timeout=30) == [11]
    post.assert_called_once()
    assert post.call_args.args[0].endswith("/topic_entity_tag/batch")
    body = post.call_args.kwargs["json"]
    assert body == {"reference_curie": CURIE,
                    "create": [{"topic": "ATP:0000082", "negated": False, "reference_curie": CURIE}],
                    "update": [{"topic_entity_tag_id": 5, "changes": {"note": None}}], "delete": [7]}
    assert post.call_args.kwargs["timeout"] == 30
    assert batch.create == [{"topic": "ATP:0000082", "negated": False}]  # the caller's batch is not changed


def test_send_tag_batch_raises_on_4xx():
    with mock.patch.object(abc.requests, "post", return_value=response(
            409, {"detail": {"message": "create 0: exists"}}, text='{"detail": "create 0: exists"}')):
        with pytest.raises(ABCRequestError, match="409.*create 0: exists"):
            abc.send_tag_batch(CURIE, TagBatch(delete=[1]), timeout=30)


def test_send_tag_batch_raises_on_timeout():
    with mock.patch.object(abc.requests, "post", side_effect=requests.Timeout("slow")):
        with pytest.raises(ABCRequestError, match="slow"):
            abc.send_tag_batch(CURIE, TagBatch(delete=[1]), timeout=30)


def test_send_tag_batch_refuses_over_cap():
    with mock.patch.object(abc.requests, "post") as post:
        with pytest.raises(ABCRequestError, match="5000"):
            abc.send_tag_batch(CURIE, TagBatch(delete=list(range(5001))), timeout=30)
    post.assert_not_called()


def test_get_ack_form_tags_filters_source():
    tags = [{"topic_entity_tag_id": 1, "topic": "ATP:0000082"}]
    with mock.patch.object(abc.requests, "post", return_value=response(200, {"tags": {CURIE: tags}})) as post:
        assert abc.get_ack_form_tags(CURIE, timeout=30) == tags
    body = post.call_args.kwargs["json"]
    assert body["curies_or_reference_ids"] == [CURIE]
    assert body["filters"]["source_methods"] == ["ACKnowledge_form"]


def test_get_ack_form_tags_no_tags():
    with mock.patch.object(abc.requests, "post", return_value=response(200, {"tags": {}})):
        assert abc.get_ack_form_tags(CURIE, timeout=30) == []


def test_get_ack_form_tags_raises_on_error():
    with mock.patch.object(abc.requests, "post", return_value=response(500, None, text="boom")):
        with pytest.raises(ABCRequestError):
            abc.get_ack_form_tags(CURIE, timeout=30)


def test_get_ack_form_source_id_is_looked_up_once():
    abc._SOURCE_IDS.clear()
    with mock.patch.object(abc.requests, "get", return_value=response(200, {"tag_source_id": 321})) as get:
        assert abc.get_ack_form_source_id(timeout=30) == 321
        assert abc.get_ack_form_source_id(timeout=30) == 321
    get.assert_called_once()
    assert get.call_args.args[0].endswith("/tag_source/ATP:0000035/ACKnowledge_form/WB/WB")


def test_get_ack_form_source_id_raises_on_error():
    abc._SOURCE_IDS.clear()
    with mock.patch.object(abc.requests, "get", return_value=response(404, None, text="no source")):
        with pytest.raises(ABCRequestError, match="404"):
            abc.get_ack_form_source_id(timeout=30)
