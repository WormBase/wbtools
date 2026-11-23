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


PERSON = "AGRKB:103000000000024"


@pytest.fixture(autouse=True)
def no_cached_persons():
    abc._WB_PERSON_IDS.clear()
    yield
    abc._WB_PERSON_IDS.clear()


def test_wb_person_id_from_cross_reference():
    xrefs = [{"curie": "ORCID:0000-0001-2345-6789", "is_obsolete": False},
             {"curie": "WB:WBPerson1843", "is_obsolete": False},
             {"curie": "WB:WBPerson9", "is_obsolete": True}]
    with mock.patch.object(abc.requests, "get", return_value=response(200, xrefs)) as get:
        assert abc.wb_person_id(PERSON, timeout=30) == "WBPerson1843"
        assert abc.wb_person_id(PERSON, timeout=30) == "WBPerson1843"
    get.assert_called_once()
    assert get.call_args.args[0].endswith(f"/person_cross_reference/person/{PERSON}")


def test_wb_person_id_none_without_wb_cross_reference():
    xrefs = [{"curie": "ORCID:1", "is_obsolete": False}]
    with mock.patch.object(abc.requests, "get", return_value=response(200, xrefs)):
        assert abc.wb_person_id(PERSON, timeout=30) is None


def test_wb_person_id_raises_on_error():
    with mock.patch.object(abc.requests, "get", return_value=response(500, None, text="boom")):
        with pytest.raises(ABCRequestError):
            abc.wb_person_id(PERSON, timeout=30)


def test_get_ack_form_tags_maps_abc_persons_back_to_wb_ids():
    """SCRUM-6729: the ABC records a WB person it knows under their ABC curie. ACKnowledge tells contributors apart
    by WB person id, so the tag is read back as the WBPerson that wrote it: otherwise the next save would see a new
    contributor and create a second copy for the same person."""
    tags = [{"topic_entity_tag_id": 1, "created_by": "Kimberly Van Auken", "created_by_user_id": PERSON},
            {"topic_entity_tag_id": 2, "created_by": "WBPerson13824", "created_by_user_id": "WBPerson13824"},
            {"topic_entity_tag_id": 3, "created_by": "unknown_author", "created_by_user_id": "unknown_author"}]
    with mock.patch.object(abc.requests, "post", return_value=response(200, {"tags": {CURIE: tags}})), \
            mock.patch.object(abc.requests, "get", return_value=response(
                200, [{"curie": "WB:WBPerson1843", "is_obsolete": False}])) as get:
        read = abc.get_ack_form_tags(CURIE, timeout=30)
    assert [t["created_by_user_id"] for t in read] == ["WBPerson1843", "WBPerson13824", "unknown_author"]
    assert read[0]["abc_created_by_user_id"] == PERSON
    get.assert_called_once()


def test_get_ack_form_tags_keeps_a_person_without_wb_id():
    tags = [{"topic_entity_tag_id": 1, "created_by_user_id": PERSON}]
    with mock.patch.object(abc.requests, "post", return_value=response(200, {"tags": {CURIE: tags}})), \
            mock.patch.object(abc.requests, "get", return_value=response(404, None)):
        assert abc.get_ack_form_tags(CURIE, timeout=30)[0]["created_by_user_id"] == PERSON
