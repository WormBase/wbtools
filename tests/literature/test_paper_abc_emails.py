import unittest
from unittest import mock

from wbtools.literature.corpus import CorpusManager
from wbtools.literature.paper import WBPaper, ABCRequestError
from wbtools.literature.person import WBAuthor, WBPerson


class FakePersonDBManager(object):
    """In-memory stand-in for the WB person tables (two_email / two_old_email)."""

    def __init__(self, person_id_by_email, current_email_by_person_id):
        self.person_id_by_email = {email.lower(): person_id for email, person_id in person_id_by_email.items()}
        self.current_email_by_person_id = current_email_by_person_id

    def get_person_id_from_email_address(self, email_address):
        return self.person_id_by_email.get(email_address.lower())

    def get_current_email_address_for_person(self, person_id):
        return self.current_email_by_person_id.get(person_id)

    def get_person(self, person_id):
        return WBPerson(person_id=person_id, email=self.current_email_by_person_id.get(person_id))


class FakePaperDBManager(object):

    def __init__(self, person_db_manager):
        self.person_db_manager = person_db_manager

    def get_db_manager(self, cls):
        return self.person_db_manager


def build_paper(person_id_by_email=None, current_email_by_person_id=None, authors=None, main_text=''):
    person_db_manager = FakePersonDBManager(person_id_by_email or {}, current_email_by_person_id or {})
    return WBPaper(agr_curie="AGRKB:101000000000001", paper_id="00001234", authors=authors or [],
                   main_text=main_text, db_manager=FakePaperDBManager(person_db_manager))


def contacts(result):
    return [(person.person_id, email) for person, email in result] if result else result


@mock.patch("wbtools.literature.paper.generate_headers", return_value={})
@mock.patch("wbtools.literature.paper.get_authentication_token", return_value="token")
class TestAuthorsWithEmailAddressFromABC(unittest.TestCase):

    def test_requests_emails_of_the_paper_curie(self, *_):
        paper = build_paper()
        with mock.patch("wbtools.literature.paper.get_data_from_url", return_value=[]) as get_data:
            paper.get_abc_email_addresses()
        self.assertTrue(get_data.call_args[0][0].endswith("/reference/AGRKB:101000000000001/emails"))

    def test_abc_address_of_wb_person_returns_current_and_abc_address(self, *_):
        paper = build_paper(person_id_by_email={"old@lab.edu": "two1"},
                            current_email_by_person_id={"two1": "new@lab.edu"})
        abc_emails = [{"reference_email_id": 1, "email_address": "old@lab.edu"}]
        with mock.patch("wbtools.literature.paper.get_data_from_url", return_value=abc_emails):
            result = paper.get_authors_with_email_address_in_wb(blacklisted_email_addresses=[])
        self.assertEqual(contacts(result), [("two1", "new@lab.edu"), ("two1", "old@lab.edu")])

    def test_abc_address_equal_to_current_address_is_returned_once(self, *_):
        paper = build_paper(person_id_by_email={"pi@lab.edu": "two1"},
                            current_email_by_person_id={"two1": "pi@lab.edu"})
        abc_emails = [{"reference_email_id": 1, "email_address": "pi@lab.edu"}]
        with mock.patch("wbtools.literature.paper.get_data_from_url", return_value=abc_emails):
            result = paper.get_authors_with_email_address_in_wb(blacklisted_email_addresses=[])
        self.assertEqual(contacts(result), [("two1", "pi@lab.edu")])

    def test_blacklisted_addresses_are_excluded(self, *_):
        paper = build_paper(person_id_by_email={"old@lab.edu": "two1", "b@lab.edu": "two2"},
                            current_email_by_person_id={"two1": "new@lab.edu", "two2": "b2@lab.edu"})
        abc_emails = [{"reference_email_id": 1, "email_address": "old@lab.edu"},
                      {"reference_email_id": 2, "email_address": "b@lab.edu"}]
        with mock.patch("wbtools.literature.paper.get_data_from_url", return_value=abc_emails):
            result = paper.get_authors_with_email_address_in_wb(
                blacklisted_email_addresses=["new@lab.edu", "b@lab.edu"])
        self.assertEqual(contacts(result), [("two1", "old@lab.edu"), ("two2", "b2@lab.edu")])

    def test_blacklist_defaults_to_empty(self, *_):
        paper = build_paper(person_id_by_email={"pi@lab.edu": "two1"},
                            current_email_by_person_id={"two1": "pi@lab.edu"})
        abc_emails = [{"reference_email_id": 1, "email_address": "pi@lab.edu"}]
        with mock.patch("wbtools.literature.paper.get_data_from_url", return_value=abc_emails):
            result = paper.get_authors_with_email_address_in_wb()
        self.assertEqual(contacts(result), [("two1", "pi@lab.edu")])

    def test_first_only_returns_first_matching_abc_address(self, *_):
        paper = build_paper(person_id_by_email={"unknown-first@lab.edu": None, "a@lab.edu": "two1",
                                                "b@lab.edu": "two2"},
                            current_email_by_person_id={"two1": "a@lab.edu", "two2": "b@lab.edu"})
        abc_emails = [{"reference_email_id": 1, "email_address": "unknown-first@lab.edu"},
                      {"reference_email_id": 2, "email_address": "a@lab.edu"},
                      {"reference_email_id": 3, "email_address": "b@lab.edu"}]
        with mock.patch("wbtools.literature.paper.get_data_from_url", return_value=abc_emails):
            result = paper.get_authors_with_email_address_in_wb(blacklisted_email_addresses=[], first_only=True)
        self.assertEqual(contacts(result), [("two1", "a@lab.edu")])

    def test_addresses_in_paper_text_are_ignored(self, *_):
        paper = build_paper(person_id_by_email={"intext@lab.edu": "two9", "abc@lab.edu": "two1"},
                            current_email_by_person_id={"two9": "intext@lab.edu", "two1": "abc@lab.edu"},
                            main_text="Correspondence to intext@lab.edu")
        abc_emails = [{"reference_email_id": 1, "email_address": "abc@lab.edu"}]
        with mock.patch("wbtools.literature.paper.get_data_from_url", return_value=abc_emails):
            result = paper.get_authors_with_email_address_in_wb(blacklisted_email_addresses=[])
        self.assertEqual(contacts(result), [("two1", "abc@lab.edu")])

    def test_no_abc_address_matching_a_wb_person_falls_back_to_wb_author_emails(self, *_):
        authors = [WBAuthor(person_id="two5", email="author@lab.edu"),
                   WBAuthor(person_id="two6", email=None),
                   WBAuthor(person_id="two7", email="blocked@lab.edu")]
        paper = build_paper(authors=authors)
        abc_emails = [{"reference_email_id": 1, "email_address": "stranger@lab.edu"}]
        with mock.patch("wbtools.literature.paper.get_data_from_url", return_value=abc_emails):
            result = paper.get_authors_with_email_address_in_wb(blacklisted_email_addresses=["blocked@lab.edu"])
        self.assertEqual(contacts(result), [("two5", "author@lab.edu")])

    def test_empty_abc_response_falls_back_to_wb_author_emails(self, *_):
        paper = build_paper(authors=[WBAuthor(person_id="two5", email="author@lab.edu")])
        with mock.patch("wbtools.literature.paper.get_data_from_url", return_value=[]):
            result = paper.get_authors_with_email_address_in_wb(blacklisted_email_addresses=[])
        self.assertEqual(contacts(result), [("two5", "author@lab.edu")])

    def test_no_contacts_returns_none(self, *_):
        paper = build_paper()
        with mock.patch("wbtools.literature.paper.get_data_from_url", return_value=[]):
            result = paper.get_authors_with_email_address_in_wb(blacklisted_email_addresses=[])
        self.assertIsNone(result)

    def test_abc_failure_raises_instead_of_falling_back(self, *_):
        paper = build_paper(authors=[WBAuthor(person_id="two5", email="author@lab.edu")])
        with mock.patch("wbtools.literature.paper.get_data_from_url", return_value=None):
            with self.assertRaises(ABCRequestError):
                paper.get_authors_with_email_address_in_wb(blacklisted_email_addresses=[])

    def test_abc_emails_are_requested_once_per_paper(self, *_):
        paper = build_paper(person_id_by_email={"pi@lab.edu": "two1"},
                            current_email_by_person_id={"two1": "pi@lab.edu"})
        abc_emails = [{"reference_email_id": 1, "email_address": "pi@lab.edu"}]
        with mock.patch("wbtools.literature.paper.get_data_from_url", return_value=abc_emails) as get_data:
            paper.get_authors_with_email_address_in_wb(blacklisted_email_addresses=[])
            paper.get_authors_with_email_address_in_wb(blacklisted_email_addresses=[])
        self.assertEqual(get_data.call_count, 1)

    def test_failed_request_is_not_cached(self, *_):
        paper = build_paper()
        abc_emails = [{"reference_email_id": 1, "email_address": "pi@lab.edu"}]
        with mock.patch("wbtools.literature.paper.get_data_from_url", side_effect=[None, abc_emails]):
            self.assertIsNone(paper.get_abc_email_addresses())
            self.assertEqual(paper.get_abc_email_addresses(), ["pi@lab.edu"])


class FakeWBDBManager(object):
    """Minimal WBDBManager for CorpusManager.load_from_wb_database with an explicit paper id list."""

    def __init__(self, *args, **kwargs):
        self.paper = mock.MagicMock()
        self.paper.get_paper_curie.side_effect = lambda paper_id: "AGRKB:" + paper_id
        self.generic = mock.MagicMock()
        self.generic.get_blacklisted_email_addresses.return_value = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class TestCorpusAuthorEmailFilter(unittest.TestCase):

    def load(self, contacts_by_paper_id):
        def fake_get_authors(paper, **kwargs):
            outcome = contacts_by_paper_id[paper.paper_id]
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        cm = CorpusManager()
        with mock.patch("wbtools.literature.corpus.WBDBManager", FakeWBDBManager), \
                mock.patch.object(WBPaper, "load_bib_info", return_value=True), \
                mock.patch.object(WBPaper, "get_authors_with_email_address_in_wb", autospec=True,
                                  side_effect=fake_get_authors):
            cm.load_from_wb_database("db", "user", "passwd", "host", paper_ids=list(contacts_by_paper_id),
                                     load_pdf_files=False, load_curation_info=False,
                                     exclude_no_author_email=True)
        return sorted(paper.paper_id for paper in cm.get_all_papers())

    def test_papers_with_abc_failure_or_no_contacts_are_skipped(self):
        paper_ids = self.load({"00000001": [(WBPerson(person_id="two1"), "pi@lab.edu")],
                               "00000002": None,
                               "00000003": ABCRequestError("ABC down")})
        self.assertEqual(paper_ids, ["00000001"])


if __name__ == '__main__':
    unittest.main()
