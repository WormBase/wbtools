import unittest
from unittest import mock

from wbtools.literature.paper import WBPaper, ABCRequestError

MAIN_MD = ("# Neuronal regulation of aging in C. elegans\n\n"
           "## Abstract\n\n"
           "We show that daf-16 controls lifespan. The effect needs neurons.\n\n"
           "## Results\n\n"
           "Mutants of unc-119 lived longer than wild type animals.\n")
SUPPLEMENT_MD = ("# Supplementary Information\n\n"
                 "## Supplementary methods\n\n"
                 "Worms were grown on NGM plates at 20 degrees.\n")


def ref_file(referencefile_id, file_class, file_extension="md", mods=(None,), display_name="file"):
    return {"referencefile_id": referencefile_id, "file_class": file_class, "file_extension": file_extension,
            "display_name": display_name,
            "referencefile_mods": [{"mod_abbreviation": mod} for mod in mods]}


def has_sentence(sentences, text):
    # the parser joins a heading to the first sentence that follows it, e.g. "Results Mutants of unc-119 ..."
    return any(text in sentence for sentence in sentences)


class FakeAbc(object):
    """serves /show_all and /download_file responses the way get_data_from_url returns them"""

    def __init__(self, files, contents):
        self.files = files
        self.contents = contents

    def __call__(self, url, headers=None, file_type='json'):
        if "/referencefile/show_all/" in url:
            return self.files
        if "/referencefile/download_file/" in url:
            return self.contents.get(int(url.rsplit("/", 1)[1]))
        raise AssertionError(f"unexpected URL {url}")


@mock.patch("wbtools.literature.paper.generate_headers", return_value={})
@mock.patch("wbtools.literature.paper.get_authentication_token", return_value="token")
class TestLoadTextFromAbcMarkdown(unittest.TestCase):

    def load(self, files, contents):
        paper = WBPaper(paper_id="00068500", agr_curie="AGRKB:101000001193987")
        with mock.patch("wbtools.literature.paper.get_data_from_url", side_effect=FakeAbc(files, contents)):
            result = paper.load_text_from_abc_markdown()
        return paper, result

    def test_loads_main_and_wb_supplements_as_sentences(self, *_):
        paper, result = self.load(
            [ref_file(1, "main", "pdf", mods=("WB",)), ref_file(2, "converted_merged_main"),
             ref_file(3, "converted_merged_supplement", mods=("WB",))],
            {2: MAIN_MD.encode("utf-8"), 3: SUPPLEMENT_MD.encode("utf-8")})
        self.assertTrue(result)
        self.assertIsInstance(paper.main_text, list)
        self.assertTrue(has_sentence(paper.main_text, "Mutants of unc-119 lived longer than wild type animals."))
        self.assertEqual(len(paper.supplemental_docs), 1)
        self.assertTrue(has_sentence(paper.supplemental_docs[0], "Worms were grown on NGM plates at 20 degrees."))
        self.assertTrue(paper.has_main_text())
        self.assertIn("daf-16", paper.get_text_docs(include_supplemental=True, return_concatenated=True))

    def test_prefers_wb_or_shared_main_file(self, *_):
        paper, _ = self.load(
            [ref_file(2, "converted_merged_main", mods=("ZFIN",)), ref_file(4, "converted_merged_main", mods=("WB",))],
            {2: b"# Other\n\n## Results\n\nZebrafish text only here.\n", 4: MAIN_MD.encode("utf-8")})
        self.assertTrue(has_sentence(paper.main_text, "Mutants of unc-119 lived longer than wild type animals."))

    def test_falls_back_to_a_main_file_of_another_mod(self, *_):
        paper, _ = self.load([ref_file(2, "converted_merged_main", mods=("ZFIN",))], {2: MAIN_MD.encode("utf-8")})
        self.assertTrue(has_sentence(paper.main_text, "Mutants of unc-119 lived longer than wild type animals."))

    def test_supplements_of_other_mods_are_ignored(self, *_):
        paper, _ = self.load(
            [ref_file(2, "converted_merged_main"), ref_file(3, "converted_merged_supplement", mods=("ZFIN",))],
            {2: MAIN_MD.encode("utf-8"), 3: SUPPLEMENT_MD.encode("utf-8")})
        self.assertEqual(paper.supplemental_docs, [])

    def test_no_main_markdown_raises(self, *_):
        with self.assertRaises(ABCRequestError):
            self.load([ref_file(1, "main", "pdf", mods=("WB",))], {})

    def test_file_list_failure_raises(self, *_):
        paper = WBPaper(paper_id="00068500", agr_curie="AGRKB:101000001193987")
        with mock.patch("wbtools.literature.paper.get_data_from_url", return_value=None):
            with self.assertRaises(ABCRequestError):
                paper.load_text_from_abc_markdown()

    def test_main_download_failure_raises(self, *_):
        with self.assertRaises(ABCRequestError):
            self.load([ref_file(2, "converted_merged_main")], {2: None})

    def test_main_markdown_without_sentences_raises(self, *_):
        with self.assertRaises(ABCRequestError):
            self.load([ref_file(2, "converted_merged_main")], {2: b"\n\n   \n"})

    def test_supplement_failure_keeps_main_text(self, *_):
        paper, result = self.load(
            [ref_file(2, "converted_merged_main"), ref_file(3, "converted_merged_supplement", display_name="S1")],
            {2: MAIN_MD.encode("utf-8"), 3: None})
        self.assertTrue(result)
        self.assertTrue(paper.main_text)
        self.assertEqual(paper.supplemental_docs, [])

    def test_latin1_markdown_is_decoded(self, *_):
        latin1_markdown = "# Title\n\n## Results\n\nThe prot\xe9ine is expressed in neurons.\n".encode("latin-1")
        paper, _ = self.load([ref_file(2, "converted_merged_main")], {2: latin1_markdown})
        self.assertTrue(has_sentence(paper.main_text, "expressed in neurons"))


if __name__ == '__main__':
    unittest.main()
