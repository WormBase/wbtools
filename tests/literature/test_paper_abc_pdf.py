import io
import json
import unittest
from unittest import mock

from wbtools.literature.paper import WBPaper


def pdf_ref_file(referencefile_id, file_class="main", mods=("WB",)):
    return {"referencefile_id": referencefile_id, "file_class": file_class, "file_extension": "pdf",
            "file_publication_status": "final", "pdf_type": None,
            "referencefile_mods": [{"mod_abbreviation": mod} for mod in mods]}


@mock.patch("wbtools.literature.paper.generate_headers", return_value={})
@mock.patch("wbtools.literature.paper.get_authentication_token", return_value="token")
class TestAddFileFromAbcPdf(unittest.TestCase):

    def test_failed_download_is_skipped(self, *_):
        # get_data_from_url returns None when the ABC answers with an HTTP error (e.g. 500 on a broken file)
        paper = WBPaper(paper_id="00068500", agr_curie="AGRKB:101000001193987")
        with mock.patch("wbtools.literature.paper.get_data_from_url", return_value=None), \
                mock.patch("wbtools.literature.paper.convert_pdf_to_txt") as convert:
            result = paper.add_file_from_abc_reffile_obj(pdf_ref_file(4487781))
        self.assertFalse(result)
        convert.assert_not_called()
        self.assertFalse(paper.main_text)

    def test_one_failed_file_does_not_stop_the_others(self, *_):
        files = [pdf_ref_file(1), pdf_ref_file(2, file_class="supplement")]
        paper = WBPaper(paper_id="00068500", agr_curie="AGRKB:101000001193987", db_manager=mock.Mock())
        show_all = mock.MagicMock()
        show_all.__enter__.return_value = io.BytesIO(json.dumps(files).encode("utf8"))
        with mock.patch("wbtools.literature.paper.urllib.request.urlopen", return_value=show_all), \
                mock.patch("wbtools.literature.paper.get_data_from_url",
                           side_effect=lambda url, *args, **kwargs: None if url.endswith("/1") else b"%PDF"), \
                mock.patch("wbtools.literature.paper.convert_pdf_to_txt",
                           return_value=["Worms were grown on NGM plates."]):
            result = paper.load_text_from_pdf_files(main_file_only=False)
        self.assertTrue(result)
        self.assertFalse(paper.main_text)
        self.assertEqual(paper.supplemental_docs, [["Worms were grown on NGM plates."]])


if __name__ == '__main__':
    unittest.main()
