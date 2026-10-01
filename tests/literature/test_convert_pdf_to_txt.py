import tempfile
import unittest
from unittest import mock

from wbtools.literature.paper import convert_pdf_to_txt

TEI_XML = """<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0">
<teiHeader>
  <fileDesc>
    <titleStmt><title level="a" type="main">Neuronal regulation of aging</title></titleStmt>
    <sourceDesc><biblStruct><analytic>
      <author><persName><forename type="first">Kacy</forename><surname>Gordon</surname></persName>
        <email>kacy.gordon@unc.edu</email></author>
      <author><email>no.surname@albany.edu</email></author>
      <title level="a" type="main">Neuronal regulation of aging</title>
    </analytic></biblStruct></sourceDesc>
  </fileDesc>
  <profileDesc><abstract><div><p><s>We show that daf-16 controls lifespan.</s></p></div></abstract></profileDesc>
</teiHeader>
<text>
  <body>
    <div><head n="1">Results</head><p><s>Mutants of unc-119 lived longer.</s></p></div>
  </body>
  <back>
    <div type="availability"><div><head>Data availability</head>
      <p><s>Data are deposited in the Gene Expression Omnibus under GSE12345.</s></p></div></div>
    <div type="references"><listBibl><biblStruct><analytic><title>Some cited paper</title></analytic>
      </biblStruct></listBibl></div>
  </back>
</text>
</TEI>"""


def convert(tei_xml):
    response = mock.Mock(is_success=True, content=tei_xml.encode("utf-8"))
    with tempfile.NamedTemporaryFile(suffix=".pdf") as pdf_file, \
            mock.patch("wbtools.literature.paper.process_fulltext_document.sync_detailed", return_value=response):
        return convert_pdf_to_txt(pdf_file.name)


class TestConvertPdfToTxt(unittest.TestCase):

    def test_keeps_body_sentences(self):
        sentences = convert(TEI_XML)
        self.assertIn("Mutants of unc-119 lived longer.", sentences)

    def test_adds_author_emails_from_the_header(self):
        sentences = convert(TEI_XML)
        self.assertIn("kacy.gordon@unc.edu", sentences)
        # GROBID can find an email without parsing the author's name
        self.assertIn("no.surname@albany.edu", sentences)

    def test_adds_back_matter(self):
        sentences = convert(TEI_XML)
        self.assertIn("Data are deposited in the Gene Expression Omnibus under GSE12345.", sentences)
        self.assertNotIn("Some cited paper", " ".join(sentences))

    def test_failed_conversion_returns_no_sentences(self):
        response = mock.Mock(is_success=False, content=b"")
        with tempfile.NamedTemporaryFile(suffix=".pdf") as pdf_file, \
                mock.patch("wbtools.literature.paper.process_fulltext_document.sync_detailed", return_value=response):
            self.assertEqual(convert_pdf_to_txt(pdf_file.name), [])


if __name__ == '__main__':
    unittest.main()
