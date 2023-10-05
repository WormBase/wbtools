import base64
import os
import re
import ssl
import tempfile
import urllib.request
from typing import List


def skip_ssl_validation():
    if not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context


def get_cgc_allele_designations():
    skip_ssl_validation()
    res = urllib.request.urlopen('https://cgc.umn.edu/laboratories')
    html_content = res.read().decode('utf-8')
    results = re.findall(r'<td><a href="https://cgc\.umn\.edu/laboratory/[A-Z]{1,3}">[A-Z]{1,3}</a></td>\n[ ]*<td>([a-z]+)</td>', html_content)
    return sorted(list(set(results)))


def get_cgc_lab_designations():
    skip_ssl_validation()
    res = urllib.request.urlopen('https://cgc.umn.edu/laboratories')
    html_content = res.read().decode('utf-8')
    results = re.findall(r'<td><a href="https://cgc\.umn\.edu/laboratory/([A-Z]{1,3})">[A-Z]{1,3}</a></td>\n[ ]*<td>[a-z]+</td>', html_content)
    return sorted(list(set(results)))


def get_curated_papers(datatype, tazendra_user, tazendra_password) -> List[str]:
    """Get the list of papers already curated for a specific data type

    The list is fetched from the WB curation status form

    Args:
        datatype (str): any valid WB data type

    Returns:
        List[str]: the list of paper ids already curated for the specified data type
    """
    if datatype == "humdis":
        datatype = "humandisease"
    curated_papers = set()
    request = urllib.request.Request("https://caltech-curation.textpressolab.com/priv/cgi-bin/curation_status.cgi?action="
                                     "listCurationStatisticsPapersPage&select_curator=two1823&method=allcur&"
                                     "listDatatype=" + datatype)
    base64string = base64.b64encode(bytes('%s:%s' % (tazendra_user, tazendra_password), 'ascii'))
    request.add_header("Authorization", "Basic %s" % base64string.decode('utf-8'))
    with urllib.request.urlopen(request) as response:
        res = response.read().decode("utf8")
        m = re.match('.*<textarea rows="4" cols="80" name="specific_papers">(.*)</textarea>.*',
                     res.replace('\n', ''))
        if m:
            curated_papers = set(m.group(1).split())
    request = urllib.request.Request("https://caltech-curation.textpressolab.com/priv/cgi-bin/curation_status.cgi?action="
                                     "listCurationStatisticsPapersPage&select_curator=two1823&method=allval%20neg&"
                                     "listDatatype=" + datatype)
    base64string = base64.b64encode(bytes('%s:%s' % (tazendra_user, tazendra_password), 'ascii'))
    request.add_header("Authorization", "Basic %s" % base64string.decode('utf-8'))
    with urllib.request.urlopen(request) as response:
        res = response.read().decode("utf8")
        m = re.match('.*<textarea rows="4" cols="80" name="specific_papers">(.*)</textarea>.*',
                     res.replace('\n', ''))
        if m:
            curated_papers = curated_papers | set(m.group(1).split())
    return list(curated_papers)


def get_supp_file_names_from_paper_dir(paper_sup_dir_url, user, password):
    request = urllib.request.Request(paper_sup_dir_url)
    base64string = base64.b64encode(bytes('%s:%s' % (user, password), 'ascii'))
    request.add_header("Authorization", "Basic %s" % base64string.decode('utf-8'))
    supp_files = set()
    with urllib.request.urlopen(request) as response:
        res = response.read().decode("utf8")
        m = re.findall('.*alt="\[   \]"></td><td><a href="([^"]+)">.*', res)
        if m:
            supp_files = set(m)
    return list(supp_files)


def download_pdf_file_from_url(url, user, password):
    tmp_file = tempfile.NamedTemporaryFile()
    request = urllib.request.Request(url)
    base64string = base64.b64encode(bytes('%s:%s' % (user, password), 'ascii'))
    request.add_header("Authorization", "Basic %s" % base64string.decode('utf-8'))
    with urllib.request.urlopen(request) as response:
        with open(tmp_file.name, 'wb') as tmp_file_stream:
            tmp_file_stream.write(response.read())
    return tmp_file
