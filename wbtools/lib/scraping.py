import os
import re
import ssl
import urllib.request


def skip_ssl_validation():
    if not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context


def get_cgc_allele_designations():
    skip_ssl_validation()
    res = urllib.request.urlopen('https://cgc.umn.edu/laboratories')
    html_content = res.read().decode('utf-8')
    results = re.findall(r'<td><a href="https://cgc\.umn\.edu/laboratory/[A-Z]{1,3}">[A-Z]{1,3}</a></td>\n[ ]*<td>([a-z]+)</td>', html_content)
    return results
