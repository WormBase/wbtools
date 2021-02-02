import unittest

from wbtools.lib.nlp.entity_extraction.email_addresses import get_email_addresses_from_text


class TestEmailAddresses(unittest.TestCase):

    def test_get_email_addresses_from_text(self):
        text = "test@email.com is a valid email address. @email is not. test@ is not. Other valid email addresses are:" \
               " t.test@stat.email.org"
        addr = get_email_addresses_from_text(text)
        self.assertEqual(len(addr), 2)


if __name__ == '__main__':
    unittest.main()
