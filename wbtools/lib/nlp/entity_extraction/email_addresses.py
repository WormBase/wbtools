import re

EMAIL_ADDRESS_REGEX = r'[\(\[]?[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+[\)\]\.]?'


def get_email_addresses_from_text(text):
    all_addresses = re.findall(EMAIL_ADDRESS_REGEX, text)
    if not all_addresses:
        text = text.replace(". ", ".")
        all_addresses = re.findall(EMAIL_ADDRESS_REGEX, text)
    return all_addresses
