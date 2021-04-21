import re

EMAIL_ADDRESS_REGEX = r'[\(\[]?([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)[\)\]\.]?'


def get_email_addresses_from_text(text):
    all_addresses = re.findall(EMAIL_ADDRESS_REGEX, text)
    return_addresses = []
    if not all_addresses:
        text = text.replace(". ", ".")
        all_addresses = re.findall(EMAIL_ADDRESS_REGEX, text)
        added_addresses = set()
        for address in all_addresses:
            address = address.strip(".")
            if address not in added_addresses:
                return_addresses.append(address)
                added_addresses.add(address)
    return return_addresses
