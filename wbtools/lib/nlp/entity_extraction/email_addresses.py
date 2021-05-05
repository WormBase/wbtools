import re

EMAIL_ADDRESS_REGEX = r'[\(\[]?([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)[\)\]\.]?'


def get_email_addresses_from_text(text):
    all_addresses = re.findall(EMAIL_ADDRESS_REGEX, text)
    if not all_addresses:
        text = text.replace(". ", ".")
        all_addresses = re.findall(EMAIL_ADDRESS_REGEX, text)
        all_addresses = [address.strip(".") for address in all_addresses]
    added_addresses = set()
    return_addresses = []
    for address in all_addresses:
        if address not in added_addresses:
            return_addresses.append(address)
            added_addresses.add(address)
    return return_addresses
