from typing import List


class WBPerson(object):

    def __init__(self, person_id: str = None, first_name: str = None, last_name: str = None, middle_name: str = None,
                 aka_first_name: str = None, aka_middle_name: str = None, aka_last_name: str = None, city: str = None,
                 state: str = None, orcid: str = None, street: str = None,
                 country: str = None, email: str = None, fax: str = None,
                 institution: str = None, lab: str = None, old_lab: str = None, lab_phone: str = None,
                 status: bool = True, main_phone: str = None, office_phone: str = None, old_emails: List[str] = None,
                 comment: str = None, contact_data: str = None):
        self.person_id = person_id
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.aka_first_name = aka_first_name
        self.aka_middle_name = aka_middle_name
        self.aka_last_name = aka_last_name
        self.orcid = orcid
        self.city = city
        self.state = state
        self.street = street
        self.country = country
        self.email = email
        self.fax = fax
        self.institution = institution
        self.lab = lab
        self.old_lab = old_lab
        self.lab_phone = lab_phone
        self.status = status
        self.main_phone = main_phone
        self.office_phone = office_phone
        self.old_emails = old_emails
        self.comment = comment
        self.contact_data = contact_data


class WBAuthor(WBPerson):

    def __init__(self, *args, corresponding: bool = False, verified: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.corresponding = corresponding
        self.verified = verified

    @classmethod
    def from_person(cls, person):
        return cls(**{k: v for k, v in person.__dict__.items() if not k.startswith('__') and not callable(k)})
