import psycopg2

from wbtools.db.abstract_manager import AbstractWBDBManager
from wbtools.literature.person import WBPerson


class WBPersonDBManager(AbstractWBDBManager):

    def __init__(self, dbname, user, password, host):
        super().__init__(dbname, user, password, host)

    def get_person_id_from_email_address(self, email_address):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT * FROM two_email WHERE two_email=%s", (email_address, ))
            res = curs.fetchone()
            if res:
                return res[0]
            else:
                curs.execute("SELECT * FROM two_old_email WHERE two_old_email=%s", (email_address, ))
                res = curs.fetchone()
                if res:
                    return res[0]
            return None

    def get_first_name(self, person_id):
        return self._get_single_field(person_id, "two_firstname")

    def get_middle_name(self, person_id):
        return self._get_single_field(person_id, "two_middlename")

    def get_last_name(self, person_id):
        return self._get_single_field(person_id, "two_lastname")

    def get_aka_first_name(self, person_id):
        return self._get_single_field(person_id, "two_aka_firstname")

    def get_aka_middle_name(self, person_id):
        return self._get_single_field(person_id, "two_aka_middlename")

    def get_aka_last_name(self, person_id):
        return self._get_single_field(person_id, "two_aka_lastname")

    def get_orcid(self, person_id):
        return self._get_single_field(person_id, "two_orcid")

    def get_city(self, person_id):
        return self._get_single_field(person_id, "two_city")

    def get_state(self, person_id):
        return self._get_single_field(person_id, "two_state")

    def get_street(self, person_id):
        return self._get_single_field(person_id, "two_street")

    def get_country(self, person_id):
        return self._get_single_field(person_id, "two_country")

    def get_email(self, person_id):
        return self._get_single_field(person_id, "two_email")

    def get_fax(self, person_id):
        return self._get_single_field(person_id, "two_fax")

    def get_institution(self, person_id):
        return self._get_single_field(person_id, "two_institution")

    def get_lab(self, person_id):
        return self._get_single_field(person_id, "two_lab")

    def get_old_lab(self, person_id):
        return self._get_single_field(person_id, "two_oldlab")

    def get_lab_phone(self, person_id):
        return self._get_single_field(person_id, "two_labphone")

    def get_status(self, person_id):
        return self._get_single_field(person_id, "two_status")

    def get_main_phone(self, person_id):
        return self._get_single_field(person_id, "two_mainphone")

    def get_office_phone(self, person_id):
        return self._get_single_field(person_id, "two_officephone")

    def get_old_emails(self, person_id):
        with psycopg2.connect(self.connection_str) as conn, conn.cursor() as curs:
            curs.execute("SELECT two_old_email FROM two_old_email WHERE joinkey=%s", (person_id, ))
            res = curs.fetchall()
            return [row[0] for row in res]

    def get_comment(self, person_id):
        return self._get_single_field(person_id, "two_comment")

    def get_contact_data(self, person_id):
        return self._get_single_field(person_id, "two_contactdata")

    def get_person(self, person_id) -> WBPerson:
        person = WBPerson()
        person.person_id = person_id
        person.first_name = self.get_first_name(person_id)
        person.middle_name = self.get_middle_name(person_id)
        person.last_name = self.get_last_name(person_id)
        person.aka_first_name = self.get_aka_first_name(person_id)
        person.aka_middle_name = self.get_aka_middle_name(person_id)
        person.aka_last_name = self.get_aka_last_name(person_id)
        person.orcid = self.get_orcid(person_id)
        person.city = self.get_city(person_id)
        person.state = self.get_state(person_id)
        person.street = self.get_street(person_id)
        person.country = self.get_country(person_id)
        person.email = self.get_email(person_id)
        person.fax = self.get_fax(person_id)
        person.institution = self.get_institution(person_id)
        person.lab = self.get_lab(person_id)
        person.old_lab = self.get_old_lab(person_id)
        person.lab_phone = self.get_lab_phone(person_id)
        person.status = self.get_status(person_id)
        person.main_phone = self.get_main_phone(person_id)
        person.office_phone = self.get_office_phone(person_id)
        person.old_emails = self.get_old_emails(person_id)
        person.comment = self.get_comment(person_id)
        person.contact_data = self.get_contact_data(person_id)
        return person

    def get_author_corresponding(self, author_id):
        return self._get_single_field(author_id, "pap_author_corresponding") == "corresponding"
