import logging

import psycopg2

from wbtools.db.abstract_manager import AbstractWBDBManager
from wbtools.db.afp import WBAFPDBManager
from wbtools.db.antibody import WBAntibodyDBManager
from wbtools.db.expression import WBExpressionDBManager
from wbtools.db.gene import WBGeneDBManager
from wbtools.db.generic import WBGenericDBManager
from wbtools.db.paper import WBPaperDBManager
from wbtools.db.person import WBPersonDBManager

logger = logging.getLogger(__name__)


class WBDBManager(AbstractWBDBManager):

    def __init__(self, dbname, user, password, host):
        super().__init__(dbname, user, password, host)
        self.generic = WBGenericDBManager(dbname, user, password, host)
        self.expression = WBExpressionDBManager(dbname, user, password, host)
        self.paper = WBPaperDBManager(dbname, user, password, host)
        self.person = WBPersonDBManager(dbname, user, password, host)
        self.gene = WBGeneDBManager(dbname, user, password, host)
        self.afp = WBAFPDBManager(dbname, user, password, host)
        self.antibody = WBAntibodyDBManager(dbname, user, password, host)

    def __enter__(self):
        self.conn = psycopg2.connect(self.connection_str)
        self.curs = self.conn.cursor()
        self.generic.conn = self.conn
        self.generic.curs = self.curs
        self.expression.conn = self.conn
        self.expression.curs = self.curs
        self.paper.conn = self.conn
        self.paper.curs = self.curs
        self.person.conn = self.conn
        self.person.curs = self.curs
        self.gene.conn = self.conn
        self.gene.curs = self.curs
        self.afp.conn = self.conn
        self.afp.curs = self.curs
        self.antibody.conn = self.conn
        self.antibody.curs = self.curs

    def __exit__(self, exc_type, exc_val, exc_tb):
        super(WBDBManager, self).__exit__(exc_type, exc_val, exc_tb)
