import logging

from wbtools.db.afp import WBAFPDBManager
from wbtools.db.antibody import WBAntibodyDBManager
from wbtools.db.expression import WBExpressionDBManager
from wbtools.db.gene import WBGeneDBManager
from wbtools.db.generic import WBGenericDBManager
from wbtools.db.paper import WBPaperDBManager
from wbtools.db.person import WBPersonDBManager

logger = logging.getLogger(__name__)


class WBDBManager(object):

    def __init__(self, dbname, user, password, host):
        self.generic = WBGenericDBManager(dbname, user, password, host)
        self.expression = WBExpressionDBManager(dbname, user, password, host)
        self.paper = WBPaperDBManager(dbname, user, password, host)
        self.person = WBPersonDBManager(dbname, user, password, host)
        self.gene = WBGeneDBManager(dbname, user, password, host)
        self.afp = WBAFPDBManager(dbname, user, password, host)
        self.antibody = WBAntibodyDBManager(dbname, user, password, host)
