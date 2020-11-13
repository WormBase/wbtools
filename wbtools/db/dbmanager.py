import logging

from wbtools.db.expression import WBExpressionDBManager
from wbtools.db.generic import WBGenericDBManager
from wbtools.db.person import WBPersonDBManager

logger = logging.getLogger(__name__)


class WBDBManager(object):

    def __init__(self, dbname, user, password, host):
        self.generic = WBGenericDBManager(dbname, user, password, host)
        self.expression = WBExpressionDBManager(dbname, user, password, host)
        self.paper = WBExpressionDBManager(dbname, user, password, host)
        self.person = WBPersonDBManager(dbname, user, password, host)
