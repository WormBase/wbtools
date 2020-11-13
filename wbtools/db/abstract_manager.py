import abc


class AbstractWBDBManager(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def __init__(self, dbname, user, password, host):
        self.connection_str = "dbname='" + dbname
        if user:
            self.connection_str += "' user='" + user
        if password:
            self.connection_str += "' password='" + password
        self.connection_str += "' host='" + host + "'"
