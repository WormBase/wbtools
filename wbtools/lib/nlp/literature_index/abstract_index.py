import abc


class AbstractLiteratureIndex(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def __init__(self):
        pass

    @abc.abstractmethod
    def num_documents(self) -> int:
        return 0

    @abc.abstractmethod
    def count_matching_documents(self, keyword: str) -> int:
        return 0
