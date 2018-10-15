from .foreground_observer import ForegroundObserver
from .traversal_observer import TraversalDisclosure
from .to_excel import to_excel

from lca_disclosures import BaseDisclosure


class AntelopeDisclosure(BaseDisclosure):
    @classmethod
    def from_foreground(cls, query, *args, **kwargs):
        """

        :param query: A catalog query
        :param args: one or more foreground nodes
        :param kwargs: folder_path, filename for disclosure
        :return:
        """
