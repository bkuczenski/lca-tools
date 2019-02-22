"""
A JSON-serializable collection of Flowable objects
"""

from .flowable import Flowable
from .cas_number import CasNumber, InvalidCasNumber
from ..synonym_dict import SynonymDict


class FlowablesDict(SynonymDict):

    _entry_group = 'Flowables'
    _syn_type = Flowable
    _ignore_case = True

    def matching_flowables(self, *args):
        """
        just exposes the match_set function
        :param args:
        :return:
        """
        for k in self._match_set(args):
            yield k.name

    def __getitem__(self, item):
        try:
            return super(FlowablesDict, self).__getitem__(item)
        except KeyError:
            try:
                cas = CasNumber(item)
            except InvalidCasNumber:
                raise KeyError(item)
            return super(FlowablesDict, self).__getitem__(str(cas))
