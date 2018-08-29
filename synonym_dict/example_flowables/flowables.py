"""
A JSON-serializable collection of Flowable objects
"""

from .flowable import Flowable, NotSupported
from .cas_number import CasNumber, InvalidCasNumber
from ..synonym_dict import SynonymDict

import json


class FlowablesDict(SynonymDict):

    _entry_group = 'Flowables'
    _syn_type = Flowable
    _ignore_case = True

    def set_name(self, term):
        raise NotSupported('Flowable names must be immutable in order to operate as keys')

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
            cas = CasNumber(item)
        except InvalidCasNumber:
            return super(FlowablesDict, self).__getitem__(item)
        return super(FlowablesDict, self).__getitem__(str(cas))
