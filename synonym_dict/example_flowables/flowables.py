"""
A JSON-serializable collection of Flowable objects
"""

from .flowable import Flowable, NotSupported
from .cas_number import CasNumber, InvalidCasNumber
from ..synonym_dict import SynonymDict

import json


class FlowablesDict(SynonymDict):

    def _add_from_dict(self, j):
        name = j['name']
        syns = j.pop('synonyms', [])
        self.new_object(name, *syns, merge=False)

    def load(self, filename=None):
        if filename is None:
            if self._filename is None:
                return
            filename = self._filename
        with open(filename, 'r') as fp:
            fbs = json.load(fp)
        for fb in fbs['Flowables']:
            self._add_from_dict(fb)

    def __init__(self, source_file=None):
        super(FlowablesDict, self).__init__(ignore_case=True, syn_type=Flowable)
        self._filename = source_file
        self.load()

    def add_or_update_object(self, obj, merge=True, create_child=False):
        if create_child is True and not isinstance(obj, CasNumber):
            raise NotSupported('Flowables can only have CAS Number children')
        return super(FlowablesDict, self).add_or_update_object(obj, merge=merge, create_child=create_child)

    def __getitem__(self, item):
        try:
            cas = CasNumber(item)
        except InvalidCasNumber:
            return super(FlowablesDict, self).__getitem__(item)
        return super(FlowablesDict, self).__getitem__(str(cas))

    def save(self, filename=None):
        """
        if filename is specified, it overrules any prior filename
        :param filename:
        :return:
        """
        if filename is not None:
            self._filename = filename
        fb = [f.serialize() for f in self.objects]
        with open(filename, 'w') as fp:
            json.dump({'Flowables': fb}, fp, indent=2)
