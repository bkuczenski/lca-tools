"""
It is unfortunate that "context managers" are a thing in python and other languages- but here they mean something
different.

So I'm going to bow to the reserved keyword and call these things compartments instead of contexts.


"""

from ..synonym_dict import SynonymDict
from .context import Context
import json


class CompartmentManager(SynonymDict):

    def _add_from_dict(self, j):
        """
        JSON dict has mandatory 'name', optional 'parent', 'sense', and 'synonyms'
        We POP from it because it only gets processed once
        :param j:
        :return:
        """
        name = j.pop('name')
        syns = j.pop('synonyms', [])
        parent = j.pop('parent', None)
        self.new_object(name, *syns, parent=parent, **j)

    def load(self, filename=None):
        """
        Load the specified file, using the default file if none is provided as an argument.  note that specifying an
        alternate file as argument does not alter the default file.

        The list of compartments must be ordered such that no compartment appears before its parent.
        :return:
        """
        if filename is None:
            if self._filename is None:
                return
            filename = self._filename
        with open(filename, 'r') as fp:
            comps = json.load(fp)
        for c in comps['Compartments']:
            self._add_from_dict(c)

    def save(self, filename=None):
        """
        if filename is specified, it overrules any prior filename
        :param filename:
        :return:
        """
        if filename is not None:
            self._filename = filename
        comps = []
        for tc in self.top_level_compartments:
            for c in tc.self_and_subcompartments:
                comps.append(c.serialize())
        with open(filename, 'w') as fp:
            json.dump({'Compartments': comps}, fp, indent=2)

    @property
    def top_level_compartments(self):
        for v in self.objects:
            if v.parent is None:
                yield v

    def new_object(self, *args, parent=None, **kwargs):
        if parent is not None:
            if not isinstance(parent, Context):
                parent = self._d[parent]
        return super(CompartmentManager, self).new_object(*args, parent=parent, **kwargs)

    def __init__(self, source_file=None):
        super(CompartmentManager, self).__init__(ignore_case=True, syn_type=Context)
        self._filename = source_file
        self.load()

    def add_compartments(self, comps):
        """
        comps should be a list of Context objects or strings, in descending order
        :param comps:
        :return: the last (most specific) Context created
        """
        current = None
        for c in comps:
            if c in self._d:
                new = self.get(c)
            else:
                new = self.new_object(c, parent=current)
            current = new
        return current
