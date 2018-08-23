"""
It is unfortunate that "context managers" are a thing in python and other languages- but here they mean something
different.

So I'm going to bow to the reserved keyword and call these things compartments instead of contexts.


"""

from ..synonym_dict import SynonymDict
from .context import Context
import json


class CompartmentManager(SynonymDict):

    _entry_group = 'Compartments'
    _syn_type = Context
    _ignore_case = True

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

    def _list_objects(self):
        comps = []
        for tc in self.top_level_compartments:
            for c in tc.self_and_subcompartments:
                comps.append(c.serialize())
        return comps

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
