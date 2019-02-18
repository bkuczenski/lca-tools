"""
It is unfortunate that "context managers" are a thing in python and other languages- but here they mean something
different.

So I'm going to bow to the reserved keyword and call these things compartments instead of contexts.


"""

from ..synonym_dict import SynonymDict
from .compartment import Compartment

NullCompartment = Compartment.null()

# these are not-really-subcompartments whose names should be modified if they have parents
NONSPECIFIC_LOWER = {'unspecified', 'non-specific', 'nonspecific', 'unknown', 'undefined', 'none'}


class NonSpecificCompartment(Exception):
    pass


class InconsistentLineage(Exception):
    pass


class CompartmentManager(SynonymDict):

    _entry_group = 'Compartments'
    _syn_type = Compartment
    _ignore_case = True

    _null_entry = NullCompartment

    def __init__(self, source_file=None):
        super(CompartmentManager, self).__init__()
        self.new_object('Resources', sense='source')
        self.new_object('Emissions', sense='sink')
        self.load(source_file)

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
                comps.append(c)
        return comps

    @property
    def top_level_compartments(self):
        for v in self.objects:
            if v.parent is None:
                yield v

    def new_object(self, *args, parent=None, **kwargs):
        """
        If a new object is added with unmodified non-specific synonyms like "unspecified", modify them to include their
        parents' name
        :param args:
        :param parent:
        :param kwargs:
        :return:
        """
        if parent is not None:
            if not isinstance(parent, Compartment):
                parent = self._d[parent]
            pn = parent.name
            args = tuple([', '.join([pn, k]) if k.lower() in NONSPECIFIC_LOWER else k for k in args])
        else:
            for k in args:
                if k.lower() in NONSPECIFIC_LOWER:
                    raise NonSpecificCompartment(k)
        return super(CompartmentManager, self).new_object(*args, parent=parent, **kwargs)

    def add_compartments(self, comps, conflict=None):
        """
        comps should be a list of Compartment objects or strings, in descending order
        :param comps:
        :param conflict: [None] strategy to resolve inconsistent lineage problems.  None raises exception
          'match' hunts among the subcompartments of parent for a regex find
          'skip' simply drops the conflicting entry
        :return: the last (most specific) Compartment created
        """
        current = None
        for c in comps:
            if c in self._d:
                new = self.get(c)
                while not new.is_subcompartment(current):  # slightly dangerous stratagem
                    if current is None:
                        break
                    if conflict is None:
                        raise InconsistentLineage('"%s": existing parent "%s" | incoming parent "%s"' % (c,
                                                                                                         new.parent,
                                                                                                         current))
                    elif conflict == 'match':
                        try:
                            new = next(s for s in current.subcompartments if s.contains_string(c, ignore_case=True))
                        except StopIteration:
                            conflict = None
                    elif conflict == 'skip':
                        new = current

            else:
                new = self.new_object(c, parent=current)
            current = new
        return current

    def __getitem__(self, item):
        if str(item).lower() in NONSPECIFIC_LOWER:
            return self._null_entry
        return super(CompartmentManager, self).__getitem__(item)
