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

    def _add_from_dict(self, j):
        """
        JSON dict has mandatory 'name', optional 'parent', and 'synonyms'
        We POP from it because it only gets processed once
        :param j:
        :return:
        """
        name = j.pop('name')
        syns = j.pop('synonyms', [])
        parent = j.pop('parent', None)
        self.new_entry(name, *syns, parent=parent, **j)

    def load_dict(self, j):
        comps = j[self._entry_group]
        subs = []
        while len(comps) > 0:
            for obj in comps:
                if 'parent' in obj:
                    try:
                        self._d[obj['parent']]
                    except KeyError:
                        subs.append(obj)
                        continue
                self._add_from_dict(obj)
            comps = subs
            subs = []

    def _list_entries(self):
        return [c for c in self.objects]

    @property
    def top_level_compartments(self):
        for v in self.entries:
            if v.parent is None:
                yield v

    @property
    def objects(self):
        for tc in self.top_level_compartments:
            for c in tc.self_and_subcompartments:
                yield c

    def new_entry(self, *args, parent=None, **kwargs):
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
                if str(k).lower() in NONSPECIFIC_LOWER:
                    raise NonSpecificCompartment(k)
        return super(CompartmentManager, self).new_entry(*args, parent=parent, **kwargs)

    def _merge(self, existing_entry, ent):
        """
        Need to check lineage. We adopt the rule: merge is acceptable if both entries have the same top-level
        compartment or if ent has no parent.  existing entry will obviously be dominant
        :param existing_entry:
        :param ent:
        :return:
        """
        print('merging %s into %s' % (ent, existing_entry))
        if ent.parent is not None:
            if not ent.top() is existing_entry.top():
                raise InconsistentLineage('"%s": existing top %s | incoming top %s' % (ent,
                                                                                       existing_entry.top(),
                                                                                       ent.top()))

        super(CompartmentManager, self)._merge(existing_entry, ent)
        for sub in list(ent.subcompartments):
            sub.parent = existing_entry

    @staticmethod
    def _tuple_to_name(comps):
        return '; '.join(filter(None, comps))

    def _check_subcompartment_lineage(self, current, c):
        """
        Determines whether the incoming compartment name 'c' already exists in the database with an inconsistent
        lineage from the current parent 'current'.

        If the term is not found, creates a new subcompartment with current as parent.

        If the term is found and has a valid lineage, the found subcompartment is used.

        If the term is found to be an orphan, then current is assigned as its parent, unless the "orphan" is an
        elementary root context ('emissions' or 'resources'), in which case all terms in current + parents are removed
        from the dictionary and assigned to _disregarded

        Otherwise, raises InconsistentLineage
        :param current:
        :param c:
        :return:
        """
        if c in self._d:
            new = self.get(c)
            if current is None:
                return new
            if new.is_subcompartment(current):
                return new
            if new.parent is None:
                new.parent = current
                return new
            raise InconsistentLineage('"%s": existing parent "%s" | incoming parent "%s"' % (c,
                                                                                             new.parent,
                                                                                             current))
        else:
            new = self.new_entry(c, parent=current)
            return new

    def add_compartments(self, comps, conflict='rename'):
        """
        comps should be a list of Compartment objects or strings, in descending order
        :param comps:
        :param conflict: ['rename'] strategy to resolve inconsistent lineage problems.
          'rename' changes the name of the conflicting entry to include its native (nonconflicting) parent
          'match' hunts among the subcompartments of parent for a regex find
          'skip' simply drops the conflicting entry
          None or else: raise InconsistentLineage

        :return: the last (most specific) Compartment created
        """
        if len(comps) == 0 or comps is None:
            return self._null_entry
        current = None
        auto_name = self._tuple_to_name(comps)
        if auto_name in self._d:
            return self[auto_name]
        for c in comps:
            try:
                new = self._check_subcompartment_lineage(current, c)
            except InconsistentLineage as e:
                if conflict == 'match':
                    try:
                        new = next(s for s in current.subcompartments if s.contains_string(c, ignore_case=True))
                    except StopIteration:
                        raise e
                elif conflict == 'skip':
                    new = current
                elif conflict == 'rename':
                    new_c = ', '.join([current.name, c])
                    new = self._check_subcompartment_lineage(current, new_c)
                else:
                    raise e

            current = new
        self.add_synonym(current.name, auto_name)
        return current

    def __getitem__(self, item):
        if isinstance(item, tuple):
            item = self._tuple_to_name(item)
        if str(item).lower() in NONSPECIFIC_LOWER:
            return self._null_entry
        return super(CompartmentManager, self).__getitem__(item)
