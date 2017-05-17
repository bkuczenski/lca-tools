"""
The reinvention of the flow database in its proper form: the quantity database.
Every LCA study has different flows, but they all use the same quantities-- that is, extensive properties by
which flows are characterized.

The quantity database is meant to provide a link between archive- or study-specific quantities and generic,
semantically concrete ones such as might be part of the QUDT ontology.

To serve that purpose, the qdb has a list of quantity synonyms that map to a set of canonical quantity entities:
the ones found in the ELCD 3 reference.  The ILCD entities mapped by the synonyms become the focal point for
semantic enrichment.

Qdb instances can add quantities that operate as LCIA indicators, drawn from outside archives. Those entities
are given semantic significance through their origin archives, as implemented in the LcCatalog interface.

The Qdb also maintains a reference list of flowables, also a synonym list, with the objective of harmonizing this
list with the ecoinvent, ILCD, and TRACI 2 lists (and expecting that the synonym list will thus be harmonized
with e.g. the EPA list automatically)

Finally, the Qdb maintains a hierarchical collection of compartments
"""

import os
from collections import defaultdict

from lcatools.from_json import from_json
from lcatools.providers.base import LcArchive
from lcatools.flowdb.compartments import Compartment, CompartmentManager  # load_compartments, save_compartments, traverse_compartments, REFERENCE_EFLOWS
from lcatools.characterizations import Characterization
from synlist import SynList, Flowables


REF_QTYS = os.path.join(os.path.dirname(__file__), 'data', 'elcd_reference_quantities.json')
Q_SYNS = os.path.join(os.path.dirname(__file__), 'data', 'quantity_synlist.json')
F_SYNS = os.path.join(os.path.dirname(__file__), 'data', 'flowable_synlist.json')


class CLookup(object):
    """
    A CLookup is a fuzzy mapping from compartment to characterization factor. It's basically a dict with
    Compartments as keys and sets of Characterizations as values.
    This should move into compartments
    """
    def __init__(self):
        self._dict = defaultdict(set)

    def __getitem__(self, item):
        if isinstance(item, Compartment):
            if item in self._dict:
                return self._dict[item]
            return set()
        return None

    def __setitem__(self, key, value):
        if not isinstance(value, Characterization):
            print('Value is not a Characterization')
            return False
        if not isinstance(key, Compartment):
            print('Key is not a Compartment: %s' % key)
            return False
        if value is not None:
            self._dict[key].add(value)
        return True

    def first(self, item):
        return list(self._dict[item])[0]

    def compartments(self):
        return self._dict.keys()

    def find(self, item, dist=3, return_first=True):
        """
        Hunt for a matching compartment. 'dist' param controls the depth of search:
          dist = 0: equivalent to __getitem__
          dist = 1: also check compartment's children
          dist = 2: also check compartment's parent
          XXX dist = 3: also check compartment's siblings XXX - canceled because this results in spurious matches
        By default (dist==3), checks compartment self and children, parent, and siblings. Returns a set.
        :param item: a Compartment
        :param dist: how far to search (with limits) (default: 1= compartment + children)
        :param return_first: stop hunting as soon as a cf is found
        :return:
        """
        if not isinstance(item, Compartment):
            return set()

        def found(res):
            return len(res) > 0 and return_first
        results = self.__getitem__(item)
        if found(results):
            return results

        if dist > 0:
            for s in item.subcompartments():
                if s in self._dict.keys():
                    results = results.union(self._dict[s])
        if found(results):
            return results

        if dist > 1:
            if item.parent in self._dict.keys():
                results = results.union(self._dict[item.parent])
        '''
        if found(results):
            return results

        if dist > 2:
            for s in item.parent.subcompartments():
                if s in self._dict.keys():
                    results = results.union(self._dict[s])
        '''
        return results


class Qdb(LcArchive):
    def __init__(self, source=REF_QTYS, quantities=Q_SYNS, flowables=F_SYNS, compartments=None,
                 ref=None, **kwargs):
        if ref is None:
            ref = 'local.qdb'
        super(Qdb, self).__init__(source, ref=ref, **kwargs)
        self.load_json(from_json(source))

        if isinstance(compartments, CompartmentManager):
            self.c_mgr = compartments
        else:
            self.c_mgr = CompartmentManager(compartments)

        self._f = Flowables.from_json(from_json(F_SYNS))
        self._q = SynList.from_json(from_json(Q_SYNS))

        self._index_quantities()

        self._q_dict = defaultdict(set)  # dict of quantity index to set of characterized flowables (by index)
        self._fq_dict = defaultdict(CLookup)  # dict of (flowable index, quantity index) to c_lookup
        self._f_dict = defaultdict(set)  # dict of flowable index to set of characterized quantities (by index)

    def add_new_quantity(self, q):
        ind = self._q.add_set((q.get_link(), q['Name'], str(q)), merge=True)
        if self._q.entity(ind) is None:
            self._q.set_entity(ind, q)

    def _index_quantities(self):
        for q in self.quantities():
            try:
                self._q.set_entity(q['Name'], q)
            except KeyError:
                self.add_new_quantity(q)

    @property
    def flow_properties(self):
        for q in self.quantities():
            if not q.is_lcia_method():
                yield q

    @property
    def indicators(self):
        """
        An indicator is a quantity with the 'Indicator' property
        :return:
        """
        for q in self.quantities():
            if q.is_lcia_method():
                yield q

    def get_quantity(self, synonym):
        """
        return a quantity by its synonym
        :param synonym:
        :return:
        """
        return self._q.entity(synonym)

    def is_elementary(self, flow):
        return self.c_mgr.is_elementary(flow)
