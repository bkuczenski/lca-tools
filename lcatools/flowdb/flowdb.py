from lcatools.flowdb.create_synonyms import load_synonyms, SYNONYMS
from lcatools.flowdb.synlist import InconsistentIndices
from lcatools.flowdb.compartments import load_compartments, traverse_compartments, Compartment, COMPARTMENTS
from lcatools.catalog import CFRef, CatalogRef

from collections import defaultdict, namedtuple

class MissingFlow(Exception):
    pass


class MissingCompartment(Exception):
    pass


class CLookup(object):
    """
    A CLookup is a fuzzy mapping from compartment to characterization factor. It's basically a dict with
    Compartments as keys and sets of CharacterizationRefs as values.
    """
    def __init__(self):
        self._dict = defaultdict(set)

    def __getitem__(self, item):
        return self._dict[item]

    def __setitem__(self, key, value):
        if not isinstance(value, CFRef):
            print('Value is not a CharacterizationRef')
            return False
        if not isinstance(key, Compartment):
            print('Key is not a Compartment')
            return False
        self._dict[key].add(value)
        return True

    def find(self, item, dist=1):
        """
        Hunt for a matching compartment. 'dist' param controls the depth of search:
          dist = 0: equivalent to __getitem__
          dist = 1: also check compartment's children
          dist = 2: also check compartment's parent
          dist = 3: also check compartment's siblings
        By default (dist==1), checks compartment self and children, parent, and siblings. Returns a set.
        :param item: a Compartment
        :param dist: how far to search (with limits)
        :return:
        """
        results = self.__getitem__(item)
        if dist > 0:
            for s in item.subcompartments():
                if s in self._dict.keys():
                    results = results.union(self._dict[s])
        if dist > 1:
            if item.parent in self._dict.keys():
                results = results.union(self._dict[item.parent])

        if dist > 2:
            for s in item.parent.subcompartments():
                if s in self._dict.keys():
                    results = results.union(self._dict[s])
        return results


def compartment_string(compartment):
    return '; '.join(compartment)


class FlowDB(object):
    """
    The purpose of this interface is to allow users to easily register / lookup elementary flow characterizations.

    The interface uses two static inputs: a Flowables collection of substance synonyms, and a hierarchical Compartments
    collection.

    There are two main modes of operation:
     - Adding characterization factors. Simply providing a CharacterizationRef is enough, since it includes the
     characterization embedded.  The FlowDB will update a set of lookup systems for cross-reference

     - Querying characterization factors. Provide a flow and a quantity. The flow's name, CAS number, and/or
      other identifying features will be mapped to a flowable, and the compartment matching will be used to find
       a suitable CF.  The CF will be returned, and it can / should be added to the flow in the foreground.

     - It is entirely possible that the FlowDB can also be used to encapsulate LCIA lookups for intermediate
     flows, but this will be subject to development outcomes.
    """
    def __init__(self, catalog, flows=SYNONYMS, compartments=COMPARTMENTS):
        """

        :param catalog: the CatalogInterface to which CharacterizationRefs refer
        :param flows: JSON file containing the Flowables set
        :param compartments: JSON file containing the Compartment hierarchy
        """
        self._catalog = catalog
        self.flowables = load_synonyms(flows)
        self.compartments = load_compartments(compartments)

        self._q_dict = defaultdict(set)  # dict of quantity uuid to set of characterized flowables
        self._f_dict = defaultdict(CLookup)  # dict of (flowable index, quantity uuid) to c_lookup
        self._c_dict = dict()  # dict of '; '.join(compartments) to Compartment

    def is_elementary(self, flow):
        comp = self.find_matching_compartment(flow['Compartment'])
        return comp.elementary

    def find_matching_compartment(self, compartment):
        """

        :param compartment: should be an iterable, as-stored in an archive
        :return: the Compartment. Also adds it to self._c_dict
        """
        cs = compartment_string(compartment)
        if cs in self._c_dict.keys():
            return self._c_dict[cs]

        match = traverse_compartments(self.compartments, compartment)
        if match is None:
            raise MissingCompartment('%s' % cs)
        else:
            self._c_dict[cs] = match
            return match

    def _add_cf(self, flowables, comp, cf):
        """
        Herein lies the salvation of the fractured synonyms problem - duplicated CFs!
        :param flowables:
        :param comp:
        :param cf:
        :return:
        """
        q = cf.characterization.quantity.get_uuid()
        for i in flowables:
            self._q_dict[q].add(i)
            self._f_dict[(i, q)][comp] = cf

    def _parse_flow(self, flow):
        terms = set(filter(None, (flow['Name'], flow['CasNumber'], flow.get_uuid())))
        flowables = self.flowables.find_indices(terms)
        comp = self.find_matching_compartment(flow['Compartment'])  # will raise MissingCompartment if not found
        return flowables, comp

    def import_cfs(self, archive):
        """
        adds all CFs from flows found in the archive, specified as a nickname or index.
         Returns a list of flows that did not match the flowable set.  For compartments that do not match,
          MissingCompartment should be caught and corrected
        :param archive:
        :return: list of flows
        """
        missing_flows = []
        idx = self._catalog.get_index(archive)
        for f in self._catalog[archive].flows():
            flowables, comp = self._parse_flow(f)
            if len(flowables) == 0:
                missing_flows.append(f)
                continue
            for cf in f.characterizations():
                self._add_cf(flowables, comp, CFRef(idx, cf))

        return missing_flows

    def lookup_cfs(self, flow, quantity):
        cfs = set()
        flowables, comp = self._parse_flow(flow)
        q = quantity.get_uuid()
        for i in flowables:
            if i in self._q_dict[q]:
                cfs = cfs.union(self._f_dict[(i, q)][comp])

        return cfs

    def lookup_single_cf(self, flow, quantity):
        cfs = self.lookup_cfs(flow, quantity)
        if len(cfs) == 0:
            return None
        return cfs.pop()  # choose one at random- obv an early simplification
