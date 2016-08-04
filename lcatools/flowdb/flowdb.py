from lcatools.flowdb.create_synonyms import load_synonyms, SYNONYMS
from lcatools.flowdb.synlist import InconsistentIndices
from lcatools.flowdb.compartments import load_compartments, traverse_compartments, Compartment, COMPARTMENTS
from lcatools.catalog import CFRef

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

    def find(self, item):
        """
        Hunt for a matching compartment. Checks compartment self, parent, and siblings. Returns a set.
        :param item: a Compartment
        :return:
        """
        results = set()
        if item.parent in self._dict.keys():
            results = results.union(self._dict[item.parent])
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

        self._q_dict = defaultdict(set)  # dict of quantity to flowables
        self._f_dict = defaultdict(CLookup)  # dict of (flowable, quantity) to c_lookup
        self._c_dict = dict()  # dict of '; '.join(compartments) to Compartment

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

    def _add_cf(self, flowable, comp, q, cf):
        """
        Herein lies the salvation of the fractured synonyms problem - duplicated CFs!
        :param flowable:
        :param comp:
        :param q:
        :param cf:
        :return:
        """

        self._q_dict[q].add(flowable)
        self._f_dict[(flowable, q)][comp] = cf

    def add_cf(self, cf):
        """
        This function updates all three dictionaries, but only if the flowable and compartment are found.
        :param cf:
        :return:
        """
        f = cf.characterization.flow
        q = cf.characterization.quantity.get_uuid()
        terms = set(filter(None, (f['Name'], f['CasNumber'], f.get_uuid())))
        flowables = self.flowables.find_indices(terms)
        if len(flowables) == 0:
            print('%s' % cf.characterization)
            raise MissingFlow('%s / %s not found' % (f['Name'], f['CasNumber']))
        comp = self.find_matching_compartment(f['Compartment'])  # will raise MissingCompartment if not found
        for i in flowables:
            self._add_cf(i, comp, q, cf)
