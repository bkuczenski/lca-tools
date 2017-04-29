from collections import defaultdict  # , namedtuple
from math import ceil, log10

from lcatools.catalog import get_entity_uuid
from lcatools.characterizations import Characterization
from lcatools.flowdb.compartments import Compartment, CompartmentManager  # load_compartments, save_compartments, traverse_compartments, REFERENCE_EFLOWS
from lcatools.flowdb.create_synonyms import load_synonyms, SYNONYMS
from lcatools.flowdb.synlist import cas_regex
from lcatools.foreground.dynamic_grid import dynamic_grid
from lcatools.interact import pick_one
from lcatools.providers.interfaces import uuid_regex


class MissingFlow(Exception):
    pass


class MultipleCFs(Exception):
    pass


class CLookup(object):
    """
    A CLookup is a fuzzy mapping from compartment to characterization factor. It's basically a dict with
    Compartments as keys and sets of Characterizations as values.
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


class FlowDB(object):
    """
    The purpose of this interface is to allow users to easily register / lookup elementary flow characterizations.

    The interface uses two static inputs: a Flowables collection of substance synonyms, and a hierarchical Compartments
    collection.

    There are two main modes of operation:
     - Adding characterization factors. The FlowDB will update a set of lookup systems for cross-reference

     - Querying characterization factors. Provide a flow and a quantity. The flow's name, CAS number, and/or
      other identifying features will be mapped to a flowable, and the compartment matching will be used to find
       a suitable CF.  The CF will be returned, and it can / should be added to the flow in the foreground.

     - It is entirely possible that the FlowDB can also be used to encapsulate LCIA lookups for intermediate
     flows, but this will be subject to development outcomes.
    """
    def __init__(self, flows=SYNONYMS, compartments=None):
        """

        :param flows: JSON file containing the Flowables set
        :param compartments: JSON file containing the Compartment hierarchy
        """
        self.flowables = load_synonyms(flows)
        if isinstance(compartments, CompartmentManager):
            self.compartments = compartments
        else:
            self.compartments = CompartmentManager(compartments)

        self._q_dict = defaultdict(set)  # dict of quantity uuid to set of characterized flowables
        self._q_id = dict()  # store the quantities themselves for reference
        self._f_dict = defaultdict(CLookup)  # dict of (flowable index, quantity uuid) to c_lookup

    def known_quantities(self):
        for q in self._q_id.values():
            yield q

    def load_compartments(self, file):
        self.compartments.set_local(file)

    def friendly_flowable(self, i, width=4):
        print('(%*d) %11s %d %.95s' % (width, i, self.flowables.cas(i),
                                       len([cf for cf in self.all_cfs(i)]),
                                       sorted(
                                            filter(lambda x: not bool(cas_regex.match(x)),
                                                   filter(lambda x: not bool(uuid_regex.match(x)),
                                                          self.flowables[i])))
                                       )
              )

    def friendly_search(self, regex):
        """

        :param regex:
        :return:
        """
        results = sorted(list(self.flowables.search(regex)))
        g = int(ceil(log10(max(results)+1)))
        for i in results:
            self.friendly_flowable(i, width=g)

        return results

    def compartments_for(self, flowables, quantity):
        """

        :param flowables: an iterable of flowables
        :param quantity: a quantity to inspect
        :return: a sorted list of compartments (sorted by compartment.to_list()
        """
        q = get_entity_uuid(quantity)

        cmps = set()
        for i in flowables:
            for k in self._f_dict[(i, q)].compartments():
                if isinstance(k, Compartment):
                    cmps.add(k)
        return sorted(cmps, key=lambda x: x.to_list())

    def factors_for_quantity(self, quantity):
        """
        Finally, my text mode chart expertise pays off!
        :param quantity:
        :return:
        """
        q = get_entity_uuid(quantity)

        rows = sorted(self._q_dict[q], key=lambda x: self.flowables.name(x))
        cols = self.compartments_for(rows, quantity)

        print('%s' % quantity)
        print('Characterization Factors\n ')
        dynamic_grid(cols, rows, lambda x, y: self._f_dict[(x, q)][y],
                     ('CAS Number ', lambda x: self.flowables.cas(x)),
                     ('Flowable', lambda x: self.flowables.name(x)),
                     returns_sets=True)

    def factors_for_flow(self, flow, quantities=None, single=True, **kwargs):
        """
        straight lookup. returns a dict of quantity uuid to set of cfs
        :param flow:
        :param quantities:
        :param single: [True] if multiple CFs found, ask user to select one. If False, will return sets instead of
         singleton cfs
        :return:
        """
        cfs = dict()
        if quantities is None:
            quantities = [q for q in self.known_quantities()]
        for qu in quantities:
            if single:
                hits_q = self.lookup_single_cf(flow, qu, **kwargs)
            else:
                hits_q = self.lookup_cfs(flow, qu, **kwargs)
            cfs[qu] = hits_q

        return cfs

    def cfs_for_flowable(self, flowable, **kwargs):

        rows = [cf for cf in self.all_cfs(flowable, **kwargs)]
        cols = [self._q_id[k] for k, v in self._q_dict.items() if flowable in v]

        print('%s [%s]' % (self.flowables.name(flowable), self.flowables.cas(flowable)))
        print('Characterization Factors\n ')

        dynamic_grid(cols, rows, lambda x, y: x if y == x.quantity else None,
                     ('Locations', lambda x: x.list_locations()),
                     ('Compartment', lambda x: x.flow['Compartment']))

    def all_cfs(self, flowable, category=None, quantity=None):
        """
        generator - produces all characterizations matching the flowable, optionally filtering for a single category
        or a single quantity
        :param flowable:
        :param category: (default all)
        :param quantity: (default all)
        :return:
        """
        if quantity is not None:
            if isinstance(quantity, str):
                qs = [quantity]
            else:
                qs = [quantity.get_uuid()]
        else:
            qs = [k for k, v in self._q_dict.items() if flowable in v]
        for q in qs:
            comps = self._f_dict[(flowable, q)]
            if category is None:
                for k in comps.compartments():
                    for cf in comps[k]:
                        yield cf
            else:
                for cf in comps[category]:
                    yield cf

    def _add_cf(self, flowables, comp, cf):
        """
        Herein lies the salvation of the fractured synonyms problem - duplicated CFs!
        :param flowables: set of indices to flowables
        :param comp: a compartment
        :param cf: a cf--- add it to all applicable flowables
        :return:
        """
        q = cf.quantity.get_uuid()
        self._q_id[q] = cf.quantity
        for i in flowables:
            self._q_dict[q].add(i)
            self._f_dict[(i, q)][comp] = cf

    def parse_flow(self, flow):
        terms = list(filter(None, (flow['CasNumber'].strip(), flow.get_uuid(), flow['Name'].strip())))
        flowables = {}
        while len(flowables) == 0:
            try:
                term = [terms.pop(0)]
            except IndexError:
                break
            flowables = self.flowables.find_indices(term)
        comp = self.compartments.find_matching(flow['Compartment'])  # will raise MissingCompartment if not found
        return flowables, comp

    def import_cfs(self, flow):
        flowables, comp = self.parse_flow(flow)
        if len(flowables) == 0:
            return flow
        for cf in flow.characterizations():
            if cf.value is not None:
                self._add_cf(flowables, comp, cf)
        return None

    def import_quantity(self, archive, quantity):
        missing_flows = set()
        for f in archive.flows():
            if f.has_characterization(quantity):
                k = self.import_cfs(f)
                if k is not None:
                    missing_flows.add(k)
        return missing_flows

    def export_quantity(self, quantity):
        """
        exports a set of characterized flows for the given quantity, which can then be added to an archive and
        reloaded.
        :param quantity:
        :return: list of flows
        """
        flows = set()
        for fb in self._q_dict[quantity.get_uuid()]:
            for cf in self.all_cfs(fb, quantity=quantity):
                flows.add(cf.flow)
        return flows

    def import_archive_cfs(self, archive):
        """
        adds all CFs from flows found in the archive
         Returns a list of flows that did not match the flowable set.  For compartments that do not match,
          MissingCompartment should be caught and corrected
        :param archive:
        :return: list of flows
        """
        missing_flows = set()
        for f in archive.flows():
            k = self.import_cfs(f)
            if k is not None:
                missing_flows.add(k)
        return missing_flows

    def lookup_cf_from_flowable(self, flowable, compartment, quantity, location='GLO', dist=1):
        try:
            fb = self.flowables.index(flowable)
        except KeyError:
            return None
        comp = self.compartments.find_matching(compartment)
        if comp is None:
            print('flowable %s' % flowable)
            print('no compartment found: %s' % compartment)
        q = quantity.get_uuid()
        if fb in self._q_dict[q]:
            cfs = self._f_dict[(fb, q)].find(comp, dist=dist, return_first=True)
            if len(cfs) > 0:
                cf1 = [cf for cf in cfs if location in cf.locations()]
                if len(cf1) == 1:
                    return cf1[0]
                vals = set([cf[location] for cf in cf1])
                if len(vals) == 1:
                    return cf1[0]
                for k in cf1:
                    print(str(k))
                raise MultipleCFs(flowable)
            if len(cfs) == 1:
                return list(cfs)[0]
        return None

    def lookup_cfs(self, flow, quantity, dist=3, intermediate=False):
        cfs = set()
        flowables, comp = self.parse_flow(flow)
        if comp is None:  # elementary flows must have non-None compartments
            return cfs
        if not comp.elementary and not intermediate:
            return cfs
        q = quantity.get_uuid()
        for i in flowables:
            if i in self._q_dict[q]:
                cfs = cfs.union(self._f_dict[(i, q)].find(comp, dist=dist, return_first=True))

        return cfs

    @staticmethod
    def _reduce_cfs(flow, quantity, cfs, location='GLO'):
        """
        Take a list of CFs and try to find the best one--- failing all else, ask the user
        :param flow:
        :param quantity:
        :param cfs:
        :param location:
        :return:
        """
        if len(cfs) == 1:
            return list(cfs)[0]
        elif len(cfs) > 1:
            cf1 = [cf for cf in cfs if cf.flow == flow]
            if len(cf1) == 1:
                return cf1[0]
            elif len(cf1) > 1:
                cfs = cf1
            cf1 = [cf for cf in cfs if cf.flow.match(flow)]
            if len(cf1) == 1:
                return cf1[0]
            elif len(cf1) > 1:
                cfs = cf1  # this reduces the list (presumably)

        cf1 = [cf for cf in cfs if location in cf.locations()]
        if len(cf1) == 1:
            return cf1[0]
        elif len(cf1) > 1:
            cfs = cf1  # this reduces the list (presumably)

        vals = [cf[location] for cf in cfs]
        try:
            if len(set(vals)) > 1:
                print('Multiple CFs found: %s' % vals)
                print('Flow: %s [%s]' % (flow, flow.unit()))
                print('Quantity: %s' % quantity)
                print('Pick characterization to apply')
                return pick_one(list(cfs))
        except TypeError:
            print(vals)
            raise
        # print('All characterizations have the same value- picking first one')
        return list(cfs)[0]

    def lookup_single_cf(self, flow, quantity, location='GLO', dist=3):
        """
        this is a hack- but is it wrong?
        flow takes precedence over db if you want a single value- seems right to me
        """
        if flow.has_characterization(quantity):
            return flow.factor(quantity)
        if location == '':
            location = 'GLO'
        # first, find all CFs ignoring location
        cfs = self.lookup_cfs(flow, quantity, dist=dist)
        if len(cfs) == 0:
            return None
        # only use location if the result is ambiguous --
        return self._reduce_cfs(flow, quantity, cfs, location=location)
