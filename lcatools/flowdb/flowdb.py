from math import ceil, log10

from lcatools.flowdb.create_synonyms import load_synonyms, SYNONYMS
from lcatools.flowdb.synlist import cas_regex
from lcatools.flowdb.compartments import load_compartments, save_compartments, traverse_compartments, Compartment, COMPARTMENTS
from lcatools.catalog import CFRef, CatalogRef, get_entity_uuid
from lcatools.interfaces import uuid_regex
from lcatools.foreground.dynamic_grid import dynamic_grid
from lcatools.interact import pick_one, _pick_list

from collections import defaultdict, namedtuple


class MissingFlow(Exception):
    pass


class MissingCompartment(Exception):
    pass


def merge_compartment(compartment, missing):
    my_missing = []
    my_missing.extend(missing)
    while len(my_missing) > 0:
        sub = traverse_compartments(compartment, my_missing[:1])
        if sub is not None:
            my_missing.pop(0)
            compartment = sub
        else:
            print('Missing compartment: %s' % my_missing[0])
            subs = sorted(s.name for s in compartment.subcompartments())
            print('subcompartments of %s:' % compartment)
            c = _pick_list(subs, 'Merge "%s" into %s' % (my_missing[0], compartment),
                           'Create new Subcompartment of %s' % compartment)
            if c == (None, 0):
                compartment.add_syn(my_missing.pop(0))
            elif c == (None, 1):  # now we add all remaining compartments
                while len(my_missing) > 0:
                    new_sub = my_missing.pop(0)
                    compartment.add_sub(new_sub)
                    compartment = compartment[new_sub]
            elif c == (None, None):
                raise ValueError('User break')
            else:
                compartment = compartment[subs[c[0]]]
    return compartment


class CLookup(object):
    """
    A CLookup is a fuzzy mapping from compartment to characterization factor. It's basically a dict with
    Compartments as keys and sets of CharacterizationRefs as values.
    """
    def __init__(self):
        self._dict = defaultdict(set)

    def __getitem__(self, item):
        if isinstance(item, Compartment):
            return self._dict[item]
        return None

    def __setitem__(self, key, value):
        if not isinstance(value, CFRef):
            print('Value is not a CharacterizationRef')
            return False
        if not isinstance(key, Compartment):
            print('Key is not a Compartment')
            return False
        self._dict[key].add(value)
        return True

    def first(self, item):
        return list(self._dict[item])[0]

    def compartments(self):
        return self._dict.keys()

    def find(self, item, dist=1, return_first=True):
        """
        Hunt for a matching compartment. 'dist' param controls the depth of search:
          dist = 0: equivalent to __getitem__
          dist = 1: also check compartment's children
          dist = 2: also check compartment's parent
          dist = 3: also check compartment's siblings
        By default (dist==1), checks compartment self and children, parent, and siblings. Returns a set.
        :param item: a Compartment
        :param dist: how far to search (with limits) (default: 1= compartment + children)
        :param return_first: stop hunting as soon as a cf is found
        :return:
        """
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
        if found(results):
            return results

        if dist > 2:
            for s in item.parent.subcompartments():
                if s in self._dict.keys():
                    results = results.union(self._dict[s])
        return results


def compartment_string(compartment):
    return '; '.join(list(filter(None, compartment)))


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
        self._q_id = dict()  # store the quantities themselves for reference
        self._f_dict = defaultdict(CLookup)  # dict of (flowable index, quantity uuid) to c_lookup
        self._c_dict = dict()  # dict of '; '.join(compartments) to Compartment

    def is_elementary(self, flow):
        comp = self.find_matching_compartment(flow['Compartment'])
        return comp.elementary

    def load_compartments(self, file=COMPARTMENTS):
        self.compartments = load_compartments(file)

    def find_matching_compartment(self, compartment, interact=True, save_file=COMPARTMENTS):
        """

        :param compartment: should be an iterable, as-stored in an archive
        :param interact: if true, interactively prompt user to merge and save missing compartments
        :param save_file: where to save the updated compartments
        :return: the Compartment. Also adds it to self._c_dict
        """
        cs = compartment_string(compartment)
        if cs in self._c_dict.keys():
            return self._c_dict[cs]

        match = traverse_compartments(self.compartments, compartment)
        if match is None:
            if interact:
                c = merge_compartment(self.compartments, compartment)
                match = traverse_compartments(self.compartments, compartment)
                if c is match and c is not None:
                    print('match: %s' % match.to_list())
                    print('Updating compartments...')
                    save_compartments(self.compartments, file=save_file)

            else:
                raise MissingCompartment('%s' % cs)
        else:
            self._c_dict[cs] = match
            return match

    def friendly_flowable(self, i, width=4):
        print('[%*d] %11s %.100s' % (width, i, self.flowables.cas(i),
                                     sorted(
                                         filter(lambda x: not bool(cas_regex.match(x)),
                                                filter(lambda x: not bool(uuid_regex.match(x)),
                                                       self.flowables[i])))
                                     )
              )

    def friendly_search(self, regex, max_hits=100):
        """

        :param regex:
        :param max_hits: maximum number of results to return (default 100)
        :return:
        """
        results = sorted(list(self.flowables.search(regex)))
        g = int(ceil(log10(max(results)+1)))
        for i in results:
            self.friendly_flowable(i, width=g)

        return results

    '''
    def _compartment_grid(self, q_id, f_list, c_list):
        print('%s' % self._q_id[q_id])
        h_str = 'CAS Number  '
        for i in range(len(c_list)):
            h_str += '|%-8.8s' % ('  C%d' % i)
        h_str += '  Flowable'
        print('%s' % h_str)
        print('-' * len(h_str))
        for i in f_list:
            f_str = '%11s  ' % self.flowables.cas(i)
            for k in range(len(c_list)):
                try:
                    cfs = self._f_dict[(i, q_id)][c_list[k]]
                    if len(cfs) > 1:
                        f_str += '%-8s ' % ('*' * len(cfs))
                    else:
                        f_str += '%8.3g ' % list(cfs)[0].characterization.value
                except IndexError:
                    f_str += "   --    "
            print('%s %s' % (f_str, self.flowables.name(i)))
        print('%s' % h_str)
        print('\nCompartments:')
        for i in range(len(c_list)):
            print('C%d: %s' % (i, c_list[i]))
    '''

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

    def factors_for_flow(self, flow, quantities, single=True, **kwargs):
        """
        straight lookup. returns a dict of quantity uuid to set of cfs
        :param flow:
        :param quantities:
        :param single: [True] if multiple CFs found, ask user to select one. If False, will return sets instead of
         singleton cfs
        :return:
        """
        cfs = dict()
        for qu in quantities:
            if single:
                hits_q = self.lookup_single_cf(flow, qu, **kwargs)
            else:
                hits_q = self.lookup_cfs(flow, qu, **kwargs)
            cfs[qu.get_uuid()] = hits_q

        return cfs

    def cfs_for_flowable(self, flowable, **kwargs):

        rows = [cf.characterization for cf in self.all_cfs(flowable, **kwargs)]
        cols = [self._q_id[k] for k, v in self._q_dict.items() if flowable in v]

        print('%s [%s]' % (self.flowables.name(flowable), self.flowables.cas(flowable)))
        print('Characterization Factors\n ')

        dynamic_grid(cols, rows, lambda x, y: x if y == x.quantity else None,
                     ('Locations', lambda x: x.list_locations()),
                     ('Compartment', lambda x: x.flow['Compartment']))

    def all_cfs(self, flowable, category=None):
        """
        generator - produces all characterizations matching the flowable, optionally filtering for a single category
        :param flowable:
        :param category:
        :return:
        """
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
        :param flowables:
        :param comp:
        :param cf:
        :return:
        """
        q = cf.characterization.quantity.get_uuid()
        self._q_id[q] = cf.characterization.quantity
        for i in flowables:
            self._q_dict[q].add(i)
            self._f_dict[(i, q)][comp] = cf

    def parse_flow(self, flow):
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
            flowables, comp = self.parse_flow(f)
            if len(flowables) == 0:
                missing_flows.append(f)
                continue
            for cf in f.characterizations():
                self._add_cf(flowables, comp, CFRef(self._catalog, idx, cf))

        return missing_flows

    def lookup_cfs(self, flow, quantity, dist=1):
        cfs = set()
        flowables, comp = self.parse_flow(flow)
        q = quantity.get_uuid()
        for i in flowables:
            if i in self._q_dict[q]:
                cfs = cfs.union(self._f_dict[(i, q)].find(comp, dist=dist, return_first=True))

        return cfs

    @staticmethod
    def _reduce_cfs(flow, cfs, location='GLO'):
        """
        Take a list of CFs and try to find the best one--- failing all else, ask the user
        :param flow:
        :param cfs:
        :param location:
        :return:
        """
        if len(cfs) == 1:
            return list(cfs)[0]
        elif len(cfs) > 1:
            cf1 = [cf for cf in cfs if cf.characterization.flow.match(flow)]
            if len(cf1) == 1:
                return cf1[0]
            elif len(cf1) > 1:
                cfs = cf1  # this reduces the list (presumably)

        cf1 = [cf for cf in cfs if location in cf.characterization.locations()]
        if len(cf1) == 1:
            return cf1[0]
        elif len(cf1) > 1:
            cfs = cf1  # this reduces the list (presumably)

        vals = [cf.characterization[location] for cf in cfs]
        if len(set(vals)) > 1:
            print('Multiple CFs found: %s' % vals)
            print('Pick characterization to apply')
            return pick_one(cfs)
        print('All characterizations have the same value- picking first one')
        return cfs[0]

    def lookup_single_cf(self, flow, quantity, location='GLO', dist=1):
        cfs = self.lookup_cfs(flow, quantity, dist=dist)
        if len(cfs) == 0:
            return None
        return self._reduce_cfs(flow, cfs, location=location)
