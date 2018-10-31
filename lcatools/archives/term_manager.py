"""
Term Manager - used to handle string references to entities that are known by several synonyms.  Specifically:
 - contexts, which are drawn from flows' specified 'Compartment' attributes
 - flowables, which are drawn from flows' uuid, Name, CasNumber, and string representation

The main objective of the TermManager is to enable the use of generalized synonym matching and context determination
in performing LCIA. It collects two kinds of mapping information: mapping string terms to flowables and contexts; and
mapping (quantity, flowable, context) tuples to [regionalized] characterization objects.

LciaEngine adds a Quantity disambiguation layer, which is inherently useful only across data sources, and uses canonical
lists of flowables and contexts that would be redundant if loaded into individual archives.  Someday, it might make
sense to expose it as a massive, central graph db.
"""
from synonym_dict.example_compartments import Context, CompartmentManager
from synonym_dict.example_flowables import FlowablesDict
from synonym_dict import MergeError

from .clookup import CLookup, SCLookup


from collections import defaultdict
import re


def _flowable_terms(flow):
    try:
        yield flow['Name']
    except KeyError:
        pass
    yield str(flow)
    yield flow.uuid
    if flow.origin is not None:
        yield flow.link
    cas = flow.get('CasNumber')
    if cas is not None and len(cas) > 0:
        yield cas
    syns = flow.get('Synonyms')
    if syns is not None and len(syns) > 0:
        if isinstance(syns, str):
            yield syns
        else:
            for x in flow['Synonyms']:
                yield x


class TermManager(object):
    """
    A TermManager is an archive-specific mapping of string terms to flowables and contexts.  During normal operation
    it is captive to an archive and automatically harvests flowable and context terms from flows when added to the
    archive.  It also hosts a set of CLookups for the archive's quantities, which enable fuzzy traversal of
    context hierarchies to find best-fit characterization factors.

    When a new entity is added to the archive, there are two connections to make: combine uuids and human-readable
    descriptors into a common database of synonyms, and connect those sets of synonyms to a set of entities known
    to the local archive.

    When harvesting terms from a new entity, the general approach is to merge the new entry with an existing entry
    if the existing one uniquely matches.  If there is more than one match, there are three general strategies:

      'prune': trim off the parts that match existing entries and make a new entry with only the distinct terms.
       this creates a larger more diffuse

      'merge': merge all the terms from the new entry and any existing entries together

    The mapping is prolific in both cases- adds a flowable-to-flow mapping for every flowable that matches a given flow
    (merge_strategy='merge' ensures that this is exactly one flowable), and adds the CF to every flowable that matches.

    USAGE MODEL: TODO

    """
    def __init__(self, contexts=None, flowables=None, merge_strategy='prune', quiet=True,
                 strict_clookup=True):
        """
        :param contexts: optional filename to initialize CompartmentManager
        :param flowables: optional filename to initialize FlowablesDict
        :param merge_strategy:
        :param quiet:
        :param strict_clookup: [True] whether to allow multiple CFs for each quantity / flowable / context tuple
           'prune':

        """
        # the synonym sets
        self._cm = CompartmentManager(source_file=contexts)
        self._fm = FlowablesDict(source_file=flowables)

        # the CF lookup
        _cl_typ = {True: SCLookup,
                   False: CLookup}[strict_clookup]
        self._q_dict = defaultdict(lambda: defaultdict(_cl_typ))  # this is BEYOND THE PALE...

        # the reverse mappings
        self._flow_map = defaultdict(set)
        self._fq_map = defaultdict(set)  # to enable listing of all cfs by flowable

        # config
        self._merge_strategy = merge_strategy
        self._quiet = bool(quiet)

    '''
    Utilities
    '''
    def __getitem__(self, item):
        """
        Getitem exposes only the contexts, since flow external_refs are used as flowable synonyms
        :param item:
        :return:
        """
        if item is None:
            return None
        try:
            return self._cm.__getitem__(item)
        except KeyError:
            return None

    def _print(self, *args):
        if not self._quiet:
            print(*args)

    @staticmethod
    def is_context(cx):
        return isinstance(cx, Context)

    @property
    def quiet(self):
        return self._quiet

    '''
    Info Storage
    '''
    def add_compartments(self, compartments):
        return self._cm.add_compartments(compartments)

    def add_flow(self, flow, merge_strategy=None):
        """
        Add a flow's terms to the flowables list and add a flowable-to-flow mapping
        :param flow:
        :param merge_strategy: overrule default merge strategy
        :return: the Flowable object to which the flow's terms have been added
        """
        merge_strategy = merge_strategy or self._merge_strategy
        try:
            fb = self._fm.new_object(*_flowable_terms(flow))
        except MergeError:
            if merge_strategy == 'prune':
                self._print('\nPruning entry for %s' % flow)
                s1 = tuple([t for t in filter(lambda z: z not in self._fm, _flowable_terms(flow))])
                if len(s1) == 0:
                    fb = None
                    self._print('No unique terms')
                    s2 = set()
                else:
                    fb = self._fm.new_object(*s1, prune=True)
                    s2 = set(self._fm.synonyms(fb.name))
                if not self._quiet:
                    for k in sorted(set(s1).union(s2), key=lambda x: x in s2):
                        if k in s2:
                            print(k)
                        else:
                            print('*%s [%s]' % (k, self._fm[k]))

            elif merge_strategy == 'merge':
                self._print('Merging')
                raise NotImplemented
            else:
                raise ValueError('merge strategy %s' % self._merge_strategy)
        for _tf in self._fm.matching_flowables(*_flowable_terms(flow)):
            self._flow_map[_tf].add(flow)
        return fb

    def add_cf(self, quantity, cf):
        if cf.quantity is cf.flow.reference_entity:
            return
        self.add_flow(cf.flow, merge_strategy='prune')  # don't want to alter existing flowables
        fbs = self._fm.matching_flowables(*_flowable_terms(cf.flow))
        for fb in fbs:
            self.qlookup(quantity)[fb].add(cf)  # that some cray shit
            self._fq_map[fb].add(quantity)

    '''
    Info Retrieval
    '''
    def qlookup(self, quantity):
        return self._q_dict[quantity.uuid]

    def factors_for_flowable(self, flowable, quantity=None, compartment=None, dist=0):
        """
        :param flowable:
        :param quantity:
        :param compartment:
        :param dist: [0] only used if compartment is specified. by default report only exact matches.
        :return:
        """
        try:
            fb = self._fm[flowable]
        except KeyError:
            return
        if quantity is None:
            for q in self._fq_map[fb]:
                for cf in self.factors_for_flowable(fb, quantity=q, compartment=compartment, dist=dist):
                    yield cf
        else:
            if compartment is None:
                for cf in self.qlookup(quantity)[fb].cfs():
                    yield cf
            else:
                comp = self[compartment]
                for cf in self.qlookup(quantity)[fb].find(comp, dist=dist):
                    yield cf

    def factors_for_quantity(self, quantity, flowable=None, compartment=None, dist=0):
        """

        :param quantity:
        :param flowable:
        :param compartment:
        :param dist: [0] only used if compartment is specified. by default report only exact matches.
        :return:
        """
        if flowable is not None:
            for k in self.factors_for_flowable(flowable, quantity=quantity, compartment=compartment, dist=dist):
                yield k
        else:
            for f in self.qlookup(quantity).keys():
                for k in self.factors_for_flowable(f, quantity=quantity, compartment=compartment, dist=dist):
                    yield k

    def get_flowable(self, term):
        return self._fm[term]

    def flowables(self, search=None):
        """
        WARNING: does not search on all synonyms, only on canonical terms
        :param search:
        :return:
        """
        for fb in self._fm.objects:
            if search is None:
                yield str(fb)
            else:
                if bool(re.search(search, str(fb), flags=re.IGNORECASE)):
                    yield str(fb)
