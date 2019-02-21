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
from synonym_dict.example_flowables import FlowablesDict
from synonym_dict import MergeError

from .contexts import ContextManager, Context
from .clookup import CLookup, SCLookup

from lcatools.characterizations import Characterization


class UnknownQuantityRef(Exception):
    pass


class OriginStrangeness(Exception):
    pass


from collections import defaultdict
import re


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
    The Term Manager is basically a giant pile of Characterization objects, structured by two synonym sets mapping
    string terms to canonical names for flowables and contexts (The LciaEngine adds a third synonym set aggregating
    synonymous names for quantities).

    There are thus four things the Term Manager must be able to do:
     - store names of flowables and contexts from source data
     - retrieve canonical flowable and context names from the store
     - store characterizations, properly indexed by canonical names
     - retrieve characterizations according to a query

    The components used to accomplish this are:
     _cm and _fm: the context and flowable synonym sets
     _flow_map: reverse-maps flowable terms to flows

     _q_dict: a 3-level nested dictionary:
         defaultdict: quantity uuid -> defaultdict: flowable -> CLookup: context-> {CFs}
       - first level defaultdict maps quantity uuid to second level
         - second level defaultdict maps flowable canonical name to CLookup / subclass
           - third level CLookup maps context to a set of CFs
     _fq_map: reverse-maps flowable canonical name to a set of quantities that characterize it

    The LciaEngine adds another synonym set that maps quantity terms to canonical quantity entities.
    """
    def __init__(self, contexts=None, flowables=None, merge_strategy='prune', quiet=True,
                 strict_clookup=True):
        """
        :param contexts: optional filename to initialize CompartmentManager
        :param flowables: optional filename to initialize FlowablesDict
        :param merge_strategy:
           'prune': - on conflict, trim off known synonyms and add the remaining as a new flowable
        :param quiet:
        :param strict_clookup: [True] whether to allow multiple CFs for each quantity / flowable / context tuple

        """
        # the synonym sets
        self._cm = ContextManager(source_file=contexts)
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
    def set_context(self, context_manager):
        """
        A flow will set its own context- but it needs a context manager to do so.

        Not sure whether to (a) remove FlowWithoutContext exception and test for None, or (b) allow set_context to
        abort silently if context is already set. Currently chose (b) because I think I still want the exception.


        :param context_manager:
        :return:
        """
        if context_manager.is_context(self._context):
            # cannot change context once it's set
            return
        if self.has_property('Compartment'):
            _c = context_manager.add_compartments(self['Compartment'])
        elif self.has_property('Category'):
            _c = context_manager.add_compartments(self['Category'])
        else:
            _c = context_manager.get('none')
            # raise AttributeError('Flow has no contextual attribute! %s' % self)
        if not context_manager.is_context(_c):
            raise TypeError('Context manager did not return a context! %s (%s)' % (_c, type(_c)))
        self._context = _c
        self._flowable = context_manager.add_flow(self)
    '''
    def _check_context(self, flow):
        """
        The point of this function is simply to log which local context corresponds to the flow's presented context
        :param flow:
        :return:
        """
        _c = self._cm.add_compartments(flow.context)
        _c.add_origin(flow.origin)

    def _add_flow_terms(self, flow, merge_strategy=None):
        merge_strategy = merge_strategy or self._merge_strategy
        try:
            fb = self._fm.new_object(*flow.synonyms)
        except MergeError:
            if merge_strategy == 'prune':
                self._print('\nPruning entry for %s' % flow)
                s1 = tuple([t for t in filter(lambda z: z not in self._fm, flow.synonyms)])  # unfamiliar terms
                if len(s1) == 0:
                    fb = self._fm[flow.name]
                    self._print('No unique terms')
                    s2 = set()
                else:
                    fb = self._fm.new_object(*s1, prune=True)
                    flow.name = fb.name  # flow name in intersection of flow synonyms and new object synonyms
                    s2 = set(self._fm.synonyms(fb.name))  # known terms synonymous to new object
                if not self._quiet:
                    for k in sorted(set(s1).union(s2), key=lambda x: x in s2):
                        if k in s2:
                            print(k)  # normal
                        else:
                            print('*%s --> [%s]' % (k, self._fm[k]))  # pruned; maps to

            elif merge_strategy == 'merge':
                # this is trivial but
                self._print('Merging')
                raise NotImplemented
            else:
                raise ValueError('merge strategy %s' % self._merge_strategy)
        for _tf in self._fm.matching_flowables(*flow.synonyms):
            self._flow_map[_tf].add(flow)
        return fb

    def add_flow(self, flow, merge_strategy='prune'):
        """
        We take a flow from outside and add its terminology. That means harmonizing its context with local context
        (assigning context if none is found); and adding flowable terms to the flowables list and mapping flowable
        to flow (assigning flowable if none is found)

        :param flow:
        :param merge_strategy: overrule default merge strategy
        :return: the Flowable object to which the flow's terms have been added
        """
        self._check_context(flow)
        fb = self._add_flow_terms(flow, merge_strategy=merge_strategy)

    '''# I can't figure out what this function is here for
    def add_cf(self, quantity, cf):
        if cf.quantity is cf.flow.reference_entity:
            return
        self.add_flow(cf.flow, merge_strategy='prune')  # don't want to alter existing flowables
        fbs = self._fm.matching_flowables(*_flowable_terms(cf.flow))
        for fb in fbs:
            self.qlookup(quantity)[fb].add(cf)  # that some cray shit
            self._fq_map[fb].add(self._canonical_q(quantity))
    '''

    def _find_exact_cf(self, quantity, flowable, context, origin):
        try:
            cfs = self.qlookup(quantity)[flowable].find(context, dist=0, origin=origin)
        except UnknownQuantityRef:
            return None
        if len(cfs) > 0:
            if len(cfs) > 1:  # can only happen if quantity's origin is None ??
                try:
                    return next(cf for cf in cfs if cf.origin is None)
                except StopIteration:
                    # this should never happen
                    return next(cf for cf in cfs if cf.origin == quantity.origin)
            return list(cfs)[0]
        return None

    def add_characterization(self, flowable, ref_quantity, query_quantity, value, context=None, origin=None,
                             location='GLO', overwrite=False):
        """
        Replacement for flow-based add_characterization.  THE ONLY place to create Characterization objects.
        Add them to all flowables that match the supplied flow.
        :param flowable: if not known to the flowables dict, added as a new flowable
        :param ref_quantity: [entity or ref]
        :param query_quantity: [entity or ref]
        :param value: mandatory. either a numeric value or a dict mapping locations to values
        :param context: the context for which the characterization applies.  If None, applies to all contexts.
          If the characterization describes the reference product of a process, the context can be the process
          external ref.  If the characterization describes an LCIA factor, the context should be a compartment.
        :param overwrite: whether to overwrite an existing value if it already exists (ignored if value is a dict)
        :param location: ['GLO'] (ignored if value is a dict)
        :param origin: (optional; origin of value; defaults to quantity.origin)
        :return:
        """
        if origin is None:
            origin = query_quantity.origin
        cx = self._cm[context]
        try:
            fb = self._fm[flowable]
        except KeyError:
            fb = self._fm.new_object(flowable)

        cf = self._find_exact_cf(query_quantity, fb, cx, origin)
        if cf is None:
            # create our new Characterization with the supplied info
            new_cf = Characterization(fb, ref_quantity, query_quantity, context=cx, origin=origin)
            if isinstance(value, dict):
                new_cf.update_values(**value)
            else:
                new_cf.add_value(value, location=location)

            # add our new CF to the lookup tree
            self._qaccess(query_quantity)[fb].add(new_cf)
            self._fq_map[fb].add(self._canonical_q(query_quantity))

            return new_cf
        else:
            # update entry in the lookup tree
            if isinstance(value, dict):
                cf.update_values(**value)
            else:
                cf.add_value(value, location=location, overwrite=overwrite)
            return cf

    '''
    Info Retrieval
    '''
    def get_canonical(self, quantity):
        try:
            return self._canonical_q(quantity)
        except UnknownQuantityRef:
            return None

    def _canonical_q(self, quantity):
        try:
            return next(q for q in self._q_dict.keys() if q == quantity)
        except StopIteration:
            raise UnknownQuantityRef(quantity)

    def _canonical_q_ref(self, quantity):
        return self._canonical_q(quantity).external_ref

    def qlookup(self, quantity):
        """
        Returns a defaultdict that maps flowable to CLookup or SCLookup object.  raises UnknownQuantityRef if not known
        :param quantity:
        :return:
        """
        return self._q_dict[self._canonical_q(quantity)]

    def _qaccess(self, quantity):
        """
        Suppresses the error, for use when new data is being assigned
        :param quantity:
        :return:
        """
        try:
            return self.qlookup(quantity)
        except UnknownQuantityRef:
            if quantity.entity_type == 'quantity':
                return self._q_dict[quantity]
            else:
                raise TypeError('Not a valid quantity: %s' % quantity)

    def _factors_for_flowable(self, fb, quantity, context, dist):
        """
        detach lookup for cleanness
        :param fb:
        :param quantity:
        :param context:
        :param dist:
        :return:
        """
        if context is None:
            for cf in self.qlookup(quantity)[fb].cfs():
                yield cf
        else:
            comp = self[context]
            for cf in self.qlookup(quantity)[fb].find(comp, dist=dist):
                yield cf

    def factors_for_flowable(self, flowable, quantity=None, context=None, dist=0):
        """
        This is the method that actually performs the lookup.  Other methods are wrappers for this
        :param flowable:
        :param quantity:
        :param context:
        :param dist: [0] only used if compartment is specified. by default report only exact matches.
        :return:
        """
        try:
            fb = self._fm[flowable]
        except KeyError:
            return
        if quantity is None:
            for q_ref in self._fq_map[fb]:
                for cf in self.factors_for_flowable(fb, quantity=q_ref, context=context, dist=dist):
                    yield cf
        else:
            try:
                for cf in self._factors_for_flowable(fb, quantity, context, dist):
                    yield cf
            except UnknownQuantityRef:
                for k in ():
                    yield k

    def factors_for_quantity(self, quantity, flowable=None, context=None, dist=0):
        """

        :param quantity:
        :param flowable:
        :param context:
        :param dist: [0] only used if compartment is specified. by default report only exact matches.
        :return:
        """
        if flowable is not None:
            for k in self.factors_for_flowable(flowable, quantity=quantity, context=context, dist=dist):
                yield k
        else:
            for f in self._qaccess(quantity).keys():
                for k in self.factors_for_flowable(f, quantity=quantity, context=context, dist=dist):
                    yield k

    def get_flowable(self, term):
        return self._fm[term]

    def flowables(self, search=None, origin=None):
        """
        WARNING: does not search on all synonyms, only on canonical terms
        :param origin: not used
        :param search:
        :return:
        """
        for fb in self._fm.objects:
            if search is None:
                yield str(fb)
            else:
                if bool(re.search(search, str(fb), flags=re.IGNORECASE)):
                    yield str(fb)

    '''
    De/Serialization
    '''
    def _serialize_qdict(self, origin, quantity, values=False):
        _ql = self._qaccess(quantity)
        return {str(fb): cl.serialize_for_origin(origin, values=values) for fb, cl in _ql.items()}

    def serialize_factors(self, origin, *quantities, values=False):
        """
        This does not exactly work because, if done from an lcia-engine the result will not be closed to the origin
        specified.  Anyway, in the single-origin case this should be the default.

        in LciaEngine We [may] need a way to record the original flowable + context names, so that origin-specific
        serialization stays closed.
        :param origin:
        :param quantities:
        :param values:
        :return:
        """
        if len(quantities) == 0:
            quantities = self._q_dict.keys()
        j = dict()
        for q in quantities:
            if q.origin != origin:
                continue
            _sq = self._serialize_qdict(origin, q, values=values)
            if len(_sq) > 0:
                j[self._canonical_q_ref(q)] = _sq
        return j

    def _add_from_json(self, j, origin=None):
        """
        Argument: the contents of archive['characterizations'], which looks like this (general case):
        'characterizations': {
          query_quantity.external_ref: {
            flowable: {
              context: {
                origin: {
                  ref_quantity: xxx,
                  value: {
                    locale: val
                  }
                }
              }
            }
          }
        }

        (single origin case):
        'characterizations': {
          query_quantity.external_ref: {
            flowable: {
              context: {
                ref_quantity: xxx,
                value: {
                  locale: val
                }
              }
            }
          }
        }

        :param j:
        :return:
        """
        if origin is None:
            for query_ext_ref, fbs in j.items():
                query_q = self._canonical_q(query_ext_ref)
                for fb, cxs in fbs.items():
                    flowable = self._fm[fb]
                    for cx, cfs in cxs.items():
                        context = self._cm[cx]
                        for org, spec in cfs.items():
                            origin = org
                            ref_q = self._canonical_q(spec['ref_quantity'])
                            self.add_characterization(flowable, ref_q, query_q, spec['value'], context=context,
                                                      origin=origin)
        else:
            for query_ext_ref, fbs in j.items():
                query_q = self._canonical_q(query_ext_ref)
                for fb, cxs in fbs.items():
                    flowable = self._fm[fb]
                    for cx, spec in cxs.items():
                        context = self._cm[cx]
                        ref_q = self._canonical_q(spec['ref_quantity'])
                        self.add_characterization(flowable, ref_q, query_q, spec['value'], context=context,
                                                  origin=origin)
