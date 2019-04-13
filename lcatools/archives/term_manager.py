"""
Edelen and Ingwersen et al 2017:
Recommendations: "... separating flowable names from context and unit information ..."

Term Manager - used to handle string references to entities that are known by several synonyms.  Specifically:
 - contexts, which are drawn from flows' specified 'Compartment' or other classifying properties
 - flowables, which are drawn from flows' uuid and external link, and Name, CasNumber, Synonyms properties
 - quantities, which should be recognizable by external ref or uuid

The main objective of the TermManager is to enable the use of generalized synonym matching in performing LCIA. It
collects two kinds of mapping information: mapping string terms to flowables and contexts; and mapping (quantity,
flowable, context) tuples to [regionalized] characterization objects.

LciaEngine is designed to handle information from multiple origins to operate as a qdb in a catalog context.  The
subclass adds canonical lists of flowables and contexts that would be redundant if loaded into individual archives,
introduces "smart" hierarchical context lookup (CLookup), and adds the ability to quell biogenic CO2 emissions.
Someday, it might make sense to expose it as a massive, central graph db.
"""
from synonym_dict import SynonymDict

from lcatools.interfaces import EntityNotFound
from lcatools.contexts import ContextManager, Context, NullContext
from .quantity_manager import QuantityManager

from lcatools.characterizations import Characterization


class FactorCollision(Exception):
    pass


class QuantityConflict(Exception):
    pass


class NoFQEntry(Exception):
    """
    This exception has the specific meaning that there is no lookup for the named flow-quantity pair
    """
    pass


class OriginStrangeness(Exception):
    pass


from collections import defaultdict


class TermManager(object):
    """
    A TermManager is an archive-specific mapping of string terms to flowables and contexts.  During normal operation
    it is captive to an archive and automatically harvests flowable and context terms from flows when added to the
    archive.  It also hosts a set of CLookups for the archive's quantities, which enable fuzzy traversal of
    context hierarchies to find best-fit characterization factors.

    When a new entity is added to the archive, there are two connections to make: combine uuids and human-readable
    descriptors into a common database of synonyms, and connect those sets of synonyms to a set of entities known
    to the local archive.

    When harvesting terms from a new flow, the general approach is to merge the new entry with an existing entry
    if the existing one uniquely matches.  If there is more than one match, there are three general strategies:

      'prune': trim off the parts that match existing entries and make a new entry with only the distinct terms.
       this creates a larger more diffuse

      'merge': merge all the terms from the new entry and any existing entries together

    The mapping is prolific in both cases- adds a flowable-to-flow mapping for every flowable that matches a given flow
    (merge_strategy='merge' ensures that this is exactly one flowable), and adds the CF to every flowable that matches.

    USAGE MODEL: TODO
    The Term Manager is basically a giant pile of Characterization objects, grouped by canonical query quantity,
    flowable, and context. The basic TermManager enforces a restriction of one CF per qq | fb | cx combination.  The
    LciaEngine subclass uses a CLookup that can either be strict or non-strict, but nonetheless enforces one CF per
    qq | fb | cx | origin combination.

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
    """
    def __init__(self, contexts=None, flowables=None, quantities=None, merge_strategy='prune', quiet=True):
        """
        :param contexts: optional filename to initialize CompartmentManager
        :param flowables: optional filename to initialize FlowablesDict
        :param merge_strategy:
           'prune': - on conflict, trim off known synonyms and add the remaining as a new flowable
        :param quiet:

        """
        # the synonym sets
        self._cm = ContextManager(source_file=contexts)
        self._fm = SynonymDict(source_file=flowables)
        self._qm = QuantityManager(source_file=quantities)

        self._q_dict = dict()  # dict of dicts. _q_dict[canonical quantity][canonical flowable] -> a context lookup
        #
        self._cl_typ = dict

        # the reverse mappings
        self._flow_map = defaultdict(set)  # maps flowable to flows having that flowable
        self._fq_map = dict()  # maps flowable to quantities characterized by that flowable

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

    def add_terms(self, term_type, *terms, **kwargs):
        d = {'context': self._cm,
             'flow': self._fm,
             'flowable': self._fm,
             'quantity': self._qm}[term_type]
        d.new_entry(*terms, **kwargs)

    @staticmethod
    def is_context(cx):
        return isinstance(cx, Context)

    @property
    def quiet(self):
        return self._quiet

    def add_quantity(self, quantity):
        if quantity.entity_type != 'quantity':
            raise TypeError('Must be quantity type')
        if quantity.link in self._qm:
            ex = self._qm[quantity.link]
            if not ex is quantity:
                raise QuantityConflict('Incoming %s does not match existing\n%s' % (ex, quantity))
        else:
            self._qm.add_quantity(quantity)
        return self._canonical_q(quantity)

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
    def _context_and_parents(self, cx):
        c = self[cx]
        while c is not None:
            yield c
            c = c.parent

    def add_context(self, context):
        """

        :param context:
        :return:
        """
        if isinstance(context, str):
            context = (context,)
        return self._cm.add_compartments(tuple(context))

    def _check_context(self, flow):
        """
        The point of this function is simply to log which local context corresponds to the flow's presented context
        :param flow:
        :return:
        """
        try:
            _c = self._cm[flow.context]
        except KeyError:
            _c = self._cm.add_compartments(flow.context)
        _c.add_origin(flow.origin)

    def _create_flowable(self, name, *syns, prune=False):
        fb = self._fm.new_entry(name, *syns, prune=prune).object
        if fb in self._fq_map:
            self._print('Adding terms to existing flowable %s' % fb)
        else:
            self._print('Adding new flowable %s' % fb)
            self._fq_map[fb] = set()
        return fb

    def _add_pruned_terms(self, name, new_terms):
        s1 = tuple(new_terms)  # unfamiliar terms
        if len(s1) == 0:
            fb = self._fm[name]
            self._print('No unique terms')
            s2 = set()
        else:
            fb = self._create_flowable(*s1, prune=True)
            s2 = set(self._fm.synonyms(str(fb)))  # known terms synonymous to new object
        if not self._quiet:
            for k in sorted(set(s1).union(s2), key=lambda x: x in s2):
                if k in s2:
                    print(k)  # normal
                else:
                    print('*%s --> [%s]' % (k, self._fm[k]))  # pruned; maps to
        return fb

    def _merge_terms(self, dominant, *syns):
        """
        Two parts to this: merge the entries in the flowables manager; update local reverse mappings:
         _q_dict
         _fq_map (nonconflicting)
         _flow_map (nonconflicting)
        Before we can do either, we need to check for collisions
        This just brute forces it which must be ungodly slow, and btw it also hasn't been tested
        :param dominant:
        :param syns:
        :return:
        """
        fq_conflicts = []
        for f_dict in self._q_dict.values():
            try:
                combo = f_dict[dominant]
            except KeyError:
                combo = self._cl_typ()
            for syn in syns:
                if syn not in f_dict:
                    continue
                cl = f_dict[syn]
                for k in cl.keys():
                    if k in combo:
                        cand = (combo[k], cl[k])
                        if cand[0].value == cand[1].value:
                            continue
                        fq_conflicts.append(cand)  # but it's really only a conflict if the cf values differ

        if len(fq_conflicts) > 0:
            print('%d Merge conflicts encountered' % len(fq_conflicts))
            return fq_conflicts

        for f_dict in self._q_dict.values():
            try:
                combo = f_dict[dominant]
            except KeyError:
                combo = self._cl_typ()
            for syn in syns:
                if syn not in f_dict:
                    continue
                combo.update(f_dict[syn])
            f_dict[dominant] = combo

        if dominant not in self._fq_map:
            self._fq_map[dominant] = set()
        for syn in syns:
            self._fq_map[dominant] += self._fq_map.pop(syn, set())
            self._flow_map[dominant] += self._flow_map.pop(syn, set())

        for syn in syns:
            self._fm.merge(dominant, syn)
        return dominant

    def _add_flow_terms(self, flow, merge_strategy=None):
        """
        This process takes in an inbound FlowInterface instance, identifies the flowable(s) that match its terms, and
        returns the local flowable that matches the flow's name [sets the flow's name on prune]
        :param flow:
        :param merge_strategy:
        :return:
        """
        merge_strategy = merge_strategy or self._merge_strategy
        fb_map = defaultdict(list)
        for syn in flow.synonyms:
            # make a list of all the existing flowables that match the incoming flow
            fb_map[self._fm.get(syn)].append(syn)
        new_terms = fb_map.pop(None, [])
        if len(fb_map) == 0:  # all new terms
            if len(new_terms) == 0:
                raise AttributeError('Flow appears to have no terms: %s' % flow)
            fb = self._create_flowable(flow.name, *new_terms)
        elif len(fb_map) == 1:  # one existing match
            fb = list(fb_map.keys())[0]
            for term in new_terms:
                self._fm.add_synonym(term, str(fb))
        else:  # > 2 matches-- invoke merge strategy
            if merge_strategy == 'prune':
                self._print('\nPruning entry for %s' % flow)
                fb = self._add_pruned_terms(flow.name, new_terms)
                flow.name = str(fb)

            elif merge_strategy == 'merge':
                # this is trivial but
                self._print('Merging')
                fb = self._merge_terms(*fb_map.keys())
                if isinstance(fb, list):
                    raise FactorCollision(fb)
                for term in new_terms:
                    self._fm.add_synonym(term, str(fb))
            else:
                raise ValueError('merge strategy %s' % self._merge_strategy)
        # log the flowables that match the flow
        self._flow_map[fb].add(flow)
        for _tf in fb_map.keys():
            self._flow_map[_tf].add(flow)
        return fb

    def add_flow(self, flow, merge_strategy='prune'):
        """
        We take a flow from outside and add its known terms. That means
         - adding flow's reference quantity
         - merging any context with the local context tree;
         - adding flowable terms to the flowables list;
         - mapping flow to all flowables (assigning flowable if none is found)

        :param flow:
        :param merge_strategy: overrule default merge strategy
        :return: the Flowable object to which the flow's terms have been added
        """
        if flow.reference_entity is not None:
            self.add_quantity(flow.reference_entity)  # ensure exists
        self._check_context(flow)
        self._add_flow_terms(flow, merge_strategy=merge_strategy)

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

        try:
            cx = self._cm[context]
        except KeyError:
            cx = self.add_context(context)

        try:
            fb = self._fm[flowable]
        except KeyError:
            fb = self._create_flowable(flowable)

        try:
            rq = self._canonical_q(ref_quantity)
        except KeyError:
            rq = self.add_quantity(ref_quantity)

        try:
            qq = self._canonical_q(query_quantity)
        except KeyError:
            qq = self.add_quantity(query_quantity)

        cf = self._find_exact_cf(qq, fb, cx, origin)

        if cf is None:
            # create our new Characterization with the supplied info
            new_cf = Characterization(fb, rq, qq, context=cx, origin=origin)
            if isinstance(value, dict):
                new_cf.update_values(**value)
            else:
                new_cf.add_value(value, location=location)

            # add our new CF to the lookup tree
            self._qassign(qq, fb, new_cf)

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
    def _find_exact_cf(self, qq, fb, cx, origin):
        """
        The purpose of this function is to retrieve an exact CF if one exists.

        origin used in subclasses
        :param qq:
        :param fb:
        :param cx:
        :param origin:
        :return:
        """
        try:
            clookup = self._qlookup(qq, fb)
        except NoFQEntry:
            return None
        if cx in clookup:
            return clookup[cx]
        return None

    def get_canonical(self, quantity):
        try:
            return self._canonical_q(quantity)
        except KeyError:
            raise EntityNotFound(quantity)

    def _canonical_q(self, quantity):
        """
        override here
        :param quantity: a quantity entity or descriptor
        :return: a canonical quantity or raise KeyError
        """
        '''
        try:
            return next(q for q in self._q_dict.keys() if q == quantity)
        except StopIteration:
            raise UnknownQuantityRef(quantity)
        '''
        if isinstance(quantity, str):
            return self._qm[quantity]
        return self._qm[quantity.external_ref]

    def _canonical_q_ref(self, quantity):
        return self._canonical_q(quantity).external_ref

    def _qlookup(self, qq, fb):
        """
        Returns a mapping from context to CF
        :param qq: a canonical quantity
        :param fb: a canonical flowable
        :return:
        """
        try:
            return self._q_dict[qq][fb]
        except KeyError:
            raise NoFQEntry

    def _qaccess(self, qq, fb=None):
        """
        creates an entry if none exists
        :param qq: a canonical quantity
        :param fb [None]: a canonical flowable. If None, return the quantity dict
        :return:
        """
        if qq in self._q_dict:
            qd = self._q_dict[qq]
        else:
            qd = dict()
            self._q_dict[qq] = qd
        if fb is None:
            return qd
        elif fb in qd:
            return qd[fb]
        else:
            cl = self._cl_typ()
            qd[fb] = cl
            return cl

    @staticmethod
    def _store_cf(cl, context, new_cf):
        """
        Assigns the cf to the mapping; does subclass-specific collision checking
        :param cl:
        :param new_cf:
        :return:
        """
        if context in cl:
            raise FactorCollision
        cl[context] = new_cf

    def _qassign(self, qq, fb, new_cf, context=None):
        """
        Assigns the new_cf to the canonical quantity and flowable, taking context and origin from new_cf
        :param qq: a canonical quantity
        :param fb: a canonical flowable
        :param new_cf: a characterization, having a canonical context
        :return:
        """
        if context is None:
            context = new_cf.context
        cl = self._qaccess(qq, fb)
        self._store_cf(cl, context, new_cf)
        self._fq_map[fb].add(qq)

    def _factors_for_flowable(self, fb, qq, cx, **kwargs):
        """
        detach lookup for cleanness. canonical everything
        :param fb:
        :param qq:
        :param cx:
        :param kwargs: used in subclasses
        :return:
        """
        try:
            cl = self._qlookup(qq, fb)
        except NoFQEntry:
            return
        if cx is NullContext:
            for v in cl.values():
                yield v
        else:
            if cx in cl:
                yield cl[cx]

    def factors_for_flowable(self, flowable, quantity=None, context=None, **kwargs):
        """
        This is the method that actually performs the lookup.  Other methods are wrappers for this.

        Core to this is getting a canonical context, which is done by __getitem__
        :param flowable: a string
        :param quantity: a quantity known to the quantity manager
        :param context: [None] default provide all contexts; must explicitly provide 'none' to filter by null context
        :return:
        """
        try:
            fb = self._fm[flowable]
            cx = self._cm[context]
        except KeyError:
            return
        if quantity is None:
            for qq in self._fq_map[fb]:
                for cf in self._factors_for_flowable(fb, qq, cx, **kwargs):
                    yield cf
        else:
            qq = self._canonical_q(quantity)
            for cf in self._factors_for_flowable(fb, qq, cx, **kwargs):
                yield cf

    def factors_for_quantity(self, quantity, flowable=None, context=None, **kwargs):
        """
        param dist [0] only used if compartment is specified. by default report only exact matches.

        :param quantity:
        :param flowable:
        :param context:
        :return:
        """
        if flowable is not None:
            for k in self.factors_for_flowable(flowable, quantity=quantity, context=context, **kwargs):
                yield k
        else:
            qq = self._canonical_q(quantity)
            for f in self._qaccess(qq).keys():
                for k in self.factors_for_flowable(f, quantity=qq, context=context, **kwargs):
                    yield k

    def get_flowable(self, term):
        return self._fm[term]

    def flowables(self, search=None, origin=None):
        """
        :param origin: used in subclass
        :param search:
        :return:
        """
        if search is None:
            for fb in self._fm.objects:
                yield str(fb)
        else:
            for fb in self._fm.objects_with_string(search):
                    yield str(fb)

    def unmatched_flowables(self, flowables):
        """
        Given an iterable of flowable strings, return a list of entries that were not recognized as synonyms to known
        flowables
        :param flowables:
        :return:
        """
        unknown = []
        for fb in flowables:
            try:
                self._fm[fb]
            except KeyError:
                unknown.append(fb)
        return unknown

    def synonyms(self, term):
        """
        Search for synonyms, first in contexts, then flowables, then quantities.
        The somewhat awkward structure here is because of the dynamics of returning generators-- using
        try: return self._cm.synonyms(term) except KeyError: ... the KeyError was not getting caught because the
        generator was already returned before iterating.
        :param term:
        :return:
        """
        try:
            obj = self._cm[term]
            it = self._cm.synonyms(obj)
        except KeyError:
            try:
                obj = self._fm[term]
                it = self._fm.synonyms(obj)
            except KeyError:
                try:
                    obj = self._qm[term]
                    it = self._qm.synonyms(obj)
                except KeyError:
                    it = ()
        for k in it:
            yield k

    def contexts(self, search=None, origin=None):
        for cx in self._cm.objects:
            if origin is not None:
                if not cx.has_origin(origin):
                    continue
            if search is None:
                yield cx
            else:
                if cx.contains_string(search):
                    yield cx

    def quantities(self, search=None, origin=None):
        for q in self._qm.objects:
            if origin is not None:
                if not q.origin.startswith(origin):
                    continue
            if search is None:
                yield q
            else:
                if q.contains_string(search):
                    yield q

    '''
    De/Serialization
    
    Strategy here is that term manager serializations should be totally self-contained and also minimal. 
    
    Well, not totally self-contained, since quantities themselves are not going to be messed with.
    
    So aside from quantities, which are not going to be messed with, the term manager serialization should be specified
    by quantity. The routine must determine, for all query quantities listed:
     (a) the set of reference quantities encountered
     (b) the set of flowables encountered
     (c) the set of contexts encountered
    The reference and query quantities must all be serialized per normal, with others omitted.  The term manager 
    section should include flowables, contexts, and characterizations, but only the minimal ones required to cover the
    specified quantities.
    
    Then de-serialization is straightforward: after quantities are loaded, simply apply all flowables, contexts, and
    then characterizations- flows not required!
    '''
    def _serialize_qdict(self, origin, quantity, values=False):
        _ql = self._qaccess(quantity)
        return {str(fb):
                    {str(c): cf.serialize(values=values, concise=True) for c, cf in cl.items() if cf.origin == origin}
                for fb, cl in _ql.items()}

    def _serialize_factors(self, origin, *quantities, values=False):
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
            qqs = self._q_dict.keys()
        else:
            qqs = [self._canonical_q(q) for q in quantities]
        j = dict()
        for q in qqs:
            if q.origin != origin:
                continue
            _sq = self._serialize_qdict(origin, q, values=values)
            if len(_sq) > 0:
                j[self._canonical_q_ref(q)] = _sq
        return j

    def serialize(self, origin, *quantities, values=False):
        """

        :param origin:
        :param quantities:
        :param values:
        :return: 3-tuple:
         - serialized term manager as dict,
         - set of query quantity external refs,
         - set of reference quantity uuids
           == not sure WHY UUIDs rather than external refs, but that is what characterizations.py gives us
        """
        qqs = set()  # query quantities to serialize
        rqs = set()  # reference quantities to serialize
        fbs = set()  # flowables to serialize
        cxs = set()  # contexts to serialize

        # serialize factors themselves
        j = {'Characterizations': self._serialize_factors(origin, *quantities, values=values)}

        # identify terms + qs
        for qq, factors in j['Characterizations'].items():
            qqs.add(qq)
            for fb, char in factors.items():
                fbs.add(fb)
                for cx, cf in char.items():
                    for c in self._context_and_parents(cx):
                        cxs.add(str(c))
                    try:
                        rqs.add(cf['ref_quantity'])
                    except KeyError:
                        print('%s\n%s' % (cx, cf))
                        raise

        # add flowables and contexts
        j.update(self._fm.serialize(obj for obj in self._fm.entries if str(obj) in fbs))
        j.update(self._cm.serialize(obj for obj in self._cm.entries if str(obj) in cxs))

        return j, qqs, rqs

    def add_from_json(self, j, q_map, origin=None):
        """

        :param j:
        :param q_map: a dict whose keys are external_refs and uuids, and whose values are quantities
        :param origin:
        :return:
        """
        self._cm.load_dict(j)  # automatically pulls out 'Compartments'
        self._fm.load_dict(j)
        for f in self._fm.objects:
            if f not in self._fq_map:
                self._fq_map[f] = set()
        self._add_from_json(j['Characterizations'], q_map, origin)

    def _add_from_json(self, j, q_map, origin):
        """
        Argument: the contents of archive['characterizations'], which looks like this (general case):
        'termManager': {
          'SynonymSets': [ {flowable}..],
          'Compartments': [ {context}..],
          'Characterizations': {
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
          'SynonymSets': [ {flowable}..],
          'Compartments': [ {context}..],
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
        for query_ext_ref, fbs in j.items():
            query_q = q_map[query_ext_ref]
            for fb, cxs in fbs.items():
                if origin is None:
                    for cx, cfs in cxs.items():
                        for org, spec in cfs.items():
                            ref_q = q_map[spec['ref_quantity']]
                            self.add_characterization(fb, ref_q, query_q, spec['value'], context=cx,
                                                      origin=org)
                else:
                    for cx, spec in cxs.items():
                        ref_q = q_map[spec['ref_quantity']]
                        self.add_characterization(fb, ref_q, query_q, spec['value'], context=cx,
                                                  origin=origin)
