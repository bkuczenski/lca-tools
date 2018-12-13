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
import re
from collections import defaultdict

from .quantity import QdbQuantityImplementation

from lcatools.from_json import from_json
from lcatools.lcia_results import LciaResult
from lcatools.archives import BasicArchive
from lcatools.basic_query import BasicQuery
from lcatools.flowdb.compartments import Compartment, CompartmentManager, MissingCompartment
from lcatools.characterizations import Characterization
# from lcatools.dynamic_grid import dynamic_grid
# from lcatools.interact import pick_one
from synlist import SynList, Flowables, InconsistentIndices, ConflictingCas, EntityFound


REF_QTYS = os.path.join(os.path.dirname(__file__), 'data', 'elcd_reference_quantities.json')
Q_SYNS = os.path.join(os.path.dirname(__file__), 'data', 'quantity_synlist.json')
F_SYNS = os.path.join(os.path.dirname(__file__), 'data', 'flowable_synlist.json')


biogenic = re.compile('(biotic|biogenic|non-fossil)', flags=re.IGNORECASE)


class NotAQuantity(Exception):
    pass


class QuantityNotKnown(Exception):
    pass


class QuantityNotLoaded(Exception):
    pass


class ConversionReferenceMismatch(Exception):
    pass


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
        return set()

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

    def cfs(self):
        for comp, cfs in self._dict.items():
            for cf in cfs:
                yield cf

    def find(self, item, dist=1, return_first=True):
        """
        Hunt for a matching compartment. 'dist' param controls the depth of search:
          dist = 0: equivalent to __getitem__
          dist = 1: also check compartment's children
          dist = 2: also check compartment's parent
          dist = 3: also check all compartment's parents until root. Useful for finding unit conversions.
        By default (dist==1), checks compartment self and children. Returns a set.
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
            item = item.parent
            if item in self._dict.keys():
                results = results.union(self._dict[item])

        if found(results):
            return results

        if dist > 2:
            while item.parent is not None:
                item = item.parent
                if item in self._dict.keys():
                    results = results.union(self._dict[item])
                if found(results):
                    return results

        return results


class Qdb(BasicArchive):
    def __init__(self, source=REF_QTYS, quantities=Q_SYNS, flowables=F_SYNS, compartments=None,
                 quell_biogenic_CO2=False, quell_biogenic_co2=False,
                 ref=None, **kwargs):
        """

        :param source: for LcArchive and core quantities (ELCD)
        :param quantities: [Q_SYNS] synonym list for quantities
        :param flowables: [F_SYNS] synonym list for flowables
        :param compartments: either a compartment manager, or a compartment definition file
        :param quell_biogenic_CO2: [False] if True, return '0' for all inputs of CO2 from air and all CO2 emissions
         matching a 'biogenic' search criterion.  The CO2 flowable is distinguished by its CAS number, 124-38-9.
         No other flowables are affected.

         The criterion is not quantity-based, but depends on the flowable and compartment only.

         The regex test is currently case-insensitive '(biogenic|biotic|non-fossil)'. It can be tested with
         Qdb.is_biogenic(term)

         The quell_biogenic_CO2 parameter can be overridden at query-time as a keyword param.

         Note that the Qdb's architecture does not permit it to distinguish between two different flows of the same
         substance into the same compartment, without making a special exception.  Frankly, to make it do so is
         somewhat distasteful.
        :param ref:
        :param kwargs:
        """
        if ref is None:
            ref = 'local.qdb'
        if not os.path.exists(source):
            print('Using default reference quantities')
            source = REF_QTYS
        self._f = Flowables.from_json(from_json(flowables))
        self._q = SynList.from_json(from_json(quantities))

        super(Qdb, self).__init__(source, ref=ref, **kwargs)
        self.load_json(from_json(source))

        if isinstance(compartments, CompartmentManager):
            self.c_mgr = compartments
        else:
            self.c_mgr = CompartmentManager(compartments)

        self._index_quantities()

        self._q_dict = defaultdict(set)  # dict of quantity index to set of characterized flowables (by index)
        self._fq_dict = defaultdict(CLookup)  # dict of (flowable index, quantity index) to c_lookup
        self._f_dict = defaultdict(set)  # dict of flowable index to set of characterized quantities (by index)

        # following are to implement special treatment for biogenic CO2
        self._quell_biogenic_co2 = quell_biogenic_CO2 or quell_biogenic_co2
        self._co2_index = self._f.index('124-38-9')
        self._comp_from_air = self.c_mgr.find_matching('Resources from air')

        # load CFs from reference flows
        for f in self.entities_by_type('flow'):
            for cf in f.characterizations():
                if cf.quantity is not f.reference_entity:
                    self.add_cf(cf)

    def make_interface(self, iface):
        if iface == 'quantity':
            return QdbQuantityImplementation(self)
        else:
            return super(Qdb, self).make_interface(iface)

    def _add_or_merge_quantity(self, q):
        """
        This function should only be called from _get_q_ind, after it determines the quantity does not exist
        :param q:
        :return:
        """
        if q.entity_type != 'quantity':
            raise NotAQuantity('Not adding non-quantity to Qdb: %s' % q)
        if q.is_lcia_method():
            ind = self._q.add_set(self._q_terms(q), merge=False)  # allow different versions of the same LCIA method
            if ind is None:  # major design flaw in SynList- add_set should not return None
                ind = self._q.index(next(self._q_terms(q)))
        else:
            ind = self._q.add_set(self._q_terms(q), merge=True)  # squash together different versions of a ref quantity
        if self._q.entity(ind) is None:
            self._q.set_entity(ind, q)
            try:
                self.add(q)
            except KeyError:
                pass
        return ind

    def _get_q_ind(self, quantity):
        """
        get index for an actual entity or synonym. Create a new entry if none is found.
        :param quantity:
        :return:
        """
        if quantity is None:
            return None
        if isinstance(quantity, int):
            if quantity < len(self._q):
                return quantity
            raise IndexError('Quantity index %d out of range' % quantity)
        elif isinstance(quantity, str):
            try:
                ind = self._q.index(quantity)
            except KeyError:
                raise QuantityNotKnown(quantity)
        else:
            ind = self._add_or_merge_quantity(quantity)
        return ind

    def __getitem__(self, item):
        try:
            return self._get_canonical(item)
        except IndexError:
            pass
        except QuantityNotKnown:
            pass
        except NotAQuantity:
            pass
        return super(Qdb, self).__getitem__(item)

    def _get_canonical(self, item):
        i = self._get_q_ind(item)
        if i is not None:
            q = self._q.entity(i)
            if q is not None:
                return q
        raise QuantityNotKnown(item)

    def get_canonical(self, item):
        q = self._get_canonical(item)
        return q.make_ref(BasicQuery(self))


    def save(self):
        self.write_to_file(self.source, characterizations=True, values=True)  # leave out exchanges

    @property
    def quell_biogenic_co2(self):
        return self._quell_biogenic_co2

    def is_known(self, q):
        """
        Checks whether the given quantity has been indexed by checking for a list of flowables
        :param q:
        :return:
        """
        ind = self._get_q_ind(q)
        if ind in self._q_dict:
            return True
        return False

    def quantify(self, flowable, quantity, compartment=None):
        """
        Perform flow-quantity lookup. not likely to work very well until the CLookup gets sorted.
        :param flowable:
        :param quantity:
        :param compartment:
        :return:
        """
        f_idx = self._f.index(flowable)
        q_idx = self._get_q_ind(quantity)
        if compartment is None:
            return self._fq_dict[f_idx, q_idx].cfs()
        return self._fq_dict[f_idx, q_idx].find(compartment)

    def _index_quantities(self):
        for q in self.entities_by_type('quantity'):
            try:
                self._q.set_entity(q['Name'], q)
                self.add_synonyms(q['Name'], *self._q_terms(q))
            except EntityFound:
                pass
            except KeyError:
                self._get_q_ind(q)

    def add_flowable_from_flow(self, flow):
        return self.add_new_flowable(*self._flow_terms(flow))

    def compartments_for(self, q_ref, flowables=None):
        """
        Return a sorted list of compartments that appear in a given quantity. Optionally filter for a set of flowables.
        :param q_ref:
        :param flowables: optional iterable of flowables to look for (defaults to full list of flowables for quantity)
        :return:
        """
        q_ind = self._get_q_ind(q_ref)
        if flowables is None:
            flowables = self._q_dict[q_ind]
        comps = set()
        for i in flowables:
            for k in self._fq_dict[i, q_ind].compartments():
                comps.add(k)
        return sorted(comps, key=lambda x: x.to_list())

    ''' # this needs to get migrated into antelope_reports.tables (or maybe it already is!)
    def cf_table(self, q_ref):
        """
        Draws a retro-style dynamic table of CFs for a quantity-- rows are flowables, columns are compartments.
        Doesn't return anything.
        :param q_ref:
        :return:
        """
        q_ind = self._get_q_ind(q_ref)

        rows = sorted(self._q_dict[q_ind], key=lambda x: self._f.name(x))
        cols = self.compartments_for(q_ref)

        print('%s' % q_ref)
        print('Characterization Factors\n ')
        dynamic_grid(cols, rows, lambda x, y: self._fq_dict[x, q_ind][y],
                     ('CAS Number ', lambda x: self._f.cas(x)),
                     ('Flowable', lambda x: self._f.name(x)),
                     returns_sets=True)
    '''

    def flows_for_quantity(self, q_ref):
        try:
            q_ind = self._get_q_ind(q_ref)
            for f in sorted(self._q_dict[q_ind]):
                yield f
        except QuantityNotKnown:
            for f in []:
                yield f

    def cfs_for_quantity(self, q_ref, compartment=None):
        """
        generator. Yields CFs associated with a quantity, ordered by flowable index.
        :param q_ref:
        :param compartment
        :return:
        """
        try:
            q_ind = self._get_q_ind(q_ref)
            for f in sorted(self._q_dict[q_ind]):
                if compartment is None:
                    for k in self._fq_dict[f, q_ind].cfs():
                        yield k
                else:
                    for k in self._fq_dict[f, q_ind][compartment]:
                        yield k

        except QuantityNotKnown:
            for f in []:
                yield f

    def add_new_flowable(self, *terms):
        try:
            ind = self._f.add_set(terms, merge=True)
        except ConflictingCas:
            # print('Conflicting CAS numbers found for: %s' % '; '.join(list(terms)))  # Flowables already warns us
            ind = self._f.add_set(terms, merge=False)
            print('New flowable: %s' % self._f[ind])
        except InconsistentIndices:
            k = self._f.find_indices(terms)
            unmatched = k.pop(None, set())
            if len(unmatched) == 0:
                ind = None
            else:
                inds = sorted(k.keys())
                if not self._quiet:
                    self._print('Inconsistent Indices; adding to highest index:')
                    self._print('None %.100s' % '; '.join(unmatched))
                    for i in inds:
                        self._print('%4d %.100s' % (i, '; '.join(k[i])))
                ind = inds[-1]
                self._f.add_synonyms(ind, *unmatched)
        return ind

    def find_flowables(self, flow):
        return list(set([k for k in self._find_flowables(*self._flow_terms(flow))]))

    def parse_flow(self, flow):
        """
        Return a valid flowable name and a compartment object as a 2-tuple.

        To find the flowable: Start with the flow's link, then the flow's name, then the flow's cas.
        To return: preferentially return CAS number, then name if CAS is none.
        :param flow:
        :return:
        """
        fb = None
        for k in self._flow_terms(flow):
            fb = self.f_index(k)
            if fb is not None:
                break
        if fb is None:
            fn = flow['Name']
        else:
            if self._f.cas(fb) is None:
                fn = self._f.name(fb)
            else:
                fn = self._f.cas(fb)
        comp = self.c_mgr.find_matching(flow['Compartment'])
        return fn, comp

    def flowables(self):
        for k in range(len(self._f)):
            yield self._f.cas(k), self._f.name(k)

    def f_index(self, term):
        """
        Wrapper to expose flowable index
        :param term:
        :return:
        """
        return self._f.index(term)

    def f_cas(self, term):
        """
        Wrapper to expose CAS number
        :param term:
        :return:
        """
        return self._f.cas(term)

    def f_name(self, term):
        """
        Wrapper to expose canonical name
        :param term:
        :return:
        """
        try:
            fn = self._f.name(term)
        except KeyError:
            fn = term
        return fn

    def f_syns(self, term):
        for s in self._f.synonyms_for(term):
            yield s

    def q_syns(self, term):
        for s in self._q.synonyms_for(term):
            yield s

    def _find_flowables(self, *terms):
        """

        :param terms: each term provided as a separate argument
        :return: generates flowable indices
        """
        inds = self._f.find_indices(terms)
        if None in inds:
            inds.pop(None)
        for k in inds:
            yield k

    @property
    def flow_properties(self):
        for q in self.entities_by_type('quantity'):
            if not q.is_lcia_method():
                yield q

    @property
    def indicators(self):
        """
        An indicator is a quantity with the 'Indicator' property
        :return:
        """
        for q in self.entities_by_type('quantity'):
            if q.is_lcia_method():
                yield q

    def is_elementary(self, flow):
        return self.c_mgr.is_elementary(flow)

    '''
    Collecting new CFs
    '''
    @staticmethod
    def _flow_terms(flow):
        if flow['CasNumber'] is None or len(flow['CasNumber']) < 5:
            return flow.link, flow['Name']
        return flow.link, flow['Name'], flow['CasNumber']

    @staticmethod
    def _q_terms(q):
        """
        ordered from most- to least-specific
        Leave out q['Name'] because allows for false-positive matches e.g. between 'Mass [t]' and 'Mass [kg]'.
        We do want this to be strict.
        :param q:
        :return:
        """
        for i in q.uuid, q.link, q.q_name:
            yield i
        if q.has_property('Synonyms'):
            syns = q['Synonyms']
            if isinstance(syns, str):
                yield syns
            else:
                for syn in syns:
                    yield syn

    def add_synonyms(self, main_term, *terms):
        if self._q[main_term] is None:
            if self._f[main_term] is None:
                raise KeyError(main_term)
            else:
                # no way to save new flowable synonyms-- do this after ContextRefactor
                return NotImplemented
        else:
            # we can, however, save quantity synonyms in the local archive
            self._q.add_synonyms(main_term, *terms)
            ent = self._q.entity(main_term)
            if ent.has_property('Synonyms'):
                syns = ent['Synonyms']
                if isinstance(syns, str):
                    syns = [syns]
            else:
                syns = []
            syns.extend(terms)
            ent['Synonyms'] = syns
            # self.save()
            # also update the entity ref
            ent.make_ref(BasicQuery(self))['Synonyms'] = syns

    def add_cf(self, factor, flow=None, interact=False):
        """
        factor should be a Characterization, with the behavior:
          * flow[x] for x in {'Name', 'CasNumber', 'Compartment'} operable
          * flow.reference_entity operable and known to qdb
          * quantity or quantity CatalogRef
          * __getitem__ operable
        :param factor:
        :param flow: [None] source for flowables
        :param interact: [False] raise MissingCompartment; True: interactively add/merge compartment
        :return:
        """
        q_ind = self._get_q_ind(factor.quantity)
        if flow is None:
            flow = factor.flow
        f_terms = self._flow_terms(flow)
        self.add_new_flowable(*f_terms)
        f_inds = self._find_flowables(*f_terms)

        self._get_q_ind(factor.flow.reference_entity)

        comp = self.c_mgr.find_matching(factor.flow['Compartment'], interact=interact)

        for f_ind in f_inds:
            self._q_dict[q_ind].add(f_ind)
            self._f_dict[f_ind].add(q_ind)
            self._fq_dict[f_ind, q_ind][comp] = factor

    def _lookup_cfs(self, f_inds, compartment, q_ind):
        if isinstance(f_inds, int):
            f_inds = [f_inds]
        cfs = set()
        for f_ind in f_inds:
            for item in self._fq_dict[f_ind, q_ind].find(compartment, dist=2, return_first=True):
                self._print('  ** _lookup_cfs found %s' % item)
                cfs.add(item)
        return cfs

    def _flow_cf_generator(self, flow, from_q):
        for cf in flow.characterizations():
            if self._get_q_ind(cf.quantity) == from_q:
                yield cf

    def _conversion_from_flow(self, flow, from_q, locale='GLO'):
        try:
            cf = next(self._flow_cf_generator(flow, from_q))
            value = cf[locale]
        except StopIteration:
            value = None
        return value

    def _conversion_generator(self, f_inds, compartment, from_q, to_q):
        for f_ind in f_inds:
            for item in self._fq_dict[f_ind, from_q].find(compartment, dist=3):
                if self._get_q_ind(item.flow.reference_entity) == to_q:
                    yield item

    def convert_reference(self, flow, from_q, locale='GLO'):
        """
        Return a conversion factor for expressing a flow's quantities with respect to a different reference quantity.

        Flow Characterizations are given in units of query_q / ref_q where ref_q is the flow's reference quantity.
        This function returns a factor with the dimension from_q / ref_q.  This allows a foreign reference with the
        dimension of query_q / from_q to be converted by multiplication into the dimension query_q / ref_q.
        :param flow:
        :param from_q:
        :param locale: optional location spec
        :return: from_q / ref_q for the given flow, or None if no conversion was found
        """
        from_q_ind = self._get_q_ind(from_q)
        ref_q_ind = self._get_q_ind(flow.reference_entity)
        if from_q_ind == ref_q_ind:
            return 1.0
        conv = self._conversion_from_flow(flow, from_q_ind)
        if conv is None:
            f_inds = [fb for fb in self._find_flowables(*self._flow_terms(flow))]
            compartment = self.c_mgr.find_matching(flow['Compartment'], interact=False)
            conv, origin = self._lookfor_conversion(f_inds, compartment, from_q_ind, ref_q_ind)
            if conv is not None:
                flow.add_characterization(self._q.entity(from_q), location=locale, value=conv, origin=origin)
        if conv is None:
            return 0.0
        return conv

    def _lookfor_conversion(self, f_inds, compartment, from_q, to_q, locale='GLO'):
        """

        Convert is supposed to supply characterization factors in the dimension of query_q / ref_q

        fq_dict stores CFs that have dimension query_q / cf_ref_q: query unit per CF's flows' ref unit
        If CF ref_unit does not match the supplied ref_unit, we need to find and return a conversion.
        The conversion should have dimension cf_ref_q / ref_q.

        The conversion generator returns a CF with dimension from_q / to_q.  If the inverse CF is found, this needs
        to be inverted.

        :param f_inds:
        :param compartment:
        :param from_q:
        :param to_q:
        :return:
        """
        # TODO: This should be replaced with a DFS, at least until we re-implement the whole kit+kaboodle as a graph db
        # first straight from-to
        origin = None
        try:
            cf = next(self._conversion_generator(f_inds, compartment, from_q, to_q))
            value = cf[locale]
        except StopIteration:
            try:
                cf = next(self._conversion_generator(f_inds, compartment, to_q, from_q))
                value = 1.0 / cf[locale]
                origin = cf.origin(locale)
            except StopIteration:
                value = None
        return value, origin

    @staticmethod
    def is_biogenic(term):
        return bool(biogenic.search(term))

    def _convert_values(self, f_inds, comp, ref_q_ind, query_q_ind, flow=None, locale='GLO'):
        """
        Produces a list of conversion factors with the dimension query_q / ref_q
        :param f_inds: a list of matching flowables
        :param comp: an actual compartment
        :param ref_q_ind: index into _q for reference quantity
        :param query_q_ind: index into _q for query quantity
        :param flow: if present, checked first to resolve inconsistent reference quantities
        :param locale: for conversion
        :return:
        """
        cfs = self._lookup_cfs(f_inds, comp, query_q_ind)
        _conv_error = False
        if len(cfs) == 0:
            self._print(' !! No cfs returned !!')

        # now to check reference quantity
        vals = []
        for cf in cfs:
            factor = cf[locale]
            cf_ref_q_ind = self._get_q_ind(cf.flow.reference_entity)
            if cf_ref_q_ind != ref_q_ind:
                self._print('Flow: %s\nreference quantities don\'t match: cf:%s, ref:%s' % (f_inds,
                                                                                            self._q.name(cf_ref_q_ind),
                                                                                            self._q.name(ref_q_ind)))
                ref_conversion = None
                if flow is not None:
                    # first take a look at the flow's builtin CFs, use them if present
                    ref_conversion = self._conversion_from_flow(flow, cf_ref_q_ind)

                if ref_conversion is not None:
                    # found it in the flow; move on
                    factor *= ref_conversion

                else:
                    # next, try to consult fq_dict
                    ref_conversion, origin = self._lookfor_conversion(f_inds, comp, cf_ref_q_ind, ref_q_ind,
                                                                      locale=locale)

                    if ref_conversion is None:
                        self._print('Unable to find conversion... bailing')
                        _conv_error = True
                        continue

                    factor *= ref_conversion
                    if flow is not None:
                        # if we have the flow, we should document the characterization used
                        flow.add_characterization(cf.flow.reference_entity, value=ref_conversion, location=locale,
                                                  origin=origin)

            self._print('  ** found value %g\n%s' % (factor, cf))
            vals.append(factor)

        if _conv_error and len(vals) == 0:
            raise ConversionReferenceMismatch

        return vals

    def convert(self, flow=None, flowable=None, compartment=None, reference=None, query=None, query_q_ind=None,
                locale='GLO',
                quell_biogenic_co2=None):
        """
        Implement the flow-quantity relation.  The query must supply a flowable, compartment, reference quantity, and
        query quantity, with optional locale.  The first three arguments can be supplied implicitly with an LcFlow.

        :param flow: must supply EITHER flow LcFlow OR (flowable AND compartment AND reference)
        :param flowable:
        :param compartment:
        :param reference:
        :param query: must supply either query LcQuantity OR query_q_ind (to save time)
        :param query_q_ind:
        :param locale:
        :param quell_biogenic_co2: [None] override the Qdb setting.
        :return: a floating point conversion
        """
        if query_q_ind is None:
            query_q_ind = self._get_q_ind(query)
        if flow is None:
            ref_q_ind = self._get_q_ind(reference)
            f_inds = [self._f.index(flowable)]
            _biogenics = (y for y in [flowable])
        else:
            if flowable or compartment or reference:
                raise ValueError('Too many elements specified')
            ref_q_ind = self._get_q_ind(flow.reference_entity)
            compartment = flow['Compartment']
            f_inds = [fb for fb in self._find_flowables(*self._flow_terms(flow))]
            _biogenics = (y for y in self._flow_terms(flow))

        if ref_q_ind is None:
            self._print('  ** convert - no ref_q_ind found')
            return None

        if ref_q_ind == query_q_ind:
            self._print('  ** convert - ref and query are the same')
            return 1.0

        comp = self.c_mgr.find_matching(compartment, interact=False)

        if len(f_inds) == 0:
            self._print(' !! No matching flowables !!')

        for f_ind in f_inds:
            if f_ind == self._co2_index:
                if quell_biogenic_co2 or (quell_biogenic_co2 is None and self.quell_biogenic_co2):
                    self._print('#detected CO2 flow and quell is on')
                    if comp.is_subcompartment_of(self.c_mgr.emissions):
                        self._print('  is an emission')
                        if any([self.is_biogenic(term) for term in _biogenics]):
                            self._print('   is biogenic - quelling')
                            return 0.0
                    elif comp.is_subcompartment_of(self._comp_from_air):
                        self._print('   is from air - quelling')
                        return 0.0

        vals = self._convert_values(f_inds, comp, ref_q_ind, query_q_ind, flow=flow, locale=locale)

        if len(vals) == 0:
            self._print('  ** no values found')
            return None
        if len(set(vals)) > 1:
            print('Multiple CFs found: %s' % vals)
            print('Flow: %s [%s]' % (flow, flow.unit()))
            print('Quantity: %s' % self._q.entity(query_q_ind))
            # TODO: implement semantic disambiguator to pick best CF
        return vals[0]

    def do_lcia(self, quantity, inventory, locale='GLO', refresh=False, debug=False, **kwargs):
        """
        takes a quantity and an exchanges generator; returns an LciaResult for the given quantity.
        For now, does NOT pre-load quantity LCIA methods. that is a catalog action. the Qdb doesn't do catalog stuff.
        I am open to re-thinking it, though.
        :param quantity:
        :param inventory: generates exchanges
        :param locale: ['GLO']
        :param refresh: [False] whether to rewrite characterization factors from the database
        :param debug: [False] print extra information to screen
        :param kwargs: just quell_biogenic_co2 for the moment
        :return: an LciaResult whose components are the flows of the exchanges
        """
        q = self[quantity.link]
        q_ind = self._get_q_ind(q)
        _is_quiet = self._quiet
        if debug:
            self._quiet = False
        self._print('q_ind: %d' % q_ind)
        r = LciaResult(q)
        for x in inventory:
            if refresh or not x.flow.has_characterization(q):
                try:
                    factor = self.convert(flow=x.flow, query_q_ind=q_ind, locale=locale, **kwargs)
                except MissingCompartment:
                    print('Missing compartment %s; abandoning this exchange' % x.flow['Compartment'])
                    continue
                except ConversionReferenceMismatch:
                    print('Mismatch %s' % x)
                    factor = None

                if factor is not None:
                    self._print('factor %g %s' % (factor, x))
                    x.flow.add_characterization(q, value=factor, overwrite=refresh)
                else:
                    self._print('factor NONE %s' % x)
                    x.flow.add_characterization(q)
            if x.flow.cf(q) is not None:
                r.add_component(x.flow.external_ref, entity=x.flow)
                fac = x.flow.factor(q)
                fac.set_natural_direction(self.c_mgr)
                r.add_score(x.flow.external_ref, x, fac, locale)
        self._quiet = _is_quiet
        return r

    def cf(self, flow, query_quantity, locale='GLO'):
        """

        :param flow:
        :param query_quantity:
        :param locale:
        :return:
        """
        return self.convert_reference(flow, query_quantity, locale=locale)
