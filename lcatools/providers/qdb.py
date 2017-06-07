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

from lcatools.from_json import from_json
from lcatools.lcia_results import LciaResult
from lcatools.providers.base import LcArchive
from lcatools.flowdb.compartments import Compartment, CompartmentManager  # load_compartments, save_compartments, traverse_compartments, REFERENCE_EFLOWS
from lcatools.characterizations import Characterization
from lcatools.interact import pick_one
from synlist import SynList, Flowables, InconsistentIndices, ConflictingCas


REF_QTYS = os.path.join(os.path.dirname(__file__), 'data', 'elcd_reference_quantities.json')
Q_SYNS = os.path.join(os.path.dirname(__file__), 'data', 'quantity_synlist.json')
F_SYNS = os.path.join(os.path.dirname(__file__), 'data', 'flowable_synlist.json')


biogenic = re.compile('(biotic|biogenic)', flags=re.IGNORECASE)


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


class Qdb(LcArchive):
    def __init__(self, source=REF_QTYS, quantities=Q_SYNS, flowables=F_SYNS, compartments=None,
                 quell_biogenic_CO2=False,
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

         The regex test is currently case-insensitive '(biogenic|biotic)'. It can be tested with Qdb.is_biogenic(term)

         The quell_biogenic_CO2 parameter can be overridden at query-time as a keyword param.

         Note that the Qdb's architecture does not permit it to distinguish between two different flows of the same
         substance into the same compartment, without making a special exception.  Frankly, to make it do so is
         somewhat distasteful.
        :param ref:
        :param kwargs:
        """
        if ref is None:
            ref = 'local.qdb'
        super(Qdb, self).__init__(source, ref=ref, **kwargs)
        self.load_json(from_json(source))

        if isinstance(compartments, CompartmentManager):
            self.c_mgr = compartments
        else:
            self.c_mgr = CompartmentManager(compartments)

        self._f = Flowables.from_json(from_json(flowables))
        self._q = SynList.from_json(from_json(quantities))

        self._index_quantities()

        self._q_dict = defaultdict(set)  # dict of quantity index to set of characterized flowables (by index)
        self._fq_dict = defaultdict(CLookup)  # dict of (flowable index, quantity index) to c_lookup
        self._f_dict = defaultdict(set)  # dict of flowable index to set of characterized quantities (by index)

        # following are to implement special treatment for biogenic CO2
        self._quell_biogenic_co2 = quell_biogenic_CO2
        self._co2_index = self._f.index('124-38-9')
        self._comp_from_air = self.c_mgr.find_matching('Resources from air')

    @property
    def quell_biogenic_co2(self):
        return self._quell_biogenic_co2

    def add_new_quantity(self, q):
        ind = self._q.add_set(self._q_terms(q), merge=True)
        if self._q.entity(ind) is None:
            self._q.set_entity(ind, q)
            self.add(q)
        return ind

    def get_canonical_quantity(self, q):
        """
        q is an actual quantity entity
        :param q:
        :return: the canonical entity.  If none exists, the supplied quantity is made canonical
        """
        ind = self._get_q_ind(q)
        return self._q.entity(ind)

    def is_loaded(self, q):
        """
        Checks whether the given quantity has been indexed by checking for a list of flowables
        :param q:
        :return:
        """
        ind = self._get_q_ind(q)
        if ind in self._q_dict:
            return True
        return False

    def _index_quantities(self):
        for q in self.quantities():
            try:
                self._q.set_entity(q['Name'], q)
            except KeyError:
                self.add_new_quantity(q)

    def add_new_flowable(self, *terms):
        try:
            ind = self._f.add_set(terms, merge=True)
        except (InconsistentIndices, ConflictingCas):
            print('Inconsistent indices found for: %s' % '; '.join(list(terms)))
            ind = self._f.add_set(terms, merge=False)
            print(self._f[ind])
        return ind

    def find_flowables(self, *terms):
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

    def is_elementary(self, flow):
        return self.c_mgr.is_elementary(flow)

    '''
    Collecting new CFs
    '''
    def _get_q_ind(self, quantity):
        """
        get index for an actual entity or synonym
        :param quantity:
        :return:
        """
        if quantity is None:
            return None
        if isinstance(quantity, int):
            if quantity < len(self._q):
                return quantity
            raise IndexError('Quantity index %d out of range' % quantity)
        if isinstance(quantity, str):
            try:
                ind = self._q.index(quantity)
            except KeyError:
                raise QuantityNotKnown(quantity)
        else:
            try:
                ind = self._q.index(next(k for k in self._q_terms(quantity)
                                         if self._q.index(k) is not None))

            except StopIteration:
                ind = self.add_new_quantity(quantity)
        return ind

    @staticmethod
    def _flow_terms(flow):
        if flow['CasNumber'] is None or len(flow['CasNumber']) < 5:
            return flow['Name'], flow.link
        return flow['Name'], flow['CasNumber'], flow.link

    @staticmethod
    def _q_terms(q):
        return q.external_ref, q.link, q['Name'], str(q)

    def add_cf(self, factor):
        """
        factor should be a Characterization, with the behavior:
          * flow[x] for x in {'Name', 'CasNumber', 'Compartment'} operable
          * flow.reference_entity operable and known to qdb
          * quantity or quantity CatalogRef
          * __getitem__ operable
        :param factor:
        :return:
        """
        q_ind = self._get_q_ind(factor.quantity)
        f_terms = self._flow_terms(factor.flow)
        self.add_new_flowable(*f_terms)
        f_inds = self.find_flowables(*f_terms)

        self.add_new_quantity(factor.flow.reference_entity)

        comp = self.c_mgr.find_matching(factor.flow['Compartment'])

        for f_ind in f_inds:
            self._q_dict[q_ind].add(f_ind)
            self._f_dict[f_ind].add(q_ind)
            self._fq_dict[f_ind, q_ind][comp] = factor

    def _lookup_cfs(self, f_inds, compartment, q_ind):
        if isinstance(f_inds, int):
            f_inds = [f_inds]
        cfs = set()
        for f_ind in f_inds:
            for item in self._fq_dict[f_ind, q_ind].find(compartment, dist=0):
                cfs.add(item)
        return cfs

    @staticmethod
    def is_biogenic(term):
        return bool(biogenic.search(term))

    def convert(self, flow=None, flowable=None, compartment=None, reference=None, query=None, query_q_ind=None,
                locale='GLO',
                quell_biogenic_co2=None):
        """
        EITHER flow OR (flowable AND compartment AND reference) must be non-None.
        :param flow:
        :param flowable:
        :param compartment:
        :param reference:
        :param query:
        :param query_q_ind: index of query quantity, to save time
        :param locale:
        :param quell_biogenic_co2: [None] override the Qdb setting.
        :return: a floating point conversion
        """
        if query_q_ind is None:
            query_q_ind = self._get_q_ind(query)
        if flow is None:
            ref_q_ind = self._get_q_ind(reference)
            f_inds = [self._f.index(flowable)]
            _biogenics = (y for y in (flowable))
        else:
            if flowable or compartment or reference:
                raise ValueError('Too many elements specified')
            ref_q_ind = self._get_q_ind(flow.reference_entity)
            compartment = flow['Compartment']
            f_inds = [fb for fb in self.find_flowables(*self._flow_terms(flow))]
            _biogenics = (y for y in self._flow_terms(flow))

        if ref_q_ind is None:
            return None

        comp = self.c_mgr.find_matching(compartment)

        for f_ind in f_inds:
            if f_ind == self._co2_index:
                if quell_biogenic_co2 or (quell_biogenic_co2 is None and self.quell_biogenic_co2):
                    if any([self.is_biogenic(term) for term in _biogenics]):
                        if any([comp.is_subcompartment_of(x) for x in (self.c_mgr.emissions, self._comp_from_air)]):
                            return 0.0

        cfs = self._lookup_cfs(f_inds, comp, query_q_ind)

        # now to check reference quantity
        vals = []
        for cf in cfs:
            factor = cf[locale]
            cf_ref_q_ind = self._get_q_ind(cf.flow.reference_entity)
            if cf_ref_q_ind != ref_q_ind:
                print('reference quantities don\'t match: cf:%s, ref:%s' % (self._q.name(cf_ref_q_ind),
                                                                            self._q.name(ref_q_ind)))
                if flow is not None:
                    ref_conversion = flow.cf(self.get_quantity(cf_ref_q_ind))
                    if ref_conversion == 0:
                        print('No conversion to %d.. bailing' % cf_ref_q_ind)
                        continue
                    factor *= ref_conversion

                else:
                    raise ConversionReferenceMismatch('[%d] vs [%d]%s' % (ref_q_ind, cf_ref_q_ind, cf))
            vals.append(factor)
        if len(vals) == 0:
            return None
        if len(set(vals)) > 1:
            print('Multiple CFs found: %s' % vals)
            print('Flow: %s [%s]' % (flow, flow.unit()))
            print('Quantity: %s' % query)
        return vals[0]

    def do_lcia(self, quantity, inventory, locale='GLO'):
        """
        takes a quantity and an exchanges generator; returns an LciaResult for the given quantity
        :param quantity:
        :param inventory: generates exchanges
        :param locale: ['GLO']
        :return: an LciaResult whose components are the flows of the exchanges
        """
        if isinstance(quantity, str):
            quantity = self.get_canonical_quantity(quantity)
        if not self.is_loaded(quantity):
            if quantity.is_entity:
                raise QuantityNotLoaded('%s is not a catalogRef' % quantity)
            else:
                for cf in quantity.factors():
                    self.add_cf(cf)
        q = self.get_quantity(quantity.link)
        q_ind = self._get_q_ind(q)
        r = LciaResult(q)
        for x in inventory:
            if not x.flow.has_characterization(q):
                factor = self.convert(flow=x.flow, query_q_ind=q_ind, locale=locale)
                if factor is not None:
                    x.flow.add_characterization(q, value=factor)
                else:
                    x.flow.add_characterization(q)
            if x.flow.cf(q) is not None:
                r.add_component(x.flow.external_ref, entity=x.flow)
                fac = x.flow.factor(q)
                fac.set_natural_direction(self.c_mgr)
                r.add_score(x.flow.external_ref, x, fac, locale)
        return r

    '''
    Quantity Interface
    '''
    def get_quantity(self, synonym):
        """
        return a quantity by its synonym
        :param synonym:
        :return:
        """
        return self._q.entity(synonym)

    def synonyms(self, item):
        """
        Return a list of synonyms for the object -- quantity, flowable, or compartment
        :param item:
        :return: list of strings
        """
        if self._f.index(item) is not None:
            for k in self._f.synonyms_for(item):
                yield k
        elif self._q.index(item) is not None:
            for k in self._q.synonyms_for(item):
                yield k
        else:
            comp = self.c_mgr.find_matching(item, interact=False)
            if comp is not None:
                for k in comp.synonyms:
                    yield k

    def flowables(self, quantity=None, compartment=None):
        """
        Return a list of flowable strings. Use quantity and compartment parameters to narrow the result
        set to those characterized by a specific quantity, those exchanged with a specific compartment, or both
        :param quantity:
        :param compartment: not implemented
        :return: list of pairs: CAS number, name
        """
        if quantity is not None:
            q_ind = self._q.index(quantity)
            for k in self._q_dict[q_ind]:
                yield self._f.cas(k), self._f.name(k)
        else:
            for k in range(len(self._f)):
                yield self._f.cas(k), self._f.name(k)

    def compartments(self, quantity=None, flowable=None):
        """
        Return a list of compartment strings. Use quantity and flowable parameters to narrow the result
        set to those characterized for a specific quantity, those with a specific flowable, or both
        :param quantity:
        :param flowable:
        :return: list of strings
        """
        pass

    def factors(self, quantity, flowable=None, compartment=None):
        """
        Return characterization factors for the given quantity, subject to optional flowable and compartment
        filter constraints. This is ill-defined because the reference unit is not explicitly reported in current
        serialization for characterizations (it is implicit in the flow)-- but it can be added to a web service layer.
        :param quantity:
        :param flowable:
        :param compartment:
        :return:
        """
        if flowable is not None:
            flowable = self._f.index(flowable)
        if compartment is not None:
            compartment = self.c_mgr.find_matching(compartment)
        q_ind = self._q.index(quantity)
        for f_ind in self._q_dict[q_ind]:
            if flowable is not None and flowable != f_ind:
                continue
            c_lookup = self._fq_dict[f_ind, q_ind]
            if compartment is not None:
                for cf in c_lookup[compartment]:
                    yield cf
            else:
                for cf in c_lookup.cfs():
                    yield cf

    def quantity_relation(self, ref_quantity, flowable, compartment, query_quantity, locale='GLO'):
        """
        Return a single number that converts the a unit of the reference quantity into the query quantity for the
        given flowable, compartment, and locale (default 'GLO').  If no locale is found, this would be a great place
        to run a spatial best-match algorithm.
        :param ref_quantity:
        :param flowable:
        :param compartment:
        :param query_quantity:
        :param locale:
        :return:
        """
        pass
