from __future__ import print_function, unicode_literals

import uuid

from collections import namedtuple

from lcatools.entities.entities import LcEntity
from lcatools.entities.flows import LcFlow
from lcatools.exchanges import Exchange, ExchangeValue, DuplicateExchangeError, AmbiguousReferenceError
from lcatools.lcia_results import LciaResult, LciaResults


class MissingAllocation(Exception):
    pass


class NoReferenceFound(Exception):
    pass


class MultipleReferencesFound(Exception):
    pass


class ReferenceSettingFailed(Exception):
    pass


# a shorthand for storing operable reference exchanges in a process ref. maybe this will need to be a class, we'll see.
RxRef = namedtuple('RxRef', ['flow', 'direction'])


class LcProcess(LcEntity):

    _ref_field = 'referenceExchange'
    _new_fields = ['SpatialScope', 'TemporalScope']

    @classmethod
    def new(cls, name, **kwargs):
        """
        :param name: the name of the process
        :return:
        """
        return cls(uuid.uuid4(), Name=name, **kwargs)

    def __init__(self, entity_uuid, **kwargs):
        """
        THe process's data is a set of exchanges.

        A process's reference entity is a subset of these.  It is an error for these exchanges to have terminations
        (if they're terminated, they're not reference flows- they're dependencies). These references can be used
        as allocation keys for the exchanges.

        The entities in reference_entity and _exchanges are not necessarily the same, although they should hash the
        same.  Not sure whether this is a design flaw or not- but the important thing is that reference entities do
        not need to have exchange values associated with them (although they could).

        process.find_reference(key), references() [generator], and reference(flow) all return entries from _exchanges,
        not entries from reference_entity.  The only public interface to the objects in reference_entity is
        reference_entity itself.
        :param entity_uuid:
        :param kwargs:
        """
        self._exchanges = dict()  # maps exchange key to exchange
        super(LcProcess, self).__init__('process', entity_uuid, **kwargs)
        if self.reference_entity is not None:
            raise AttributeError('How could the reference entity not be None?')
        self.reference_entity = set()  # it is not possible to specify a valid reference_entity on init

        if 'SpatialScope' not in self._d:
            self._d['SpatialScope'] = 'GLO'
        if 'TemporalScope' not in self._d:
            self._d['TemporalScope'] = '0'

    def _make_ref_ref(self, query):
        return [RxRef(x.flow.make_ref(query), x.direction) for x in self.reference_entity]

    def __str__(self):
        return '%s [%s]' % (self._d['Name'], self._d['SpatialScope'])

    def add_scenario(self, scenario, reference):
        """
        Here's how scenarios work:
        a scenario acts equivalently to an alternative reference flow in an allocation exchange.
        When a user wants to define a new scenario parameter- first the process needs to know it is parameterized
        within that scenario- and what reference to use for un-parameterized exchanges. that's what add_scenario is for.
        Then, the user can parameterize a specific exchange-> the exchange gets upgraded to an AllocatedExchange
        if it is not already so, the scenario id is used as the reference flow, and the param value is installed
        as the exchange value.
        When a user makes an exchange query, they specify a reference flow (or no reference).  If the reference flow
        shows up in the list of scenarios, then the internal exchange answering mechanism switches gears:
         - if the scenario is a reference in the exchange, it uses that;
         - otherwise it uses the reference pointed to by the process's scenarios dict (set here)

        implication is that an allocated exchange should be earlier in inheritance than an exchange value- but the
        current workaround is satisfactory for ff params and pf params excluding dissipation (dissipation
        would inherit from exchangevalue). LCIA params (as
        previously observed) should / could be new quantities- flow property params in general need a reworking
         of the current [location] spec- which is the same as the param interface- but not today.

         reference: fragment traversal- TODO
        :param scenario:
        :param reference:
        :return:
        """
        if reference in self.reference_entity:
            self._scenarios[scenario] = reference

    def _validate_reference(self, ref_set):
        for x in ref_set:
            if not super(LcProcess, self)._validate_reference(x):
                return False
        return True

    def _print_ref_field(self):
        return 'see exchanges'

    def _set_reference(self, ref_entity):
        """
        is it a problem that there's no way to un-set reference exchanges? my feeling is no, at least at present.
        :param ref_entity:
        :return:
        """
        self._validate_reference({ref_entity})

        if ref_entity.key in self._exchanges:
            if self._exchanges[ref_entity.key].set_ref(self):
                self.reference_entity.add(ref_entity)
            else:
                raise ReferenceSettingFailed('%s\n%s' % (self, ref_entity))

    def _find_reference_by_string(self, term, strict=False):
        """
        Select a reference based on a search term--- check against flow.  test get_uuid().startswith, or ['Name'].find()
        If multiple results found- if strict, return None; if strict=False, return first

        This method returns an item from the reference_entity, not an item from _exchanges - the extraction is done by
        find_reference()
        :param term:
        :param strict: [False] raise error if ambiguous search term; otherwise return first
        :return: the exchange entity
        """
        hits = [None] * len(self.reference_entity)
        for i, e in enumerate(self.reference_entity):
            if e.flow.get_uuid().startswith(term):
                hits[i] = e
            elif e.flow['Name'].lower().find(term.lower()) >= 0:
                hits[i] = e
        hits = list(filter(None, hits))
        if strict:
            if len(hits) > 1:
                raise MultipleReferencesFound('process:%s key: %s' % (self, term))
        if len(hits) == 0:
            raise NoReferenceFound('process:%s key: %s' % (self, term))
        return hits[0]

    def inventory(self, reference=None):
        """
        Convenience wrapper around self.exchanges() which:
         * sorts the exchanges by reference, then by direction
         * prints the exchanges to output
         * provides an enumeration of exchanges for interactive access
         = returns the exchanges as a sorted list.
        :param reference:
        :return:
        """
        num = 0
        it = sorted(self.exchanges(reference), key=lambda x: (not x.is_reference, x.direction))
        if reference is None:
            print('%s' % self)
        else:
            print('Reference: %s' % reference)
        for i in it:
            print('%2d %s' % (num, i))
            num += 1
        return it

    def exchange_values(self, flow, direction=None):
        """
        Generate a list of exchanges matching the supplied flow and direction. This will yield multiple exchanges
        only in the event that several different terminations exist for the same flow and direction.
        :param flow:
        :param direction:
        :return:
        """
        if isinstance(flow, LcFlow):
            for x in self._exchanges.values():
                if x.flow == flow:
                    if direction is None:
                        yield x
                    else:
                        if x.direction == direction:
                            yield x
        elif flow in self._scenarios:
            for x in self._exchanges.values():
                if x.flow == self._scenarios[flow]:
                    yield x
        else:
            raise TypeError('LcProcess.exchange input %s %s' % (flow, type(flow)))

    def has_exchange(self, flow, direction=None):
        try:
            next(self.exchange_values(flow, direction=direction))
        except StopIteration:
            return False
        return True

    def exchanges(self, reference=None, strict=False):
        """
        generate a process's exchanges.  If no reference is supplied, generate unallocated exchanges, including all
        reference exchanges.  If a reference is supplied AND the process is allocated with respect to that reference,
        generate ExchangeValues as allocated to that reference flow, and exclude reference exchanges.  If a reference
        is supplied but the process is NOT allocated to that reference, generate unallocated ExchangeValues (excluding
        the reference itself).  Reference must be a flow or exchange found in the process's reference entity.

        TESTING: if a reference is supplied but the process is NOT allocated to that reference,

        :param reference:
        :param strict: [False] whether to use strict flow name matching [default- first regex match]
        :return:
        """
        # chk_alloc = None
        if reference is not None:
            try:
                reference = self.find_reference(reference, strict=strict)
                if not self.is_allocated(reference):
                    return
            except NoReferenceFound:
                reference = None
        for i in self._exchanges.values():
            if reference is None:
                yield i
            # elif not chk_alloc:
            #     if i != reference:
            #         yield i
            else:
                if i in self.reference_entity:
                    continue
                else:
                    # this pushes the problem up to ExchangeValue
                    yield ExchangeValue.from_allocated(i, reference)

    def find_reference(self, reference=None, strict=False):
        """
        returns an exchange. NOTE: this has been refactored.
        :param reference: could be None, string (name or uuid), flow, or exchange
        :param strict:
        :return:
        """
        if reference is None:
            if len(self.reference_entity) > 1:
                raise NoReferenceFound('Must specify reference!')
            ref = next(x for x in self.reference_entity)
        elif isinstance(reference, str):
            ref = self._find_reference_by_string(reference, strict=strict)
        elif isinstance(reference, LcFlow):
            try:
                ref = next(rf for rf in self.reference_entity if rf.flow == reference)
            except StopIteration:
                raise NoReferenceFound('No reference exchange found with flow %s' % reference)
        elif isinstance(reference, Exchange):
            if reference in self.reference_entity:
                ref = reference
            else:
                raise NoReferenceFound('Exchange is not a reference exchange %s' % reference)
        else:
            raise NoReferenceFound('Unintelligible reference %s' % reference)
        return self._exchanges[ref.key]

    def add_reference(self, flow, dirn):
        rx = Exchange(self, flow, dirn)
        self._set_reference(rx)
        return self._exchanges[rx.key]

    def remove_reference(self, reference):
        self._exchanges[reference.key].unset_ref(self)
        self.remove_allocation(reference)
        if reference in self.reference_entity:
            self.reference_entity.remove(reference)

    def references(self, flow=None):
        for rf in self.reference_entity:
            if flow is None:
                yield self._exchanges[rf.key]
            else:
                if rf.flow == flow:
                    yield self._exchanges[rf.key]

    def reference(self, flow=None):
        if isinstance(flow, Exchange):
            flow = flow.flow
        ref = [rf for rf in self.references(flow)]
        if len(ref) == 0:
            raise NoReferenceFound
        elif len(ref) > 1:
            raise AmbiguousReferenceError('Multiple matching references found')
        return ref[0]

    def allocate_by_quantity(self, quantity):
        """
        Apply allocation factors to all non-reference exchanges, determined by the quantity specified.  For each
        reference exchange, computes the magnitude of the quantity output from the unallocated process. Reference flows
        lacking characterization in that quantity will receive zero allocation.

        Each magnitude is the allocation numerator for that reference, and the sum of the magnitudes is the allocation
        denominator.
        :param quantity: an LcQuantity
        :return:
        """
        exchs = dict()
        mags = dict()
        for rf in self.reference_entity:
            exchs[rf.flow] = self._exchanges[rf.key].value
            mags[rf.flow] = exchs[rf.flow] * rf.flow.cf(quantity)

        total = sum([v for v in mags.values()])

        for rf in self.references():
            alloc_factor = mags[rf.flow] / total  # sum of all allocated exchanges should equal unallocated value
            for x in self.exchanges():
                if x not in self.reference_entity:
                    x[rf] = x.value * alloc_factor
        self['AllocatedByQuantity'] = quantity

    def is_allocated(self, reference, strict=False):
        """
        Tests whether a process's exchanges contain allocation factors for a given reference.
        :param reference:
        :param strict: [False] if True, raise an exception if some (but not all) exchanges are missing allocations.
        :return: True - allocations exist; False - no allocations exist; raise MissingFactor - some allocations exist
        """
        if reference is None:
            return False
        try:
            reference = self.find_reference(reference)
        except NoReferenceFound:
            print('Not a reference exchange.')
            return False
        missing_allocations = []
        has_allocation = []
        for x in self._exchanges.values():
            if x in self.reference_entity:
                continue
            if x.is_allocated(reference):
                has_allocation.append(x)
            else:
                missing_allocations.append(x)
            if not strict:
                if len(has_allocation) > 0:
                    return True  # for nonstrict, bail out as soon as any allocation is detected
        if len(has_allocation) * len(missing_allocations) == 0:
            if len(has_allocation) == 0:
                return False
            return True
        if strict:
            for x in missing_allocations:
                print('in process %s [%s]\nReference: %s' % (self['Name'], self.uuid, reference.flow.uuid))
                print('%s' % x)
                raise MissingAllocation('Missing allocation factors for above exchanges')

    def remove_allocation(self, reference):
        for x in self._exchanges.values():
            x.remove_allocation(reference)

    def add_exchange(self, flow, dirn, reference=None, value=None, termination=None, add_dups=False):
        """
        This is used to create Exchanges and ExchangeValues and AllocatedExchanges.

        If the flow+dir+term is already in the exchange set:
            if no reference is specified and/or no value is specified- nothing to do
            otherwise (if reference and value are specified):
                upgrade the exchange to an allocatedExchange and add the new reference exch val
        otherwise:
            if reference is specified, create an AllocatedExchange
            otherwise create an Exchange / ExchangeValue

        :param flow:
        :param dirn:
        :param reference:
        :param value:
        :param termination:
        :param add_dups: (False) set to true to handle "duplicate exchange" errors by cumulating their values
        :return:
        """
        _x = (self.external_ref, flow.external_ref, dirn, termination)
        if _x in self._exchanges:
            if value is None or value == 0:
                return None
            e = self._exchanges[_x]
            if not isinstance(e, ExchangeValue):
                # upgrade to ExchangeValue
                new = ExchangeValue(self, flow, dirn, termination=termination)
                if e.is_reference:
                    new.set_ref(self)
                e = new
                assert _x == e.key
                self._exchanges[e.key] = e
                # assert self._exchanges[_x] is self._exchanges[e]  # silly me, always skeptical of hashing
            if reference is None:
                if isinstance(value, dict):
                    e.update(value)
                else:
                    try:
                        e.value = value  # this will catch already-set errors
                    except DuplicateExchangeError:
                        if add_dups:
                            e.add_to_value(value)
                        else:
                            print('Duplicate exchange in process %s:\n%s' % (self.get_uuid(), e))
                            raise
                return e

            else:
                try:
                    e[reference] = value  # this will catch already-set errors
                except DuplicateExchangeError:
                    if add_dups:
                        e.add_to_value(value, reference=reference)
                    else:
                        print('Duplicate exchange in process %s:\n%s' % (self.get_uuid(), e))
                        raise
                except ValueError:
                    print('Error adding [%s] = %10.3g for exchange\n%s\nto process\n%s' % (
                        reference.flow.external_ref, value, e, self.external_ref))
                    raise

                return e

        else:
            if value is None or value == 0:
                e = Exchange(self, flow, dirn, termination=termination)
            elif isinstance(value, float):
                if reference is None:
                    e = ExchangeValue(self, flow, dirn, value=value, termination=termination)
                else:
                    if reference not in self.reference_entity:
                        raise KeyError('Specified reference is not registered with process: %s' % reference)
                    e = ExchangeValue(self, flow, dirn, value=None, termination=termination)
                    e[reference] = value

            elif isinstance(value, dict):
                e = ExchangeValue(self, flow, dirn, value_dict=value, termination=termination)
            else:
                raise TypeError('Unhandled value type %s' % type(value))
            # self._exchanges.add(e)
            self._exchanges[e.key] = e
            return e

    def lcias(self, quantities, **kwargs):
        results = LciaResults(entity=self)
        for q in quantities:
            results[q.get_uuid()] = self.lcia(q, **kwargs)
        return results

    def lcia(self, quantity, ref_flow=None, flowdb=None):
        result = LciaResult(quantity)
        result.add_component(self.get_uuid(), entity=self)
        for ex in self.exchanges(ref_flow, ):
            if not ex.flow.has_characterization(quantity):
                if flowdb is not None:
                    if quantity in flowdb.known_quantities():
                        factor = flowdb.lookup_single_cf(ex.flow, quantity, self['SpatialScope'])
                        if factor is None:
                            ex.flow.add_characterization(quantity)
                        else:
                            ex.flow.add_characterization(factor)
            factor = ex.flow.factor(quantity)
            result.add_score(self.get_uuid(), ex, factor, self['SpatialScope'])
        return result

    def merge(self, other):
        raise NotImplemented('This should be done via fragment construction + aggregation')

    def serialize(self, exchanges=False, **kwargs):
        j = super(LcProcess, self).serialize()
        j.pop(self._ref_field)  # reference reported in exchanges
        if exchanges:
            # if exchanges is true, report all exchanges
            j['exchanges'] = sorted([x.serialize(**kwargs) for x in self._exchanges.values()],
                                    key=lambda x: (x['direction'], x['flow']))
        else:
            # if exchanges is false, only report reference exchanges
            j['exchanges'] = sorted([x.serialize(**kwargs) for x in self._exchanges.values()
                                     if x in self.reference_entity],
                                    key=lambda x: (x['direction'], x['flow']))
        return j
