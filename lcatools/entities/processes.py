from __future__ import print_function, unicode_literals
from numbers import Number

import uuid

# from collections import namedtuple

from lcatools.entities.entities import LcEntity
from lcatools.interfaces import InventoryRequired
from lcatools.exchanges import (Exchange, ExchangeValue, DuplicateExchangeError, AmbiguousReferenceError,
                                NoReferenceFound)
from lcatools.lcia_results import LciaResult, LciaResults


class MissingAllocation(Exception):
    pass


class MultipleReferencesFound(Exception):
    """
    Whereas AmbiguousReferenceError indicates that further filtering is possible; MultipleReferencesFound indicates
    that there is no way to provide additional information.
    """
    pass


class NotAReference(Exception):
    pass


class ReferenceSettingFailed(Exception):
    pass


# a shorthand for storing operable reference exchanges in a process ref. maybe this will need to be a class, we'll see.
# on second thought, why not just use exchanges? The main reason is that the process ref....
# RxRef = namedtuple('RxRef', ['flow', 'direction', 'value'])
class RxRef(object):
    """
    A placeholder object to store reference exchange info for process_refs.  It can be modified to interoperate in
    places where exchanges are expected, e.g by having equivalent equality tests, hashes, etc., as needed.
    """
    def __init__(self, process_uuid, flow, direction):
        self._process_ref = None
        self._flow_ref = flow
        self._direction = direction
        # self._value = value
        # self._hash_tuple = (process.uuid, flow.external_ref, direction, None)
        self._hash = hash((process_uuid, flow.external_ref, direction, None))
        # self._is_alloc = process.is_allocated(self)

    @property
    def process(self):
        return self._process_ref

    @process.setter
    def process(self, value):
        if self._process_ref is None:
            self._process_ref = value
        else:
            raise AttributeError('Process ref already set!')

    @property
    def flow(self):
        return self._flow_ref

    @property
    def direction(self):
        return self._direction

    @property
    def value(self):
        # print('ACCESSING RxRef VALUE')  # need to be cautious about when / why this is used
        return self.process.reference_value(self.flow)

    @property
    def termination(self):
        return None

    @property
    def is_reference(self):
        return True

    @property
    def _value_string(self):
        try:
            return '%.3g' % self.process.reference_value(self.flow.external_ref)
        except InventoryRequired:
            return ' --- '

    @property
    def entity_type(self):
        return 'exchange'

    '''
    @property
    def is_alloc(self):
        return self._is_alloc
    '''

    @property
    def key(self):
        return self._hash

    @property
    def lkey(self):
        return self.flow.external_ref, self._direction, None

    @property
    def link(self):
        return '%s/reference/%s' % (self.process.link, self._flow_ref.external_ref)

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        if other is None:
            return False
        if not hasattr(other, 'entity_type'):
            return False
        if other.entity_type != 'exchange':
            return False
        return self.lkey == other.lkey

    def __str__(self):
        ref = '(*)'
        return '%6.6s: %s [%s %s] %s' % (self.direction, ref, self._value_string, self.flow.unit(), self.flow)


class LcProcess(LcEntity):

    _ref_field = 'referenceExchange'
    _new_fields = ['SpatialScope', 'TemporalScope', 'Classifications']

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
        self._alloc_by_quantity = None
        self._alloc_sum = 0.0

        if 'SpatialScope' not in self._d:
            self._d['SpatialScope'] = 'GLO'
        if 'TemporalScope' not in self._d:
            self._d['TemporalScope'] = '0'
        if 'Classifications' not in self._d:
            self._d['Classifications'] = []

    def _make_ref_ref(self, query):
        return [RxRef(self.uuid, x.flow.make_ref(query), x.direction) for x in self.references()]

    def __str__(self):
        return '%s [%s]' % (self._d['Name'], self._d['SpatialScope'])

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
        if self.alloc_qty is not None:
            self.allocate_by_quantity(self._alloc_by_quantity)

    @property
    def alloc_qty(self):
        if self._alloc_sum != 0:
            return self._alloc_by_quantity
        return None

    @property
    def alloc_total(self):
        return self._alloc_sum

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
            if e.flow.external_ref == term:
                hits[i] = e
            elif e.flow.get_uuid().startswith(term):
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

    def show_inventory(self, reference=None):
        """
        Convenience wrapper around self.inventory() which:
         * sorts the exchanges by reference, then by direction
         * prints the exchanges to output
         * provides an enumeration of exchanges for interactive access
         = returns the exchanges as a sorted list.
        :param reference:
        :return:
        """
        num = 0
        it = sorted(self.inventory(reference), key=lambda x: (not x.is_reference, x.direction))
        if reference is None:
            print('%s' % self)
        else:
            print('Reference: %s' % reference)
        for i in it:
            print('%2d %s' % (num, i))
            num += 1
        return it

    def _gen_exchanges(self, flow=None, direction=None):
        """
        Generate a list of exchanges matching the supplied flow and direction.
        :param flow:
        :param direction:
        :return:
        """
        for x in self._exchanges.values():
            if flow is not None:
                if x.flow != flow:
                    continue
            if direction is not None:
                if x.direction != direction:
                    continue
            yield x

    def get_exchange(self, key):
        return self._exchanges[key]

    def exchanges(self, flow=None, direction=None):
        for x in self._gen_exchanges(flow=flow, direction=direction):
            yield x.trim()

    def exchange_values(self, flow, direction=None):
        """
        Yield full exchanges matching flow specification.  Flow specification required.
        Will only yield multiple results if there are multiple terminations for the same flow.
        :param flow:
        :param direction:
        :return:
        """
        for x in self._gen_exchanges(flow=flow, direction=direction):
            yield x

    def has_exchange(self, flow, direction=None):
        try:
            next(self.exchange_values(flow, direction=direction))
        except StopIteration:
            return False
        return True

    def inventory(self, ref_flow=None, strict=False, direction=None):
        """
        generate a process's exchanges.  If no reference is supplied, generate unallocated exchanges, including all
        reference exchanges.  If a reference is supplied AND the process is allocated with respect to that reference,
        generate ExchangeValues as allocated to that reference flow, and exclude reference exchanges.  If a reference
        is supplied but the process is NOT allocated to that reference, generate unallocated ExchangeValues (excluding
        the reference itself).  Reference must be a flow or exchange found in the process's reference entity.

        :param ref_flow:
        :param strict: [False] whether to use strict flow name matching [default- first regex match]
        :param direction: could help with finding reference
        :return:
        """
        if ref_flow is not None:
            try:
                ref_flow = self.find_reference(ref_flow, strict=strict, direction=direction)
            except (NoReferenceFound, MultipleReferencesFound):  # unresolvable exceptions
                ref_flow = None
            except NotAReference:  # pedantic exception
                pass  # just use ref_flow as spec
        for i in self._exchanges.values():
            if ref_flow is None:
                yield i
            else:
                if i in self.reference_entity:
                    continue
                else:
                    # this pushes the problem up to ExchangeValue
                    yield ExchangeValue.from_allocated(i, ref_flow)

    def find_reference(self, spec=None, strict=False, direction=None):
        """
        returns an exchange matching the specification.
        If reference is None: returns a reference exchange ONLY IF the process has a single reference exchange;
         else AmbiguousReferenceError

        If reference is a flow,
         if the flow matches a reference exchange, returns the reference exchange
         elif the flow matches a non-reference exchange, return the non-reference exchange (possibly: gate this option
         to only occur if the process has an empty reference entity)
         else NoReferenceFound

        If the reference is an exchange,
         if the reference is not a reference exchange, what are you doing?? raises NotAReference

        If the reference is a string, it will check against the existing references' external_refs before falling back
         on a costly _find_reference_by_string() call

        :param spec: could be None, string (name or uuid), flow, or exchange
        :param strict:
        :param direction: could be helpful if the object is a non-reference exchange
        :return:
        """
        if spec is None:
            if len(self.reference_entity) > 1:
                raise AmbiguousReferenceError('Must specify reference!')
            try:
                ref = next(x for x in self.reference_entity)
            except StopIteration:
                raise NoReferenceFound('Process does not have a reference!')
        elif hasattr(spec, 'entity_type'):
            if spec.entity_type == 'flow':
                try:
                    ref = next(rf for rf in self.reference_entity if rf.flow == spec)
                except StopIteration:
                    candidates = [x for x in self.exchanges(spec, direction=direction)]
                    t = len(candidates)
                    if t == 0:
                        raise NoReferenceFound('Flow %s not encountered' % spec)
                    if len(candidates) > 1:
                        candidates = [c for c in filter(lambda x: x.termination is None, iter(candidates))]
                    if len(candidates) > 1:
                        raise AmbiguousReferenceError(
                            '%d un-terminated exchanges found; try specifying direction' % len(candidates))
                    elif len(candidates) == 0:
                        raise MultipleReferencesFound('%d non-ref exchanges found; all terminated' % t)
                    ref = candidates[0]
            elif spec.entity_type == 'exchange':
                if spec in self.reference_entity:
                    ref = spec
                else:
                    raise NotAReference('Exchange is not a reference exchange %s' % spec)
            else:
                ref = None
        else:
            try:
                ref = next(rf for rf in self.reference_entity if rf.flow.external_ref == spec)
            except StopIteration:
                ref = self._find_reference_by_string(spec, strict=strict)
        return self._exchanges[ref.key]

    def add_reference(self, flow, dirn):
        rx = Exchange(self, flow, dirn)
        self._set_reference(rx)
        return self._exchanges[rx.key]

    def remove_reference(self, flow, dirn):
        reference = Exchange(self, flow, dirn)
        self._exchanges[reference.key].unset_ref(self)
        self.remove_allocation(reference)
        if reference in self.reference_entity:
            self.reference_entity.remove(reference)
        if self._alloc_by_quantity is not None:
            self.allocate_by_quantity(self._alloc_by_quantity)

    def references(self, flow=None):
        for rf in self.reference_entity:
            if flow is None:
                yield self._exchanges[rf.key]
            else:
                if rf.flow == flow or rf.flow.match(flow):
                    yield self._exchanges[rf.key]

    def reference(self, flow=None):
        return self.find_reference(flow)

    ''' # don't think I want this
    def reference_value(self, flow=None):
        return self.get_exchange(self.find_reference(flow).key).value
    '''

    def has_reference(self, flow=None):
        try:
            self.find_reference(flow)
            return True
        except NoReferenceFound:
            return False

    def allocate_by_quantity(self, quantity):
        """
        Store a quantity for partitioning allocation.  All non-reference exchanges will have their exchange values
        computed based on the total, determined by the quantity specified.  For each
        reference exchange, computes the magnitude of the quantity output from the unallocated process. Reference flows
        lacking characterization in that quantity will receive zero allocation.

        Each magnitude is the allocation numerator for that reference, and the sum of the magnitudes is the allocation
        denominator.
        :param quantity: an LcQuantity (or None to remove quantity allocation)
        :return:
        """
        if quantity is None:
            self._alloc_by_quantity = None
            self._alloc_sum = 0.0
            self._d.pop('AllocatedByQuantity', None)
            return

        exchs = dict()
        mags = dict()
        for rf in self.reference_entity:
            rfx = self._exchanges[rf.key]
            if rfx.value is None:
                continue
            exchs[rf.flow] = rfx.value
            mags[rf.flow] = exchs[rf.flow] * rf.flow.cf(quantity)

        total = sum([v for v in mags.values()])
        if total == 0:
            return

        self._alloc_by_quantity = quantity
        self._alloc_sum = total
        self['AllocatedByQuantity'] = quantity

    def is_allocated(self, reference, strict=False):
        """
        Tests whether a process's exchanges contain allocation factors for a given reference.
        :param reference:
        :param strict: [False] if True, raise an exception if some (but not all) exchanges are missing allocations.
        :return: True - allocations exist; False - no allocations exist; raise MissingFactor - some allocations exist
        """
        try:
            reference = self.find_reference(reference)
        except NoReferenceFound:
            print('Not a reference exchange.')
            return False
        if self.alloc_qty is not None:
            if reference.flow.cf(self.alloc_qty) == 0:
                return False
            return True
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
        _x = hash((self.uuid, flow.external_ref, dirn, termination))
        if _x in self._exchanges:
            if value is None or value == 0:
                return None
            e = self._exchanges[_x]
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
            if isinstance(value, Number) or value is None:
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

    def lcia(self, quantity, ref_flow=None):
        if not quantity.is_entity:
            # only works for quantity refs-- in other words, always works
            return quantity.do_lcia(self.inventory(ref_flow=ref_flow), locale=self['SpatialScope'])
        else:
            result = LciaResult(quantity)
            result.add_component(self.get_uuid(), entity=self)
            for ex in self.inventory(ref_flow):
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
            j.pop('allocationFactors', None)  # added just for OpenLCA JSON-LD, but could be generalized
        return j
