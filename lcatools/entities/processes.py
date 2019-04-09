from __future__ import print_function, unicode_literals
from numbers import Number

import uuid

from collections import defaultdict

from ..interfaces import InventoryRequired

from lcatools.entities.entities import LcEntity
from lcatools.exchanges import Exchange, ExchangeValue, DuplicateExchangeError, AmbiguousReferenceError


class MissingAllocation(Exception):
    pass


class NoExchangeFound(Exception):
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
    def __init__(self, process_ref, flow, direction, comment=None):
        self._process_ref = None
        self._flow_ref = flow
        self._direction = direction
        # self._hash_tuple = (process.uuid, flow.external_ref, direction, None)
        self._hash = hash((process_ref, flow.external_ref, direction, None))
        self._comment = comment
        # self._is_alloc = process.is_allocated(self)
        self._cached_value = None

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
    def unit(self):
        return self._flow_ref.unit()

    @property
    def comment(self):
        if self._comment is None:
            return ''
        return self._comment

    @property
    def value(self):
        '''
        # print('ACCESSING RxRef VALUE')  # need to be cautious about when / why this is used
        if self._cached_value is None:
            try:
                self._cached_value = self.process.reference_value(self.flow.external_ref)
            except InventoryRequired:
                return None
        return self._cached_value
        '''
        raise InventoryRequired("Let's go back to being cautious about this")

    @property
    def termination(self):
        return None

    @property
    def is_reference(self):
        return True

    @property
    def _value_string(self):
        if self._cached_value is not None:
            return '%.3g' % self._cached_value
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
        u = uuid.uuid4()
        return cls(str(u), entity_uuid=u, Name=name, **kwargs)

    def __init__(self, external_ref, **kwargs):
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
        self._exch_map = defaultdict(set)  # maps flow external_ref to exchanges having that flow

        super(LcProcess, self).__init__('process', external_ref, **kwargs)
        if self.reference_entity is not None:
            raise AttributeError('How could the reference entity not be None?')
        self._reference_entity = set()  # it is not possible to specify a valid reference_entity on init
        self._alloc_by_quantity = None
        self._alloc_sum = 0.0

        if 'SpatialScope' not in self._d:
            self._d['SpatialScope'] = 'GLO'
        if 'TemporalScope' not in self._d:
            self._d['TemporalScope'] = '0'
        if 'Classifications' not in self._d:
            self._d['Classifications'] = []

    @property
    def name(self):
        return self['Name']

    def _make_ref_ref(self, query):
        return [RxRef(self.external_ref, x.flow.make_ref(query), x.direction, comment=x.comment)
                for x in self.references()]

    def __str__(self):
        return '%s [%s]' % (self._d['Name'], self._d['SpatialScope'])

    def __len__(self):
        return len(self._exchanges)

    def _validate_reference(self, ref_set):
        for x in ref_set:
            if x.termination is not None:
                return False
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
                ref_entity.set_ref(self)
                self._reference_entity.add(ref_entity)
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

    def _gen_exchanges(self, flow=None, direction=None, reference=None):
        """
        Generate a list of exchanges matching the supplied flow and direction.

        If this is too slow, we could hash the process's exchanges by flow
        :param flow: either a flow entity, flow ref, or external_ref
        :param direction: [None] or filter by direction
        :param reference: [None] find any exchange; True: only reference exchanges; False: only non-reference exchanges
        :return:
        """
        if flow is None:
            if reference is True:
                _x_gen = (self._exchanges[x.key] for x in self.reference_entity)
            elif reference is False:
                _x_gen = (x for x in self._exchanges.values() if not x.is_reference)
            else:
                _x_gen = (x for x in self._exchanges.values())
        else:
            if hasattr(flow, 'entity_type'):
                if flow.entity_type != 'flow':
                    raise TypeError('Flow argument must be a flow')
                flow = flow.external_ref
            if reference is True:
                _x_gen = (x for x in self._exch_map[flow] if x.is_reference)
            elif reference is False:
                _x_gen = (x for x in self._exch_map[flow] if not x.is_reference)
            else:
                _x_gen = (x for x in self._exch_map[flow])
        for x in _x_gen:
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

    def inventory(self, ref_flow=None, direction=None):
        """
        generate a process's exchanges.  If no reference is supplied, generate unallocated exchanges, including all
        reference exchanges.  If a reference is supplied AND the process is allocated with respect to that reference,
        generate ExchangeValues as allocated to that reference flow, and exclude reference exchanges.  If a reference
        is supplied but the process is NOT allocated to that reference, generate unallocated ExchangeValues (excluding
        the reference itself).  Reference must be a flow or exchange found in the process's reference entity.

        :param ref_flow:
        :param direction: could help with finding reference
        :return:
        """
        if ref_flow is None:
            ref_exch = None
        else:
            try:
                ref_exch = self.find_exchange(ref_flow, direction=direction)
            except MultipleReferencesFound:
                ref_exch = self.find_exchange(ref_flow, direction=direction, reference=True)
        for i in self._exchanges.values():
            if ref_exch is None:
                # generate unallocated exchanges
                yield i
            elif ref_exch.is_reference:
                # generate allocated, normalized, non-reference exchanges
                if i in self.reference_entity:
                    continue
                else:
                    yield ExchangeValue.from_allocated(i, ref_exch)
            else:
                # generate un-allocated, normalized, non-query exchanges
                if i is ref_exch:
                    continue
                else:
                    yield ExchangeValue.from_allocated(i, ref_exch)

    def find_exchange(self, spec=None, reference=None, direction=None):
        """
        returns an exchange matching the specification.

        If multiple results are found, filters out terminated exchanges

        :param spec: could be None, external_ref, flow, flow ref, or exchange
        :param reference: [None] find any exchange; True: only find references; False: only non-references
        :param direction: could be helpful if the object is a non-reference exchange
        :return:
        """
        if hasattr(spec, 'entity_type'):
            if spec.entity_type == 'exchange':
                if direction is None:
                    direction = spec.direction
                _x_gen = self._gen_exchanges(flow=spec.flow.external_ref, direction=direction, reference=reference)
            elif spec.entity_type == 'flow':
                _x_gen = self._gen_exchanges(flow=spec.external_ref, direction=direction, reference=reference)
            else:
                raise TypeError('Cannot interpret specification %s (%s)' % (spec, type(spec)))
        else:  # works for spec=external_ref or spec is None
            _x_gen = self._gen_exchanges(spec, direction=direction, reference=reference)

        candidates = [x for x in _x_gen]

        if len(candidates) == 0:
            raise NoExchangeFound
        elif len(candidates) > 1:
            nonterms = [x for x in candidates if x.termination is None]
            if len(nonterms) > 1:
                raise MultipleReferencesFound(
                    '%d un-terminated exchanges found; try specifying direction' % len(nonterms))
            elif len(nonterms) == 0:
                raise AmbiguousReferenceError(
                    '%d exchanges found, all terminated. Not supported brah.' % len(candidates))
            else:
                return nonterms[0]
        else:
            return candidates[0]

    def _strip_term(self, flow, dirn):
        """
        Removes an existing terminated exchange and replaces it with an unterminated one
        """
        exs = [k for k in self._gen_exchanges(flow, dirn)]
        if len(exs) > 1:
            raise DuplicateExchangeError('%d exchanges found for %s: %s' % (len(exs), dirn, flow))
        elif len(exs) == 0:
            raise NoExchangeFound
        ex = exs[0]
        if ex.termination is None:
            return

        old = self._exchanges.pop(ex.key)
        new = old.reterminate(None)
        self._exchanges[new.key] = new
        self._exch_map[new.flow.external_ref].remove(old)
        self._exch_map[new.flow.external_ref].add(new)

    def set_reference(self, flow, dirn):
        """
        Exchange must already exist. If the exchange is currently terminated, the termination is removed.
        :param flow:
        :param dirn:
        :return:
        """
        self._strip_term(flow, dirn)
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
        return self.find_exchange(flow, reference=True)

    ''' # don't think I want this
    def reference_value(self, flow=None):
        return self.get_exchange(self.find_reference(flow).key).value
    '''

    def has_reference(self, flow=None):
        try:
            self.find_exchange(flow, reference=True)
            return True
        except NoExchangeFound:
            return False

    def _alloc_dict(self, quantity=None):
        """
        Returns a dict mapping reference key to NON-NORMALIZED allocation factor.  This factor reports the amount of
        the reference flow, in dimensions of the specified quantity, that is produced by a UNIT activity of the process.
        Normalized allocation factors are obtained by dividing by the sum of these amounts.
        :param quantity:
        :return:
        """
        if quantity is None:
            if self._alloc_by_quantity is None:
                raise ValueError('An allocation quantity is required to compute normalized allocation factors')
            quantity = self._alloc_by_quantity

        return {rf: self._exchanges[rf.key].value * quantity.cf(rf.flow)
                for rf in self.reference_entity
                if self._exchanges[rf.key].value is not None}

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

        mags = self._alloc_dict(quantity)

        total = sum([v for v in mags.values()])
        if total == 0:
            print('zero total found; not setting allocation by qty %s' % quantity)
            return

        self._alloc_by_quantity = quantity
        self._alloc_sum = total
        self['AllocatedByQuantity'] = quantity

    def allocation_factors(self, quantity=None):
        """
        Returns a dict mapping reference exchange to that reference's allocation factor according to the specified
        allocation quantity.
        If no quantity is specified, the current allocation quantity is used.  DOES NOT AFFECT CURRENT ALLOCATION.
        :param quantity: allocation quantity
        :return:
        """
        d = self._alloc_dict(quantity)
        s = sum(d.values())
        return {k: v / s for k, v in d.items()}

    def is_allocated(self, reference, strict=False):
        """
        Tests whether a process's exchanges contain allocation factors for a given reference.
        :param reference:
        :param strict: [False] if True, raise an exception if some (but not all) exchanges are missing allocations.
        :return: True - allocations exist; False - no allocations exist; raise MissingFactor - some allocations exist
        """
        try:
            reference = self.find_exchange(reference, reference=True)
        except NoExchangeFound:
            print('Not a reference exchange.')
            return False
        if self.alloc_qty is not None:
            if self.alloc_qty.cf(reference.flow) == 0:
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
                print('in process %s [%s]\nReference: %s' % (self['Name'], self.external_ref,
                                                             reference.flow.external_ref))
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
        :param termination: None for reference or cutoff flows; a context for elementary flows; a valid external_ref
         for terminated intermediate flows.
        :param add_dups: (False) set to true to handle "duplicate exchange" errors by cumulating their values
        :return:
        """
        _x = hash((self.external_ref, flow.external_ref, dirn, termination))
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
                            print('Duplicate exchange in process %s:\n%s' % (self.external_ref, e))
                            raise
                return e

            else:
                try:
                    e[reference] = value  # this will catch already-set errors
                except DuplicateExchangeError:
                    if add_dups:
                        e.add_to_value(value, reference=reference)
                    else:
                        print('Duplicate exchange in process %s:\n%s' % (self.external_ref, e))
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

            # This is the only point an exchange (must be ExchangeValue) is added to the process (see also _strip_term)
            self._exchanges[e.key] = e
            self._exch_map[e.flow.external_ref].add(e)
            return e

    '''
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
    '''

    def merge(self, other):
        raise NotImplemented('This should be done via fragment construction + aggregation')

    def serialize(self, exchanges=False, domesticate=False, drop_fields=(), **kwargs):
        j = super(LcProcess, self).serialize(domesticate=domesticate, drop_fields=drop_fields)
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
