from __future__ import print_function, unicode_literals

import uuid
from itertools import chain

from lcatools.exchanges import Exchange, ExchangeValue, AllocatedExchange, DuplicateExchangeError
from lcatools.characterizations import Characterization


def concatenate(*lists):
    return chain(*lists)

entity_types = ('process', 'flow', 'quantity')


class OriginExists(Exception):
    pass


class LcEntity(object):
    """
    All LC entities behave like dicts, but they all have some common properties, defined here.
    """
    _pre_fields = ['EntityType', 'Name']
    _new_fields = []
    _ref_field = ''
    _post_fields = ['Comment']

    def __init__(self, entity_type, entity_uuid, **kwargs):

        if isinstance(entity_uuid, uuid.UUID):
            self._uuid = entity_uuid
        else:
            self._uuid = uuid.UUID(entity_uuid)
        self._d = dict()

        self.entity_type = entity_type
        self.reference_entity = None
        self._origin = None

        self._d['Name'] = ''
        self._d['Comment'] = ''

        self._external_ref = None

        for k, v in kwargs.items():
            self[k] = v

    @property
    def origin(self):
        return self._origin

    @origin.setter
    def origin(self, value):
        if self._origin is None:
            self._origin = value
        else:
            raise OriginExists('Origin already set to %s' % self._origin)

    @classmethod
    def signature_fields(cls):
        return concatenate(cls._pre_fields, cls._new_fields,
                           [cls._ref_field] if cls._ref_field is not [] else [], cls._post_fields)

    def set_external_ref(self, ref):
        """
        Specify how the entity is referred to in the source dataset. If this is unset, the UUID is assumed
        to be used externally.
        :param ref:
        :return:
        """
        self._external_ref = ref

    def get_external_ref(self):
        return '%s' % self._uuid if self._external_ref is None else self._external_ref

    def get_signature(self):
        k = dict()
        for i in self.signature_fields():
            k[i] = self[i]
        return k

    def get_uuid(self):
        return str(self._uuid)

    def _validate_reference(self, ref_entity):
        if ref_entity is None:
            # raise ValueError('Null reference')
            return False  # allow none references
        if ref_entity.entity_type != entity_refs[self.entity_type]:
            raise TypeError("Type Mismatch on reference entity")
        return True

    def _set_reference(self, ref_entity):
        """
        set the entity's reference value.  Can be overridden
        :param ref_entity:
        :return:
        """
        self._validate_reference(ref_entity)
        self.reference_entity = ref_entity

    def has_property(self, prop):
        return prop in self._d

    def properties(self):
        return [i for i in self._d.keys() if i not in self.signature_fields()]

    def get_properties(self):
        """
        dict of properties and values for a given entity
        :return:
        """
        d = dict()
        for i in self.properties():
            d[i] = self._d[i]
        return d

    def update(self, d):
        self._d.update(d)

    def validate(self):
        valid = True
        if self.entity_type not in entity_types:
            print('Entity type %s not valid!' % self.entity_type)
            valid = False
        if self.reference_entity is not None:
            try:
                self._validate_reference(self.reference_entity)
            except TypeError:
                print("Reference entity type %s is wrong for %s (%s)" %
                      (self.reference_entity.entity_type,
                       self.entity_type,
                       entity_types[self.entity_type]))
                valid = False
        for i in self.signature_fields():
            try:
                self[i]
            except KeyError:
                print("Required field %s does not exist" % i)
                valid = False
        return valid

    def _print_ref_field(self):
        if self.reference_entity is None:
            return '%s' % None
        else:
            return '%s' % self.reference_entity.get_external_ref()

    def serialize(self):
        j = {
            'entityId': self.get_uuid(),
            'entityType': self.entity_type,
            'externalId': self.get_external_ref(),
            'origin': self.origin,
            self._ref_field: self._print_ref_field(),
        }
        j.update(self._d)
        return j

    def __getitem__(self, item):
        if item.lower() == self._ref_field.lower():
            return self.reference_entity
        elif item == 'EntityType':
            return self.entity_type
        else:
            # don't catch KeyErrors here-- leave that to subclasses
            return self._d[item]

    def __setitem__(self, key, value):
        if key == 'EntityType':
            raise ValueError('Entity Type cannot be changed')
        elif key.lower() == self._ref_field.lower():
            self._set_reference(value)
        elif key.lower() in ('entityid', 'entitytype', 'externalid', 'origin'):
            raise KeyError('Disallowed Keyname %s' % key)
        else:
            self._d[key] = value

    def keys(self):
        return self._d.keys()

    def __str__(self):
        return 'LC %s: %s' % (self.entity_type, self._d['Name'])

    def __eq__(self, other):
        """
        two entities are equal if their types, origins, and external references are the same.
        internal refs do not need to be equal; reference entities do not need to be equal
        :return:
        """
        if other is None:
            return False
        return (self.get_external_ref() == other.get_external_ref() and
                self.origin == other.origin and
                self.entity_type == other.entity_type)


class LcProcess(LcEntity):

    _ref_field = 'referenceExchange'
    _new_fields = ['SpatialScope', 'TemporalScope']

    @classmethod
    def new(cls, name, ref_exchange, **kwargs):
        """
        :param name: the name of the process
        :param ref_exchange: the reference exchange
        :return:
        """
        return cls(uuid.uuid4(), Name=name, ReferenceExchange=ref_exchange, **kwargs)

    def __init__(self, entity_uuid, **kwargs):
        self._exchanges = set()
        super(LcProcess, self).__init__('process', entity_uuid, **kwargs)
        if self.reference_entity is None:
            self.reference_entity = set()
        else:
            self.reference_entity = {self.reference_entity}

        if 'SpatialScope' not in self._d:
            self._d['SpatialScope'] = 'GLO'
        if 'TemporalScope' not in self._d:
            self._d['TemporalScope'] = '0'

    def __str__(self):
        return '%s [%s]' % (self._d['Name'], self._d['SpatialScope'])

    def _validate_reference(self, ref_set):
        for x in ref_set:
            if super(LcProcess, self)._validate_reference(x):
                if isinstance(x, AllocatedExchange):
                    x._check_ref()
            else:
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

        self.reference_entity.add(ref_entity)

    def inventory(self):
        for i in self.exchanges():
            print('%s' % i)

    def exchanges(self):
        for i in sorted(self._exchanges, key=lambda x: x.direction):
            yield i

    def allocated_exchanges(self, reference):
        for i in sorted(self._exchanges, key=lambda x: x.direction):
            if isinstance(i, AllocatedExchange):
                yield ExchangeValue.from_allocated(i, reference)

    def add_reference(self, flow, dirn):
        rx = Exchange(self, flow, dirn)
        self._set_reference(rx)
        return rx

    def add_exchange(self, flow, dirn, reference=None, value=None, add_dups=False, **kwargs):
        """
        This is used to create Exchanges and ExchangeValues and AllocatedExchanges.

        If the flow+dirn is already in the exchange set:
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
        :param add_dups: (False) set to true to handle "duplicate exchange" errors by cumulating their values
        :return:
        """
        _x = Exchange(self, flow, dirn, **kwargs)
        if _x in self._exchanges:
            if value is None or value == 0:
                return None
            e = [x for x in self._exchanges if x == _x][0]
            if reference is None:
                if isinstance(e, AllocatedExchange):
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
                        exch = ExchangeValue.from_exchange(e, value=value)  # this will catch already-set errors
                        self._exchanges.remove(e)
                        self._exchanges.add(exch)
                        return exch
                    except DuplicateExchangeError:
                        if add_dups:
                            e.add_to_value(value)
                            return e
                        else:
                            print('Duplicate exchange in process %s:\n%s' % (self.get_uuid(), e))
                            raise

            else:
                exch = AllocatedExchange.from_exchange(e)
                if isinstance(value, dict):
                    exch.update(value)
                else:
                    try:
                        exch[reference] = value  # this will catch already-set errors
                    except DuplicateExchangeError:
                        if add_dups:
                            exch.add_to_value(value, reference=reference)
                        else:
                            print('Duplicate exchange in process %s:\n%s' % (self.get_uuid(), e))
                            raise

                self._exchanges.remove(e)
                self._exchanges.add(exch)
                return exch

        else:
            if value is None or value == 0:
                e = _x
            elif isinstance(value, float):
                if reference is None:
                    e = ExchangeValue(self, flow, dirn, value=value, **kwargs)
                else:
                    if reference not in self.reference_entity:
                        raise KeyError('Specified reference is not registered with process: %s' % reference)
                    e = AllocatedExchange(self, flow, dirn, value=value, **kwargs)
                    e[reference] = value

            elif isinstance(value, dict):
                e = AllocatedExchange.from_dict(self, flow, dirn, value=value, **kwargs)
            else:
                raise TypeError('Unhandled value type %s' % type(value))
            if e in self._exchanges:
                raise KeyError('Exchange already present')
            self._exchanges.add(e)
            return e

    def serialize(self, exchanges=False, **kwargs):
        j = super(LcProcess, self).serialize()
        j.pop(self._ref_field)  # reference reported in exchanges
        if exchanges:
            # if exchanges is true, report all exchanges
            j['exchanges'] = sorted([x.serialize(**kwargs) for x in self._exchanges],
                                    key=lambda x: (x['direction'], x['flow']))
        else:
            # if exchanges is false, only report reference exchanges
            j['exchanges'] = sorted([x.serialize(**kwargs) for x in self._exchanges
                                     if x in self.reference_entity],
                                    key=lambda x: (x['direction'], x['flow']))
        return j


class LcFlow(LcEntity):

    _ref_field = 'referenceQuantity'
    _new_fields = ['CasNumber', 'Compartment']

    @classmethod
    def new(cls, name, ref_qty, **kwargs):
        """
        :param name: the name of the flow
        :param ref_qty: the reference quantity
        :return:
        """
        return cls(uuid.uuid4(), Name=name, ReferenceQuantity=ref_qty, **kwargs)

    def __init__(self, entity_uuid, **kwargs):
        super(LcFlow, self).__init__('flow', entity_uuid, **kwargs)

        self._characterizations = dict()

        self._ref_quantity_factor = 1.0

        for k in self._new_fields:
            if k not in self._d:
                self._d[k] = ''

    def set_local_unit(self, factor):
        self._ref_quantity_factor = factor

    def __str__(self):
        cas = self._d['CasNumber']
        if cas is None:
            cas = ''
        if len(cas) > 0:
            cas = ' (CAS ' + cas + ')'
        comp = ', '.join((i for i in self._d['Compartment'] if i is not None))
        return '%s%s [%s]' % (self._d['Name'], cas, comp)

    def profile(self):
        for cf in self._characterizations:
            print('%s' % cf)

    def add_characterization(self, quantity, reference=False, value=None, **kwargs):
        if reference:
            self._set_reference(quantity)
            if value is None:
                value = 1.0
            self.set_local_unit(value)

        q = quantity.get_uuid()
        c = Characterization(self, quantity)
        if q in self._characterizations.keys():
            if value is None:
                return
            c = self._characterizations[q]
        else:
            self._characterizations[q] = c
        if value is not None:
            c.add_value(value=value, **kwargs)

    def has_characterization(self, quantity, location='GLO'):
        if quantity.get_uuid() in self._characterizations.keys():
            if location in self._characterizations[quantity.get_uuid()].locations():
                return True
        return False

    def characterizations(self):
        for i in self._characterizations.values():
            yield i

    def cf(self, quantity, location='GLO'):
        if quantity.get_uuid() in self._characterizations:
            try:
                return self._characterizations[quantity.get_uuid()][location]
            except KeyError:
                return self._characterizations[quantity.get_uuid()].value
        return 0.0

    def convert(self, val, to=None, fr=None, location='GLO'):
        """
        converts the value (in
        :param val:
        :param to: to quantity
        :param fr: from quantity
        :param location: cfs are localized to unrestricted strings babee
        the flow's reference quantity is used if either is unspecified
        :return: value * self.char(to)[loc] / self.char(fr)[loc]
        """
        out = self.cf(to or self.reference_entity, location=location)
        inn = self.cf(fr or self.reference_entity, location=location)
        return val * out / inn

    def serialize(self, characterizations=False, **kwargs):
        j = super(LcFlow, self).serialize()
        j.pop(self._ref_field)  # reference reported in characterizations
        if characterizations:
            j['characterizations'] = sorted([x.serialize(**kwargs) for x in self._characterizations.values()],
                                            key=lambda x: x['quantity'])
        else:
            j['characterizations'] = [x.serialize(**kwargs) for x in self._characterizations.values()
                                      if x.quantity == self.reference_entity]

        return j


class LcQuantity(LcEntity):

    _ref_field = 'referenceUnit'
    _new_fields = []

    @classmethod
    def new(cls, name, ref_unit, **kwargs):
        """
        :param name: the name of the quantity
        :param ref_unit: the string representation of the reference unit for the quantity
        :return:
        """
        return cls(uuid.uuid4(), Name=name, ReferenceUnit=ref_unit, **kwargs)

    def __init__(self, entity_uuid, **kwargs):
        super(LcQuantity, self).__init__('quantity', entity_uuid, **kwargs)

    def convert(self, from_unit, to=None):
        """
        Perform unit conversion within a quantity, using a 'UnitConversion' table stored in the object properties.
        For instance, if the quantity name was 'mass' and the reference unit was 'kg', then
        quantity.convert('lb') would[should] return 0.4536...
        quantity.convert('lb', to='ton') should return 0.0005

        This function requires that the quantity have a 'UnitConversion' property that works as a dict, with
        the unit names being keys. The requirement is that the values for every key all correspond to the same
        quantity.  For instance, if the quantity was mass, then the following would be equivalent:

        quantity['UnitConversion'] = { 'kg': 1, 'lb': 2.204622, 'ton': 0.0011023, 't': 0.001 }
        quantity['UnitConversion'] = { 'kg': 907.2, 'lb': 2000.0, 'ton': 1, 't': 0.9072 }

        If the quantity's reference unit is missing from the dict, it is assumed to be 1 implicitly.

        :param from_unit:
        :param to: unit to convert to (default is the reference unit)
        :return: a float indicating how many to_units there are in one from_unit
        """
        try:
            uc_table = self._d['UnitConversion']
        except KeyError:
            print('No unit conversion table found.')
            return None

        try:
            inbound = uc_table[from_unit]
        except KeyError:
            print('Inbound unit %s not found in unit conversion table.' % from_unit)
            return None

        if to is None:
            to = self.reference_entity

        try:
            outbound = uc_table[to]
        except KeyError:
            if to == self.reference_entity:
                outbound = 1.0
            else:
                print('Outbound unit %s not found in unit conversion table.' % to)
                return None

        return outbound / inbound


class LcUnit(object):
    """
    Dummy class to store a reference to a unit definition
    Design decision: even though ILCD unitgroups have assigned UUIDs, we are not maintaining unitgroups
    """
    entity_type = 'unit'

    def __init__(self, unitstring, unit_uuid=None):
        if unit_uuid is not None:
            self._uuid = uuid.UUID(unit_uuid)
        else:
            self._uuid = None
        self._unitstring = unitstring
        self._external_ref = None

    def set_external_ref(self, external_ref):
        self._external_ref = external_ref

    def get_external_ref(self):
        return '%s' % self._unitstring if self._external_ref is None else self._external_ref

    def unitstring(self):
        return self._unitstring

    def get_uuid(self):
        return self._unitstring  # needed for upward compat

    def __str__(self):
        return '[%s]' % self._unitstring


entity_refs = {
    'process': 'exchange',
    'flow': 'quantity',
    'quantity': 'unit'
}
