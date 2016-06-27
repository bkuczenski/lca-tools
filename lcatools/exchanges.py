directions = ('Input', 'Output')


class DirectionlessExchangeError(Exception):
    pass


class Exchange(object):
    """
    An exchange is an affiliation of a process, a flow, and a direction. An exchange does
    not include an exchange value- though presumably a valued exchange would be a subclass.

    An exchange may specify a quantity different from the flow's reference quantity; by default
    the reference quantity is used.
    """

    entity_type = 'exchange'

    def __init__(self, process, flow, direction, quantity=None):
        """

        :param process:
        :param flow:
        :param direction:
        :param quantity:
        :return:
        """
        assert process.entity_type == 'process', "'process' must be an LcProcess!"
        assert flow.entity_type == 'flow', "'flow' must be an LcFlow"
        assert direction in directions, "direction must be a string in (%s)" % ', '.join(directions)

        self.process = process
        self.flow = flow
        self.direction = direction
        if quantity is None:
            self.quantity = flow.reference_entity
        else:
            assert quantity.entity_type == 'quantity', "'quantity' must be an LcQuantity or None!"
            self.quantity = quantity
        self.value = None

    def __hash__(self):
        return hash((self.process.get_uuid(), self.flow.get_uuid(), self.direction))

    def __eq__(self, other):
        if other is None:
            return False
        return (self.process.get_uuid() == other.process.get_uuid() and
                self.flow.get_uuid() == other.flow.get_uuid() and
                self.direction == other.direction)

    def __str__(self):
        return '%s has %s: %s %s' % (self.process, self.direction, self.flow, self.quantity.reference_entity)

    def get_external_ref(self):
        return '%s: %s' % (self.direction, self.flow.get_uuid())

    def serialize(self, **kwargs):
        j = {
            'flow': self.flow.get_uuid(),
            'direction': self.direction,
        }
        if self in self.process.reference_entity:
            j['isReference'] = True
        return j

    @classmethod
    def signature_fields(cls):
        return ['process', 'flow', 'direction', 'quantity']


class ExchangeValue(Exchange):
    """
    An ExchangeValue is an exchange with a value
    """
    @classmethod
    def from_allocated(cls, allocated, reference):
        return cls(allocated.process, allocated.flow, allocated.direction, value=allocated[reference])

    def __init__(self, *args, value=None, **kwargs):
        super(ExchangeValue, self).__init__(*args, **kwargs)
        assert isinstance(value, float), 'ExchangeValues must be floats (or subclasses)'
        self.value = value

    def __str__(self):
        return '%6.6s: [%.3g %s] %s' % (self.direction, self.value, self.quantity.reference_entity, self.flow)

    def serialize(self, values=False):
        j = super(ExchangeValue, self).serialize()
        if values:
            j['value'] = float(self.value)
        return j


class AllocatedExchange(Exchange):
    """
    An AllocatedExchange is an alternative implementation of an ExchangeValue that behaves like an
    ordinary ExchangeValue, but also stores multiple exchange values, indexed via a dict of uuids for reference
    flows.  It is assumed that no process features the same flow in both input and output directions AS REFERENCE FLOWS.

    If an AllocatedExchange's flow UUID is found in the value_dict, it is a reference exchange.
     if it belongs to the reference_entity [set] of a process.  Reference
    exchanges
    uh,
    something

    Open question is how to serialize- whether to report only the un-allocated, only the allocated exchange values, or
    both.  Then an open task is to deserialize same.-- but that just requires serializing them as dicts
    """
    @classmethod
    def from_dict(cls, process, flow, direction, value=None):
        self = cls(process, flow, direction)
        self._value_dict.update(value)  # this will fail unless value was specified
        return self

    @classmethod
    def from_exchange(cls, exchange):
        self = cls(exchange.process, exchange.flow, exchange.direction)
        self.value = exchange.value
        return self

    def __init__(self, *args, value=None, **kwargs):
        super(AllocatedExchange, self).__init__(*args, **kwargs)
        self._value = value
        self._value_dict = dict()
        self._ref_flow = None

    def make_ref(self, ref=None):
        """
        denote this exchange to be a reference exchange for the named flow.
          = self.value will return self[ref].
          = self[ref] must be nonzero.
          = self[^ref] must be zero.
        This is checked at make_ref() and at subsequent sets.
         ? if self._value is not None, raise an error?
         - raise an error if _value_dict[ref] is zero
         - raise an error if _value_dict[^ref] is nonzero

        :param ref:
        :return:
        """
        ref = self._normalize_key(ref)
        if self._check_ref(ref):
            self._ref_flow = ref
            return True
        return False

    def _check_ref(self, ref):
        if self._value is not None:
            raise ValueError('Exch generic value is already set to %g (versus ref %s)' % (self._value, ref))
        for r, v in self._value_dict.items():
            if r == ref and v == 0:
                raise ValueError('Reference exchange value cannot be zero')
            if r != ref and v != 0:
                raise ValueError('Reference exchange value must be zero for non-reference exchanges')

    @property
    def value(self):
        if self._ref_flow is not None:
            return self[self._ref_flow]
        return self._value

    @value.setter
    def value(self, exch_val):
        if self._ref_flow is not None:
            raise AttributeError('Cannot set generic value for reference exchange')
        self._value = exch_val

    @staticmethod
    def _normalize_key(key):
        if isinstance(key, Exchange):
            key = key.flow
        if hasattr(key, 'get_uuid'):
            key = key.get_uuid()
        return key

    def __getitem__(self, item):
        return self._value_dict[self._normalize_key(item)]

    def __setitem__(self, key, value):
        key = self._normalize_key(key)
        if key not in [x.flow.get_uuid() for x in self.process.reference_entity]:
            raise KeyError('Cannot set allocation for a non-reference flow')
        if not isinstance(value, float):
            raise ValueError('Allocated exchange value must be float')
        if self._ref_flow is not None:
            if (self._ref_flow == key) ^ (value == 0):
                pass
            else:
                raise ValueError('Key is ref or value is nonzero')
        self._value_dict[key] = value

    def serialize(self, values=False):
        j = super(AllocatedExchange, self).serialize()
        if values:
            j['value'] = self._value_dict
        return j




