directions = ('Input', 'Output')


def comp_dir(direction):
    if direction in directions:
        return next(k for k in directions if k != direction)
    raise InvalidDirection('%s' % direction)


class InvalidDirection(Exception):
    pass


class DirectionlessExchangeError(Exception):
    pass


class DuplicateExchangeError(Exception):
    pass


class AmbiguousReferenceError(Exception):
    pass


class Exchange(object):
    """
    An exchange is an affiliation of a process, a flow, and a direction. An exchange does
    not include an exchange value- though presumably a valued exchange would be a subclass.

    An exchange may specify a uuid of a terminating process; tests for equality will distinguish
    differently-terminated flows. (ecoinvent)
    """

    entity_type = 'exchange'

    def __init__(self, process, flow, direction, unit=None, termination=None):
        """

        :param process:
        :param flow:
        :param direction:
        :param unit: default: flow's reference quantity unit
        :param termination: string id of terminating process or None
        :return:
        """
        # assert process.entity_type == 'process', "- we'll allow null exchanges and fragment-terminated exchanges"
        assert flow.entity_type == 'flow', "'flow' must be an LcFlow"
        assert direction in directions, "direction must be a string in (%s)" % ', '.join(directions)

        self.process = process
        self.flow = flow
        self.direction = direction
        try:
            self.unit = unit or flow.reference_entity.reference_entity
        except AttributeError:
            self.unit = None
        self.termination = None
        if termination is not None:
            self.termination = str(termination)
        self.value = None

    def __hash__(self):
        return hash((self.process.get_uuid(), self.flow.get_uuid(), self.direction, self.termination))

    def __eq__(self, other):
        if other is None:
            return False
        return (self.process.get_uuid() == other.process.get_uuid() and
                self.flow.get_uuid() == other.flow.get_uuid() and
                self.direction == other.direction and
                self.termination == other.termination)

    @property
    def comp_dir(self):
        return comp_dir(self.direction)

    @property
    def tflow(self):
        """
        indicates if an exchange is terminated with '(#)'
        :return:
        """
        if self.termination is not None:
            return str(self.flow) + ' (#)'
        return str(self.flow)

    def __str__(self):
        return '%s has %s: %s %s' % (self.process, self.direction, self.tflow, self.unit)

    def f_view(self):
        return '%s of %s' % (self.direction, self.process)

    def get_external_ref(self):
        return '%s: %s' % (self.direction, self.flow.get_uuid())

    def serialize(self, **kwargs):
        j = {
            'entityType': self.entity_type,
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
    def from_exchange(cls, exch, value=None, **kwargs):
        if isinstance(exch, ExchangeValue):
            if value is not None:
                raise DuplicateExchangeError('Exchange exists and has value %g (new value %g)' % (exch.value, value))
            return exch
        return cls(exch.process, exch.flow, exch.direction, value=value, **kwargs)

    @classmethod
    def from_allocated(cls, allocated, reference):
        return cls(allocated.process, allocated.flow, allocated.direction, value=allocated[reference],
                   termination=allocated.termination)

    @classmethod
    def from_scenario(cls, allocated, scenario, fallback):
        try:
            value = allocated[scenario]
        except KeyError:
            value = allocated[fallback]
        return cls(allocated.process, allocated.flow, allocated.direction, value=value,
                   termination=allocated.termination)

    def add_to_value(self, value):
        self.value += value

    def __init__(self, *args, value=None, **kwargs):
        super(ExchangeValue, self).__init__(*args, **kwargs)
        assert isinstance(value, float), 'ExchangeValues must be floats (or subclasses)'
        self.value = value

    def __str__(self):
        if self.process.entity_type == 'fragment':
            if self.process.reference_entity is None:
                ref = '{*}'
            else:
                ref = '   '
        else:
            if self.process.reference_entity is not None and self in self.process.reference_entity:
                ref = '{*}'
            else:
                ref = '   '
        return '%6.6s: %s [%.3g %s] %s' % (self.direction, ref, self.value, self.unit, self.tflow)

    def f_view(self):
        return '%6.6s: [%.3g %s] %s' % (self.direction, self.value, self.unit, self.process)

    def serialize(self, values=False):
        j = super(ExchangeValue, self).serialize()
        if values:
            j['value'] = float(self.value)
        return j


class DissipationExchange(ExchangeValue):
    """
    Composition / Dissipation mechanics can be encapsulated entirely into the exchange object- the problem
     is reduced to a serialization / deserialization problem.

    Composition / Dissipation probably conflicts with Allocation, i.e. it is not supported to add a dissipation
    factor to an AllocatedExchange.

    This is a problem because the allocated exchange is also the secret sauce being used to implement processflow
    parameters- and would presumably be the same mechanism to implement dissipation parameters.

    best solution would be to fold reference spec higher up in inheritance- also need an ironclad way to determine
     the input flow (or to not support dissipation for processes with more than one reference flow)
    and simply make flow_quantity not None be the key to interpret the exchange value as a dissipation rate.
    except that I don't want to squash the non-dissipation exchange value.

    Maybe I should not be worrying about this right now.

    """
    def __init__(self, *args, flow_quantity=None, scale=1.0, dissipation=1.0, value=None, **kwargs):
        self.flow_quantity = flow_quantity
        self.scale = scale
        self.dissipation = dissipation
        super(ExchangeValue, self).__init__(*args, **kwargs)
        self._value = value  # used only when dissipation is not defined

    def content(self, ref_flow=None):
        """
        :param ref_flow: a flow LcEntity
        :return:
        """
        if ref_flow is None:
            ref_flow = list(self.process.reference_entity)[0]
            if len(self.process.reference_entity) > 1:
                raise AmbiguousReferenceError
        if ref_flow.cf(self.flow_quantity) != 0:
            return ref_flow.cf(self.flow_quantity)
        return None

    @property
    def value(self):
        c = self.content()
        if c is not None:
            return c * self.scale * self.dissipation
        return self._value

    def __str__(self):
        raise NotImplemented

    def serialize(self, values=False):
        raise NotImplemented


class MarketExchange(Exchange):
    """
    A MarketExchange is an alternative implementation of an ExchangeValue that handles the multiple-input process
    case, i.e. when several processes produce the same product, and the database must balance them according to
    some apportionment of market value or production volume.

    The client code has to explicitly create a market exchange.  How does it know to do that? in the case of
    ecospold2, it has to determine whether the process has duplicate [non-zero] flows with activityLinkIds.

    In other cases, it will be foreground / linker code that does it.

    Add market suppliers using dictionary notation.  Use exchange values or production volumes, but do it consistently.
    The exchange value returned is always the individual supplier's value divided by the sum of values.
    """
    def __init__(self, *args, **kwargs):
        super(MarketExchange, self).__init__(*args, **kwargs)
        self._market_dict = dict()

    def _sum(self):
        return sum([v for k, v in self._market_dict.items()])

    def keys(self):
        return self._market_dict.keys()

    def markets(self):
        return self._market_dict.items()

    def __setitem__(self, key, value):
        if key in self._market_dict:
            raise KeyError('Key already exists with value %g (new value %g)' % (self._market_dict[key], value))
        self._market_dict[key] = value

    def __getitem__(self, item):
        return self._market_dict[item] / self._sum()

    def __str__(self):
        return 'Market for %s: %d suppliers, %g total volume' % (self.flow, len(self._market_dict), self._sum())

    def serialize(self, values=False):
        j = super(MarketExchange, self).serialize()
        if values:
            j['marketSuppliers'] = self._market_dict
        else:
            j['marketSuppliers'] = [k for k in self.keys()]
        return j


class AllocatedExchange(Exchange):
    """
    An AllocatedExchange is an alternative implementation of an ExchangeValue that handles the multiple-output
    process case.  Each allocatable output must be registered as part of the process's reference entity.
    The AllocatedExchange stores multiple exchange values, indexed via a dict of uuids for reference
    flows.  (It is assumed that no process features the same flow in both input and output directions AS REFERENCE
    FLOWS.)  An allocation factor can only be set for flows that are listed in the parent process's reference entity.

    A multi-output process (with AllocatedExchanges) and a multi-input process (with MarketExchanges) are mutually
    exclusive.

    If an AllocatedExchange's flow UUID is found in the value_dict, it is a reference exchange. In this case, it
    is an error if the exchange value for the reference flow is zero, or if the exchange value for any non-
    reference-flow is nonzero.  This can be checked internally without any knowledge by the parent process.

    Open question is how to serialize- whether to report only the un-allocated, only the allocated exchange values, or
    both.  Then an open task is to deserialize same.-- but that just requires serializing them as dicts
    """
    @classmethod
    def from_dict(cls, process, flow, direction, value=None, **kwargs):
        self = cls(process, flow, direction, **kwargs)
        self._value_dict.update(value)  # this will fail unless value was specified
        return self

    @classmethod
    def from_exchange(cls, exchange):
        if isinstance(exchange, AllocatedExchange):
            return exchange
        self = cls(exchange.process, exchange.flow, exchange.direction, termination=exchange.termination)
        self._value = exchange.value
        return self

    def __init__(self, process, flow, direction, value=None, **kwargs):
        self._ref_flow = flow.get_uuid()  # shortcut
        self._value = value
        self._value_dict = dict()
        super(AllocatedExchange, self).__init__(process, flow, direction, **kwargs)

    def _check_ref(self):
        for r, v in self._value_dict.items():
            if r == self._ref_flow and v == 0:
                print('r: %s ref: %s v: %d' % (r, self._ref_flow, v))
                raise ValueError('Reference exchange value cannot be zero')
            if r != self._ref_flow and v != 0:
                print('r: %s ref: %s v: %g' % (r, self._ref_flow, v))
                raise ValueError('Reference exchange value must be zero for non-reference exchanges')

    @property
    def value(self):
        """
        Get the exchange's "generic" value.
        If the exchange is a reference exchange, then this is the exchange's self-reference
        otherwise, if the exchange has only one entry, return it
        otherwise, return _value, whatever it be
        :return:
        """
        if self._ref_flow in self._value_dict:
            return self[self._ref_flow]
        if self._value is None:
            if len(self._value_dict) == 1:
                return [v for k, v in self._value_dict][0]
        return self._value

    @value.setter
    def value(self, exch_val):
        if exch_val is None:
            return
        if self._ref_flow in self._value_dict:
            raise DuplicateExchangeError('default value is already in dictionary! %g (new value %g)' % (
                self._value_dict[self._ref_flow], exch_val))
        self._value_dict[self._ref_flow] = exch_val
        self._value = exch_val

    def add_to_value(self, value, reference=None):
        if reference is None:
            self._value_dict[self._ref_flow] = self._value_dict[self._ref_flow] + value
            self._value = self._value_dict[self._ref_flow]
        else:
            reference = self._normalize_key(reference)
            self._value_dict[reference] = self._value_dict[reference] + value

    def keys(self):
        """
        This should be a subset of [f.get_uuid() for f in self.process.reference_entity()]
        :return:
        """
        return self._value_dict.keys()

    def values(self):
        """
        bad form to rename items() to values() ? here, values refers to exchange values- but that
        is probably weak tea to an irritated client code.
        :return:
        """
        return self._value_dict.items()

    def update(self, d):
        self._value_dict.update(d)

    @staticmethod
    def _normalize_key(key):
        if isinstance(key, Exchange):
            key = key.flow
        if hasattr(key, 'get_uuid'):
            key = key.get_uuid()
        return key

    def __getitem__(self, item):
        """
        If the key is known, return it
        otherwise, if the key is the exchange's reference flow, return generic _value
        otherwise, if the key is some other reference flow, return 0
        otherwise, KeyError
        :param item:
        :return:
        """
        k = self._normalize_key(item)
        if k in self._value_dict:
            return self._value_dict[k]
        if k == self._ref_flow:
            return self._value
        if k in [x.flow.get_uuid() for x in self.process.reference_entity]:
            return 0.0
        raise KeyError('Key %s is not identified as a reference exchange for the parent process' % k)

    def __setitem__(self, key, value):
        key = self._normalize_key(key)
        if key in self._value_dict:
            # print(self._value_dict)
            raise DuplicateExchangeError('Exchange value already defined for this reference!')
        if key not in [x.flow.get_uuid() for x in self.process.reference_entity]:
            raise KeyError('Cannot set allocation for a non-reference flow')
        if self._ref_flow in self._value_dict:  # reference exchange
            if key == self._ref_flow:
                if value == 0:
                    raise ValueError('Reference exchange cannot be zero')
            else:
                if value != 0:
                    raise ValueError('Allocation for non-reference exchange must be zero')
        self._value_dict[key] = value
        if key == self._ref_flow:
            self._check_ref()

    def __str__(self):
        if self.process.entity_type == 'fragment':
            if self.process.reference_entity is None:
                ref = '{*}'
            else:
                ref = '   '
        else:
            if self in self.process.reference_entity:
                ref = '{*}'
            else:
                ref = '   '
        return '%6.6s: %s [%.3g %s] %s' % (self.direction, ref, self.value, self.unit, self.tflow)

    def f_view(self):
        return '%6.6s: [%.3g %s] %s' % (self.direction, self.value, self.unit, self.process)

    def serialize(self, values=False):
        j = super(AllocatedExchange, self).serialize()
        if values:
            j['value'] = self._value_dict
        return j
