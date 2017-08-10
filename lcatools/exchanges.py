directions = ('Input', 'Output')


def comp_dir(direction):
    if direction in directions:
        return next(k for k in directions if k != direction)
    raise InvalidDirection('%s' % direction)


class ExchangeError(Exception):
    pass


class NoAllocation(Exception):
    pass


class InvalidDirection(Exception):
    pass


class DirectionlessExchangeError(Exception):
    pass


class DuplicateExchangeError(Exception):
    pass


class AmbiguousReferenceError(Exception):
    pass


class MissingReference(Exception):
    pass


class Exchange(object):
    """
    An exchange is an affiliation of a process, a flow, and a direction. An exchange does
    not include an exchange value- though presumably a valued exchange would be a subclass.

    An exchange may specify a uuid of a terminating process; tests for equality will distinguish
    differently-terminated flows. (ecoinvent)
    """

    entity_type = 'exchange'

    def __init__(self, process, flow, direction, termination=None):
        """

        :param process:
        :param flow:
        :param direction:
        :param termination: external id of terminating process or None (note: this is uuid for ecospold2)
        :return:
        """
        # assert process.entity_type == 'process', "- we'll allow null exchanges and fragment-terminated exchanges"
        assert flow.entity_type == 'flow', "'flow' must be an LcFlow"
        assert direction in directions, "direction must be a string in (%s)" % ', '.join(directions)

        self._process = process
        self._flow = flow
        self._direction = direction
        self._termination = None
        if termination is not None:
            self._termination = str(termination)
        self._hash = (process.external_ref, flow.external_ref, direction, self._termination)
        self._is_reference = False

    @property
    def is_reference(self):
        return self._is_reference

    def set_ref(self, setter):
        if setter is self._process:
            self._is_reference = True
            return True
        return False

    def unset_ref(self, setter):
        if setter is self._process:
            self._is_reference = False
            return True
        return False

    """
    These all need to be immutable because they form the exchange's hash
    """
    @property
    def unit(self):
        try:
            unit = self.flow.reference_entity.reference_entity
        except AttributeError:
            unit = None
        return unit

    @property
    def value(self):
        return None

    @value.setter
    def value(self, exch_val):
        raise ExchangeError('Cannot set Exchange value')

    @property
    def process(self):
        return self._process

    @property
    def flow(self):
        return self._flow

    @property
    def direction(self):
        return self._direction

    @property
    def termination(self):
        return self._termination

    @property
    def key(self):
        return self._hash

    def is_allocated(self, reference):
        """
        Stub for compatibility
        :param reference:
        :return:
        """
        return False

    def __hash__(self):
        return hash(self._hash)

    def __eq__(self, other):
        if other is None:
            return False
        if not isinstance(other, Exchange):
            return False
        return self.key == other.key

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
        if self.termination is not None:
            j['termination'] = self.termination
        if self.is_reference:
            j['isReference'] = True
        return j

    @classmethod
    def signature_fields(cls):
        return ['process', 'flow', 'direction', 'quantity']


class ExchangeValue(Exchange):
    """
    An ExchangeValue is an exchange with a single value (corresponding to unallocated exchange value) plus a dict of
    values allocated to different reference flows.
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
        """
        Use to flatten an allocated process inventory into a standalone inventory
        :param allocated:
        :param reference:
        :return:
        """
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

    def add_to_value(self, value, reference=None):
        if reference is None:
            self._value += value
        else:
            if reference in self._value_dict:
                self._value_dict[reference] += value

    def trim(self):
        x = Exchange(self.process, self.flow, self.direction, termination=self.termination)
        if self.is_reference:
            x.set_ref(self.process)
        return x

    def __init__(self, *args, value=None, value_dict=None, **kwargs):
        super(ExchangeValue, self).__init__(*args, **kwargs)
        # assert isinstance(value, float), 'ExchangeValues must be floats (or subclasses)'
        self._value = value
        if value_dict is None:
            self._value_dict = dict()  # keys must live in self.process.reference_entity
        else:
            self._value_dict = value_dict

    @property
    def value(self):
        """
        unallocated value
        :return:
        """
        return self._value

    @property
    def value_string(self):
        if self._value is None:
            return ' --- '
        return '%.3g' % self._value

    @value.setter
    def value(self, exch_val):
        """
        May only be set once. Otherwise use add_to_value
        :param exch_val:
        :return:
        """
        if self._value is not None:
            raise DuplicateExchangeError('Unallocated exchange value already set to %g (new: %g)' % (self._value,
                                                                                                     exch_val))
        self._value = exch_val

    def is_allocated(self, key):
        """
        Report whether the exchange is allocated with respect to a given reference.
        :param key: an exchange
        :return:
        """
        if len(self._value_dict) > 0:
            return key in self._value_dict
        return False

    def __getitem__(self, item):
        """
        When an exchange is asked for its value with respect to a particular reference, lookup the allocation in
        the value_dict.  IF there is no value_dict, then the default _value is returned AS LONG AS the process has
        only 0 or 1 reference exchange.

        Allocated exchange values should add up to unallocated value.  When using the exchange values, don't forget to
        normalize by the chosen reference flow's input value (i.e. utilize an inbound exchange when computing
        node weight or when constructing A + B matrices)
        :param item:
        :return:
        """
        if len(self._value_dict) == 0:
            # unallocated exchanges always read the same
            return self._value
        if self.is_reference:  # if self is a reference entity, the allocation is either .value or 0
            if item.flow == self.flow and item.direction == self.direction:
                return self.value
            return 0.0
        # elif len(self.process.reference_entity) == 1:
        #    # no allocation necessary
        #    return self.value
        elif not item.is_reference:
            raise MissingReference('Allocation key is not a reference exchange')
        else:
            try:
                return self._value_dict[item]
            except KeyError:
                return 0.0
                # no need to raise on zero allocation
        # raise NoAllocation('No allocation found for key %s in process %s' % (item, self.process))

    def __setitem__(self, key, value):
        if not key.is_reference:
            raise AmbiguousReferenceError('Allocation key is not a reference exchange')
        if key in self._value_dict:
            # print(self._value_dict)
            raise DuplicateExchangeError('Exchange value already defined for this reference!')
        '''
        if self._ref_flow in self._value_dict:  # reference exchange
            if key == self._ref_flow:
                if value == 0:
                    raise ValueError('Reference exchange cannot be zero')
            else:
                if value != 0:
                    raise ValueError('Allocation for non-reference exchange must be zero')
        '''
        if self.is_reference:
            if key.flow == self.flow and key.direction == self.direction:
                self.value = value
                # if value != 1:
                #    raise ValueError('Reference Allocation for reference exchange should be 1.0')
            else:
                if value != 0:
                    raise ValueError('Non-reference Allocation for reference exchange should be 0.')
        else:
            # if it's a reference exchange, it's non-allocatable and should have an empty value_dict
            self._value_dict[key] = value

    def remove_allocation(self, key):
        """
        Removes allocation if it exists; doesn't complain if it doesn't.
        :param key: a reference exchange.
        :return:
        """
        if key in self._value_dict:
            self._value_dict.pop(key)

    def __str__(self):
        if self.process.entity_type == 'fragment':
            if self.flow == self.process.flow and self.direction == comp_dir(self.process.direction):
                ref = '{*}'
            else:
                ref = '   '
        else:
            if self.is_reference:
                ref = '{*}'
            else:
                ref = '   '
        return '%6.6s: %s [%s %s] %s' % (self.direction, ref, self.value_string, self.unit, self.tflow)

    def f_view(self):
        return '%6.6s: [%s %s] %s' % (self.direction, self.value_string, self.unit, self.process)

    def _serialize_value_dict(self):
        j = dict()
        for k, v in self._value_dict.items():
            j['%s:%s' % (k.direction, k.flow.uuid)] = v
        return j

    def serialize(self, values=False):
        j = super(ExchangeValue, self).serialize()
        if values:
            if self.value is not None:
                j['value'] = float(self.value)
            if not self.is_reference and len(self._value_dict) > 0:
                j['valueDict'] = self._serialize_value_dict()
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
