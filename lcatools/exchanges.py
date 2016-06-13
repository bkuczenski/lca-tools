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
        return '%s: %s' % (self.direction, self.flow.get_external_ref())

    def serialize(self):
        return {
            'process': self.process.get_external_ref(),
            'flow': self.flow.get_external_ref(),
            'direction': self.direction
        }

    def serialize_process(self, **kwargs):
        j = {
            'flow': self.flow.get_external_ref(),
            'direction': self.direction,
        }
        if self.process['referenceExchange'] is self:
            j['isReference'] = True
        return j

    @classmethod
    def signature_fields(cls):
        return ['process', 'flow', 'direction', 'quantity']


class ExchangeValue(Exchange):
    """
    An ExchangeValue is an exchange with a value
    """
    def __init__(self, *args, value=None, **kwargs):
        super(ExchangeValue, self).__init__(*args, **kwargs)
        self.value = value

    def __str__(self):
        return '%6.6s: [%.3g %s] %s' % (self.direction, self.value, self.quantity.reference_entity, self.flow)

    def serialize_process(self, values=False):
        j = super(ExchangeValue, self).serialize_process()
        if values:
            j['value'] = float(self.value)
        return j
