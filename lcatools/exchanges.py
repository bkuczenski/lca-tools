from lcatools.entities import LcProcess, LcFlow, LcQuantity

directions = ('Input', 'Output')


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
        assert isinstance(process, LcProcess), "'process' must be an LcProcess!"
        assert isinstance(flow, LcFlow), "'flow' must be an LcFlow"
        assert direction in directions, "direction must be a string in (%s)" % directions

        self.process = process
        self.flow = flow
        self.direction = direction
        if quantity is None:
            self.quantity = flow.reference_entity
        else:
            assert isinstance(quantity, LcQuantity), "'quantity' must be an LcQuantity or None!"
            self.quantity = quantity

    def __hash__(self):
        return hash((self.process.get_uuid(), self.flow.get_uuid(), self.direction))

    def __eq__(self, other):
        if other is None:
            return False
        return (self.process.get_uuid() == other.process.get_uuid() &
                self.flow.get_uuid() == other.flow.get_uuid() &
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

    @classmethod
    def signature_fields(cls):
        return ['process', 'flow', 'direction', 'quantity']

