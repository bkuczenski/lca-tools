from lcatools.entities import LcFlow, LcQuantity


class Characterization(object):
    """
    A characterization is an affiliation of a flow and a quantity. At this point it is merely an
    assertion that the flow can be described in terms of the quantity.
    """

    entity_type = 'characterization'

    def __init__(self, flow, quantity):
        """

        :param flow:
        :param quantity:
        :return:
        """
        assert isinstance(flow, LcFlow), "'flow' must be an LcFlow"
        assert isinstance(quantity, LcQuantity), "'quantity' must be an LcQuantity"

        self.flow = flow
        self.quantity = quantity
        self.value = None

    def __hash__(self):
        return hash((self.flow.get_uuid(), self.quantity.get_uuid()))

    def __eq__(self, other):
        if other is None:
            return False
        return (self.flow.get_uuid() == other.flow.get_uuid() &
                self.quantity.get_uuid() == other.quantity.get_uuid())

    def __str__(self):
        return '%s has %s %s' % (self.flow, self.quantity, self.quantity.reference_entity)

    @classmethod
    def signature_fields(cls):
        return ['flow', 'quantity']

