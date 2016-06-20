

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
        assert flow.entity_type == 'flow', "'flow' must be an LcFlow"
        assert quantity.entity_type == 'quantity', "'quantity' must be an LcQuantity"

        self.flow = flow
        self.quantity = quantity

    def __hash__(self):
        return hash((self.flow.get_uuid(), self.quantity.get_uuid()))

    def __eq__(self, other):
        if other is None:
            return False
        return ((self.flow.get_uuid() == other.flow.get_uuid()) &
                (self.quantity.get_uuid() == other.quantity.get_uuid()))

    def __str__(self):
        return '%s has %s %s' % (self.flow, self.quantity, self.quantity.reference_entity)

    def tupleize(self):
        return self.flow.get_uuid(), self.quantity.get_uuid()

    def serialize(self, **kwargs):
        j = {
            'quantity': self.quantity.get_uuid()
        }
        if self.quantity == self.flow['referenceQuantity']:
            j['isReference'] = True
        return j

    @classmethod
    def signature_fields(cls):
        return ['flow', 'quantity']


class CharacterizationFactor(Characterization):
    """
    A CharacterizationFactor is a characterization with a value field.
    """

    def __init__(self, *args, value=None, **kwargs):
        super(CharacterizationFactor, self).__init__(*args, **kwargs)
        self.value = value

    def serialize(self, values=False):
        d = super(CharacterizationFactor, self).serialize()
        if values:
            if self.value is not None:
                d['value'] = self.value
        return d

    def __str__(self):
        return '%s: [%.3g %s] %s' % (self.flow, self.value, self.quantity.reference_entity, self.quantity)


class CharacterizationSet(object):
    """
    A dict of characterizations, whose key is the tupleized factor, for quick retrieval
    """
    def __init__(self, overwrite=False):
        self._d = dict()
        self.overwrite = overwrite

    def __iter__(self):
        for k, cf in self._d.items():
            if cf.value != 0:
                yield cf

    def iter_zeros(self):
        for k, cf in self._d.items():
            if cf.value == 0:
                yield cf

    def add(self, char):
        if char.tupleize() in self._d:
            if self.overwrite is False:
                return
                # reject overwrite silently
                # raise KeyError('Characterization is already in-set, and overwrite is False')

        self._d[char.tupleize()] = char

    def get(self, flow=None, quantity=None):
        if flow is not None:
            if quantity is not None:
                return self._d[(flow.get_uuid(), quantity.get_uuid())]
            else:
                # flow, no quantity
                return [cf for k, cf in self._d.items() if cf.flow is flow]  # is, really??
        elif quantity is not None:
            # quantity, no flow
            return [cf for k, cf in self._d.items() if cf.quantity is quantity]  # is, really??
        else:
            return None

    def serialize(self):
        return sorted([cf.serialize() for cf in self], key=lambda x: x.flow)
