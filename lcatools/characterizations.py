
class DuplicateCharacterizationError(Exception):
    pass


class Characterization(object):
    """
    A characterization is an affiliation of a flow and a quantity. Characterizations are inherently naively spatialized,
    with factors stored in a dict of locations, and the 'GLO' location being used as the default.
    """

    entity_type = 'characterization'

    def __init__(self, flow, quantity, **kwargs):
        """

        :param flow:
        :param quantity:
        :return:
        """
        assert flow.entity_type == 'flow', "'flow' must be an LcFlow"
        assert quantity.entity_type == 'quantity', "'quantity' must be an LcQuantity"

        self.flow = flow
        self.quantity = quantity
        self._locations = dict()
        if kwargs:
            self.add_value(**kwargs)

    @property
    def is_null(self):
        return len(self._locations) == 0

    @property
    def value(self):
        if 'GLO' in self._locations:
            return self._locations['GLO']
        elif len(self._locations) == 0:
            return None
        elif len(self._locations) == 1:
            return list(self._locations.values())[0]
        else:
            return self._locations

    @value.setter
    def value(self, val):
        self._locations['GLO'] = val

    def __getitem__(self, item):
        if item in self._locations.keys():
            return self._locations[item]
        if len(self._locations) == 0:
            return None
        if 'GLO' in self._locations.keys():
            return self._locations['GLO']
        return 0.0  # today is not the day to write a location best-match finder

    def __setitem__(self, key, value):
        if key in self._locations:
            raise DuplicateCharacterizationError('Characterization value already present! %s = %g' %
                                                 (key, self._locations[key]))
        self._locations[key] = value

    def update_values(self, **kwargs):
        self._locations.update(kwargs)

    def add_value(self, value=None, location=None):
        if location is None:
            self['GLO'] = value
        else:
            self[location] = value

    def scale(self, factor):
        for k, v in self._locations.items():
            self[k] = v * factor

    def locations(self):
        return self._locations.keys()

    def list_locations(self):
        return '; '.join([k for k in self.locations()])

    def __hash__(self):
        return hash((self.flow.get_uuid(), self.quantity.get_uuid()))

    def __eq__(self, other):
        if other is None:
            return False
        return ((self.flow.get_uuid() == other.flow.get_uuid()) &
                (self.quantity.get_uuid() == other.quantity.get_uuid()))

    def __str__(self):
        if self.is_null:
            return '%s has %s %s' % (self.flow, self.quantity, self.quantity.reference_entity)
        return '%s %s %s' % ('\n'.join(['%10.3g [%s]' % (v, k) for k,v in self._locations.items()]),
                             self.quantity.reference_entity, self.flow)

    def q_view(self):
        if self.quantity is self.flow.reference_entity:
            ref = '(*)'
        else:
            ref = ' | '
        if self.value is not None:
            return '%10.3g %20.20s == %s%s%s' % (self.value, self.quantity.reference_entity.unitstring(),
                                                 self.flow.reference_entity.reference_entity.unitstring(), ref,
                                                 self.quantity)
        else:
            return '%10s %20.20s == %s%s%s' % (' ', self.quantity.reference_entity.unitstring(),
                                               self.flow.reference_entity.reference_entity.unitstring(), ref,
                                               self.quantity)

    def tupleize(self):
        return self.flow.get_uuid(), self.quantity.get_uuid()

    def serialize(self, values=False):
        j = {
            'entityType': self.entity_type,
            'quantity': self.quantity.get_uuid()
        }
        if self.quantity == self.flow['referenceQuantity']:
            j['isReference'] = True
        if values:
            if self.value is not None:
                j['value'] = self.value
        return j

    @classmethod
    def signature_fields(cls):
        return ['flow', 'quantity']


'''
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
'''