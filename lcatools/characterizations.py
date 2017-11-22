
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
        :param value: passed to add_value if present
        :param location: 'GLO' passed to add_value if present
        :param origin: of data, if applicable
        :return:
        """
        assert flow.entity_type == 'flow', "'flow' must be an LcFlow"
        assert quantity.entity_type == 'quantity', "'quantity' must be an LcQuantity"

        self.flow = flow
        self.quantity = quantity
        self._locations = dict()
        self._origins = dict()

        if kwargs:
            self.add_value(**kwargs)
        self._natural_dirn = None

    def origin(self, location='GLO'):
        org = None
        if location in self._origins:
            org = self._origins[location]
        elif 'GLO' in self._origins:
            org = self._origins['GLO']
        if org is None:
            return self.flow.origin
        return org

    @property
    def natural_direction(self):
        return self._natural_dirn

    def set_natural_direction(self, c_mgr):
        if self._natural_dirn is not None:
            return
        comp = c_mgr.find_matching(self.flow['Compartment'])
        if comp.is_subcompartment_of(c_mgr.emissions):
            self._natural_dirn = 'Output'
        elif comp.is_subcompartment_of(c_mgr.resources):
            self._natural_dirn = 'Input'
        else:
            self._natural_dirn = False

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

    def add_value(self, value=None, location=None, origin=None, overwrite=False):
        if location is None:
            location = 'GLO'
        if overwrite:
            if location in self._locations:
                self._locations.pop(location)
        self[location] = value
        self._origins[location] = origin

    def scale(self, factor):
        for k, v in self._locations.items():
            self._locations[k] = v * factor

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
        return '%s [%s / %s] %s (%s)' % ('\n'.join(['%6.3g [%s]' % (v, k) for k, v in self._locations.items()]),
                                         self.quantity.unit(), self.flow.unit(), self.flow['Name'],
                                         self.quantity['Name'])

    def q_view(self):
        if self.quantity is self.flow.reference_entity:
            ref = '(*)'
        else:
            ref = ' | '
        if self.value is not None:
            return '%6.3g %16.16s == %s%s%s' % (self.value, self.quantity.unit(),
                                                self.flow.unit(), ref,
                                                self.quantity)
        else:
            return '%6s %16.16s == %s%s%s' % (' ', self.quantity.unit(),
                                              self.flow.unit(), ref,
                                              self.quantity)

    '''
    def tupleize(self):
        return self.flow.get_uuid(), self.quantity.get_uuid()
    '''

    def serialize(self, values=False):
        j = {
            'entityType': self.entity_type,
            'quantity': self.quantity.get_uuid()
        }
        if self.quantity == self.flow.reference_entity:
            j['isReference'] = True
        if values:
            if self.value is not None:
                j['value'] = self.value
        return j

    @classmethod
    def signature_fields(cls):
        return ['flow', 'quantity']
