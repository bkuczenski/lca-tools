from collections import namedtuple


# namedtuple to store the parameters and result of a quantity-relation lookup
# the first 4 are input params, 'locale' is as-found if found, as-specified if not; origin and value are results

QRResult = namedtuple('QRResult', ('ref', 'flowable', 'context', 'query', 'locale', 'origin', 'value'))


class DuplicateCharacterizationError(Exception):
    pass


class Characterization(object):
    """
    A characterization is an affiliation of a flow and a quantity. Characterizations are inherently naively spatialized,
    with factors stored in a dict of locations, and the 'GLO' location being used as the default.
    """

    entity_type = 'characterization'

    def __init__(self, flow, quantity, context=None, **kwargs):
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
        self._context = context
        self._locations = dict()
        self._origins = dict()

        if kwargs:
            self.add_value(**kwargs)
        # self._natural_dirn = None

    def cf_origin(self, location='GLO'):
        """
        Giving this function a different name because origin is generally implemented as a @property
        :param location:
        :return:
        """
        org = None
        if location in self._origins:
            org = self._origins[location]
        elif 'GLO' in self._origins:
            org = self._origins['GLO']
        if org is None:
            return self.flow.origin
        return org

    @property
    def context(self):
        if self._context is None:
            self._context = self.flow.context
        return self._context

    '''
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
    '''

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

    def _lookup(self, locale):
        found_locale = None
        if len(self._locations) > 0:
            if locale in self._locations.keys():
                found_locale = locale
            elif 'GLO' in self._locations.keys():
                found_locale = 'GLO'
                # today is not the day to write a location best-match finder
        return found_locale

    def query(self, locale):
        found = self._lookup(locale)
        if found is None:
            return QRResult(self.flow.reference_entity, self.flow.flowable, self.flow.context, self.quantity, locale,
                            None, None)
        return QRResult(self.flow.reference_entity, self.flow.flowable, self.flow.context, self.quantity, found,
                        self.cf_origin(found), self._locations[found])

    def __getitem__(self, item):
        if item == 'quantity':  # f%&(@*$ marshmallow hack
            return self.quantity
        if item == 'flow':  # ibid.
            return self.flow
        found = self._lookup(item)
        if found is None:
            return 0.0
        return self._locations[found]

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

    '''
    def scale(self, factor):
        for k, v in self._locations.items():
            self._locations[k] = v * factor
    '''

    def locations(self):
        return self._locations.keys()

    def list_locations(self):
        return '; '.join([k for k in self.locations()])

    def __hash__(self):
        return hash((self.flow.uuid, self.quantity.uuid, self.context))

    def __eq__(self, other):
        """
        Returns true if all of other's location-specific values equal self's values for the same location
        :param other:
        :return:
        """
        if other is None:
            return False
        if ((self.flow.uuid == other.flow.uuid) &
                (self.quantity.uuid == other.quantity.uuid)):
            if all(self[l] == other[l] for l in other.locations()):
                return True
        return False

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
            'quantity': self.quantity.uuid
        }
        if self.quantity == self.flow.reference_entity:
            j['isReference'] = True
        else:  # no need to report the characterization value for the reference quantity
            if values:
                if self.value is not None:
                    j['value'] = self.value
        return j

    @classmethod
    def signature_fields(cls):
        return ['flow', 'quantity']
