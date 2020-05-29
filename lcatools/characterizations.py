"""
Characterizations are one of the two forms of quantitative information in LCA. A characterization relates two different
quantities of measurement (known as a reference quantity and a query quantity) for one particular flowable substance, in
a specific environmental context and geographic locale.

A QRResult is a namedtuple that documents the result of a "quantity relation" calculation- it reports:
 - the flowable
 - the reference quantity
 - the query quantity
 - the context / compartment
 - the locale
 - the origin of the characterization data
 - the value of the characterization

"""

from collections import namedtuple


# namedtuple to store the parameters and result of a quantity-relation lookup
# the first 4 are input params, 'locale' is as-found if found, as-specified if not; origin and value are results

QRResult = namedtuple('QRResult', ('flowable', 'ref', 'query', 'context', 'locale', 'origin', 'value'))


class DuplicateCharacterizationError(Exception):
    pass


class MissingReference(Exception):
    """
    A flow must have a reference quantity to be characterized
    """
    pass


class MissingContext(Exception):
    """
    context cannot be None
    """
    pass


class Characterization(object):
    """
    A characterization is an affiliation of a flow and a quantity. Characterizations are inherently naively spatialized,
    with factors stored in a dict of locations, and the 'GLO' location being used as the default.
    """

    entity_type = 'characterization'

    @classmethod
    def from_qrresult(cls, qrr):
        return cls(qrr.flowable, qrr.ref, qrr.quantity, qrr.context, qrr.origin, location=qrr.locale, value=qrr.value)

    @classmethod
    def from_flow(cls, flow, quantity, context=None, origin=None, **kwargs):
        if context is None:
            context = flow.context
        if origin is None:
            origin = flow.origin
        return cls(flow['Name'], flow.reference_entity, quantity, context, origin=origin, **kwargs)

    def __init__(self, flow_name, ref_quantity, query_quantity, context, origin=None, **kwargs):
        """

        :param flow_name: a canonical string
        :param quantity:
        :param value: passed to add_value if present
        :param location: 'GLO' passed to add_value if present
        :param origin: of data, if applicable
        :return:
        """
        assert ref_quantity.entity_type == 'quantity', "'ref_quantity' must be an LcQuantity"
        assert query_quantity.entity_type == 'quantity', "'query_quantity' must be an LcQuantity"
        assert ref_quantity.is_lcia_method is False, "'ref_quantity' cannot be an LCIA method"

        self.flowable = str(flow_name)
        self.quantity = query_quantity
        self._ref_q = ref_quantity
        if context is None:
            raise MissingContext('%s, %s, %s' % (flow_name, ref_quantity, query_quantity))
        self._context = context
        self._locations = dict()
        self._origin = origin or query_quantity.origin

        if context.origin is None:
            context.add_origin(self._origin)

        if kwargs:
            self.add_value(**kwargs)
        # self._natural_dirn = None

    @property
    def origin(self):
        return self._origin

    @property
    def context(self):
        return self._context

    @property
    def ref_quantity(self):
        return self._ref_q

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

    @property
    def _sval(self):
        if isinstance(self.value, str):
            return '%s' % self.value.replace('\n', '|')
        elif self.value is None:
            return ' --   '
        return '%6.3g' % self.value

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
            return QRResult(self.flowable, self.ref_quantity, self.quantity, self.context, locale,
                            None, None)
        return QRResult(self.flowable, self.ref_quantity, self.quantity, self.context, found,
                        self.origin, self._locations[found])

    def __getitem__(self, item):
        if item == 'quantity':  # f%&(@*$ marshmallow hack
            return self.quantity
        if item == 'flow':  # ibid.
            return self.flowable
        found = self._lookup(item)
        if found is None:
            return 0.0
        return self._locations[found]

    def __setitem__(self, key, value):
        if key in self._locations:
            if self._locations[key] == value:
                return  # just skip if they are the same
            if isinstance(self._locations[key], str):
                pval = '%s (incoming: %s)' % (self._locations[key], value)
            else:
                pval = '%g (incoming: %s)' % (self._locations[key], value)
            raise DuplicateCharacterizationError('%s: Characterization value already present! %s = %s\n%s' %
                                                 (self.quantity, key, pval, self.flowable))
        self._locations[key] = value

    def update_values(self, **kwargs):
        self._locations.update(kwargs)

    def add_value(self, value=None, location=None, overwrite=False):
        if location is None:
            location = 'GLO'
        if overwrite:
            if location in self._locations:
                self._locations.pop(location)
        self[location] = value

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
        return hash((self.flowable, self.ref_quantity.external_ref, self.quantity.external_ref, self.context))

    def __eq__(self, other):
        """
        Returns true if all of other's location-specific values equal self's values for the same location
        :param other:
        :return:
        """
        if other is None:
            return False
        if ((self.flowable == other.flowable) &
                (self.quantity == other.quantity)):
            if all(self[l] == other[l] for l in other.locations()):
                return True
        return False

    def __str__(self):
        if self.is_null:
            return '%s has %s %s' % (self.flowable, self.quantity, self.quantity.reference_entity)
        scs = []
        for k, v in self._locations.items():
            if isinstance(v, str):
                scs.append("'%s' [%s]" % (v, k))
            else:
                scs.append('%6.3g [%s]' % (v, k))

        return '%s [%s / %s] %s: %s (%s)' % ('\n'.join(scs),
                                             self.quantity.unit, self.ref_quantity.unit, self.flowable, self.context,
                                             self.quantity.name)

    def __repr__(self):
        if self.is_null:
            return '%s(%s, %s, %s: null (%s))' % (self.__class__.__name__, self.flowable,
                                                 self.ref_quantity.unit, self.context, self.quantity.unit)
        if len(self._locations) > 1:
            val = '%s (+%d)' % (self._sval, len(self._locations) - 1)
        else:
            val = '%s' % self._sval
        return '%s(%s, %s, %s: %s (%s))' % (self.__class__.__name__, self.flowable,
                                           self.ref_quantity.unit,
                                           self.context, val, self.quantity.unit)

    def q_view(self):
        if self.quantity is self.ref_quantity:
            ref = '(*)'
        else:
            ref = ' | '
        return '%25.25s [%s / %s]%s%s' % (self._sval, self.quantity.unit,
                                          self.ref_quantity.unit, ref,
                                          self.quantity.name)

    '''
    def tupleize(self):
        return self.flow.get_uuid(), self.quantity.get_uuid()
    '''

    def serialize(self, values=False, concise=False):
        """
        The "concise" option is used within term manager when query quantity, context, and flowable are already
        serialized
        :param values:
        :param concise:
        :return:
        """
        j = {
            'ref_quantity': self.ref_quantity.external_ref
        }
        if self.ref_quantity is self.quantity:
            j['isReference'] = True
        else:
            if not concise:
                j['entityType'] = self.entity_type
                j['query_quantity'] = self.quantity.external_ref
                j['context'] = str(self.context)
                j['flowable'] = self.flowable
            if values:
                if self.value is not None:
                    j['value'] = self._locations
        return j
