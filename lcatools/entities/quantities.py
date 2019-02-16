from __future__ import print_function, unicode_literals
import uuid


from lcatools.entities.entities import LcEntity


class NoUnitConversionTable(Exception):
    pass


class LcQuantity(LcEntity):

    _ref_field = 'referenceUnit'
    _new_fields = []

    @classmethod
    def new(cls, name, ref_unit, **kwargs):
        """
        :param name: the name of the quantity
        :param ref_unit: the string representation of the reference unit for the quantity
        :return:
        """
        return cls(uuid.uuid4(), Name=name, ReferenceUnit=LcUnit(ref_unit), **kwargs)

    def __init__(self, entity_uuid, **kwargs):
        super(LcQuantity, self).__init__('quantity', entity_uuid, **kwargs)
        self._qi = None

    def set_qi(self, qi):
        self._qi = qi

    def unit(self):
        return self.reference_entity.unitstring

    def _make_ref_ref(self, query):
        return self.reference_entity

    def is_lcia_method(self):
        return 'Indicator' in self.keys()

    """
    Quantity Interface Methods
    Quantity entities use the quantity interface provided by the parent archive; this emulates the operation of 
    quantity refs, which have access to the catalog.
    """
    def cf(self, flow, locale='GLO', **kwargs):
        return self._qi.cf(flow, self, locale=locale, **kwargs)

    def factors(self, flowable=None, context=None, **kwargs):
        return self._qi.factors(self, flowable=flowable, context=context, **kwargs)

    def profile(self, flow, **kwargs):
        """
        This is a ridiculous hack because the profile doesn't depend on self at all.  So let's make it non-ridiculous
        by defaulting to self as ref_quantity, so it's not TOTALLY ridiculous
        :param flow:
        :param kwargs:
        :return:
        """
        ref_quantity = kwargs.pop('ref_quantity', self)
        return self._qi.profile(flow, ref_quantity=ref_quantity, **kwargs)

    """
    Interior utility functions
    These are not exactly exposed by the quantity interface and maybe should be retired
    """
    def convert(self, from_unit=None, to=None):
        """
        Perform unit conversion within a quantity, using a 'UnitConversion' table stored in the object properties.
        For instance, if the quantity name was 'mass' and the reference unit was 'kg', then
        quantity.convert('lb') would[should] return 0.4536...
        quantity.convert('lb', to='ton') should return 0.0005

        This function requires that the quantity have a 'UnitConversion' property that works as a dict, with
        the unit names being keys. The requirement is that the values for every key all correspond to the same
        quantity.  For instance, if the quantity was mass, then the following would be equivalent:

        quantity['UnitConversion'] = { 'kg': 1, 'lb': 2.204622, 'ton': 0.0011023, 't': 0.001 }
        quantity['UnitConversion'] = { 'kg': 907.2, 'lb': 2000.0, 'ton': 1, 't': 0.9072 }

        If the quantity's reference unit is missing from the dict, it is assumed to be 1 implicitly.

        If the quantity is missing a unit conversion property, raises NoUnitConversionTable.  If the quantity does
        have such a table but one of the specified units is missing from it, raises KeyError

        :param from_unit: unit to convert from (default is the reference unit)
        :param to: unit to convert to (default is the reference unit)
        :return: a float indicating how many to_units there are in one from_unit
        """
        try:
            uc_table = self._d['UnitConversion']
        except KeyError:
            raise NoUnitConversionTable

        if from_unit is None:
            from_unit = self.reference_entity.unitstring

        inbound = uc_table[from_unit]

        if to is None:
            to = self.reference_entity.unitstring

        outbound = uc_table[to]

        return outbound / inbound

    def _print_ref_field(self):
        return self.reference_entity.unitstring

    def reset_unitstring(self, ustring):
        self.reference_entity.reset_unitstring(ustring)

    def __str__(self):
        name = '%s [%s]' % (self._d['Name'], self.reference_entity.unitstring)
        if self.is_lcia_method():
            return '%s [LCIA]' % name
        return name


class LcUnit(object):
    """
    Dummy class to store a reference to a unit definition
    Design decision: even though ILCD unitgroups have assigned UUIDs, we are not maintaining unitgroups
    """
    entity_type = 'unit'

    def __init__(self, unitstring, unit_uuid=None):
        if unit_uuid is not None:
            self._uuid = uuid.UUID(unit_uuid)
        else:
            self._uuid = None
        if isinstance(unitstring, LcUnit):
            unitstring = unitstring.unitstring
        self._unitstring = unitstring
        self._external_ref = None

    def set_external_ref(self, external_ref):
        self._external_ref = external_ref

    def get_external_ref(self):
        return '%s' % self._unitstring if self._external_ref is None else self._external_ref

    @property
    def unitstring(self):
        return self._unitstring

    def get_uuid(self):
        return self._unitstring  # needed for upward compat

    def __str__(self):
        return '[%s]' % self._unitstring

    def reset_unitstring(self, ustring):
        self._external_ref = ustring
        self._unitstring = ustring
