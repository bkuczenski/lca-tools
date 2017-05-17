from __future__ import print_function, unicode_literals
import uuid

from lcatools.entities.entities import LcEntity


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

    def unit(self):
        return self.reference_entity.unitstring()

    def is_lcia_method(self):
        return 'Indicator' in self.keys()

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

        :param from_unit: unit to convert from (default is the reference unit)
        :param to: unit to convert to (default is the reference unit)
        :return: a float indicating how many to_units there are in one from_unit
        """
        try:
            uc_table = self._d['UnitConversion']
        except KeyError:
            print('No unit conversion table found.')
            return None

        if from_unit is None:
            from_unit = self.reference_entity.unitstring

        try:
            inbound = uc_table[from_unit]
        except KeyError:
            print('Inbound unit %s not found in unit conversion table.' % from_unit)
            return None

        if to is None:
            to = self.reference_entity.unitstring

        try:
            outbound = uc_table[to]
        except KeyError:
            if to is self.reference_entity:
                outbound = 1.0
            else:
                print('Outbound unit %s not found in unit conversion table.' % to)
                return None

        return outbound / inbound

    def reset_unitstring(self, ustring):
        self.reference_entity.reset_unitstring(ustring)

    def _name(self):
        return '%s [%s]' % (self._d['Name'], self.reference_entity.unitstring())

    def __str__(self):
        if self.is_lcia_method():
            return '%s [LCIA]' % self._name()
        return '%s' % self._name()


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
        self._unitstring = unitstring
        self._external_ref = None

    def set_external_ref(self, external_ref):
        self._external_ref = external_ref

    def get_external_ref(self):
        return '%s' % self._unitstring if self._external_ref is None else self._external_ref

    def unitstring(self):
        return self._unitstring

    def get_uuid(self):
        return self._unitstring  # needed for upward compat

    def __str__(self):
        return '[%s]' % self._unitstring

    def reset_unitstring(self, ustring):
        self._external_ref = ustring
        self._unitstring = ustring
