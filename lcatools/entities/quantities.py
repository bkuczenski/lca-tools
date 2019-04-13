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
        u = uuid.uuid4()
        return cls(str(u), entity_uuid=u, Name=name, ReferenceUnit=LcUnit(ref_unit), **kwargs)

    def __init__(self, external_ref, **kwargs):
        super(LcQuantity, self).__init__('quantity', external_ref, **kwargs)
        self._qi = None

    def _set_reference(self, ref_entity):
        if isinstance(ref_entity, str):
            ref_entity = LcUnit(ref_entity)
        super(LcQuantity, self)._set_reference(ref_entity)

    def set_qi(self, qi):
        self._qi = qi

    def unit(self):
        return self.reference_entity.unitstring

    def _make_ref_ref(self, query):
        return self.reference_entity

    def is_lcia_method(self):
        return 'Indicator' in self.keys()

    def add_synonym(self, k):
        if self.has_property('Synonyms'):
            if isinstance(self['Synonyms'], str):
                syns = {self['Synonyms']}
            else:
                syns = set(self['Synonyms'])
        else:
            syns = set()
        syns.add(k)
        self['Synonyms'] = syns


    """
    Quantity Interface Methods
    Quantity entities use the quantity interface provided by the parent archive; this emulates the operation of 
    quantity refs, which have access to the catalog.
    """
    def cf(self, flow, locale='GLO', **kwargs):
        """
        The semantics here may be confusing, but cf is flow-centered. It converts reports the amount in self that
        corresponds to a unit of the flow's reference quantity.
        :param flow:
        :param locale:
        :param kwargs:
        :return:
        """
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

    def do_lcia(self, inventory, **kwargs):
        return self._qi.do_lcia(self, inventory, **kwargs)

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
        amount.  For instance, if the quantity was mass, then the following would be equivalent:

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
            if self.reference_entity.unitstring in uc_table:
                inbound = uc_table[self.reference_entity.unitstring]
            else:
                inbound = 1.0
        else:
            inbound = uc_table[from_unit]

        if to is None:
            if self.reference_entity.unitstring in uc_table:
                outbound = uc_table[self.reference_entity.unitstring]
            else:
                outbound = 1.0

        else:
            outbound = uc_table[to]

        return round(outbound / inbound, 12)  # round off to curtail numerical / serialization issues

    def _print_ref_field(self):
        return self.reference_entity.unitstring

    def reset_unitstring(self, ustring):
        self.reference_entity.reset_unitstring(ustring)

    @property
    def name(self):
        n = '%s [%s]' % (self._d['Name'], self.reference_entity.unitstring)
        if self.is_lcia_method():
            return '%s [LCIA]' % n
        return n

    def __str__(self):
        return self.name


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
