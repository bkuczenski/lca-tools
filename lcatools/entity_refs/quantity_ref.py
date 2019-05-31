from .base import EntityRef


class NoUnitConversionTable(Exception):
    pass


def convert(quantity, from_unit=None, to=None):
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

    :param quantity: something with a __getitem__ and a unit() function
    :param from_unit: unit to convert from (default is the reference unit)
    :param to: unit to convert to (default is the reference unit)
    :return: a float indicating how many to_units there are in one from_unit
    """
    try:
        uc_table = quantity['UnitConversion']
    except KeyError:
        raise NoUnitConversionTable

    if from_unit is None:
        if quantity.unit() in uc_table:
            inbound = uc_table[quantity.unit()]
        else:
            inbound = 1.0
    else:
        inbound = uc_table[from_unit]

    if to is None:
        if quantity.unit() in uc_table:
            outbound = uc_table[quantity.unit()]
        else:
            outbound = 1.0

    else:
        outbound = uc_table[to]

    return round(outbound / inbound, 12)  # round off to curtail numerical / serialization issues


class QuantityRef(EntityRef):
    """
    Quantities can lookup:
    """
    _etype = 'quantity'
    _ref_field = 'referenceUnit'
    _is_lcia = None

    def unit(self):
        if isinstance(self.reference_entity, str):
            return self.reference_entity
        return self.reference_entity.unitstring

    @property
    def _addl(self):
        if self.is_lcia_method():
            return '%s] [LCIA' % self.unit()
        return self.unit()

    @property
    def name(self):
        return self._name

    def serialize(self, **kwargs):
        j = super(QuantityRef, self).serialize(**kwargs)
        j['referenceUnit'] = self.unit()
        return j

    def is_lcia_method(self):
        if self._is_lcia is None:
            try:
                ind = self.get_item('Indicator')
            except KeyError:
                ind = None
            if ind is None:
                self._is_lcia = False
            elif ind == '':
                self._is_lcia = False
            else:
                self._is_lcia = True
        return self._is_lcia

    def __eq__(self, other):
        if other is None:
            return False
        return self.is_canonical(other)

    def __hash__(self):
        return hash(self.link)

    def convert(self, from_unit=None, to=None):
        return convert(self, from_unit, to)

    """
    Interface methods
    """
    def is_canonical(self, other):
        return self._query.get_canonical(other) is self

    def flowables(self, **kwargs):
        return self._query.flowables(quantity=self.external_ref, **kwargs)

    def factors(self, **kwargs):
        return self._query.factors(self.external_ref, **kwargs)

    def cf(self, flow, **kwargs):
        return self._query.cf(flow, self.external_ref, **kwargs)

    def characterize(self, flowable, ref_quantity, value, **kwargs):
        return self._query.characterize(flowable, ref_quantity, self, value, **kwargs)

    def do_lcia(self, inventory, **kwargs):
        return self._query.do_lcia(self, inventory, **kwargs)

    def quantity_relation(self, ref_quantity, flowable, context, locale='GLO', **kwargs):
        return self._query.quantity_relation(flowable, ref_quantity, self, context, locale=locale, **kwargs)
    '''
=======
    def convert(self, from_unit=None, to=None):
        """
        Reports the number of 'to' units equal to a 'from_unit'.  Uses the quantity's 'UnitConversion' property.
        Simply supplying a unit string will report the unit in terms of the quantity's reference [display] unit.
        :param from_unit: [defaults to reference unit]
        :param to: [defaults to reference unit]
        :return:
        """
        uc = self.get_item('UnitConversion')
        if from_unit is None:
            inbound = 1.0
        else:
            inbound = uc[from_unit]

        if to is None:
            outbound = 1.0
        else:
            outbound = uc[to]
        return outbound / inbound
>>>>>>> master
    '''
