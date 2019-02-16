from .base import EntityRef


class QuantityRef(EntityRef):
    """
    Quantities can lookup:
    """
    _etype = 'quantity'

    def unit(self):
        if isinstance(self.reference_entity, str):
            return self.reference_entity
        return self.reference_entity.unitstring

    @property
    def _addl(self):
        if self.is_lcia_method():
            return '%s] [LCIA' % self.unit()
        return self.unit()

    def serialize(self, **kwargs):
        j = super(QuantityRef, self).serialize(**kwargs)
        j['referenceUnit'] = self.unit()
        return j

    def is_lcia_method(self):
        try:
            ind = self.get_item('Indicator')
        except KeyError:
            return False
        if ind is None:
            return False
        elif len(ind) == 0:
            return False
        return True

    """
    Interface methods
    """
    def flowables(self, **kwargs):
        return self._query.flowables(quantity=self.external_ref, **kwargs)

    def factors(self, **kwargs):
        return self._query.factors(self.external_ref, **kwargs)

    def cf(self, flow, **kwargs):
        return self._query.cf(flow, self.external_ref, **kwargs)

    def characterize(self, flowable, ref_quantity, value, **kwargs):
        return self._query.characterize(flowable, ref_quantity, self, value, **kwargs)

    def ensure_lcia(self):
        self._query.ensure_lcia_factors(self)

    def do_lcia(self, inventory, **kwargs):
        return self._query.do_lcia(inventory, self, **kwargs)

    def quantity_relation(self, ref_quantity, flowable, compartment, locale='GLO', **kwargs):
        return self._query.quantity_relation(ref_quantity, flowable, compartment, self, locale=locale, **kwargs)
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
