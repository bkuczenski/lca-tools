from .base import EntityRef


class QuantityRef(EntityRef):
    """
    Quantities can lookup:
    """
    _etype = 'quantity'

    def unit(self):
        return self.reference_entity.unitstring

    @property
    def q_name(self):
        return '%s [%s]' % (self.get_item('Name'), self.unit())

    @property
    def _addl(self):
        return self.unit()

    def serialize(self):
        j = super(QuantityRef, self).serialize()
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

    def flowables(self, **kwargs):
        return self._query.flowables(quantity=self.external_ref, **kwargs)

    def factors(self, **kwargs):
        return self._query.factors(self.external_ref, **kwargs)

    def ensure_lcia(self):
        self._query.ensure_lcia_factors(self)

    def do_lcia(self, inventory, **kwargs):
        return self._query.do_lcia(inventory, self, **kwargs)
