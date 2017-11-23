from .base import EntityRef


class QuantityRef(EntityRef):
    """
    Quantities can lookup:
    """
    _etype = 'quantity'

    def unit(self):
        # reference entity for quantity ref is just a string
        return self.reference_entity

    @property
    def q_name(self):
        return '%s [%s]' % (self.get_item('Name'), self.reference_entity)

    @property
    def _addl(self):
        return self.unit()

    def is_lcia_method(self):
        ind = self.get_item('Indicator')
        if ind is None:
            return False
        elif len(ind) == 0:
            return False
        return True

    def flowables(self, **kwargs):
        return self._query.flowables(quantity=self.external_ref, **kwargs)

    def factors(self, **kwargs):
        return self._query.factors(self.external_ref, **kwargs)



