from .base import EntityRef


class QuantityRef(EntityRef):
    """
    Quantities can lookup:
    """
    _etype = 'quantity'

    def unit(self):
        return self.reference_entity.unitstring

    @property
    def _addl(self):
        return self.unit()

    def is_lcia_method(self):
        if self._query.get_item(self.external_ref, 'Indicator') is not None:
            return True
        return False

    def flowables(self, **kwargs):
        return self._query.flowables(quantity=self.external_ref, **kwargs)

    def factors(self, **kwargs):
        return self._query.factors(self.external_ref, **kwargs)



