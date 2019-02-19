from synonym_dict import SynonymDict, SynonymSet


class QuantityUnitMismatch(Exception):
    pass


class QuantityAlreadySet(Exception):
    pass


def _quantity_terms(quantity):
    yield quantity['Name']
    yield str(quantity)
    yield quantity.external_ref  # definitely want this
    yield quantity.uuid
    yield quantity.link
    if quantity.has_property('Synonyms'):
        syns = quantity['Synonyms']
        if isinstance(syns, str):
            yield syns
        else:
            for syn in syns:
                yield syn


class QuantitySynonyms(SynonymSet):
    """
    QuantitySynonyms are string terms that all refer to the same quantity of measure. They must all have the same
    unit, because they are used to define the unit of measure of flowables.  To repeat: quantity instances that have
    the same dimensionality but different units (e.g. kWh and MJ) are NOT SYNONYMS but distinct quantities. The
    LciaEngine should be able to handle conversions between these kinds of quantities.
    """
    @classmethod
    def new(cls, quantity):
        return cls(*_quantity_terms(quantity), quantity=quantity)

    def _validate_quantity(self, quantity):
        if not hasattr(quantity, 'entity_type'):
            return False
        if quantity.entity_type != 'quantity':
            return False
        if not hasattr(quantity, 'unit'):
            return False
        if not isinstance(quantity.unit(), str):
            return False
        if self.unit is not None and quantity.unit() != self.unit:
            raise QuantityUnitMismatch('incoming %s (set %s)' % (quantity.unit(), self.unit))
        return True

    def __init__(self, *args, quantity=None, unit=None, **kwargs):
        super(QuantitySynonyms, self).__init__(*args, **kwargs)
        self._quantity = None
        self._unit = unit
        if quantity is not None:
            self.quantity = quantity

    @property
    def quantity(self):
        return self._quantity

    @quantity.setter
    def quantity(self, item):
        if self.quantity is None:
            if self._validate_quantity(item):
                self._quantity = item
                self._unit = item.unit()
                for term in _quantity_terms(item):
                    self.add_term(term)
            else:
                raise TypeError('Quantity fails validation (%s)' % type(item))
        else:
            raise QuantityAlreadySet

    def add_child(self, other, force=False):
        if not isinstance(other, QuantitySynonyms):
            raise TypeError('Child set is not a Quantity synonym set (%s)' % type(other))
        if self._quantity is None:
            self.quantity = other.quantity
        if other.unit != self.unit:
            raise QuantityUnitMismatch('incoming %s (canonical %s)' % (other.unit, self._quantity.unit()))
        super(QuantitySynonyms, self).add_child(other, force=force)

    @property
    def object(self):
        return self._quantity

    @property
    def unit(self):
        return self._unit

    def serialize(self):
        d = super(QuantitySynonyms, self).serialize()
        if self.unit is not None:
            d['unit'] = self.unit

        return d


class QuantityManager(SynonymDict):

    _entry_group = 'Quantities'
    _syn_type = QuantitySynonyms

    def _add_from_dict(self, j):
        name = j['name']
        syns = j.pop('synonyms', [])
        unit = j.pop('unit', None)
        self.new_object(name, *syns, unit=unit, merge=False)

    def add_quantity(self, quantity):
        new_q = QuantitySynonyms.new(quantity)
        try:
            self.add_or_update_object(new_q, merge=True, create_child=True)
        except QuantityUnitMismatch:
            self.add_or_update_object(new_q, merge=False, prune=True)
