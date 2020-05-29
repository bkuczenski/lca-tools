from synonym_dict import SynonymDict, SynonymSet


class QuantityUnitMismatch(Exception):
    pass


class QuantityAlreadySet(Exception):
    pass


class QuantitySynonyms(SynonymSet):
    """
    QuantitySynonyms are string terms that all refer to the same quantity of measure. They must all have the same
    unit, because they are used to define the unit of measure of flowables.  To repeat: quantity instances that have
    the same dimensionality but different units (e.g. kWh and MJ) are NOT SYNONYMS but distinct quantities. The
    LciaEngine should be able to handle conversions between these kinds of quantities.
    """
    @classmethod
    def new(cls, quantity):
        return cls(*quantity.quantity_terms(), quantity=quantity)

    def _validate_quantity(self, quantity):
        if not hasattr(quantity, 'entity_type'):
            return False
        if quantity.entity_type != 'quantity':
            return False
        if not hasattr(quantity, 'unit'):
            return False
        if quantity.unit is not None:  # allow unit=None quantities to merge with existing quantities
            if not isinstance(quantity.unit, str):
                return False
            if self.unit is not None and quantity.unit != self.unit:
                raise QuantityUnitMismatch('incoming %s (set %s)' % (quantity.unit, self.unit))
        return True

    def __init__(self, *args, quantity=None, unit=None, **kwargs):
        self._quantity = None
        super(QuantitySynonyms, self).__init__(*args, **kwargs)
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
                self._unit = item.unit
                for term in item.quantity_terms():
                    self.add_term(term)
            else:
                raise TypeError('Quantity fails validation (%s)' % type(item))
        else:
            raise QuantityAlreadySet

    def _save_synonyms(self, other):
        """
        adds to other's synonym set-
        :param other:
        :return:
        """
        for k in other.terms:
            self._quantity.add_synonym(k)

    def add_child(self, other, force=False):
        if not isinstance(other, QuantitySynonyms):
            raise TypeError('Child set is not a Quantity synonym set (%s)' % type(other))
        if other.unit is not None and other.unit != self.unit:
            raise QuantityUnitMismatch('incoming %s (canonical %s)' % (other.unit, self._quantity.unit))
        if self._quantity is None:
            self.quantity = other.quantity
        elif self._quantity.is_entity:
            self._save_synonyms(other)
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

    _ignore_case = True

    def _add_from_dict(self, j):
        name = j['name']
        syns = j.pop('synonyms', [])
        unit = j.pop('unit', None)
        self.new_entry(name, *syns, unit=unit, merge=False)

    def find_matching_quantity(self, quantity):
        if isinstance(quantity, str):
            return self[quantity]
        for term in quantity.quantity_terms():
            if term in self:
                return self[term]
        raise KeyError(quantity)

    def add_quantity(self, quantity):
        """
        Prunes terms on quantity unit mismatch
        :param quantity:
        :return:
        """
        new_q = QuantitySynonyms.new(quantity)
        try:
            self.add_or_update_entry(new_q, merge=True, create_child=True)
        except QuantityUnitMismatch:
            # want to print a warning
            # if we get QuantityUnitMismatch, then that means we didn't get a MergeError
            me = self.match_entry(*new_q.terms)
            print('!! Warning: new quantity %s [%s] has unit conflict with existing quantity %s [%s] ' % (new_q,
                                                                                                          new_q.unit,
                                                                                                          me, me.unit))
            pe = self.add_or_update_entry(new_q, merge=False, prune=True)
            print('   Added pruned quantity %s [%s]' % (pe.name, pe.unit))

    def add_synonym(self, term, syn):
        super(QuantityManager, self).add_synonym(term, syn)
        ent = self._d[syn]
        # canonical quantity is always a ref
        for child in ent.children:
            if child.quantity is not None and child.quantity.is_entity:
                child.quantity.add_synonym(str(syn).strip())

    def __getitem__(self, item):
        if hasattr(item, 'link'):
            item = item.link
        return super(QuantityManager, self).__getitem__(item)
