from __future__ import print_function, unicode_literals
import uuid


from lcatools.entities.entities import LcEntity
from lcatools.entity_refs.quantity_ref import QuantityRef, convert


class QuantityAlreadyConfigured(Exception):
    pass


def new_quantity(name, ref_unit, external_ref=None, **kwargs):
    if external_ref is None:
        return LcQuantity.new(name, ref_unit, **kwargs)
    return LcQuantity(external_ref, Name=name, ReferenceUnit=LcUnit(ref_unit), **kwargs)


class LcQuantity(LcEntity):

    _ref_field = 'referenceUnit'
    _new_fields = ()

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
        self._is_lcia = False
        super(LcQuantity, self).__init__('quantity', external_ref, **kwargs)
        if not self.has_property('UnitConversion'):
            self._d['UnitConversion'] = {self.unit(): 1.0}
        self._qi = None

    def __setitem__(self, key, value):
        if key.lower() == 'indicator' and len(value) > 0:
            self._new_fields = ('Indicator', )
            self._is_lcia = True
        super(LcQuantity, self).__setitem__(key, value)

    def _set_reference(self, ref_entity):
        if isinstance(ref_entity, str):
            ref_entity = LcUnit(ref_entity)
        super(LcQuantity, self)._set_reference(ref_entity)

    def quantity_terms(self):
        yield self['Name']
        yield self.name
        yield str(self)  # this is the same as above for entities, but includes origin for refs
        yield self.external_ref  # do we definitely want this?  will squash versions together
        if self.uuid is not None:
            yield self.uuid
        if self.origin is not None:
            yield self.link
        if self.has_property('Synonyms'):
            syns = self['Synonyms']
            if isinstance(syns, str):
                yield syns
            else:
                for syn in syns:
                    yield syn

    @property
    def configured(self):
        return self._qi is not None

    def set_qi(self, qi):
        """
        Configures the quantity to access its native term manager.  Can only be set once; otherwise ignored.
        :param qi:
        :return:
        """
        if self._qi is None:
            self._qi = qi
        else:
            raise QuantityAlreadyConfigured(self)

    def unit(self):
        return self.reference_entity.unitstring

    def _make_ref_ref(self, query):
        return self.reference_entity

    def is_lcia_method(self):
        return self._is_lcia

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

    def convert(self, from_unit=None, to=None):
        return convert(self, from_unit, to)

    """
    Interior utility functions
    These are not exactly exposed by the quantity interface and maybe should be retired
    """

    def _print_ref_field(self):
        return self.reference_entity.unitstring

    def reset_unitstring(self, ustring):
        self.reference_entity.reset_unitstring(ustring)

    @property
    def _name(self):
        n = '%s [%s]' % (self._d['Name'], self.reference_entity.unitstring)
        if self.is_lcia_method():
            return '%s [LCIA]' % n
        return n

    @property
    def name(self):
        return self._name

    def __str__(self):
        return self._name

    def __eq__(self, other):
        if isinstance(other, QuantityRef):
            return other.is_canonical(self)
        return super(LcQuantity, self).__eq__(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if self._origin is None:
            raise AttributeError('Origin not set!')
        return hash(self.link)


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

    def __str__(self):
        return '[%s]' % self._unitstring

    def reset_unitstring(self, ustring):
        self._external_ref = ustring
        self._unitstring = ustring
