from __future__ import print_function, unicode_literals
import uuid
import re

from lcatools.characterizations import Characterization
from lcatools.entities.entities import LcEntity
from lcatools.entities.quantities import LcQuantity


def trim_cas(cas):
    try:
        return re.sub('^(0*)', '', cas)
    except TypeError:
        print('%s %s' % (cas, type(cas)))
        return ''


class MissingFactor(Exception):
    pass


class DeleteReference(Exception):
    pass


class LcFlow(LcEntity):

    _ref_field = 'referenceQuantity'
    _new_fields = ['CasNumber', 'Compartment']

    @classmethod
    def new(cls, name, ref_qty, **kwargs):
        """
        :param name: the name of the flow
        :param ref_qty: the reference quantity
        :return:
        """
        return cls(uuid.uuid4(), Name=name, ReferenceQuantity=ref_qty, **kwargs)

    def trim(self):
        """
        Return an identical LcFlow that has had its non-reference characterizations removed
        :return:
        """
        trimmed = super(LcFlow, self).trim()
        trimmed.add_characterization(self.reference_entity, reference=True, value=self._ref_quantity_factor)
        return trimmed

    def __init__(self, entity_uuid, **kwargs):
        super(LcFlow, self).__init__('flow', entity_uuid, **kwargs)

        self._characterizations = dict()

        self._ref_quantity_factor = 1.0

        for k in self._new_fields:
            if k not in self._d:
                self._d[k] = ''

        if self['CasNumber'] is None:
            self['CasNumber'] = ''

        if self.reference_entity is not None:
            if self.reference_entity.get_uuid() not in self._characterizations.keys():
                self.add_characterization(self.reference_entity, reference=True, value=self._ref_quantity_factor)

    def _set_reference(self, ref_entity):
        if self.reference_entity is not None:
            if self.reference_entity.get_uuid() == ref_entity.get_uuid():
                return
            # need to do a conversion
            print('Changing reference quantity for flow %s' % self)
            print('reference >>%s<<' % self.reference_entity)
            print('refer.new >>%s<<' % ref_entity)
            inc = self.cf(ref_entity)  # divide by 0 if not known
            if inc is None or inc == 0:
                raise MissingFactor('Flow %s missing factor for reference quantity %s' % (self, ref_entity))
            adj = 1.0 / inc
            for v in self._characterizations.values():
                v.scale(adj)
        super(LcFlow, self)._set_reference(ref_entity)

    def unit(self):
        return self.reference_entity.unit()

    def set_local_unit(self, factor):
        self._ref_quantity_factor = factor

    def match(self, other):
        return (self.get_uuid() == other.get_uuid() or
                self['Name'].lower() == other['Name'].lower() or
                (trim_cas(self['CasNumber']) == trim_cas(other['CasNumber']) and len(self['CasNumber']) > 4) or
                self.get_external_ref() == other.get_external_ref())

    def __str__(self):
        cas = self._d['CasNumber']
        if cas is None:
            cas = ''
        if len(cas) > 0:
            cas = ' (CAS ' + cas + ')'
        comp = self._d['Compartment'][-1]  # '', '.join((i for i in self._d['Compartment'] if i is not None))
        return '%s%s [%s]' % (self._d['Name'], cas, comp)

    def profile(self):
        print('%s' % self)
        out = []
        for cf in self._characterizations.values():
            print('%2d %s' % (len(out), cf.q_view()))
            out.append(cf)
        return out

    def add_characterization(self, quantity, reference=False, value=None, **kwargs):
        if isinstance(quantity, Characterization):
            for l in quantity.locations():
                self.add_characterization(quantity.quantity, reference=reference,
                                          value=quantity[l], location=l)
            return
        if not isinstance(quantity, LcQuantity):  # assume it's a CatalogRef
            quantity = quantity.entity()
        if reference:
            if value is None:
                value = 1.0
            self.set_local_unit(value)
            self._set_reference(quantity)

        q = quantity.get_uuid()
        c = Characterization(self, quantity)
        if q in self._characterizations.keys():
            if value is None:
                return
            c = self._characterizations[q]
        else:
            self._characterizations[q] = c
        if value is not None:
            if isinstance(value, dict):
                c.update_values(**value)
            else:
                c.add_value(value=value, **kwargs)

    def has_characterization(self, quantity, location='GLO'):
        if quantity.get_uuid() in self._characterizations.keys():
            if location == 'GLO':
                return True
            if location in self._characterizations[quantity.get_uuid()].locations():
                return True
        return False

    def del_characterization(self, quantity):
        if quantity is self.reference_entity:
            raise DeleteReference('Cannot delete reference quantity')
        self._characterizations.pop(quantity.get_uuid())

    def characterizations(self):
        for i in self._characterizations.values():
            yield i

    def factor(self, quantity):
        if quantity.get_uuid() in self._characterizations:
            return self._characterizations[quantity.get_uuid()]
        return Characterization(self, quantity)

    def cf(self, quantity, location='GLO'):
        """
        These are backwards.  cf should return the Characterization ; factor should return the value.  instead, it's
        the other way around.
        :param quantity:
        :param location: ['GLO']
        :return: value of quantity per unit of reference, or 0.0
        """
        if quantity.get_uuid() in self._characterizations:
            try:
                return self._characterizations[quantity.get_uuid()][location]
            except KeyError:
                return self._characterizations[quantity.get_uuid()].value
        return 0.0

    def convert(self, val, to=None, fr=None, location='GLO'):
        """
        converts the value (in
        :param val:
        :param to: to quantity
        :param fr: from quantity
        :param location: cfs are localized to unrestricted strings babee
        the flow's reference quantity is used if either is unspecified
        :return: value * self.char(to)[loc] / self.char(fr)[loc]
        """
        out = self.cf(to or self.reference_entity, location=location)
        inn = self.cf(fr or self.reference_entity, location=location)
        return val * out / inn

    def merge(self, other):
        super(LcFlow, self).merge(other)
        for k in other._characterizations.keys():
            if k not in self._characterizations:
                print('Merge: Adding characterization %s' % k)
                self.add_characterization(other._characterizations[k])

    def serialize(self, characterizations=False, **kwargs):
        j = super(LcFlow, self).serialize()
        j.pop(self._ref_field)  # reference reported in characterizations
        if characterizations:
            j['characterizations'] = sorted([x.serialize(**kwargs) for x in self._characterizations.values()],
                                            key=lambda x: x['quantity'])
        else:
            j['characterizations'] = [x.serialize(**kwargs) for x in self._characterizations.values()
                                      if x.quantity is self.reference_entity]

        return j
