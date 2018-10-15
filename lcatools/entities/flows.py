from __future__ import print_function, unicode_literals
import uuid

from lcatools.characterizations import Characterization
from .entities import LcEntity
# from lcatools.entities.quantities import LcQuantity
from ..interfaces import trim_cas


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

    def __init__(self, entity_uuid, local_unit=None, **kwargs):
        super(LcFlow, self).__init__('flow', entity_uuid, **kwargs)

        self._characterizations = dict()

        self._local_unit = None

        for k in self._new_fields:
            if k not in self._d:
                self._d[k] = ''

        if self['CasNumber'] is None:
            self['CasNumber'] = ''

        if self.reference_entity is not None:
            if self.reference_entity.uuid not in self._characterizations.keys():
                self.add_characterization(self.reference_entity, reference=True)

        if local_unit is not None:
            self.set_local_unit(local_unit)

    @property
    def context(self):
        """
        Stopgap until Context Refactor is completed.  return '; '.join(self['Compartment'])
        :return:
        """
        return '; '.join(self['Compartment'])

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
        if self._local_unit is not None:
            print('NOT YET SUPPORTED')
            return self._local_unit
        return self.reference_entity.unit()

    def set_local_unit(self, local_unit):
        """
        Controls the display of numeric data regarding the flow.

        This functionality is NOT YET SUPPORTED.

        Exchange values and CFs are all stored with respect to the reference quantity's reference unit, so the local
        unit setting does not affect any computations: only display.

        use self.mag(amount) to report the magnitudes of flows in the local unit
        use self.imag(amount) to report the magnitudes of measures for which the flow appears in the denominator

        Examples:
        flow's reference entity is volume [m3]
        flow's local unit is 'l'

        For an exchange value of 0.001 m3 per reference flow: self.mag(0.001) = 1
        For a characterization factor of 0.75 kg/m3, self.imag(0.75) = 7.5e-4

        This functionality does not allow users to report indicators in different units (e.g. water depletion potential
        in liters instead of m3).

        :param local_unit: should be a string which is a valid argument to the reference quantity's "convert" function.
        :return:
        """
        local_conv = self.reference_entity.convert(to=local_unit)
        if local_conv is not None:
            self._local_unit = local_unit

    def mag(self, amount):
        """
        Report magnitudes of the flow in the local unit (or magnitudes where the flow is in the numerator)
        :param amount:
        :return:
        """
        if self._local_unit is None:
            return amount
        return amount * self.reference_entity.convert(to=self._local_unit)

    def imag(self, amount):
        """
        Report magnitudes where the flow is in the denominator, converted in terms of the flow's local unit
        :param amount:
        :return:
        """
        if self._local_unit is None:
            return amount
        return amount * self.reference_entity.convert(fr=self._local_unit)

    def unset_local_unit(self):
        self._local_unit = None

    def match(self, other):
        if isinstance(other, str):
            return (self.uuid == other or
                    (trim_cas(self['CasNumber']) == trim_cas(other) and len(self['CasNumber']) > 4) or
                    self.external_ref == other)
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

    def add_characterization(self, quantity, reference=False, value=None, overwrite=False, **kwargs):
        """

        :param quantity: entity or catalog ref
        :param reference: [False] should this be made the flow's reference quantity
        :param value:
        :param kwargs: location, origin
        value + location + optional origin make a data tuple
        :param overwrite: [False] if True, allow values to replace existing characterizations
        :return:
        """
        ''' # we no longer want to be able to add literal characterizations. Just do it explicitly.
        if isinstance(quantity, Characterization):
            if quantity.flow.reference_entity != self.reference_entity:
                adj = self.cf(quantity.flow.reference_entity)
                if adj == 0:
                    raise MissingFactor('%s' % quantity.flow.reference_entity)
            else:
                adj = 1.0

            for l in quantity.locations():
                self.add_characterization(quantity.quantity, reference=reference,
                                          value=quantity[l] / adj, location=l, origin=quantity.origin[l])
            return
        '''
        if reference:
            if value is not None and value != 1.0:
                raise ValueError('Reference quantity always has unit value')
            value = 1.0
            self._set_reference(quantity)

        q = quantity.uuid
        if q in self._characterizations.keys():
            if value is None:
                return
            c = self._characterizations[q]
        else:
            c = Characterization(self, quantity)
            self._characterizations[q] = c
        if value is not None:
            if isinstance(value, dict):
                c.update_values(**value)
            else:
                c.add_value(value=value, overwrite=overwrite, **kwargs)
        return c

    def has_characterization(self, quantity, location='GLO'):
        """
        A flow keeps track of characterizations by link
        :param quantity:
        :param location:
        :return:
        """
        if quantity.uuid in self._characterizations.keys():
            if location == 'GLO' or location is None:
                return True
            if location in self._characterizations[quantity.uuid].locations():
                return True
        return False

    def del_characterization(self, quantity):
        if quantity is self.reference_entity:
            raise DeleteReference('Cannot delete reference quantity')
        self._characterizations.pop(quantity.uuid)

    def characterizations(self):
        for i in self._characterizations.values():
            yield i

    def factor(self, quantity):
        if quantity.uuid in self._characterizations:
            return self._characterizations[quantity.uuid]
        return Characterization(self, quantity)

    def cf(self, quantity, locale='GLO'):
        """
        These are backwards.  cf should return the Characterization ; factor should return the value.  instead, it's
        the other way around.
        :param quantity:
        :param locale: ['GLO']
        :return: value of quantity per unit of reference, or 0.0
        """
        if quantity.uuid in self._characterizations:
            try:
                return self._characterizations[quantity.uuid][locale]
            except KeyError:
                return self._characterizations[quantity.uuid].value
        return 0.0

    def convert(self, val, to=None, fr=None, locale='GLO'):
        """
        converts the value (in
        :param val:
        :param to: to quantity
        :param fr: from quantity
        :param locale: cfs are localized to unrestricted strings babee
        the flow's reference quantity is used if either is unspecified
        :return: value * self.char(to)[loc] / self.char(fr)[loc]
        """
        out = self.cf(to or self.reference_entity, locale=locale)
        inn = self.cf(fr or self.reference_entity, locale=locale)
        return val * out / inn

    def merge(self, other):
        super(LcFlow, self).merge(other)
        for k in other._characterizations.keys():
            if k not in self._characterizations:
                print('Merge: Adding characterization %s' % k)
                self.add_characterization(other._characterizations[k])

    def serialize(self, characterizations=False, domesticate=False, **kwargs):
        j = super(LcFlow, self).serialize(domesticate=domesticate)
        j.pop(self._ref_field)  # reference reported in characterizations
        if characterizations:
            j['characterizations'] = sorted([x.serialize(**kwargs) for x in self._characterizations.values()],
                                            key=lambda x: x['quantity'])
        else:
            j['characterizations'] = [x.serialize(**kwargs) for x in self._characterizations.values()
                                      if x.quantity is self.reference_entity]

        return j
