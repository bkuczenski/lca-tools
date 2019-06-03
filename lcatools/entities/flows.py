from __future__ import print_function, unicode_literals
import uuid

from .entities import LcEntity
# from lcatools.entities.quantities import LcQuantity
from lcatools.entity_refs import FlowInterface
from lcatools.interfaces import CONTEXT_STATUS_


class RefQuantityError(Exception):
    pass


def new_flow(name, ref_quantity, cas_number='', comment='', context=None, compartment=None, external_ref=None, **kwargs):
    if CONTEXT_STATUS_ == 'compat' and compartment is None:
        if context is None:
            compartment = []
        else:
            compartment = context.as_list()

    kwargs['CasNumber'] = kwargs.pop('CasNumber', cas_number)
    kwargs['Comment'] = kwargs.pop('Comment', comment)
    kwargs['Compartment'] = kwargs.pop('Compartment', compartment)

    if external_ref is None:
        return LcFlow.new(name, ref_quantity, **kwargs)
    return LcFlow(external_ref, Name=name, ReferenceQuantity=ref_quantity, **kwargs)


class LcFlow(LcEntity, FlowInterface):

    _ref_field = 'referenceQuantity'
    _new_fields = ['CasNumber']  # finally abolishing the obligation for the flow to have a Compartment

    @classmethod
    def new(cls, name, ref_qty, **kwargs):
        """
        :param name: the name of the flow
        :param ref_qty: the reference quantity
        :return:
        """
        u = uuid.uuid4()
        return cls(str(u), Name=name, entity_uuid=u, ReferenceQuantity=ref_qty, **kwargs)

    def __setitem__(self, key, value):
        self._catch_context(key, value)
        self._catch_flowable(key.lower(), value)
        super(LcFlow, self).__setitem__(key, value)

    @LcEntity.origin.setter
    def origin(self, value):  # pycharm lint is documented bug: https://youtrack.jetbrains.com/issue/PY-12803
        LcEntity.origin.fset(self, value)
        self._flowable.add_term(self.link)

    def __init__(self, external_ref, **kwargs):
        super(LcFlow, self).__init__('flow', external_ref, **kwargs)

        for k in self._new_fields:
            if k not in self._d:
                self._d[k] = ''

    def _make_ref_ref(self, query):
        if self.reference_entity is not None:
            return query.get_canonical(self.reference_entity)
        return None

    def unit(self):
        return super(LcFlow, self).unit()

    '''
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
    '''

    def __str__(self):
        cas = self.get('CasNumber')
        if cas is None:
            cas = ''
        if len(cas) > 0:
            cas = ' (CAS ' + cas + ')'
        context = '[%s]' % ';'.join(self.context)
        return '%s%s %s' % (self.get('Name'), cas, context)

    '''
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
        'x'x'x # we no longer want to be able to add literal characterizations. Just do it explicitly.
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
        'x'x'x
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
        try:
            quantity.register_cf(c)
        except FlowWithoutContext:
            pass  # add when the flow is contextualized
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
            raise RefQuantityError('Cannot delete reference quantity')
        c = self._characterizations.pop(quantity.uuid)
        c.quantity.deregister_cf(c)

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
    '''
