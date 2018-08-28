from __future__ import print_function, unicode_literals
import uuid


from lcatools.entities.entities import LcEntity


class ConversionReferenceMismatch(Exception):
    pass


class NoFactorsFound(Exception):
    pass


class NoUnitConversionTable(Exception):
    pass


class LcQuantity(LcEntity):

    _ref_field = 'referenceUnit'
    _new_fields = []

    @classmethod
    def new(cls, name, ref_unit, **kwargs):
        """
        :param name: the name of the quantity
        :param ref_unit: the string representation of the reference unit for the quantity
        :return:
        """
        return cls(uuid.uuid4(), Name=name, ReferenceUnit=LcUnit(ref_unit), **kwargs)

    def __init__(self, entity_uuid, **kwargs):
        super(LcQuantity, self).__init__('quantity', entity_uuid, **kwargs)
        self._cm = None

    def set_context(self, cm):
        self._cm = cm

    def register_cf(self, cf):
        """
        Allows the quantity to keep a list of local characterizations that use it
        :param cf: a Characterization
        :return:
        """
        if cf.quantity is self:
            if cf.cf_origin() is None or cf.cf_origin() == self.origin:
                if cf.flow.reference_entity is self:
                    return
                cf.flow.set_context(self._cm)
                self._cm.add_cf(self, cf)
            else:
                if not self._cm.quiet:
                    print('%% origin mismatch %s != %s' % (cf.cf_origin(), self.origin))
        else:
            if not self._cm.quiet:
                print('%% not self')

    def deregister_cf(self, cf):
        if cf.quantity is self:
            if cf.cf_origin() == self.origin:
                self._qlookup[cf.flow['Name']].remove(cf)

    def unit(self):
        return self.reference_entity.unitstring

    def _make_ref_ref(self, query):
        return self.reference_entity

    def is_lcia_method(self):
        return 'Indicator' in self.keys()

    def factors(self, flowable=None, compartment=None, dist=0):
        for cf in self._cm.factors_for_quantity(self, flowable=flowable, compartment=compartment, dist=dist):
            yield cf

    def quantity_relation(self, ref_quantity, flowable, compartment, locale='GLO', strategy='highest', **kwargs):
        cfs = [cf for cf in self.factors(flowable, compartment, **kwargs)]
        cfs_1 = [cf for cf in cfs if locale in cf.locations()]
        if len(cfs_1) > 1:
            cfs = cfs_1
        values = []
        if len(cfs) == 0:
            raise NoFactorsFound('%s [%s] %s', (flowable, compartment, self))
        for cf in cfs:
            if cf.flow.reference_entity is not ref_quantity:
                convert_fail = False
                factor = None
                try:
                    factor = cf.flow.reference_entity.quantity_relation(ref_quantity, cf.flow['Name'], None, **kwargs)
                except NoFactorsFound:
                    try:
                        factor = ref_quantity.convert(from_unit=cf.flow.unit())
                    except NoUnitConversionTable:
                        try:
                            factor = cf.flow.reference_entity.convert(to=ref_quantity.unit())
                        except KeyError:
                            convert_fail = True
                    except KeyError:
                        convert_fail = True
                finally:
                    if convert_fail or factor is None:
                        raise ConversionReferenceMismatch('Flow %s\nfrom %s\nto %s' % (cf.flow,
                                                                                       cf.flow.reference_entity,
                                                                                       ref_quantity))

                values.append(cf[locale] * factor)
            else:
                values.append(cf[locale])
        if len(values) > 1:
            if strategy == 'highest':
                return max(values)
            elif strategy == 'lowest':
                return min(values)
            elif strategy == 'average':
                return sum(values) / len(values)
            else:
                raise ValueError('Unknown strategy %s' % strategy)
        else:
            return values[0]

    def convert(self, from_unit=None, to=None):
        """
        Perform unit conversion within a quantity, using a 'UnitConversion' table stored in the object properties.
        For instance, if the quantity name was 'mass' and the reference unit was 'kg', then
        quantity.convert('lb') would[should] return 0.4536...
        quantity.convert('lb', to='ton') should return 0.0005

        This function requires that the quantity have a 'UnitConversion' property that works as a dict, with
        the unit names being keys. The requirement is that the values for every key all correspond to the same
        quantity.  For instance, if the quantity was mass, then the following would be equivalent:

        quantity['UnitConversion'] = { 'kg': 1, 'lb': 2.204622, 'ton': 0.0011023, 't': 0.001 }
        quantity['UnitConversion'] = { 'kg': 907.2, 'lb': 2000.0, 'ton': 1, 't': 0.9072 }

        If the quantity's reference unit is missing from the dict, it is assumed to be 1 implicitly.

        :param from_unit: unit to convert from (default is the reference unit)
        :param to: unit to convert to (default is the reference unit)
        :return: a float indicating how many to_units there are in one from_unit
        """
        try:
            uc_table = self._d['UnitConversion']
        except KeyError:
            raise NoUnitConversionTable

        if from_unit is None:
            from_unit = self.reference_entity.unitstring

        inbound = uc_table[from_unit]

        if to is None:
            to = self.reference_entity.unitstring

        outbound = uc_table[to]

        return outbound / inbound

    def _print_ref_field(self):
        return self.reference_entity.unitstring

    def reset_unitstring(self, ustring):
        self.reference_entity.reset_unitstring(ustring)

    @property
    def q_name(self):
        return '%s [%s]' % (self._d['Name'], self.reference_entity.unitstring)

    def __str__(self):
        if self.is_lcia_method():
            return '%s [LCIA]' % self.q_name
        return '%s' % self.q_name


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
        self._unitstring = unitstring
        self._external_ref = None

    def set_external_ref(self, external_ref):
        self._external_ref = external_ref

    def get_external_ref(self):
        return '%s' % self._unitstring if self._external_ref is None else self._external_ref

    @property
    def unitstring(self):
        return self._unitstring

    def get_uuid(self):
        return self._unitstring  # needed for upward compat

    def __str__(self):
        return '[%s]' % self._unitstring

    def reset_unitstring(self, ustring):
        self._external_ref = ustring
        self._unitstring = ustring
