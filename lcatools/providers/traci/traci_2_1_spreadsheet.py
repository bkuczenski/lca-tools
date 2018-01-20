"""
This implements a quantity interface
"""

import xlrd
from lcatools.providers.base import BasicArchive
from lcatools.providers.xl_dict import XlDict
from lcatools.entities import LcQuantity, LcFlow
from lcatools.characterizations import Characterization, DuplicateCharacterizationError

from .q_info import ns_uuid_21 as t_uuid
from .quantity import Traci21QuantityImplementation, transform_numeric_cas, CAS_regexp, q_info


def transform_string_cas(string_cas):
    return int(''.join([x for x in filter(lambda y: y != '-', string_cas)]))


class Traci21Factors(BasicArchive):
    _ns_uuid_required = True

    def __init__(self, source, ref=None, sheet_name='Substances', mass_quantity=None, ns_uuid=t_uuid, **kwargs):
        if ref is None:
            ref = '.'.join(['local', 'traci', '2', '1', 'spreadsheet'])
        super(Traci21Factors, self).__init__(source, ref=ref, ns_uuid=ns_uuid, **kwargs)

        print('Loading workbook %s' % self.source)
        self._xl = xlrd.open_workbook(self.source)

        self._serialize_dict['sheet_name'] = sheet_name

        self._rows = XlDict.from_sheetname(self._xl, sheet_name)

        self._methods = dict()  # store column-to-method mapping
        for col, val in q_info.items():
            q = self._create_lcia_method(val)
            self._methods[col] = q

        if mass_quantity is None:
            self._mass = self._create_quantity('Mass', 'kg')
        else:
            self.add(mass_quantity)
            self._mass = mass_quantity

    def lcia_method_iter(self):
        for k, v in self._methods.items():
            yield v

    def make_interface(self, iface, privacy=None):
        if iface == 'quantity':
            return Traci21QuantityImplementation(self, privacy=privacy)
        else:
            return super(Traci21Factors, self).make_interface(iface, privacy=privacy)

    @staticmethod
    def _flow_key(flowable, compartment):
        return ', '.join([flowable, compartment])

    def _create_quantity(self, name, unitstring):
        u = self._key_to_nsuuid(name)
        q = self[u]
        if q is None:
            q = LcQuantity(u, external_ref=name, Name=name, ReferenceUnit=self._create_unit(unitstring)[0])
            self.add(q)
        return q

    def _create_lcia_method(self, qinfo):
        q = self._create_quantity(qinfo.Category, qinfo.RefUnit)
        q['Method'] = 'TRACI 2.1'
        q['Category'] = qinfo.Category
        q['Indicator'] = qinfo.RefUnit
        return q

    def check_methods(self, col):
        if col in self._methods:
            return self._methods[col]
        return None

    def _create_flow(self, row, compartment):
        flowable = row['Substance Name'].lower()
        cas = transform_numeric_cas(row['Formatted CAS #'])
        ext_ref = self._flow_key(flowable, compartment)
        u = self._key_to_nsuuid(ext_ref)
        f = self[u]
        if f is None:
            f = LcFlow(u, external_ref=ext_ref, Name=flowable, Compartment=[compartment], ReferenceQuantity=self._mass,
                       CasNumber=cas or '')
            self.add(f)
        return f

    def _add_flowable(self, row):
        for col, i in q_info.items():
            q = self.check_methods(col)
            try:
                cf = float(row[col])
            except ValueError:
                continue
            if cf == 0.0:
                continue
            f = self._create_flow(row, i.Compartment)
            try:
                f.add_characterization(q, value=cf)
            except DuplicateCharacterizationError:
                continue

    def _fetch(self, entity, **kwargs):
        raise AttributeError('Cannot fetch -- this error should never occur')

    def _load_all(self):
        for i, row in self._rows.iterrows():
            self._add_flowable(row)

    def iterrows(self):
        for i, row in self._rows.iterrows():
            yield i, row

    """
    Quantity interface
    made the job difficult by electing not to make the archive static.  Trade 2 sec of load time every use for ~2
    hours of dev time. Pays off only if I load TRACI factors >3600 times.
    """

    def _char_from_row_compartment_method(self, row, cm, q, cf):
        flow = self._create_flow(row, cm)
        return Characterization(flow, q, value=cf)

    def cf_for_method_and_compartment(self, row, method=None, compartment=None):
        for col, val in row.items():
            if col not in q_info:
                continue
            q = self.check_methods(col)
            if method is None or q is method:
                cm = q_info[col].Compartment
                if compartment is None or cm == compartment:
                    try:
                        cf = float(val)
                    except ValueError:
                        continue
                    if cf != 0.0:
                        yield self._char_from_row_compartment_method(row, cm, q, cf)

    def row_for_key(self, key):
        if CAS_regexp.match(key):
            key = transform_string_cas(key)
            for i, row in self._rows.iterrows():
                if row['CAS #'] == key:
                    yield row
        else:
            key = key.lower()
            for i, row in self._rows.iterrows():
                if row['Substance Name'].lower() == key:
                    yield row
