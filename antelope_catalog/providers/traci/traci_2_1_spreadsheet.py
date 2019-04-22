"""
This provider needs some work:
 - the open_workbook call should not happen in the constructor, but in _load_all
 - then the provider should be marked 'static'
 - that would enable the init to be instantaneous, and the load to occur through normal channels in LcResource
 - that would also allow the loaded result to be cached in JSON
 = model: Ecoinvent LCIA

This suggests that maybe there should be a versioned LCIA base class-- probably in the data_sources hierarchy and not
the providers hierarchy though...
On that topic, it is simply foolhardy to try to build a version-agnostic provider class in the ABSENCE of multiple
versions (as is the case with TRACI)
"""

import xlrd
from ..xl_dict import XlDict
from lcatools.archives import BasicArchive
from lcatools.entities import LcQuantity, LcFlow
from lcatools.characterizations import DuplicateCharacterizationError

from .q_info import ns_uuid_21 as t_uuid
from .quantity import Traci21QuantityImplementation, transform_numeric_cas, CAS_regexp, q_info
from .index import Traci21IndexImplementation


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
        self._l_f = set()  # track which flows are loaded
        self._l_c = set()  # keep track of which methods are loaded
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

    def make_interface(self, iface):
        if iface == 'quantity':
            return Traci21QuantityImplementation(self)
        elif iface == 'index':
            return Traci21IndexImplementation(self)
        else:
            return super(Traci21Factors, self).make_interface(iface)

    @staticmethod
    def _flow_key(flowable, compartment):
        return ', '.join([flowable, compartment])

    def _create_quantity(self, name, unitstring):
        q = self[name]
        if q is None:
            q = LcQuantity(name, Name=name, ReferenceUnit=self._create_unit(unitstring)[0])
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

    def _create_flow(self, row):
        ext_ref = row['Substance Name'].lower()  # self._flow_key(flowable, compartment)
        f = self[ext_ref]
        if f is None:
            cas = transform_numeric_cas(row['Formatted CAS #'])
            f = LcFlow(ext_ref, Name=ext_ref, ReferenceQuantity=self._mass,
                       CasNumber=cas or '')
            self.add(f)
        return f

    def _add_flowable(self, row):
        flow = self._create_flow(row)
        if flow.name in self._l_f:
            return

        for col, i in q_info.items():
            q = self.check_methods(col)
            try:
                cf = float(row[col])
            except ValueError:
                continue
            if cf == 0.0:
                continue
            self._char_from_flow_compartment_method(flow, i.Compartment, q, cf)
        self._l_f.add(flow.name)
        return flow

    def _add_column(self, col):
        if col in self._l_c:
            return
        if col in q_info:
            method = self._methods[col]
            comp = q_info[col].Compartment
            for i, row in self.iterrows():
                cf = row[col]
                try:
                    cf = float(cf)
                except ValueError:
                    continue
                if cf == 0.0:
                    continue
                flow = self._create_flow(row)
                self._char_from_flow_compartment_method(flow, comp, method, cf)
        self._l_c.add(col)

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

    def _char_from_flow_compartment_method(self, flow, cm, q, cf):
        cx = self.tm.add_context(cm, origin=self.ref)
        try:
            self.tm.add_characterization(flow.name, flow.reference_entity, q, cf, context=cx)
        except DuplicateCharacterizationError:
            pass

    def add_method_and_compartment(self, method=None, compartment=None):
        for col, val in q_info.items():
            q = self.check_methods(col)
            if method is None or q is method:
                cm = val.Compartment
                if compartment is None or cm == compartment:
                    self._add_column(col)

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
