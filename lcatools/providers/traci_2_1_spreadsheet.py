"""
This implements a quantity interface
"""

import re
from collections import namedtuple
import xlrd
from lcatools.providers.base import NsUuidArchive
from lcatools.providers.xl_dict import XlDict
from lcatools.entities import LcQuantity, LcFlow
# from lcatools.catalog.quantity import QuantityInterface
from lcatools.characterizations import Characterization, DuplicateCharacterizationError


QInfo = namedtuple('QInfo', ['Category', 'Compartment', 'RefUnit'])


q_info = {
    'Global Warming Air (kg CO2 eq / kg substance)': QInfo('Global Warming Air',  'air', 'kg CO2 eq'),
    'Acidification Air (kg SO2 eq / kg substance)': QInfo('Acidification Air', 'air',  'kg SO2 eq'),
    'HH Particulate Air (PM2.5 eq / kg substance)': QInfo('Human Health Particulates Air', 'air', 'PM2.5 eq'),
    'Eutrophication Air (kg N eq / kg substance)': QInfo('Eutrophication Air', 'air', 'kg N eq'),
    'Eutrophication Water (kg N eq / kg substance)': QInfo('Eutrophication Water', 'water', 'kg N eq'),
    'Ozone Depletion Air (kg CFC-11 eq / kg substance)': QInfo('Ozone Depletion Air', 'air', 'kg CFC-11 eq'),
    'Smog Air (kg O3 eq / kg substance)': QInfo('Smog Air', 'air', 'kg O3 eq'),
    'Ecotox. CF [CTUeco/kg], Em.airU, freshwater': QInfo('Ecotoxicity, freshwater', 'urban air', 'CTUeco'),
    'Ecotox. CF [CTUeco/kg], Em.airC, freshwater': QInfo('Ecotoxicity, freshwater', 'rural air', 'CTUeco'),
    'Ecotox. CF [CTUeco/kg], Em.fr.waterC, freshwater': QInfo('Ecotoxicity, freshwater', 'fresh water', 'CTUeco'),
    'Ecotox. CF [CTUeco/kg], Em.sea waterC, freshwater': QInfo('Ecotoxicity, freshwater', 'sea water', 'CTUeco'),
    'Ecotox. CF [CTUeco/kg], Em.nat.soilC, freshwater': QInfo('Ecotoxicity, freshwater', 'soil', 'CTUeco'),
    'Ecotox. CF [CTUeco/kg], Em.agr.soilC, freshwater': QInfo('Ecotoxicity, freshwater', 'agricultural', 'CTUeco'),
    'Human health CF  [CTUcancer/kg], Emission to urban air, cancer': QInfo(
        'Human health toxicity, cancer', 'urban air', 'CTUcancer'),
    'Human health CF  [CTUnoncancer/kg], Emission to urban air, non-canc.': QInfo(
        'Human health toxicity, non-cancer', 'urban air', 'CTUnoncancer'),
    'Human health CF  [CTUcancer/kg], Emission to cont. rural air, cancer': QInfo(
        'Human health toxicity, cancer', 'rural air', 'CTUcancer'),
    'Human health CF  [CTUnoncancer/kg], Emission to cont. rural air, non-canc.': QInfo(
        'Human health toxicity, non-cancer', 'rural air', 'CTUnoncancer'),
    'Human health CF  [CTUcancer/kg], Emission to cont. freshwater, cancer': QInfo(
        'Human health toxicity, cancer', 'fresh water', 'CTUcancer'),
    'Human health CF  [CTUnoncancer/kg], Emission to cont. freshwater, non-canc.': QInfo(
        'Human health toxicity, non-cancer', 'fresh water', 'CTUnoncancer'),
    'Human health CF  [CTUcancer/kg], Emission to cont. sea water, cancer': QInfo(
        'Human health toxicity, cancer', 'sea water', 'CTUcancer'),
    'Human health CF  [CTUnoncancer/kg], Emission to cont. sea water, non-canc.': QInfo(
        'Human health toxicity, non-cancer', 'sea water', 'CTUnoncancer'),
    'Human health CF  [CTUcancer/kg], Emission to cont. natural soil, cancer': QInfo(
        'Human health toxicity, cancer', 'soil', 'CTUcancer'),
    'Human health CF  [CTUnoncancer/kg], Emission to cont. natural soil, non-canc.': QInfo(
        'Human health toxicity, non-cancer', 'soil', 'CTUnoncancer'),
    'Human health CF  [CTUcancer/kg], Emission to cont. agric. Soil, cancer': QInfo(
        'Human health toxicity, cancer', 'agricultural', 'CTUcancer'),
    'Human health CF  [CTUnoncancer/kg], Emission to cont. agric. Soil, non-canc.': QInfo(
        'Human health toxicity, non-cancer', 'agricultural', 'CTUnoncancer')
}


CAS_regexp = re.compile('^[0-9]{,6}-[0-9]{2}-[0-9]$')


def transform_numeric_cas(numeric_cas):
    if numeric_cas is None:
        return None
    if isinstance(numeric_cas, str):
        if numeric_cas == 'x':
            return None
        if bool(CAS_regexp.match(numeric_cas)):
            return numeric_cas
    ss = str(int(numeric_cas))
    return '-'.join([ss[:-3], ss[-3:-1], ss[-1:]])


def transform_string_cas(string_cas):
    return int(''.join([x for x in filter(lambda y: y != '-', string_cas)]))


class Traci21Factors(NsUuidArchive):

    def __init__(self, source, ref=None, sheet_name='Substances', mass_quantity=None, **kwargs):
        if ref is None:
            ref = '.'.join(['local', 'traci', '2', '1', 'spreadsheet'])
        super(Traci21Factors, self).__init__(source, ref=ref, **kwargs)

        print('Loading workbook')
        self._xl = xlrd.open_workbook(self.source)

        self._serialize_dict['sheet_name'] = sheet_name

        self._rows = XlDict.from_sheetname(self._xl, sheet_name)

        self._methods = dict()  # store column-to-method mapping

        if mass_quantity is None:
            self._mass = self._create_quantity('Mass', 'kg')
        else:
            self.add(mass_quantity)
            self._mass = mass_quantity

    @staticmethod
    def _flow_key(flowable, compartment):
        return ', '.join([flowable, compartment])

    def _create_quantity(self, name, unitstring):
        u = self._key_to_id(name)
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

    def _check_methods(self, col):
        if col in self._methods:
            return self._methods[col]
        q = self._create_lcia_method(q_info[col])
        self._methods[col] = q
        return q

    def _create_flow(self, row, compartment):
        flowable = row['Substance Name'].lower()
        cas = transform_numeric_cas(row['Formatted CAS #'])
        ext_ref = self._flow_key(flowable, compartment)
        u = self._key_to_id(ext_ref)
        f = self[u]
        if f is None:
            f = LcFlow(u, external_ref=ext_ref, Name=flowable, Compartment=[compartment], ReferenceQuantity=self._mass,
                       CasNumber=cas or '')
            self.add(f)
        return f

    def _add_flowable(self, row):
        for col, i in q_info.items():
            q = self._check_methods(col)
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

    def _load_all(self):
        for i, row in self._rows.iterrows():
            self._add_flowable(row)

    """
    Quantity interface
    made the job difficult by electing not to make the archive static.  Trade 2 sec of load time every use for ~2
    hours of dev time. Pays off only if I load TRACI factors >3600 times.
    """

    def lcia_methods(self, **kwargs):
        qs = set()
        for col, val in q_info.items():
            q = self._check_methods(col)
            qs.add(q)
        for q in sorted(list(qs), key=lambda x: x['Name']):
            if self._narrow_search(q, **kwargs):
                yield q

    def get_quantity(self, name):
        for col, qi in q_info.items():
            if qi.Category == name:
                return self._check_methods(col)
        return None

    def _char_from_row_compartment_method(self, row, cm, q, cf):
        flow = self._create_flow(row, cm)
        return Characterization(flow, q, value=cf)

    def _cf_for_method_and_compartment(self, row, method=None, compartment=None):
        for col, val in row.items():
            if col not in q_info:
                continue
            q = self._check_methods(col)
            if method is None or q is method:
                cm = q_info[col].Compartment
                if compartment is None or cm == compartment:
                    try:
                        cf = float(val)
                    except ValueError:
                        continue
                    if cf != 0.0:
                        yield self._char_from_row_compartment_method(row, cm, q, cf)

    def _row_for_key(self, key):
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

    def flowables(self, quantity=None, compartment=None):
        if quantity is not None:
            quantity = self.get_quantity(quantity)
        for i, row in self._rows.iterrows():
            try:
                next(self._cf_for_method_and_compartment(row, method=quantity, compartment=compartment))
            except StopIteration:
                continue
            yield transform_numeric_cas(row['CAS #']), row['Substance Name'].lower()

    def compartments(self, quantity=None, flowable=None):
        if quantity is not None:
            quantity = self.get_quantity(quantity)
        if flowable is not None:
            flowable = next(self._row_for_key(flowable))
        comps = set()
        for col, val in q_info.items():
            if quantity is None or val.Category == quantity['Category']:
                if flowable is None:
                    comps.add(val.Compartment)
                else:
                    try:
                        cf = float(flowable[col])
                    except ValueError:
                        continue
                    if cf != 0:
                        comps.add(val.Compartment)
        for c in comps:
            yield c

    def factors(self, quantity, flowable=None, compartment=None):
        quantity = self.get_quantity(quantity)
        if flowable is not None:
            for row in self._row_for_key(flowable):
                for cf in self._cf_for_method_and_compartment(row, method=quantity, compartment=compartment):
                    yield cf
        else:
            for i, row in self._rows.iterrows():
                for cf in self._cf_for_method_and_compartment(row, method=quantity, compartment=compartment):
                    yield cf
