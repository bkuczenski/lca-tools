"""
This implements a quantity interface
"""

from collections import namedtuple
import xlrd
from lcatools.providers.base import NsUuidArchive, XlDict
from lcatools.entities import LcQuantity, LcFlow
from lcatools.characterizations import DuplicateCharacterizationError


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


class Traci21Factors(NsUuidArchive):

    def __init__(self, source, ref=None, sheet_name='Substances', mass_quantity=None, **kwargs):
        if ref is None:
            ref = '.'.join(['local', 'traci', '2', '1', 'spreadsheet'])
        super(Traci21Factors, self).__init__(source, ref=ref, **kwargs)

        print('Loading workbook')
        self._xl = xlrd.open_workbook(self.source)

        self._serialize_dict['sheet_name'] = sheet_name

        self._rows = XlDict.from_sheetname(self._xl, sheet_name)

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

    def _create_flow(self, flowable, compartment, cas=None):
        ext_ref = self._flow_key(flowable, compartment)
        u = self._key_to_id(ext_ref)
        f = self[u]
        if f is None:
            f = LcFlow(u, external_ref=ext_ref, Name=flowable, Compartment=[compartment], ReferenceUnit=self._mass)
            self.add(f)
            if cas is not None:
                f['CasNumber'] = cas
        return f

    def _add_flowable(self, row):
        flowable = row['Substance Name'].lower()
        cas = row['Formatted CAS #']
        if cas == 'x':
            cas = None
        for col, i in q_info.items():
            try:
                cf = float(row[col])
            except ValueError:
                continue
            if cf == 0.0:
                continue
            f = self._create_flow(flowable, i.Compartment, cas)
            q = self._create_quantity(i.Category, i.RefUnit)
            try:
                f.add_characterization(q, value=cf)
            except DuplicateCharacterizationError:
                continue

    def _load_all(self):
        for i, row in self._rows.iterrows():
            self._add_flowable(row)

    """
    Quantity interface
    """

