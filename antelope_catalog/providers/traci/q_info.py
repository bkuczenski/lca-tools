from collections import namedtuple


QInfo = namedtuple('QInfo', ['Category', 'Compartment', 'RefUnit'])


ns_uuid_21 = '150e35c3-ac4a-485b-826b-a41807ddf43a'

q_info_21 = {
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
