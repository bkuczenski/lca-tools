"""
This module includes both CalRecycle data source unittests and also integration tests that use the CalRecycle
data source as a fixture.
"""

import unittest
from math import floor

from ... import LcCatalog

from ..local import CATALOG_ROOT, RESOURCES_CONFIG, check_enabled
from ..calrecycle_lca import CalRecycleConfig

_debug = True

if __name__ == '__main__':
    _run_calrecycle = check_enabled('calrecycle')
else:
    _run_calrecycle = check_enabled('calrecycle') or _debug


if _run_calrecycle:
    cat = LcCatalog(CATALOG_ROOT)
    crc = CalRecycleConfig(RESOURCES_CONFIG['calrecycle']['data_root'])
    fg = crc.foreground(cat)

else:
    fg = None


@unittest.skipUnless(_run_calrecycle, "CalRecycle test not enabled")
class CalRecycleTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.gwp = cat.query('calrecycle.lcia.elcd').get('370960f4-0a3a-415d-bf3e-e5ce63160bb9')
        cls.criteria = cat.query('calrecycle.lcia.traci').get('de6753e3-2e0a-4fb8-b113-331735e26e3d')

    def test_create_and_load(self):
        self.assertEqual(fg.count_by_type('fragment'), 888)

    def test_named_fragment(self):
        self.assertEqual(fg['fragments/43']['Name'], 'Fuels Displacement Mixer - distillate')

    def test_passthrough(self):
        lm = fg['fragments/3']
        inv, _ = lm.unit_inventory(None, observed=True)
        self.assertEqual(len(inv), 3)  # 3 inventory flows
        self.assertEqual(len(set(x.fragment.flow for x in inv)), 2)  # 2 distinct flows
        self.assertSetEqual(set(x.fragment.direction for x in inv if x.fragment.flow == lm.flow),
                            {'Input', 'Output'})  # 2 distinct directions for reference flow
        self.assertSetEqual(set(x.magnitude for x in inv), {1})  # all flows have magnitude 1

    def test_traversal(self):
        uom = fg['fragments/55']
        ffs = uom.traverse(None, observed=True)
        self.assertEqual(len(ffs), 332)
        inv = uom.inventory(None, observed=True)
        light_fuel = next(x for x in inv if x.flow['Name'].startswith('Light Fuel'))
        self.assertEqual(floor(light_fuel.value), 116915718)
        incin = next(x for x in inv if x.flow['Name'].startswith('UO to Incin'))
        self.assertEqual(floor(incin.value), 484433)

    def test_lcia_0_indicators(self):
        self.assertEqual(len([x for x in self.gwp.flowables()]), 100)
        self.assertEqual(len([x for x in self.criteria.flowables()]), 6)

    def test_lcia_1_rere(self):
        re_re = fg['fragments/52'].term
        res = re_re.term_node.fg_lcia(self.gwp, ref_flow=re_re.term_flow)
        self.assertAlmostEqual(res.total(), 0.32561424, places=6)
        res = re_re.term_node.fg_lcia(self.criteria, ref_flow=re_re.term_flow)
        self.assertAlmostEqual(res.total(), 0.00043044969, places=9)

    def test_lcia_2_frag_elec(self):
        elec = fg['fragments/1']
        res = elec.fragment_lcia(self.gwp)
        self.assertAlmostEqual(res.total(), 0.165043782, places=8)
        res = elec.fragment_lcia(self.criteria)
        self.assertAlmostEqual(res.total(), 0.0003245430, places=9)

    def test_lcia_2_frag_uom(self):
        uom = fg['fragments/55']
        res = uom.fragment_lcia(self.gwp)
        self.assertEqual(floor(res.total()), -94202323)
        res = uom.fragment_lcia(self.criteria)
        self.assertEqual(floor(res.total()), 25598)



if __name__ == '__main__':
    unittest.main()
