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
    fg = crc.foreground(cat, fg_path='uolca')
    if fg.frag('aed7a').is_background:
        fg.frag('aed7a')._background = False  # postfix defect in repo generation
    if 'calrecycle.antelope' not in cat.references:
        cat.new_resource('calrecycle.antelope', 'http://www.antelope-lca.net/uo-lca/api/', 'AntelopeV1Client',
                         store=False, interfaces=['index', 'inventory', 'quantity'], quiet=True)

else:
    fg = None


def _get_antelope_result(frag_id, lcia_id):
    key = 'fragments/%d' % frag_id
    lcia = 'lciamethods/%d' % lcia_id
    return cat.query('calrecycle.antelope').get(key).fragment_lcia(lcia)


@unittest.skipUnless(_run_calrecycle, "CalRecycle test not enabled")
class CalRecycleTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.gwp = cat.query('calrecycle.lcia.elcd').get('370960f4-0a3a-415d-bf3e-e5ce63160bb9')
        cls.mep = cat.query('calrecycle.lcia.elcd').get('5296e2be-060b-4e50-b033-d45f85f6ac92')
        cls.criteria = cat.query('calrecycle.lcia.traci').get('de6753e3-2e0a-4fb8-b113-331735e26e3d')
        cls.lcia_map = {2: cls.gwp, 4: cls.mep, 16: cls.criteria}

    def test_create_and_load(self):
        self.assertEqual(fg.count_by_type('fragment'), 888)

    def test_scenarios(self):
        uom = fg['fragments/55']
        self.assertSetEqual(set(uom.scenarios()), {'sp24', 'quell_eg'})

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
        self.assertEqual(len(ffs), 322)  # some ffs went away with uslci bg change 43d35c7; a few came back with 110321a
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
        # self.assertEqual(floor(res.total()), -94202323)  # legacy Antelope result-- eg fail; private processes differ
        self.assertEqual(floor(res.total()), -94671945)  # current correct value 2018/12/28
        res_eg = uom.fragment_lcia(self.gwp, scenario='quell_eg')
        self.assertEqual(floor(res_eg.total()), -94357314)  # quell_eg value 2018/12/28
        res = uom.fragment_lcia(self.criteria, scenario='quell_eg')
        self.assertEqual(floor(res.total()), 25278)  # current quell_eg value 2018/12/28

    def test_antelope_frag_2_ng_conserv(self):
        ng2 = fg['fragments/2']
        ffs_loc = [f for f in ng2.traverse(observed=True) if f.is_conserved]
        ffs_rem = [f for f in cat.query('calrecycle.antelope').get('fragments/2').traverse() if f.is_conserved]
        self.assertEqual(len(ffs_loc), len(ffs_rem))
        for ff in ffs_rem:
            ffl = next(f for f in ffs_loc if f.fragment.flow['Name'] == ff.fragment.flow['Name'])
            if ff.fragment.flow['Name'] == ng2.flow['Name']:  # reference fragment: normalization works differently now
                norm = next(x.value for x in ng2.term.term_node.exchange_values(ng2.flow))
            else:
                norm = 1.0
            self.assertAlmostEqual(ff.node_weight, ffl.node_weight / norm, places=12)

    def test_antelope_frag_2_ng_gwp(self):
        ng = fg['fragments/2']
        res = ng.fragment_lcia(self.gwp).aggregate()
        res_remote = _get_antelope_result(2, 2)
        for key in ('PE', 'US LCI', 'Natural Gas Supply'):
            self.assertAlmostEqual(res[key].cumulative_result, res_remote[key].cumulative_result, places=12)
        self.assertAlmostEqual(res['EI'].cumulative_result, res_remote['EI'].cumulative_result, places=4)
        self.assertNotAlmostEqual(res['EI'].cumulative_result, res_remote['EI'].cumulative_result, places=6)

    def _validate_antelope(self, frag_id):
        frag_l = fg['fragments/%d' % frag_id]
        for lcia_id in (2, 4, 16):
            res_a = _get_antelope_result(frag_id, lcia_id)
            res_l = frag_l.fragment_lcia(self.lcia_map[lcia_id], scenario='quell_eg')
            self.assertAlmostEqual(res_a.total(), res_l.total(), places=10)

    def test_fragment_lcia(self):
        for frag_id in (15, 20, 24, 30, 36, 42, 44, 50):
            self._validate_antelope(frag_id)


if __name__ == '__main__':
    unittest.main()
