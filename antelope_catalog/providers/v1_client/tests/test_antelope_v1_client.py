import unittest

from antelope_catalog.data_sources.local import TEST_ROOT
from antelope_catalog import LcCatalog

from lcatools.interfaces import IndexRequired

cat = LcCatalog(TEST_ROOT)
ref = 'calrecycle.antelope'

cat.new_resource(ref, 'http://www.antelope-lca.net/uo-lca/api/', 'AntelopeV1Client',
                 store=False, interfaces=['index', 'inventory', 'quantity'], quiet=True)

ar = cat.get_archive(ref)


class AntelopeV1Client(unittest.TestCase):
    def test_stages(self):
        self.assertEqual(len(ar.get_endpoint('stages')), 87)

    def test_stagename(self):
        inv = ar.make_interface('inventory')
        self.assertEqual(inv.get_stage_name('42'), 'Natural Gas')
        self.assertEqual(inv.get_stage_name('47'), 'Natural Gas Supply')
        self.assertEqual(inv.get_stage_name('81'), 'WWTP')

    def test_impactcategory(self):
        self.assertEqual(ar._get_impact_category(6), 'Cancer human health effects')
        with self.assertRaises(ValueError):
            ar._get_impact_category(5)

    def test_nonimpl(self):
        with self.assertRaises(IndexRequired):
            next(cat.query(ref).terminate('flows/87'))

    def test_traversal(self):
        ffs = cat.query(ref).get('fragments/47').traverse()
        self.assertEqual(len(ffs), 14)
        self.assertSetEqual({-0.5, -0.01163, -0.0102, 0.0, 0.5}, set(round(x.node_weight, 5) for x in ffs))

    def test_lcia(self):
        lcia = cat.query(ref).get('fragments/19').fragment_lcia('lciamethods/4')
        self.assertSetEqual(set(x.external_ref for x in lcia.component_entities()),
                            {'Crude Oil', 'Electricity', 'Natural Gas', 'Refinery'})
        self.assertSetEqual(set(round(x.cumulative_result, 10) for x in lcia.components()),
                            {0.0004522897, 0.0000733389, 0.0000419222, 0.0001582613})
        self.assertAlmostEqual(lcia.total(), 0.0007258121306, places=12)


if __name__ == '__main__':
    unittest.main()