import unittest

from ... import LcCatalog
from ..local import CATALOG_ROOT, make_config, check_enabled

_run_test = check_enabled('ipcc2007')


if _run_test:
    cat = LcCatalog(CATALOG_ROOT)
    cfg = make_config('ipcc2007')
    ref = next(cfg.references)


class GwpIpcc2007Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.query = cat.query(ref)

    def test_resources_exist(self):
        self.assertIn(ref, cat.references)

    def test_num_entities(self):
        self.assertEqual(self.query.count('quantity'), 2)
        self.assertEqual(self.query.count('flow'), 0)

    def test_gwp(self):
        gwp = next(self.query.lcia_methods())
        self.assertEqual(gwp.name, 'Global Warming Air [kg CO2 eq] [LCIA]')
        self.assertEqual(len([k for k in gwp.factors()]), 91)


if __name__ == '__main__':
    unittest.main()
