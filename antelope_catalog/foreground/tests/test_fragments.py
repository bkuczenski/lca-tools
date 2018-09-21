"""
For fragment tests, we are using the CalRecycle model to provide test cases
"""

import unittest
from math import floor

from ... import LcCatalog
from ...data_sources.local import make_config, check_enabled, CATALOG_ROOT

if check_enabled('calrecycle'):
    cat = LcCatalog(CATALOG_ROOT)
    crc = make_config('calrecycle')
    fg = crc.foreground(cat)


@unittest.skipUnless(check_enabled('calrecycle'), 'CalRecycle not enabled')
class FragmentTests(unittest.TestCase):

    def test_passthrough(self):
        lm = fg['fragments/3']
        inv, _ = lm.unit_inventory(None, observed=True)
        self.assertEqual(len(inv), 3)  # 3 inventory flows
        self.assertEqual(len(set(x.fragment.flow for x in inv)), 2)  # 2 distinct flows
        self.assertSetEqual(set(x.fragment.direction for x in inv if x.fragment.flow == lm.flow),
                            {'Input', 'Output'})  # 2 distinct directions for reference flow
        self.assertSetEqual(set(x.magnitude for x in inv), {1})  # all flows have magnitude 1

    def test_autoconsumption(self):
        pass  # no autoconsumption fragments in CalRecycle :(

    def test_traversal(self):
        uom = fg['fragments/55']
        ffs = uom.traverse(None, observed=True)
        self.assertEqual(len(ffs), 332)
        inv = uom.inventory(None, observed=True)
        light_fuel = next(x for x in inv if x.flow['Name'].startswith('Light Fuel'))
        self.assertEqual(floor(light_fuel.value), 116915718)
        incin = next(x for x in inv if x.flow['Name'].startswith('UO to Incin'))
        self.assertEqual(floor(incin.value), 484433)

    def test_lcia_gwp(self):
        pass


if __name__ == '__main__':
    unittest.main()
