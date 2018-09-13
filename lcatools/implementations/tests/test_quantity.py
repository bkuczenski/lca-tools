from ...archives import archive_from_json
from ...qdb import IPCC_2007_GWP

import unittest

ar = archive_from_json(IPCC_2007_GWP)
gwp = ar['Global Warming Air']


class QuantityImplementation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.qq_traci = ar.make_interface('quantity')

    def test_gwp_factor(self):
        q = self.qq_traci['Global Warming Air']
        self.assertEqual(self.qq_traci.cf('hfc-134, air', q), 1100.0)
        self.assertEqual(self.qq_traci.cf('hfc-143, air', q), 353.0)


if __name__ == '__main__':
    unittest.main()
