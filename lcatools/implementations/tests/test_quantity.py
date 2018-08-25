from ...archives import archive_from_json
from ...qdb import IPCC_2007_GWP
from ...interfaces import EntityNotFound

import unittest

ar = archive_from_json(IPCC_2007_GWP)
gwp = ar['Global Warming Air']


class QuantityImplementation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.qq_traci = ar.make_interface('quantity')
        cls.gwp = cls.qq_traci['Global Warming Air']

    def test_bad_ref(self):
        with self.assertRaises(EntityNotFound):
            self.qq_traci.cf('hfc-134', self.gwp)

    def test_gwp_factor(self):
        self.assertEqual(self.qq_traci.cf('hfc-134, air', self.gwp), 1100.0)
        self.assertEqual(self.qq_traci.cf('hfc-143, air', self.gwp), 353.0)


if __name__ == '__main__':
    unittest.main()
