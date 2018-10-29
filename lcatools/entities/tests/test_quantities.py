from .base_testclass import archive_from_json
import unittest

from lcatools import BasicQuery

from ...qdb import IPCC_2007_GWP, REF_QTYS

mass_uuid = '93a60a56-a3c8-11da-a746-0800200b9a66'


class QuantitiesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(QuantitiesTest, cls).setUpClass()
        cls.I = archive_from_json(REF_QTYS)
        cls.Q = archive_from_json(IPCC_2007_GWP)
        cls.gwp = cls.Q['Global Warming Air']

    def test_retrieve_quantity(self):
        self.assertIsNotNone(self.gwp)
        self.assertEqual(self.gwp.entity_type, 'quantity')

    def test_is_lcia_method(self):
        self.assertTrue(self.gwp.is_lcia_method())

    def test_convert(self):
        mass = self.I[mass_uuid]
        self.assertAlmostEqual(mass.convert('lb av'), 0.453592, 6)
        self.assertAlmostEqual(mass.convert(to='lb av'), 2.20462262, 6)

    def test_valid_make_ref(self):
        gr = self.gwp.make_ref(BasicQuery(self.Q))
        self.assertIsInstance(gr._name, str)


if __name__ == '__main__':
    unittest.main()
