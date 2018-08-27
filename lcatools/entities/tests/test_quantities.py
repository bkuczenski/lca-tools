from .base_testclass import BasicEntityTest, archive_from_json
from ..quantities import NoFactorsFound, ConversionReferenceMismatch
import unittest
from math import isclose

from ...qdb import IPCC_2007_GWP, REF_QTYS

mass_uuid = '93a60a56-a3c8-11da-a746-0800200b9a66'
volu_uuid = '93a60a56-a3c8-22da-a746-0800200c9a66'


class QuantitiesTest(BasicEntityTest):
    @classmethod
    def setUpClass(cls):
        super(QuantitiesTest, cls).setUpClass()
        cls.I = archive_from_json(REF_QTYS)
        cls.Q = archive_from_json(IPCC_2007_GWP)
        cls.gwp = cls.Q['Global Warming Air']

    def test_retrieve_quantity(self):
        self.assertIsNotNone(self.gwp)
        self.assertEqual(self.gwp.entity_type, 'quantity')
        self.assertTrue(self.gwp.is_lcia_method())

    def test_quantity_cf_flowable(self):
        cf = list(self.gwp.factors(flowable='hfc-134'))
        self.assertEqual(len(cf), 1)
        self.assertEqual(cf[0].value, 1100.0)

    def test_list_of_flowables(self):
        cf = list(self.gwp.factors())
        self.assertEqual(len(cf), 91)

    def test_q_relation(self):
        mass = self.Q['Mass']
        self.assertEqual(self.gwp.quantity_relation(mass, 'carbon dioxide', 'air'), 1.0)
        self.assertEqual(self.gwp.quantity_relation(mass, 'nitrous oxide', 'air'), 298.0)
        with self.assertRaises(NoFactorsFound):
            self.gwp.quantity_relation(mass, '10024-97-2', 'air')  # this should be rectified with an LciaEngine
        ilcd_vol = self.I[volu_uuid]
        with self.assertRaises(ConversionReferenceMismatch):
            self.gwp.quantity_relation(ilcd_vol, 'carbon tetrachloride', 'air')

    def test_convert(self):
        mass = self.I[mass_uuid]
        self.assertAlmostEqual(mass.convert('lb av'), 0.453592, 6)
        self.assertAlmostEqual(mass.convert(to='lb av'), 2.20462262, 6)


if __name__ == '__main__':
    unittest.main()
