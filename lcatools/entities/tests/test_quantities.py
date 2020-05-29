import unittest

from ..quantities import LcQuantity
from lcatools.archives import BasicArchive, Qdb


from lcatools.lcia_engine import IPCC_2007_GWP

mass_uuid = '93a60a56-a3c8-11da-a746-0800200b9a66'


class QuantitiesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(QuantitiesTest, cls).setUpClass()
        cls.I = Qdb.new()
        cls.Q = BasicArchive.from_file(IPCC_2007_GWP)
        cls.gwp = cls.Q['Global Warming Air']

    def test_default_conversion(self):
        a = LcQuantity.new('Floogles', 'fl')
        self.assertTrue(a.has_property('UnitConversion'))
        self.assertEqual(a.convert('fl'), 1.0)

    def test_retrieve_quantity(self):
        self.assertIsNotNone(self.gwp)
        self.assertEqual(self.gwp.entity_type, 'quantity')

    def test_is_lcia_method(self):
        self.assertTrue(self.gwp.is_lcia_method)

    def test_convert(self):
        mass = self.I[mass_uuid]
        self.assertAlmostEqual(mass.convert('lb av'), 0.453592, 6)
        self.assertAlmostEqual(mass.convert(to='lb av'), 2.20462262, 6)

    def test_convert_missing_refunit(self):
        q = self.I.make_interface('quantity').get_canonical('electric energy')
        self.assertTrue(q.has_property('UnitConversion'))
        self.assertNotIn(q.unit, q['UnitConversion'])
        self.assertEqual(q.convert(), 1.0)
        self.assertEqual(q.convert(to='MJ'), 3.6)

    def test_valid_make_ref(self):
        gr = self.gwp.make_ref(self.Q.query)
        self.assertIsInstance(gr.name, str)


if __name__ == '__main__':
    unittest.main()
