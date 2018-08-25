from .base_testclass import BasicEntityTest, archive_from_json
import unittest

from ...qdb import IPCC_2007_GWP


class QuantitiesTest(BasicEntityTest):
    @classmethod
    def setUpClass(cls):
        super(QuantitiesTest, cls).setUpClass()
        cls.Q = archive_from_json(IPCC_2007_GWP)
        cls.gwp = cls.Q['Global Warming Air']

    def test_retrieve_quantity(self):
        self.assertIsNotNone(self.gwp)
        self.assertEqual(self.gwp.entity_type, 'quantity')

    def test_quantity_cf_flowable(self):
        cf = list(self.gwp.factors(flowable='hfc-134'))
        self.assertEqual(len(cf), )