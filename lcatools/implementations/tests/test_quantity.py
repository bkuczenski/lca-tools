from ...archives import BasicArchive, Qdb
from ...lcia_engine import IPCC_2007_GWP
from antelope import EntityNotFound, ConversionReferenceMismatch  # , NoFactorsFound
from ..quantity import RefQuantityRequired

import unittest

ar = BasicArchive.from_file(IPCC_2007_GWP)
gwp = ar['Global Warming Air']
mass = ar['Mass']

volu_uuid = '93a60a56-a3c8-22da-a746-0800200c9a66'


class QuantityImplementation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.I = Qdb.new()
        cls.qq_traci = ar.make_interface('quantity')
        cls.gwp = cls.qq_traci['Global Warming Air'].make_ref(cls.qq_traci)

    def test_bad_ref(self):
        with self.assertRaises(RefQuantityRequired):
            self.qq_traci.cf('hfc-134', self.gwp)

    def test_gwp_factor(self):
        self.assertEqual(self.qq_traci.cf('hfc-134', self.gwp, ref_quantity=mass, context='air'), 1100.0)
        self.assertEqual(self.qq_traci.cf('hfc-143', self.gwp, ref_quantity=mass, context='air'), 353.0)

    def test_quantity_cf_flowable(self):
        cf = list(self.gwp.factors(flowable='hfc-134'))
        self.assertEqual(len(cf), 1)
        self.assertEqual(cf[0].value, 1100.0)

    def test_list_of_flowables(self):
        cf = list(self.gwp.factors())
        self.assertEqual(len(cf), 91)

    def test_q_relation(self):
        self.assertEqual(self.gwp.cf('carbon dioxide', ref_quantity=mass, context='air'), 1.0)
        self.assertEqual(self.gwp.cf('nitrous oxide', ref_quantity=mass, context='air'), 298.0)
        self.assertEqual(self.gwp.cf('10024-97-2', ref_quantity=mass, context='air'), 298.0)
        self.assertEqual(self.gwp.quantity_relation(mass, 'carbon dioxide', 'water').value, 0.0)
        # this will only work with an LciaEngine
        ilcd_vol = self.I[volu_uuid]
        ar.add(ilcd_vol)  ## no longer done automatically in get_canonical
        with self.assertRaises(ConversionReferenceMismatch):
            self.gwp.quantity_relation(ilcd_vol, 'carbon tetrachloride', 'air')


if __name__ == '__main__':
    unittest.main()
