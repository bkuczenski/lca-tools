import unittest

from .. import LciaDb
from lcatools.entities import LcQuantity


class LciaEngineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lcia = LciaDb.new()

    def test_0_init(self):
        self.assertEqual(len([x for x in self.lcia.query.contexts()]), 36)
        self.assertEqual(len([x for x in self.lcia.query.flowables()]), 4004)
        self.assertEqual(len([x for x in self.lcia.query.quantities()]), 25)

    def test_1_add_characterization(self):
        rq = self.lcia.query.get_canonical('mass')
        qq = self.lcia.query.get_canonical('volume')

        cf = self.lcia.query.characterize('water', rq, qq, .001)
        self.assertEqual(cf.origin, self.lcia.ref)

    def test_2_cf(self):
        self.assertEqual(self.lcia.query.quantity_relation('water', 'mass', 'volume', None).value, .001)
        self.assertEqual(self.lcia.query.quantity_relation('water', 'volume', 'mass', None).value, 1000.0)

    def test_3_dup_mass(self):
        dummy = 'dummy_external_ref'
        dup_mass = LcQuantity(dummy, referenceUnit='kg', Name='Mass', origin='dummy.origin')
        self.lcia.tm.add_quantity(dup_mass)
        self.assertEqual(self.lcia.query.get_canonical(dummy), self.lcia.query.get_canonical('mass'))


if __name__ == '__main__':
    unittest.main()
