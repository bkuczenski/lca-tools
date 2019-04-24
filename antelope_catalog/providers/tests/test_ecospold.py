import unittest

from lcatools.entities import LcFlow
from ..ecospold import EcospoldV1Archive, apply_conversion


class EcospoldV1Test(unittest.TestCase):
    def test_unit_conversion(self):
        ar = EcospoldV1Archive('/tmp')
        kbq = ar._create_quantity('kBq')
        bq = ar._create_quantity('Bq')
        f = LcFlow(1, referenceQuantity=bq, Name='my bq flow')
        ar.add(f)
        self.assertEqual(kbq.cf(f), 0.0)
        self.assertTrue(apply_conversion(kbq, f))
        self.assertEqual(kbq.cf(f), 0.001)


if __name__ == '__main__':
    unittest.main()
