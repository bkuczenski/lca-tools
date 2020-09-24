import unittest
from .. import LcFlow, LcQuantity
from antelope import CatalogRef


q = LcQuantity.new('Dummy quantity', 'dum', origin='test.origin')
qref = CatalogRef(q.origin, q.external_ref, entity_type='quantity')


class EntityRefTest(unittest.TestCase):
    def test_equality(self):
        self.assertEqual(q, qref)
        self.assertEqual(qref, q)

    def test_hash(self):
        self.assertEqual(hash(q), hash(qref))

    def test_hash_equiv(self):
        f = LcFlow('dummy flow', referenceQuantity=q, origin=q.origin)
        fref = CatalogRef(f.origin, f.external_ref, entity_type='flow')
        s = set()
        s.add(f)
        s.add(fref)
        self.assertEqual(len(s), 1)
        d = dict()
        d[f] = 42
        self.assertIn(fref, d)
        self.assertEqual(d[fref], 42)


if __name__ == '__main__':
    unittest.main()
