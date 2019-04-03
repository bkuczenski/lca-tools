from ..entities import LcEntity
import unittest
from uuid import uuid4


class LcEntityTest(unittest.TestCase):
    def test_init(self):
        self.assertIsInstance(LcEntity('goober', uuid4()), LcEntity)

    def test_case(self):
        e = LcEntity('my goober', uuid4(), Quorum=42)
        self.assertEqual(e['quorum'], 42)
        self.assertEqual(e['comment'], '')

    def test_contains(self):
        e = LcEntity('the biggest goober', uuid4(), Annotation='a subtle yet profound statement', domain='society')
        self.assertTrue('annotation' in e.keys())
        self.assertTrue(e.has_property('annotation'))
        self.assertTrue('Annotation' in e.keys())
        self.assertTrue('dOmAiN' in e.keys())
        self.assertTrue(e.has_property('dOmAiN'))
        self.assertFalse('SoCiEtY' in e.keys())
        self.assertFalse('annotat' in e.keys())


if __name__ == '__main__':
    unittest.main()
