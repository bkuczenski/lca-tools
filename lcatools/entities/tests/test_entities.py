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

    def test_null_property(self):
        e = LcEntity('my sharona', uuid4(), Shame=None)
        self.assertFalse(e.has_property('Shame'))

    def test_contains(self):
        e = LcEntity('the biggest goober', uuid4(), Annotation='a subtle yet profound statement', domain='society')
        self.assertTrue(e.has_property('annotation'))
        self.assertTrue(e.has_property('Annotation'))
        self.assertTrue(e.has_property('dOmAiN'))
        self.assertTrue(e.has_property('dOmAiN'))
        self.assertFalse(e.has_property('SoCiEtY'))
        self.assertFalse(e.has_property('annotat'))


if __name__ == '__main__':
    unittest.main()
