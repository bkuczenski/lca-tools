from ..lower_dict import LowerDict
import unittest


class LcEntityTest(unittest.TestCase):
    def test_iterable(self):
        e = LowerDict((('a', 1), ('b', 2), ('c', 3)))
        self.assertEqual(e['A'], 1)
        self.assertEqual(e['C'], 3)
        with self.assertRaises(KeyError):
            print(e['d'])

    def test_case(self):
        e = LowerDict(Quorum=42, Comment='')
        self.assertEqual(e['quorum'], 42)
        self.assertEqual(e['comment'], '')

    def test_contains(self):
        e = LowerDict(Annotation='a subtle yet profound statement', domain='society')
        self.assertTrue('annotation' in e.keys())
        self.assertTrue('Annotation' in e.keys())
        self.assertTrue('dOmAiN' in e.keys())
        self.assertFalse('SoCiEtY' in e.keys())
        self.assertFalse('annotat' in e.keys())


if __name__ == '__main__':
    unittest.main()
