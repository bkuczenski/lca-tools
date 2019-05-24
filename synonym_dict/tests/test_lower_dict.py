from ..lower_dict import LowerDict
import unittest


class LcEntityTest(unittest.TestCase):
    def test_iterable(self):
        e = LowerDict((('a', 1), ('b', 2), ('c', 3)))
        self.assertEqual(e['A'], 1)
        self.assertEqual(e['C'], 3)
        with self.assertRaises(KeyError):
            print(e['d'])

    def test_get(self):
        e = LowerDict(Quorum=42, Comment='')
        self.assertEqual(e.get('Quorum'), 42)

    def test_case(self):
        e = LowerDict(Quorum=42, Comment='')
        self.assertEqual(e['quorum'], 42)
        self.assertEqual(e['comment'], '')

    def test_contains(self):
        e = LowerDict(Annotation='a subtle yet profound statement', domain='society')
        self.assertTrue('annotation' in e.keys())
        self.assertTrue('Annotation ' in e.keys())
        self.assertTrue(' dOmAiN' in e.keys())
        self.assertFalse('SoCiEtY' in e.keys())
        self.assertFalse('annotat' in e.keys())

    def test_pop(self):
        e = LowerDict(Quorum=42, comment='no comment.')
        self.assertEqual(e.pop('quorum'), 42)
        self.assertIsNone(e.get('Quorum'))
        self.assertEqual(e.pop('COMMENT'), 'no comment.')
        self.assertIsNone(e.get('comment'))

    def test_update(self):
        e = LowerDict(comment='argh')
        self.assertEqual(e['Comment '], 'argh')
        d = {'Comment': 'no comment', 'Name': 'no name'}
        e.update(d)
        self.assertTrue('comment' in e)
        self.assertTrue('Comment ' in e)
        self.assertEqual(e['comment'], 'no comment')
        self.assertTrue('name' in e)
        self.assertTrue('Name' in e.keys())
        self.assertFalse('namee' in e.keys())

    def test_items(self):
        e = LowerDict(Name='James Bond', Number='007')
        d = {k: v for k, v in e.items()}
        self.assertEqual(d.pop('Number'), '007')


if __name__ == '__main__':
    unittest.main()
