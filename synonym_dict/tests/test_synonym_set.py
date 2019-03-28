from ..synonym_set import SynonymSet, RemoveNameError, DuplicateChild
import unittest


class SynonymSetTest(unittest.TestCase):
    def test_empty(self):
        s = SynonymSet()
        self.assertListEqual([k for k in s.terms], [])

    def test_add_empty(self):
        s = SynonymSet('hello', 'aloha', 'Ni hao')
        self.assertNotIn('', s)
        s.add_term('')
        self.assertNotIn('', s)

    def test_terms(self):
        """
        first term is name, always shows first
        remaining terms show in alphabetical order
        :return:
        """
        s = SynonymSet('hello', 'aloha', 'Ni hao')
        self.assertListEqual(['hello', 'Ni hao', 'aloha'], [k for k in s.terms])

    def test_name(self):
        s = SynonymSet('hello', 'aloha', 'Ni hao')
        self.assertEqual(str(s), 'hello')
        self.assertEqual(s.object, 'hello')
        s.set_name('aloha')
        self.assertEqual(s.object, 'aloha')

    def test_add(self):
        s = SynonymSet('the answer', 'the answer to the ultimate question', 42)
        s.add_term('the ultimate answer')
        self.assertEqual(len([k for k in s.terms]), 4)

    def test_remove(self):
        s = SynonymSet('hello', 'aloha', 'Ni hao')
        with self.assertRaises(RemoveNameError):
            s.remove_term('hello')
        s.set_name('Ni hao')
        s.remove_term('hello')
        self.assertEqual(len([k for k in s.terms]), 2)

    def test_child(self):
        s = SynonymSet('hello', 'aloha', 'Ni hao')
        t = SynonymSet('bonjour', 'hola', 'hello', 'Hi')
        s.add_term(t)
        terms = [k for k in s.terms]
        self.assertTrue(s.has_child(t))
        self.assertEqual(len(terms), 6)
        self.assertSetEqual(set(terms), {'hello', 'aloha', 'Ni hao', 'bonjour', 'hola', 'Hi'})

    def test_remove_from_child(self):
        s = SynonymSet('hello', 'aloha', 'Ni hao')
        t = SynonymSet('bonjour', 'hola', 'hello', 'Hi')
        s.add_child(t)
        s.remove_term('hola')
        self.assertNotIn('hola', t)
        self.assertNotIn('hola', s)

    def test_contains(self):
        s = SynonymSet('hello', 'aloha', 'Ni hao')
        self.assertTrue('aloha' in s)
        self.assertFalse('greetings' in s)

    def test_strip(self):
        s = SynonymSet(' [Resources] ')
        self.assertEqual(s.name, '[Resources]')
        self.assertIn(' [Resources] ', s)

    def test_contains_string(self):
        s = SynonymSet('hello', 'aloha', 'Ni hao')
        self.assertEqual(s.contains_string('hao'), 'Ni hao')
        self.assertFalse(s.contains_string('goober'))

    def test_hashable(self):
        s = SynonymSet('hello', 'aloha', 'Ni hao')
        t = SynonymSet('bonjour', 'hola', 'hello', 'Hi')
        g = {s, t}
        self.assertEqual(len(g), 2)

    def test_avoid_duplicate_subsets(self):
        s = SynonymSet('hello', 'hi', 'greetings', 'salutations')
        t = SynonymSet('bonjour', 'aloha', 'ni hao', 'hola')
        u = SynonymSet('hola', 'bonjour', 'ni hao', 'aloha')
        s.add_term(t)
        self.assertEqual(len(s._children), 1)
        with self.assertRaises(DuplicateChild):
            s.add_term(u)
        self.assertEqual(len(s._children), 1)
        self.assertFalse(s.has_child(u))


if __name__ == '__main__':
    unittest.main()
