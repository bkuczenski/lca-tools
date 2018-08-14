from ..synonym_set import SynonymSet, RemoveNameError
import unittest


class SynonymSetTest(unittest.TestCase):
    def test_empty(self):
        s = SynonymSet()
        self.assertListEqual([k for k in s.terms], [])

    def test_terms(self):
        s = SynonymSet('hello', 'aloha', 'Ni hao')
        self.assertListEqual(['Ni hao', 'aloha', 'hello'], [k for k in s.terms])

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

    def test_contains(self):
        s = SynonymSet('hello', 'aloha', 'Ni hao')
        self.assertTrue('aloha' in s)
        self.assertFalse('greetings' in s)

    def test_hashable(self):
        s = SynonymSet('hello', 'aloha', 'Ni hao')
        t = SynonymSet('bonjour', 'hola', 'hello', 'Hi')
        g = {s, t}
        self.assertEqual(len(g), 2)


if __name__ == '__main__':
    unittest.main()
