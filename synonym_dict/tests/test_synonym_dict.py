from ..synonym_dict import SynonymDict, TermExists, MergeError
from ..synonym_set import SynonymSet, RemoveNameError
import unittest


class SynonymDictTest(unittest.TestCase):
    def test_new_object(self):
        g = SynonymDict()
        g.new_object('hello', 'hola', 'hi')
        self.assertEqual(g.get('hola'), 'hello')

    def test_add_object(self):
        g = SynonymDict()
        g.new_object('hello', 'hola', 'hi')
        s = SynonymSet('goodbye', 'au revoir', 'adios')
        g.add_or_update_object(s)
        self.assertEqual(g.get('adios'), 'goodbye')

    def test_implicit_merge(self):
        g = SynonymDict()
        g.new_object('hello', 'hola', 'hi', 'aloha')
        with self.assertRaises(TermExists):
            g.new_object('goodbye', 'au revoir', 'aloha', merge=False)
        g.new_object('goodbye', 'au revoir', 'adios')
        with self.assertRaises(MergeError):
            g.new_object('yes', 'no', 'goodbye', 'hello')
        g.new_object('hi', 'Ni hao', 'hallo')
        self.assertEqual(g['hello'], g['hallo'])

    def test_explicit_merge(self):
        g = SynonymDict()
        o = g.new_object('hello', 'hola', 'hi', 'aloha')
        g.new_object('Hello', 'HELLO', 'Hi', 'HI')
        self.assertNotEqual(g['hi'], g['HI'])
        g.merge('hi', 'HI')
        self.assertEqual(g['hi'], g['HI'])
        self.assertListEqual([k for k in g.objects], [o])  # sort order by string

    def test_child(self):
        g = SynonymDict()
        ob1 = g.new_object('hello', 'hola', 'hi', 'aloha')
        ob2 = g.new_object('hi', 'Ni hao', 'hallo', create_child=True)
        self.assertTrue(ob1.has_child(ob2))
        ob3 = g.new_object('greetings', 'salutations', 'hello')
        self.assertFalse(ob1.has_child(ob3))
        self.assertEqual(g.get('greetings'), g.get('hola'))

    def test_objects(self):
        g = SynonymDict()
        o1 = g.new_object('hello', 'hola', 'hi', 'aloha')
        o2 = g.new_object('Hello', 'HELLO', 'Hi', 'HI')
        self.assertListEqual([k for k in g.objects], [o2, o1])  # sort order by string

    def test_remove_child(self):
        g = SynonymDict()
        ob1 = g.new_object('hello', 'hola', 'hi', 'aloha')
        ob2 = g.new_object('hi', 'Ni hao', 'hallo', create_child=True)
        self.assertEqual(g['hello'], g['hallo'])
        with self.assertRaises(TermExists):
            g.unmerge_child(ob2)
        self.assertTrue(ob1.has_child(ob2))
        with self.assertRaises(RemoveNameError):
            ob2.remove_term('hi')
        ob2.set_name('hallo')
        ob2.remove_term('hi')
        g.unmerge_child(ob2)
        self.assertNotEqual(g['hello'], g['hallo'])
        self.assertEqual(g['Ni hao'], str(ob2))

    def test_case_insensitive(self):
        g = SynonymDict(ignore_case=True)
        g.new_object('hello', 'hola', 'hi', 'aloha')
        g.new_object('Hi', 'Bonjour')
        self.assertEqual(g['bonjour'], 'hello')

    def test_remove_term_case_insensitive(self):
        g = SynonymDict(ignore_case=True)
        o = g.new_object('hello', 'hola', 'hi', 'aloha')
        self.assertIn('Hi', g)
        self.assertIn('hi', o)
        self.assertNotIn('Hi', o)
        g.del_term('Hi')
        self.assertNotIn('Hi', g)
        self.assertNotIn('hi', o)

    def test_remove_object(self):
        g = SynonymDict()
        o1 = g.new_object('hello', 'hola', 'hi', 'aloha')
        o2 = g.new_object('Hello', 'HELLO', 'Hi', 'HI')
        self.assertEqual(g['HELLO'], str(o2))
        g.remove_object(o2)
        self.assertListEqual([k for k in g.objects], [o1])
        with self.assertRaises(KeyError):
            _ = g['HELLO']


if __name__ == '__main__':
    unittest.main()
