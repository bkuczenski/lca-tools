from ..synonym_dict import SynonymDict, TermExists, MergeError
from ..synonym_set import SynonymSet, RemoveNameError
import unittest


class SynonymDictTest(unittest.TestCase):
    def test_new_object(self):
        g = SynonymDict()
        g.new_entry('hello', 'hola', 'hi')
        self.assertEqual(g.get('hola'), 'hello')

    def test_add_object(self):
        g = SynonymDict()
        g.new_entry('hello', 'hola', 'hi')
        s = SynonymSet('goodbye', 'au revoir', 'adios')
        g.add_or_update_entry(s)
        self.assertEqual(g.get('adios'), 'goodbye')

    def test_implicit_merge(self):
        g = SynonymDict()
        g.new_entry('hello', 'hola', 'hi', 'aloha')
        with self.assertRaises(TermExists):
            g.new_entry('goodbye', 'au revoir', 'aloha', merge=False)
        g.new_entry('goodbye', 'au revoir', 'adios')
        with self.assertRaises(MergeError):
            g.new_entry('yes', 'no', 'goodbye', 'hello')
        g.new_entry('hi', 'Ni hao', 'hallo')
        self.assertEqual(g['hello'], g['hallo'])

    def test_explicit_merge(self):
        g = SynonymDict()
        o = g.new_entry('hello', 'hola', 'hi', 'aloha')
        p = g.new_entry('Hello', 'HELLO', 'Hi', 'HI')
        self.assertSetEqual({k for k in g.objects}, {o.object, p.object})
        self.assertNotEqual(g['hi'], g['HI'])
        g.merge('hi', 'HI')
        self.assertEqual(g['hi'], g['HI'])
        self.assertListEqual([k for k in g.objects], [o.object])

    def test_child(self):
        g = SynonymDict()
        ob1 = g.new_entry('hello', 'hola', 'hi', 'aloha')
        ob2 = g.new_entry('hi', 'Ni hao', 'hallo', create_child=True)
        self.assertTrue(ob1.has_child(ob2))
        ob3 = g.new_entry('greetings', 'salutations', 'hello')
        self.assertFalse(ob1.has_child(ob3))
        self.assertEqual(g.get('greetings'), g.get('hola'))

    def test_objects(self):
        g = SynonymDict()
        o1 = g.new_entry('hello', 'hola', 'hi', 'aloha')
        o2 = g.new_entry('Hello', 'HELLO', 'Hi', 'HI')
        self.assertListEqual([k for k in g.objects], [o2, o1])  # sort order by string
        #self.assertListEqual([k for k in g.objects], [o2.object, o1.object])  # sort order by string

    def test_remove_child(self):
        g = SynonymDict()
        ob1 = g.new_entry('hello', 'hola', 'hi', 'aloha')
        ob2 = g.new_entry('hi', 'Ni hao', 'hallo', create_child=True)
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
        g.new_entry('hello', 'hola', 'hi', 'aloha')
        g.new_entry('Hi', 'Bonjour')
        self.assertEqual(g['bonjour'], 'hello')

    def test_remove_term_case_insensitive(self):
        g = SynonymDict(ignore_case=True)
        o = g.new_entry('hello', 'hola', 'hi', 'aloha')
        self.assertIn('Hi', g)
        self.assertIn('hi', o)
        self.assertNotIn('Hi', o)
        g.del_term('Hi')
        self.assertNotIn('Hi', g)
        self.assertNotIn('hi', o)

    def test_whitespace(self):
        g = SynonymDict(ignore_case=True)
        g.new_entry('hello', 'bonjour')
        self.assertEqual(g[' bonjour'], 'hello')
        g.add_synonym(' GREETINGS  ', 'bonjour ')
        self.assertEqual(g['greetings'], 'hello')

    def test_disallow_empty_string_silently(self):
        g = SynonymDict()
        g.new_entry('hello', 'bonjour')
        g.add_synonym('', 'hello')
        self.assertNotIn('', g)

    def test_cast_to_str(self):
        g = SynonymDict()
        a = g.new_entry('the answer', 42)
        self.assertEqual(g['42'], 'the answer')
        self.assertIsNone(g.get(42))
        self.assertIn('42', a)
        self.assertNotIn(42, a)

    def test_remove_object(self):
        g = SynonymDict()
        o1 = g.new_entry('hello', 'hola', 'hi', 'aloha')
        o2 = g.new_entry('Hello', 'HELLO', 'Hi', 'HI')
        self.assertEqual(g['HELLO'], str(o2))
        g.remove_entry(o2)
        self.assertListEqual([k for k in g.objects], [o1])
        with self.assertRaises(KeyError):
            _ = g['HELLO']

    def test_synonyms(self):
        g = SynonymDict(ignore_case=True)
        o1 = g.new_entry('hello', 'hola', 'hi', 'aloha')
        o2 = g.new_entry('Hello', 'HELLO', 'Hi', 'HI')
        self.assertIs(o1, o2)
        self.assertSetEqual(set(g.synonyms('hello')), set(o1.terms))
        self.assertSetEqual(set(g.synonyms(o2)), set(o1.terms))

    def test_add_synonym(self):
        g = SynonymDict()
        o1 = g.new_entry('hello', 'hola', 'hi', 'aloha')
        g.add_synonym('greetings', 'hi')
        self.assertIn('greetings', o1)
        self.assertEqual(g['greetings'], g['hello'])

    def test_prune(self):
        g = SynonymDict()
        o1 = g.new_entry('hello', 'hola', 'hi', 'aloha')
        o2 = g.new_entry('goodbye', 'farewell', 'aloha', prune=True)
        self.assertIn('goodbye', g)
        self.assertEqual(g['aloha'], 'hello')
        self.assertEqual(g['farewell'], 'goodbye')
        self.assertIn('aloha', o1)
        self.assertNotIn('aloha', o2)

    def test_prune_idempotent(self):
        g = SynonymDict()
        o1 = g.new_entry('hello', 'hola', 'hi', 'aloha')
        o2 = g.new_entry('hello', 'hola', 'hi', 'aloha', prune=True)
        self.assertIs(o2, o1)

    def test_pruned_set_name_closure(self):
        g = SynonymDict()
        g.new_entry('hello', 'hola', 'hi', 'aloha')
        g.new_entry('goodbye', 'adios')
        with self.assertRaises(MergeError):
            g.new_entry('aloha', 'goodbye', 'farewell', 'sayonara')
        o3 = g.new_entry('aloha', 'goodbye', 'farewell', 'sayonara', prune=True)
        self.assertIn(o3.name, g.synonyms(o3))

    def test_type_consistency(self):
        """
        new_entry should return entry, get and __getitem__ should return object
        :return:
        """
        g = SynonymDict()
        o1 = g.new_entry('a', 'b', 'c')
        o2 = g['a']
        o3 = g.get('b')
        self.assertEqual(o1.object, o2)
        self.assertEqual(o1.object, o3)
        self.assertEqual(type(o1.object), type(o2))
        self.assertEqual(type(o2), type(o3))


if __name__ == '__main__':
    unittest.main()
