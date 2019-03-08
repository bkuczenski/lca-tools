"""
Unit Tests for SynList and Flowables basic functionality

Things to test for:
 - all the problems I ran into when first creating the flowables, in unit form. this can actually be very constructive.
"""

from synlist.synlist import SynList, InconsistentIndices
from synlist.flowables import Flowables

import unittest
import json


synlist_json = '''\
{
  "SynList": [
    {
      "name": "The Great Houdini",
      "synonyms": [
        "Henry VII",
        "Arthur the Great",
        "Alexander the terrible, horrible, no-good, very-bad"
      ]
    },
    {
      "name": "Zeke",
      "synonyms": [
        "Zeke",
        "zeke",
        "your cousin",
        "your cousin Zeke"
      ]
    }
  ]
}
'''


class SynListTestCase(unittest.TestCase):
    """
    user-facing methods:
     cls.from_json()
     index(term) - return instantiation-specific index for an item
     all_terms - just dict-keys- not very useful to test
     name(term) - the canonical name of an item
     add_term(term) - if term is known, return its index; else create a new and return its index
     find_indices(iterable) - map terms in iterable to sets in a dict; keys are indices
     new_set(iterable, name) - create a new set for all the terms not already known in iterable
     add_set(iterable, merge, name) - add (unknown) terms to existing or new item depending on merge
     merge - squash items together; first one listed keeps its name
     add_synonyms() - add_set
     synonyms_for(term) - return all synonyms for item containing term
     search() - regexp - returns a set of indices
     synonym_set(index) - access an item via its index
     [] - synonyms_for
     serialize - to dict, filter out empty items

    internal methods:
     _get_index
     _new_term
     _
    """
    ma_synonyms = {'Marie Antoinette'}
    z_synonyms = {'Zeke', 'zeke', 'your cousin', 'your cousin Zeke'}

    def setUp(self):
        """
        deserialize, __init__, add_set without merge (=>
        :return:
        """
        self.synlist = SynList.from_json(json.loads(synlist_json))

    def tearDown(self):
        pass

    def test_name(self):
        self.synlist.add_term('bob the builder')
        self.assertEqual(self.synlist.name('bob the builder'), 'bob the builder')

    def test_lookup(self):
        """
        synonyms_for, _get_index
        :return:
        """
        self.assertSetEqual(self.synlist.synonyms_for('zeke'), self.z_synonyms)

    def test_find_indices(self):
        """
        find_indices
        :return:
        """
        indices = self.synlist.find_indices(('The Great Houdini', 'Henry VIII'))

        self.assertSetEqual(indices[None], {'Henry VIII'})
        self.assertSetEqual(indices[0], {'The Great Houdini'})

    def test_add_synonyms(self):
        self.assertEqual(self.synlist.add_term('Henry VII'), 0)
        self.assertEqual(self.synlist.add_term('Conan the barbarian'), 2)
        self.assertEqual(self.synlist.add_synonyms('Conan the barbarian', 'your brother steve'), 2)

        with self.assertRaises(InconsistentIndices):
            self.synlist.add_set(('Conan the barbarian', 'Henry VII'), merge=True)

        self.assertEqual(self.synlist.add_set(('Conan the barbarian', 'Henry VII')), None)  # both terms already exist

    def test_add_merge(self):
        """
        Tests the ability to shunt off non-matching phrases to a new item even when some synonyms match existing items
        :return:
        """
        self.assertEqual(self.synlist.add_set(('Conan the barbarian', 'bob the builder'), merge=False), 2)
        self.assertEqual(self.synlist.add_set(('bob the builder', 'Nicky "legs" Capote'), merge=False), 3)
        self.assertEqual(self.synlist.add_set(('Jack the Ripper', 'Nicky "legs" Capote'), merge=True), 3)
        with self.assertRaises(InconsistentIndices):
            self.synlist.add_set(('bob the builder', 'Nicky "legs" Capote'), merge=True)

    def test_new_set(self):
        """
        Create a new set with some terms that may already exist
        :return:
        """
        self.assertEqual(self.synlist.add_set(('Henry VII', 'Marie Antoinette')), 2)
        self.assertSetEqual(self.synlist.synonyms_for('Marie Antoinette'), self.ma_synonyms)

    def test_reassign_name(self):
        """
        If a specified name for a set to be merged is already in the target set, no error will be raised and the name
        gets updated.
        :return:
        """
        self.synlist.add_set(('Bob', 'bob the builder', 'your cousin', 'your cousin bob'), merge=True)
        self.synlist.set_name('your cousin')
        self.assertEqual(self.synlist.name('Bob'), 'your cousin')
        self.assertTrue(self.synlist.are_synonyms('Bob', 'Zeke'))
        self.assertEqual(len(self.synlist), 2)

    def _case_sensitivity(self, synlist):
        self.assertTrue(synlist.are_synonyms('i need you', 'i love you'))
        self.assertFalse(synlist.are_synonyms('I love you', 'i love you'))
        self.assertFalse(synlist.are_synonyms('i love you', 'i want you'))

    def test_merge(self):
        """
        Make sure the proper sets are identified. make sure the terms in those sets are all synonyms. make sure the
        merged indices are no longer valid. make sure it works for >2 inputs
        :return:
        """
        set1 = self.synlist.add_set(('hello', 'I love you', 'Won\'t you tell me your name'), merge=True)
        self.assertEqual(len(self.synlist), 3)
        self.assertEqual(self.synlist.index('hello'), set1)

        set2 = self.synlist.add_set(('The hills are alive', 'with the sound of music', 'the sound of music'))
        set3 = self.synlist.add_set(('i want you', 'i want you so bad', 'i want you so bad it\'s driving me mad'))
        set4 = self.synlist.add_set(('i love you', 'i want you', 'i need you'))  # i love you != I love you
        self._case_sensitivity(self.synlist)

        self.synlist.merge('hello', 'i need you', 'Henry VII')
        self.assertTrue(self.synlist.are_synonyms('Won\'t you tell me your name', 'The Great Houdini'))
        self.assertTrue(self.synlist.are_synonyms('i need you', 'The Great Houdini'))
        self.assertEqual(self.synlist.index('Henry VII'), set1)
        self.assertEqual(self.synlist.synonym_set(set4), None)
        self.assertEqual(len(self.synlist), 4)


class FlowablesBasicTest(SynListTestCase):
    """
    Apply the same tests to the subclass to make sure the inheritance didn't break anything: override setUp
    need to adjust for case-insensitivity for terms of length > 3: override synonym testing
    """
    z_synonyms = {'Zeke', 'your cousin', 'your cousin Zeke'}

    def setUp(self):
        j = json.loads(synlist_json)
        j['Flowables'] = j.pop('SynList')
        self.synlist = Flowables.from_json(j)

    def _case_sensitivity(self, synlist):
        self.assertFalse(synlist.are_synonyms('i need you', 'i love you'))
        self.assertTrue(synlist.are_synonyms('I love you', 'i love you'))
        self.assertFalse(synlist.are_synonyms('i love you', 'i want you'))


if __name__ == '__main__':
    unittest.main()
