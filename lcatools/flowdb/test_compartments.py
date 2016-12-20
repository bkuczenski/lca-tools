import os
import json
from lcatools.flowdb.compartments import Compartment, CompartmentManager

import unittest

subcompartment_json = '''
{
  "name": [
    "A branching subcompartment",
    "a subcompartment added as a branch"
  ],
  "subcompartments": [
    {
      "name": ["The real deal"],
      "subcompartments": []
    },
    {
      "name": [
          "An alternate deal",
          "deal with extra real"
        ],
      "subcompartments": []
    }
  ]
}
'''


class CompartmentTestCase(unittest.TestCase):
    """
    Base class for testing compartment class functionality.
    Functions to test:
      * builtin operators: __contains__, __eq__, __getitem__, __str__
      * core Compartment operations: synonyms, add_syn, add_syns, add_sub, add_subs, elementary behavior, to_list
      * advanced functionality: merge_sub, merge_subs, collapse, uproot
      * serialize
    """
    def setUp(self):
        """
        we create proxy compartments to test initialization. Test certain core operations during setup (proxy
        will fail to build if errors are encountered) and then validate the creations in individual tests
        :return:
        """
        self._test_file = 'test_compartment.json'
        self.base_compartment = Compartment('Dummy compartment')
        self.base_compartment.add_syns({'dummy root', 'test root'})

    def tearDown(self):
        if os.path.exists(self._test_file):
            os.remove(self._test_file)

    def test_operations(self):
        self.assertTrue('dummy root' in self.base_compartment)  # test construction; __contains__
        self.assertEqual(str(self.base_compartment), 'Dummy compartment')

    def test_getitem_subcompartments(self):
        self.base_compartment.add_branch_from_json(json.loads(subcompartment_json))
        self.assertEqual(len([i for i in self.base_compartment['A branching subcompartment'].subcompartments()]),
                         2, "Wrong number of subcompartments found")
        self.assertIs(self.base_compartment['A branching subcompartment'],
                      next(self.base_compartment.subcompartments()),
                      "__getitem__ reference does not match subcompartment iterator")

    def test_synonyms(self):
        self.assertSetEqual(self.base_compartment.synonyms, {'Dummy compartment', 'dummy root', 'test root'})

    def test_build_merge(self):
        self.base_compartment.add_subs(['Branch 1', 'branch 1a', 'node 1a'])
        self.base_compartment.add_subs(['Branch 2', 'branch 2b', 'node 2b'])

    def test_collapse(self):
        b1 = self.base_compartment.add_sub('Branch 1')
        b1.add_subs(['Branch 1 redundant', 'also redundant'])
        b1.collapse('Branch 1 redundant')
        with self.assertRaises(StopIteration, ):
            next(b1.subcompartments())
        self.assertSetEqual(b1.synonyms, {'Branch 1', 'Branch 1 redundant', 'also redundant'}, "collapse failed")


class CompartmentManagerTestCase(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
