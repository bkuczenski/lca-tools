import os
import json
from lcatools.flowdb.compartments import Compartment, CompartmentManager, MissingCompartment
from lcatools.entities import LcFlow

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

local_cm_json = '''

{
  "name": [
    "test root",
    "dummy test"
  ],
  "subcompartments": [
    {
      "name": ["Intermediate Flows"],
      "subcompartments": [
        {
          "name": ["Utilities"],
          "subcompartments": []
        },
        {
          "name": ["Materials"],
          "subcompartments": []
        }
      ]
    },
    {
      "name": ["Elementary Flows"],
      "subcompartments": [
        {
          "name": ["Emissions"],
          "subcompartments": [
            {
              "name": ["Air", "blorgle air"],
              "subcompartments": [
                {
                  "name": ["blorgle air"],
                  "subcompartments": []
                },
                {
                  "name": ["fooferaw"],
                  "subcompartments": []
                }
              ]
            }
          ]
        }
      ]
    },
    {
      "name": ["Other kinds of flows"],
      "subcompartments": [
        {
          "name": ["exciting flows"],
          "subcompartments": []
        },
        {
          "name": ["dull flows"],
          "subcompartments": []
        }
      ]
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
      * advanced functionality: merge_subs, collapse, uproot
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
        self._parent_child()
        if os.path.exists(self._test_file):
            os.remove(self._test_file)

    def _parent_child(self, compartment=None):
        """
        added to tearDown to validate the tree that was built
        :param compartment:
        :return:
        """
        if compartment is None:
            compartment = self.base_compartment
        for sub in compartment.subcompartments():
            self.assertIs(sub.parent, compartment, "Parent [%s] Child [%s] error" % (compartment, sub))
            self._parent_child(sub)

    def test_operations(self):
        """
        tests methods:
        __init__
        __contains__
        __str__
        add_syns -> add_syn
        :return:
        """
        self.assertTrue('dummy root' in self.base_compartment)  # test construction; __contains__
        self.assertEqual(str(self.base_compartment), 'Dummy compartment')

    def test_getitem_subcompartments(self):
        """
        tests:
        from_json
        add_branch_from_json
        _add_subs_from_json
        __getitem__
        subcompartments
        :return:
        """
        self.base_compartment.add_branch_from_json(json.loads(subcompartment_json))
        self.assertEqual(len([i for i in self.base_compartment['A branching subcompartment'].subcompartments()]),
                         2, "Wrong number of subcompartments found")
        self.assertIs(self.base_compartment['a subcompartment added as a branch'],
                      next(self.base_compartment.subcompartments()),
                      "__getitem__ reference does not match subcompartment iterator")

    def test_synonyms(self):
        """
        tests:
        synonyms
        add_syn
        to_list
        :return:
        """
        bc = self.base_compartment
        self.assertSetEqual(bc.synonyms, {'Dummy compartment', 'dummy root', 'test root'})

        bc.add_subs(['Branch 1', 'branch 1a', 'node 1a'])
        bca = bc['Branch 1']['branch 1a']
        bca.add_syn('Ores')

        self.assertEqual(bc['Branch 1']['Ores']['node 1a'].to_list(), ['Branch 1', 'branch 1a', 'node 1a'])

    @staticmethod
    def _merge_expected_fields():
        return ['Dummy compartment', 'dummy root', 'test root',
                'Branch 1', 'branch 1a', 'node 1a',
                'Branch 2', 'branch 2b', 'node 2b']

    def _build_merge(self):
        bc = self.base_compartment
        bc.add_subs(['Branch 2', 'branch 2b', 'node 2b'])
        bc.add_subs(['Branch 1', 'branch 1a', 'node 1a'])

        self.assertEqual(bc.known_names(), self._merge_expected_fields(),
                         "built tree doesn't match expected merge names")

        bc._merge_sub(bc['Branch 1'])  # this should have no effect
        self.assertEqual(bc.known_names(), self._merge_expected_fields(),
                         "merge_sub modified node list")

    @staticmethod
    def _merge_subs_expected_fields():
        return ['Dummy compartment', 'dummy root', 'test root',
                'Branch 1', 'Branch 2',
                'branch 1a', 'node 1a', 'branch 2b', 'node 2b']

    def test_merge_subs(self):
        self._build_merge()
        bc = self.base_compartment

        bc.merge_subs(bc['Branch 2'], bc['Branch 1'])

        self.assertEqual(bc.known_names(), self._merge_subs_expected_fields(),
                         "merge_subs unexpected node list %s" % bc.known_names())

        self.assertSetEqual(bc['Branch 2'].synonyms, {'Branch 1', 'Branch 2'})

    @staticmethod
    def _uproot_expected_fields():
        return ['Dummy compartment', 'dummy root', 'test root',
                'Branch 2', 'Branch 1',
                'branch 1a', 'node 1a', 'branch 2b', 'node 2b']

    def test_uproot(self):
        self._build_merge()
        bc = self.base_compartment

        bc.uproot('Branch 1', 'Branch 2')
        self.assertEqual(bc.known_names(), self._uproot_expected_fields(),
                         "uproot unexpected node list %s" % bc.known_names())

        self.assertSetEqual(bc['Branch 2'].synonyms, {'Branch 2'})

        self.assertSetEqual({k.name for k in bc['Branch 2'].subcompartments()}, {'Branch 1', 'branch 2b'},
                            "wrong subcompartments after uproot")

    def test_collapse(self):
        b1 = self.base_compartment.add_sub('Branch 1')
        b1.add_subs(['Branch 1 redundant', 'also redundant'])
        b1.collapse('Branch 1 redundant')
        with self.assertRaises(StopIteration, ):
            next(b1.subcompartments())
        self.assertSetEqual(b1.synonyms, {'Branch 1', 'Branch 1 redundant', 'also redundant'}, "collapse failed")

    def test_elementary(self):
        """
        setting a node to elementary should set all child nodes


        :return:
        """
        pass


class CompartmentManagerTestCase(unittest.TestCase):
    """
    The compartment manager allows client code to do the following:
     - load a compartment hierarchy from a json file
     - crawl a compartment hierarchy to find a matching compartment from a list of strings
     - act as a wrapper for adding new compartments or synonyms
     - quickly check to see if a flow is elementary
    """
    def setUp(self):
        self._test_file = 'test_compartment_manager.json'
        self.cm = CompartmentManager()

    def tearDown(self):
        if os.path.exists(self._test_file):
            os.remove(self._test_file)

    def test_read_reference(self):
        kn = self.cm.known_names
        self.assertEqual(len(kn), 111, "Length does not match")
        self.assertEqual(kn[42], 'Heavy metals to industrial soil')
        self.assertEqual(kn[-1], 'Intermediate Flows')

    def test_idempotency(self):
        kn = self.cm.known_names
        self.cm.set_local(self._test_file)  # saves to file (since it doesn't exist)
        self.cm.set_local(self._test_file)  # loads + merges the file (since it does exist)
        self.assertEqual(kn, self.cm.known_names)

    def test_crawl(self):
        self.assertEqual(self.cm.find_matching('Emissions to soil').to_list(),
                         ['Elementary Flows', 'Emissions', 'soil'])

        self.assertEqual(self.cm.find_matching(['Soil']).to_list(),
                         ['Elementary Flows', 'Emissions', 'soil'])

        self.assertEqual(self.cm.find_matching(['Emissions to soil'], check_elem=True).to_list(),
                         ['Elementary Flows', 'Emissions', 'soil'])

        self.assertEqual(self.cm.find_matching(['Emissions', 'soil'], check_elem=True).to_list(),
                         ['Elementary Flows', 'Emissions'])

    def test_load_local(self):
        with open(self._test_file, 'w') as fp:
            fp.write(local_cm_json)
        self.cm.set_local(self._test_file)
        self.assertEqual(self.cm.compartments.known_names()[17:20],
                         ['Radioactive emissions to air', 'blorgle air', 'fossil'])
        self.assertSetEqual({i.name for i in self.cm.compartments.subcompartments()},
                            {'Intermediate Flows', 'Elementary Flows'})
        self.assertEqual(self.cm.find_matching('fooferaw').name, 'fooferaw')
        self.assertEqual(self.cm.find_matching('Materials').to_list(), ['Intermediate Flows', 'Materials'])

    def test_elementary(self):
        self.cm.set_local(self._test_file)
        f1 = LcFlow.new("Dummy flow", None, Compartment=['Emissions to air'])
        f2 = LcFlow.new("Dummy flow", None, Compartment=['Products'])
        self.assertTrue(self.cm.is_elementary(f1))
        self.assertFalse(self.cm.is_elementary(f2))

    def test_add_intflows(self):
        self.cm.set_local(self._test_file)
        f2 = LcFlow.new("Dummy flow", None, Compartment=['Products'])
        self.assertIsNone(self.cm.find_matching(f2['Compartment'], interact=False))

        products = self.cm.add_compartment(f2['Compartment'])
        self.assertIs(self.cm.find_matching(f2['Compartment']), products)

if __name__ == '__main__':
    unittest.main()
