from .compartment import Compartment, InvalidSense, InconsistentSense
from .compartment_manager import CompartmentManager
import unittest


class CompartmentTest(unittest.TestCase):

    def test_sense(self):
        c = Compartment('emissions to air', sense='sink')
        self.assertEqual(c.sense, 'Sink')
        with self.assertRaises(InvalidSense):
            Compartment('emissions to Mars', sense='extraterrestrial')
        with self.assertRaises(InconsistentSense):
            Compartment('resources from urban air', parent=c, sense='source')

    def test_parent(self):
        c = Compartment('emissions', sense='sink')
        d = Compartment('emissions to air', parent=c)
        e = Compartment('emissions to urban air', parent=d)
        f = Compartment('emissions to rural air', 'emissions from high stacks', parent=d)
        self.assertEqual(e.sense, 'Sink')
        self.assertSetEqual(set(i for i in d.subcompartments), {e, f})
        self.assertListEqual([str(k) for k in c.self_and_subcompartments], ['emissions', 'emissions to air',
                                                                            'emissions to rural air',
                                                                            'emissions to urban air'])

    def test_serialize(self):
        c = Compartment('emissions', sense='sink')
        d = Compartment('emissions to air', parent=c)
        j = d.serialize()
        self.assertEqual(j['name'], 'emissions to air')
        self.assertSetEqual(set(j.keys()), {'name', 'synonyms', 'parent'})


class CompartmentManagerTest(unittest.TestCase):
    """
    What do we want context managers to do?

     - one, keep track of contexts that are encountered
      = add them (hierarchicaly)
      = add synonyms as appropriate
     - two, retrieve a context from a string (the syndict already handles this)
     - three, that's really it.  managing the set of canonical contexts is a separate task.
    """
    def setUp(self):
        self.cm = CompartmentManager()
        self.cm.new_object('resources', sense='source')
        self.cm.new_object('emissions', sense='sink')

    def test_add_from_dict(self):
        d = {'name': 'water emissions',
             'synonyms': [
                 'emissions to water',
                 'emissions to surface water',
                 'water'
             ],
             'parent': 'emissions'}
        self.cm._add_from_dict(d)
        self.assertEqual(str(self.cm['water']), 'water emissions')

    def test_add_hier(self):
        self.cm.add_compartments(['emissions', 'emissions to air', 'emissions to urban air'])
        self.assertIs(self.cm['emissions to air'], self.cm['emissions to urban air'].parent)


if __name__ == '__main__':
    unittest.main()
