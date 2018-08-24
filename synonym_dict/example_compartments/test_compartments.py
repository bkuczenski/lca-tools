from .context import Context, InvalidSense, InconsistentSense
from .compartment_manager import CompartmentManager, NonSpecificContext
import unittest


class CompartmentTest(unittest.TestCase):

    def test_sense(self):
        c = Context('emissions to air', sense='sink')
        self.assertEqual(c.sense, 'Sink')
        with self.assertRaises(InvalidSense):
            Context('emissions to Mars', sense='extraterrestrial')
        with self.assertRaises(InconsistentSense):
            Context('resources from urban air', parent=c, sense='source')

    def test_parent(self):
        c = Context('emissions', sense='sink')
        d = Context('emissions to air', parent=c)
        e = Context('emissions to urban air', parent=d)
        f = Context('emissions to rural air', 'emissions from high stacks', parent=d)
        self.assertEqual(e.sense, 'Sink')
        self.assertSetEqual(set(i for i in d.subcompartments), {e, f})
        self.assertListEqual([str(k) for k in c.self_and_subcompartments], ['emissions', 'emissions to air',
                                                                            'emissions to rural air',
                                                                            'emissions to urban air'])

    def test_serialize(self):
        c = Context('emissions', sense='sink')
        d = Context('emissions to air', parent=c)
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
        self.assertEqual(self.cm['water'].sense, 'Sink')

    def test_add_hier(self):
        self.cm.add_compartments(['emissions', 'emissions to air', 'emissions to urban air'])
        self.assertIs(self.cm['emissions to air'], self.cm['emissions to urban air'].parent)

    def test_toplevel(self):
        self.cm.add_compartments(['social hotspots', 'labor', 'child labor'])
        self.assertIn('social hotspots', (str(x) for x in self.cm.top_level_compartments))

    def test_unspecified(self):
        c = self.cm.add_compartments(['emissions', 'water', 'unspecified'])
        self.assertEqual(c.name, 'water, unspecified')
        self.assertEqual(c.parent.name, 'water')

    def test_top_level_nonspecific(self):
        with self.assertRaises(NonSpecificContext):
            self.cm.add_compartments(['unspecified', 'unspecified water'])


if __name__ == '__main__':
    unittest.main()
