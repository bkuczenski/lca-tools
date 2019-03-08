from .compartment import Compartment, InvalidSense, InconsistentSense
from .compartment_manager import CompartmentManager, NonSpecificCompartment, NullCompartment, InconsistentLineage
import unittest


class CompartmentTest(unittest.TestCase):

    def test_sense(self):
        c = Compartment('emissions to air', sense='sink')
        self.assertEqual(c.sense, 'Sink')
        with self.assertRaises(InvalidSense):
            Compartment('emissions to Mars', sense='extraterrestrial')
        with self.assertRaises(InconsistentSense):
            Compartment('resources from urban air', parent=c, sense='source')

    def test_elementary(self):
        c = Compartment('eMissions', sense='sink')
        c1 = Compartment('emissions to boot', parent=c)
        d = Compartment('emulsions', sense='sink')
        self.assertTrue(c1.elementary)
        self.assertFalse(d.elementary)

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

    def test_iter(self):
        c = Compartment('emissions', sense='sink')
        d = Compartment('emissions to air', parent=c)
        e = Compartment('emissions to urban air', parent=d)
        self.assertListEqual(e.as_list(), ['emissions', 'emissions to air', 'emissions to urban air'])
        self.assertTupleEqual(tuple(e), ('emissions', 'emissions to air', 'emissions to urban air'))


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

    def _add_water_dict(self):
        d = {'name': 'water emissions',
             'synonyms': [
                 'emissions to water',
                 'emissions to surface water',
                 'water'
             ],
             'parent': 'emissions'}
        self.cm._add_from_dict(d)

    def test_add_from_dict(self):
        self._add_water_dict()
        self.assertEqual(str(self.cm['water']), 'water emissions')
        self.assertEqual(self.cm['water'].sense, 'Sink')

    def test_0_add_hier(self):
        self.cm.add_compartments(['emissions', 'emissions to air', 'emissions to urban air'])
        self.assertIs(self.cm['emissions to air'], self.cm['emissions to urban air'].parent)

    def test_idempotent(self):
        """
        getting a context should return the context
        :return:
        """
        cx = self.cm['resources']
        self.assertIs(self.cm[cx], cx)

    def test_null(self):
        cx = self.cm[None]
        self.assertIs(cx, NullCompartment)

    def test_add_null(self):
        cx = self.cm.add_compartments(())
        self.assertIs(cx, NullCompartment)

    def test_toplevel(self):
        self.cm.add_compartments(['social hotspots', 'labor', 'child labor'])
        self.assertIn('social hotspots', (str(x) for x in self.cm.top_level_compartments))

    def test_unspecified(self):
        c = self.cm.add_compartments(['emissions', 'water', 'unspecified'])
        self.assertEqual(c.name, 'water, unspecified')
        self.assertEqual(c.parent.name, 'water')

    def test_top_level_nonspecific(self):
        with self.assertRaises(NonSpecificCompartment):
            self.cm.add_compartments(['unspecified', 'unspecified water'])

    def test_retrieve_by_tuple(self):
        self._add_water_dict()
        w = self.cm['water']
        self.assertIs(w, self.cm[('emissions', 'water emissions')])

    '''
    Potential Glitch cases:
     * relative add
     * omitted descendant -> still valid
     * conflict in specified parent -> InconsistentLineage
    '''
    def test_relative_add(self):
        self._add_water_dict()
        uw = self.cm['water']
        ud = self.cm.add_compartments(['water', 'lake water'])
        self.assertIs(uw, ud.parent)
        self.assertListEqual(ud.as_list(), ['Emissions', 'water emissions', 'lake water'])

    def test_omitted_descendant(self):
        ua = self.cm.add_compartments(['emissions', 'to air', 'to urban air'])  # confirm that this exists
        uc = self.cm.add_compartments(['emissions', 'to urban air', 'to urban center'])
        self.assertIs(ua, uc.parent)

    def test_inconsistent_lineage(self):
        self._add_water_dict()
        with self.assertRaises(InconsistentLineage):
            self.cm.add_compartments(['resources', 'water'])

    def test_inconsistent_lineage_match(self):
        """
        When an intermediate descendant conflicts, we can either raise the exception (cautious) or do some clever
        regex-based predictive guessing (reckless)
        :return:
        """
        self._add_water_dict()
        rw = self.cm.add_compartments(['resources', 'from water'])
        fw = self.cm.add_compartments(['resources', 'water', 'fresh water'], conflict='match')

        self.assertIs(fw.parent, rw)
        self.assertEqual(fw.sense, 'Source')

    def test_inconsistent_lineage_skip(self):
        """
        When an intermediate descendant conflicts, we can either raise the exception (cautious) or do some clever
        regex-based predictive guessing (reckless)
        :return:
        """
        self._add_water_dict()
        rw = self.cm.add_compartments(['resources', 'from water'])
        fw = self.cm.add_compartments(['resources', 'water', 'fresh water'], conflict='skip')

        self.assertIs(fw.parent, rw.parent)
        self.assertEqual(fw.sense, 'Source')


if __name__ == '__main__':
    unittest.main()
