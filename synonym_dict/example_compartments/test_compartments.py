from synonym_dict.tests.test_synonym_dict import TestContainer
from .compartment import Compartment
from .compartment_manager import CompartmentManager, NonSpecificCompartment, InconsistentLineage
import unittest


class CompartmentContainer(object):
    class CompartmentTest(unittest.TestCase):

        def test_parent(self):
            c = Compartment('emissions')
            d = Compartment('emissions to air', parent=c)
            e = Compartment('emissions to urban air', parent=d)
            f = Compartment('emissions to rural air', 'emissions from high stacks', parent=d)
            self.assertSetEqual(set(i for i in d.subcompartments), {e, f})
            self.assertListEqual([str(k) for k in c.self_and_subcompartments], ['emissions', 'emissions to air',
                                                                                'emissions to rural air',
                                                                                'emissions to urban air'])

        def test_serialize(self):
            c = Compartment('emissions')
            d = Compartment('emissions to air', parent=c)
            j = d.serialize()
            self.assertEqual(j['name'], 'emissions to air')
            self.assertSetEqual(set(j.keys()), {'name', 'synonyms', 'parent'})

        def test_iter(self):
            c = Compartment('emissions')
            d = Compartment('emissions to air', parent=c)
            e = Compartment('emissions to urban air', parent=d)
            self.assertListEqual(e.as_list(), ['emissions', 'emissions to air', 'emissions to urban air'])
            self.assertTupleEqual(tuple(e), ('emissions', 'emissions to air', 'emissions to urban air'))


    class CompartmentManagerTest(TestContainer.SynonymDictTest):
        """
        What do we want compartment managers to do?

         - one, keep track of compartments that are encountered
          = add them (hierarchicaly)
          = add synonyms as appropriate
         - two, retrieve a compartment from a string (the syndict already handles this)
         - three, that's really it.  managing a set of canonical compartments is a separate task.
        """
        def _test_class(self, ignore_case=True):
            if ignore_case is False:
                self.skipTest('skipping case sensitive test')
            else:
                return CompartmentManager()

        def test_add_object(self):
            with self.assertRaises(TypeError):
                super(CompartmentContainer.CompartmentManagerTest, self).test_add_object()

        def setUp(self):
            self.cm = CompartmentManager()
            #

        def _add_water_dict(self):
            self.cm.new_entry('Emissions')
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

        def test_0_add_hier(self):
            self.cm.add_compartments(['emissions', 'emissions to air', 'emissions to urban air'])
            self.assertIs(self.cm['emissions to air'], self.cm['emissions to urban air'].parent)

        def test_strip_name(self):
            c = self.cm.add_compartments(['NETL Elementary Flows', ' [Resources] '])
            self.assertEqual(c.name, '[Resources]')

        def test_idempotent(self):
            """
            getting a context should return the context
            :return:
            """
            ca = self.cm.new_entry('Resources')
            cx = self.cm['resources']
            self.assertIs(self.cm[cx], cx, ca)

        def test_null(self):
            cx = self.cm[None]
            self.assertIs(cx, self.cm._null_entry)

        def test_add_null(self):
            cx = self.cm.add_compartments(())
            self.assertIs(cx, self.cm._null_entry)

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
                self.cm.add_compartments(['resources', 'water'], conflict=None)

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

        def test_inconsistent_lineage_rename(self):
            """
            If we get an inconsistent lineage error on a subcompartment, this is caused by a synonym collision. We can solve
            it by renaming the new subcompartment to be based on its parent, which must be non-conflicting.

            For rename we use a comma-separated join so that semicolon remains the operative parent-child separator
            :return:
            """
            self._add_water_dict()
            self.cm.new_entry('Resources')
            rw = self.cm.add_compartments(['resources', 'water'], conflict='rename')
            self.assertIs(rw.parent, self.cm['resources'])
            self.assertEqual(rw.name, 'Resources, water')


class CompartmentTest(CompartmentContainer.CompartmentTest):
    pass


class CompartmentManagerTest(CompartmentContainer.CompartmentManagerTest):
    pass


if __name__ == '__main__':
    unittest.main()
