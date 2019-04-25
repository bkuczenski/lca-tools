import unittest
from synonym_dict.example_compartments.test_compartments import CompartmentContainer, InconsistentLineage
from ..contexts import Context, ContextManager, InconsistentSense
from lcatools.interfaces.iindex import InvalidSense
from ..lcia_engine.lcia_engine import DEFAULT_CONTEXTS


class ContextTest(CompartmentContainer.CompartmentTest):
    def test_sense(self):
        c = Context('emissions to air', sense='sink')
        self.assertEqual(c.sense, 'Sink')
        with self.assertRaises(InvalidSense):
            Context('emissions to Mars', sense='extraterrestrial')
        with self.assertRaises(InconsistentSense):
            Context('resources from urban air', parent=c, sense='source')

    def test_elementary(self):
        c = Context('eMissions', sense='sink')
        c1 = Context('emissions to boot', parent=c)
        d = Context('emulsions', sense='sink')
        self.assertTrue(c1.elementary)
        self.assertFalse(d.elementary)

    def test_parent(self):
        c = Context('emissions', sense='sink')
        d = Context('emissions to air', parent=c)
        e = Context('emissions to urban air', parent=d)
        f = Context('emissions to rural air', 'emissions from high stacks', parent=d)
        self.assertSetEqual(set(i for i in d.subcompartments), {e, f})
        self.assertListEqual([str(k) for k in c.self_and_subcompartments], ['emissions', 'emissions to air',
                                                                            'emissions to rural air',
                                                                            'emissions to urban air'])
        self.assertEqual(e.sense, 'Sink')


class ContextManagerTest(CompartmentContainer.CompartmentManagerTest):
    def _test_class(self, ignore_case=True):
        if ignore_case is False:
            self.skipTest('skipping case sensitive test')
        else:
            return ContextManager()

    def setUp(self):
        self.cm = ContextManager()

    def test_add_from_dict(self):
        self._add_water_dict()
        self.assertEqual(str(self.cm['to water']), 'water emissions')
        self.assertEqual(self.cm['to water'].sense, 'Sink')

    def test_retrieve_by_tuple(self):
        self._add_water_dict()
        w = self.cm['to water']
        self.assertIs(w, self.cm[('emissions', 'water emissions')])

    def test_relative_add(self):
        self._add_water_dict()
        uw = self.cm['to water']
        ud = self.cm.add_compartments(['to water', 'lake water'])
        self.assertIs(uw, ud.parent)
        self.assertListEqual(ud.as_list(), ['Emissions', 'water emissions', 'lake water'])

    def test_merge_inconsistent_sense(self):
        d = [
            {
                "name": "to water",
                "parent": "Emissions"
            },
            {
                "name": "fresh water",
                "synonyms": [
                    "freshwater"
                ],
                "parent": "to water"
            },
            {
                "name": "ground-",
                "parent": "fresh water"
            },
            {
                "name": "from ground",
                "synonyms": [
                    "ground-",
                ],
                "parent": "Resources"
            }
        ]
        for k in d[:3]:
            self.cm._add_from_dict(k)
        with self.assertRaises(InconsistentSense):
            self.cm._add_from_dict(d[3])

    def _add_water_context(self):
        self._add_water_dict()
        c = self.cm.add_compartments(('to water', 'to groundwater'))
        self.cm.add_synonym('ground-', c)

    def test_unspecified(self):
        c = self.cm.add_compartments(['emissions', 'water', 'unspecified'])
        self.assertEqual(c.name, 'to water, unspecified')
        self.assertEqual(c.parent.name, 'to water')

    def test_inconsistent_lineage(self):
        self._add_water_context()
        with self.assertRaises(InconsistentLineage):
            self.cm.add_compartments(['resources', 'water', 'ground-'], conflict=None)

    def test_inconsistent_lineage_match(self):
        """
        When an intermediate descendant conflicts, we can either raise the exception (cautious) or do some clever
        regex-based predictive guessing (reckless)
        :return:
        """
        self.skipTest('Too hard to replicate this case in ContextManager')

    def test_inconsistent_lineage_rename(self):
        self._add_water_context()
        c = self.cm.add_compartments(['resources', 'water', 'ground-'], conflict='rename')
        self.assertEqual(c.name, 'from water, ground-')

    def test_inconsistent_lineage_skip(self):
        """
        When an intermediate descendant conflicts, we can either raise the exception (cautious) or do some clever
        regex-based predictive guessing (reckless)
        :return:
        """
        self._add_water_context()
        rw = self.cm.add_compartments(('resources', 'from water'))
        fw = self.cm.add_compartments(['resources', 'water', 'ground-'], conflict='skip')

        self.assertIs(fw, rw)

    def test_disregard(self):
        n = self.cm.add_compartments(['elementary flows', 'emissions', 'air', 'urban air'])
        self.assertNotIn('elementary flows', self.cm)
        self.assertIn('elementary flows', self.cm.disregarded_terms)
        self.assertListEqual(n.as_list(), ['Emissions', 'to air', 'urban air'])

    def test_protected(self):
        """
        Protected terms are 'air', 'water', and 'ground'- if these are added as subcompartments to compartments with
        non-None sense, they are modified to e.g. 'to air' or 'from air' appropriate to the sense.
        :return:
        """
        pass


class DefaultContextsTest(unittest.TestCase):
    def setUp(self):
        self.cm = ContextManager(source_file=DEFAULT_CONTEXTS)

    def test_load(self):
        self.assertEqual(len(self.cm), 34)
        self.assertSetEqual({k.name for k in self.cm.top_level_compartments}, {'Emissions', 'Resources'})

    def test_matching_compartment(self):
        foreign_cm = ContextManager()
        fx = foreign_cm.add_compartments(('resources', 'water', 'CA', 'CA-QC'))
        fx.add_origin('dummy.test')
        cx = self.cm.find_matching_context(fx)
        self.assertEqual(cx.sense, 'Source')
        self.assertIs(cx.top(), self.cm['Resources'])
        self.assertListEqual(cx.as_list(), ['Resources', 'from water'])  # superfluous information trimmed
        self.assertIs(self.cm['dummy.test:CA-QC'], cx)

    def test_context_hint(self):
        self.cm.add_context_hint('dummy.test', 'air', 'to air')
        tgt = self.cm['to air']
        self.assertIs(self.cm['dummy.test:air'], tgt)

    def test_matching_sublineage(self):
        self.cm.add_context_hint('dummy.test', '[resources]', 'Resources')
        tgt = self.cm['from ground']
        self.assertIs(self.cm['dummy.test:[resources]'], tgt.parent)
        foreign_cm = ContextManager()
        fx = foreign_cm.add_compartments(('Elementary Flows', 'NETL Coal Elementary Flows', 'NETL Elementary Flows',
                                          ' [Resources] ', 'ground'))
        # context gets its origin added when used in a Characterization or an Exchange- for now we do it manually
        fx.add_origin('dummy.test')
        self.assertIs(self.cm.find_matching_context(fx), tgt)
        self.assertIs(self.cm['dummy.test:ground'], tgt)


if __name__ == '__main__':
    unittest.main()