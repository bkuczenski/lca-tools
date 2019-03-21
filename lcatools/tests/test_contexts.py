import unittest
from synonym_dict.example_compartments.test_compartments import CompartmentContainer
from ..contexts import Context, ContextManager, InconsistentSense, InvalidSense


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
        self.assertEqual(str(self.cm['water']), 'water emissions')
        self.assertEqual(self.cm['water'].sense, 'Sink')

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

    def test_disregard(self):
        n = self.cm.add_compartments(['elementary flows', 'emissions', 'air', 'urban air'])
        self.assertNotIn('elementary flows', self.cm)
        self.assertIn('elementary flows', self.cm.disregarded_terms)
        self.assertListEqual(n.as_list(), ['Emissions', 'air', 'urban air'])


if __name__ == '__main__':
    unittest.main()