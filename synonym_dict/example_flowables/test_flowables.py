from .flowables import Flowable, FlowablesDict
import unittest
import json


flowables_json = '''{
"Flowables": [
    {
        "name": "carbon dioxide",
        "synonyms": [
            "124-38-9",
            "CO2"
        ]
    },
    {
        "name": "nitrous oxide",
        "synonyms": [
            "N2O",
            "dinitrogen monoxide",
            "10024972"
        ]
    },
    {
        "name": "methane",
        "synonyms": [
            "CH4",
            "methane (biogenic)",
            "methane, fossil"
        ]
    }
]
}'''


class FlowableTest(unittest.TestCase):
    def test_remove_cas_number(self):
        f = Flowable('methane', '74-82-8', 'CH4')
        self.assertIn('74-82-8', f)
        f.remove_term('000074828')
        self.assertNotIn('74-82-8', f)
        self.assertSetEqual(set(f.cas_numbers), set())

    def test_add_duplicate_cas(self):
        f = Flowable('methane', '74-82-8')
        self.assertSetEqual({t for t in f.terms}, {'methane', '74828', '74-82-8', '000074828', '000074-82-8'})
        c = 74828
        f.add_term(c)
        self.assertSetEqual({t for t in f.terms}, {'methane', '74828', '74-82-8', '000074828', '000074-82-8'})

    def test_rename(self):
        f = Flowable('CO2, biogenic', '124-38-9', 'carbon dioxide (biogenic)')
        self.assertEqual(f.name, 'CO2, biogenic')
        f.set_name('carbon dioxide (biogenic)')
        self.assertEqual(f.name, 'carbon dioxide (biogenic)')
        self.assertIn('CO2, biogenic', f)


class FlowablesTest(unittest.TestCase):
    def setUp(self):
        self.f = FlowablesDict()
        j = json.loads(flowables_json)
        for fb in j['Flowables']:
            self.f._add_from_dict(fb)

    def test_load_preserve_child(self):
        g = FlowablesDict()
        n = g.new_entry('nitrous oxide')
        o = g.new_entry('N2O', 'nitrous oxide', create_child=True)
        self.assertTrue(n.has_child(o))
        j = json.loads(flowables_json)
        for fb in j['Flowables']:
            g._add_from_dict(fb)
        self.assertEqual(g['N2O'], g['10024-97-2'])
        self.assertIs(g['N2O'], n)
        self.assertTrue(n.has_child(o))

    def test_cas_equivalence(self):
        self.assertEqual(self.f[124389], 'carbon dioxide')

    def test_add_set(self):
        self.f.new_entry('methane, biogenic', 'ch4', '74-82-8')
        self.assertEqual(self.f.get('000074-82-8'), 'methane')

    def test_return_type(self):
        self.assertIsInstance(self.f['carbon dioxide'], Flowable)

    def test_key_stability_under_rename(self):
        co2 = self.f['co2']
        self.assertEqual(co2.name, 'carbon dioxide')
        d = {co2: 420}
        self.f.set_name('co2')
        self.assertEqual(co2.name, 'CO2')
        self.assertEqual(d[self.f['co2']], 420)

    def test_serialize_deserialize(self):
        self.assertIn('124389', self.f)
        j = {self.f._entry_group: [f.serialize() for f in self.f._list_entries()]}
        self.assertEqual(len(j[self.f._entry_group]), len(self.f))
        co2 = next(f for f in j[self.f._entry_group] if f['name'] == 'carbon dioxide')
        self.assertListEqual(co2['synonyms'], ['CO2', '124-38-9'])
        g = FlowablesDict()
        for f in j[self.f._entry_group]:
            g._add_from_dict(f)
        self.assertEqual(len(g), len(self.f))
        self.assertIn('124389', g)


if __name__ == '__main__':
    unittest.main()
