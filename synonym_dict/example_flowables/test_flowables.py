from .flowables import FlowablesDict
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


class FlowablesTest(unittest.TestCase):
    def setUp(self):
        self.f = FlowablesDict()
        j = json.loads(flowables_json)
        for fb in j['Flowables']:
            self.f._add_from_dict(fb)

    def test_cas_equivalence(self):
        self.assertEqual(self.f[124389], 'carbon dioxide')

    def test_add_set(self):
        self.f.new_object('methane, biogenic', 'ch4', '74-82-8')
        self.assertEqual(self.f.get('000074-82-8'), 'methane')


if __name__ == '__main__':
    unittest.main()
