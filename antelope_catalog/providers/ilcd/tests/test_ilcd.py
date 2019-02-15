import unittest
import os

from ..ilcd import IlcdArchive

TEST_ARCHIVE = os.path.join(os.path.dirname(__file__), 'data', 'ilcd_test')
test_flow = 'f579de8c-8897-4bdb-9a0a-b36f8b13282e'

class IlcdTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.A = IlcdArchive(TEST_ARCHIVE, ref='test.ilcd')

    def test_archive_exists(self):
        self.assertEqual(self.A.ref, 'test.ilcd')

    def test_retrieve_flow(self):
        f = self.A.retrieve_or_fetch_entity(test_flow)
        self.assertEqual(f.entity_type, 'flow')
        self.assertEqual(f['Name'], 'RNA: natural gas, at consumer')

    def test_flowproperties(self):
        f = self.A.retrieve_or_fetch_entity(test_flow)
        cfs = [round(cf.value, 4) for cf in f.profile()]
        self.assertEqual(len(cfs), 4)
        self.assertSetEqual(set(cfs), {1.0, 0.0204, 0.015})

if __name__ == '__main__':
    unittest.main()
