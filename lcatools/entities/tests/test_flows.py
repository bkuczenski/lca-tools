import unittest
from .base_testclass import BasicEntityTest


class FlowsTest(BasicEntityTest):
    def test_synonyms(self):
        f = self.A["018872d0-ebce-3dfe-9552-3ff163fb468c"]
        self.assertIn(f.name, self.A.tm.synonyms(f.link))


if __name__ == '__main__':
    unittest.main()
