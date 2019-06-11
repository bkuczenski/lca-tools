import unittest
import os
from shutil import rmtree

from .. import LcCatalog

WORKING_DIR = os.path.join(os.path.dirname(__file__), 'test-foreground')
test_ref = 'test.foreground'


class FgImplementationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cat = LcCatalog(WORKING_DIR)
        cls.fg = cls.cat.create_foreground('fg', ref=test_ref)

    def test_new_flow(self):
        f = self.fg.new_flow('Test flow', 'mass')
        ar = self.cat.get_archive(self.fg.origin)
        self.assertIs(ar[f.external_ref], f)
        self.assertEqual(self.fg.get(f.external_ref), f)

    def test_frag(self):
        f = self.fg.new_flow('a silly flow', 'number of items')
        frag = self.fg.new_fragment(f, 'Output')
        self.assertIs(self.fg.frag(frag.uuid[:3]), frag)

    @classmethod
    def tearDownClass(cls):
        rmtree(WORKING_DIR)


if __name__ == '__main__':
    unittest.main()
