from ..term_manager import TermManager
from ..clookup import Context
import unittest


class TermManagerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tm = TermManager()
        cls.tm.add_compartments(['emissions', 'emissions to air', 'emissions to urban air'])

    def test_idempotent_context(self):
        """
        getitem(context) should just return the context
        :return:
        """
        cx = self.tm['emissions to air']
        self.assertIsInstance(cx, Context)
        self.assertIs(cx, self.tm[cx])
