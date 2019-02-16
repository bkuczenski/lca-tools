from ..term_manager import TermManager
from ..clookup import Context
import unittest


class TermManagerTest(unittest.TestCase):
    """
    This needs some serious development.

    Things to test:
    __getitem__: finds context by synonym; else returns None
    add_flow: multiple test cases:
     - add new item
     - add existing item (idempotency)
     - check reverse mapping
     - add conflicting item; prune
     - add conflicting item; merge
    add_cf-- why does this exist? Either this should call add_characterization or vice versa
    add_characterization



    """
    @classmethod
    def setUpClass(cls):
        cls.tm = TermManager(quiet=False)
        cls.tm.add_compartments(['emissions', 'emissions to air', 'emissions to urban air'])

    def test_idempotent_context(self):
        """
        getitem(context) should just return the context
        :return:
        """
        cx = self.tm['emissions to air']
        self.assertIsInstance(cx, Context)
        self.assertIs(cx, self.tm[cx])

    def test_none_item(self):
        self.assertIs(self.tm[None], None)

    def test_undefined_item(self):
        for k in ('unspecified', 'UNKNOWN', 'Undefined', 'none'):
            self.assertIs(self.tm[k], self.tm._cm._null_context)

    def test_add_flow(self):
        """
        We want to test several things:
         * extrac
        :return:
        """
