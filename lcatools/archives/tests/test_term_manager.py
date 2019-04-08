from ..term_manager import TermManager, QuantityConflict
from lcatools.entity_refs.flow_interface import DummyFlow
from lcatools.entities import LcQuantity
from lcatools.contexts import Context
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
    add_cf-- why does this exist? We do need add_cfs for LciaEngine but not here
    add_characterization -- really need to specify use cases; use Traci experience to write tests and Ecoinvent LCIA to
     evaluate their robustness

    """
    @classmethod
    def setUpClass(cls):
        cls.tm = TermManager(quiet=False)
        cls.tm.add_context(['emissions', 'emissions to air', 'emissions to urban air'])

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
            self.assertIs(self.tm[k], self.tm._cm._null_entry)

    def test_add_flow(self):
        """
        We want to test several things:
         * extrac
        :return:
        """
        flow = DummyFlow()
        for k in ('phosphene', 'phxphn', '1234567'):
            flow._flowable.add_term(k)
        flow.origin = 'test.origin'
        self.tm.add_flow(flow)
        self.assertEqual(self.tm._fm['1234567'], 'phosphene')

    def test_add_flow_prune(self):
        pass

    def test_add_flow_merge(self):
        pass

    def test_add_characterization(self):
        rq = LcQuantity.new('mass', 'kg', origin='test')
        qq = LcQuantity.new('volume', 'm3', origin='test')
        cf = self.tm.add_characterization('water', rq, qq, 0.001)
        self.assertEqual(cf.value, .001)

    def test_dup_mass(self):
        dummy = 'dummy_external_ref'
        dup_mass = LcQuantity(dummy, referenceUnit='kg', Name='Mass', origin='dummy.origin')
        with self.assertRaises(QuantityConflict):
            self.tm.add_quantity(dup_mass)


if __name__ == '__main__':
    unittest.main()
