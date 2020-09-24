import unittest

from lcatools.lcia_results import LciaResult
from antelope import CatalogRef


class LciaResultTestCase(unittest.TestCase):
    def test_catalog_ref(self):
        """
        Create an LCIA result using a catalog ref as quantity
        :return:
        """
        qty = CatalogRef('fictitious.origin', 'dummy_ext_ref', entity_type='quantity', Name='Test Quantity')
        res = LciaResult(qty)
        self.assertEqual(res.total(), 0.0)


if __name__ == '__main__':
    unittest.main()
