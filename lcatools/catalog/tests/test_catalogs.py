
from lcatools.catalog.catalog import LcCatalog
from lcatools.catalog.lc_resource import LcResource
from lcatools.interfaces.catalog_query import CatalogQuery, READONLY_INTERFACE_TYPES


import os
import unittest
from shutil import rmtree


uslci_fg = LcResource('test.uslci', '/data/LCI/USLCI/USLCI_Processes_ecospold1.zip', 'ecospold',
                      interfaces='inventory',
                      priority=40,
                      static=False,
                      prefix='USLCI_Processes_ecospold1/USLCI_Processes_ecospold1')


uslci_bg = LcResource('test.uslci.allocated', '/data/GitHub/lca-tools-datafiles/catalogs/uslci_clean_allocated.json.gz',
                      'json',
                      interfaces=READONLY_INTERFACE_TYPES,
                      priority=90,
                      static=True)

work_dir = os.path.join(os.path.dirname(__file__), 'scratch')


class LcCatalogFixture(unittest.TestCase):
    work_dir = work_dir

    @classmethod
    def setUpClass(cls):
        os.makedirs(cls.work_dir)
        cls._cat = LcCatalog(cls.work_dir)
        cls._cat.add_resource(uslci_fg)
        cls._cat.add_resource(uslci_bg)

    @classmethod
    def tearDownClass(cls):
        rmtree(cls.work_dir)

    def test_resolver_index(self):
        self.assertSetEqual({r for r in self._cat.references}, {'local.qdb', 'test.uslci', 'test.uslci.allocated'})

    def test_priority(self):
        q = CatalogQuery('test.uslci', catalog=self._cat)
        p = q.get('Acetic acid, at plant')
        self.assertEqual(p.origin, 'test.uslci')

    def test_inventory(self):
        q = self._cat.query('test.uslci')
        inv = [x for x in q.inventory('Acetic acid, at plant')]
        self.assertEqual(len(inv), 21)

    @unittest.skip
    def test_find_source(self):
        """
        Need to determine a set of testing conditions that ensure the resolver.get_resource() works properly
        :return:
        """
        pass


if __name__ == '__main__':
    unittest.main()
