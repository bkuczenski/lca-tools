
from lcatools.catalog.catalog import LcCatalog
from lcatools.catalog.lc_resource import LcResource
from lcatools.interfaces.iquery import CatalogQuery, READONLY_INTERFACE_TYPES


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
os.makedirs(work_dir, exist_ok=True)


class LcCatalogTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # do this once because there's no reason to repeatedly initialize the catalog / load the data files
        cls._cat = LcCatalog(work_dir)
        cls._cat.add_resource(uslci_fg)
        cls._cat.add_resource(uslci_bg)

    @classmethod
    def tearDownClass(cls):
        rmtree(work_dir)

    def test_resolver_index(self):
        self.assertSetEqual({r for r in self._cat.references}, {'test.uslci', 'test.uslci.allocated'})

    def test_priority(self):
        q = CatalogQuery('test.uslci', catalog=self._cat)
        p = q.get('Acetic acid, at plant')
        self.assertEqual(p.origin, 'test.uslci')

    def test_inventory(self):
        q = self._cat.query('test.uslci')
        q.on_debug()
        inv = [x for x in q.inventory('Acetic acid, at plant')]
        self.assertEqual(len(inv), 21)


if __name__ == '__main__':
    unittest.main()
