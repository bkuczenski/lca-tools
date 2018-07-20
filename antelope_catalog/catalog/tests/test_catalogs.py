
from ...data_sources.local import TEST_ROOT

from .. import LcCatalog
from ...lc_resource import LcResource
from ...catalog_query import CatalogQuery, READONLY_INTERFACE_TYPES


import os
import unittest
from shutil import rmtree


uslci_fg = LcResource('test.uslci', '/data/LCI/USLCI/USLCI_Processes_ecospold1.zip', 'ecospold',
                      interfaces='inventory',
                      priority=40,
                      static=False,
                      prefix='USLCI_Processes_ecospold1/USLCI_Processes_ecospold1')


uslci_fg_dup = LcResource('test.uslci', '/data/LCI/USLCI/USLCI_Processes_ecospold1.zip', 'ecospold',
                          interfaces='inventory',
                          priority=40,
                          static=False,
                          prefix='USLCI_Processes_ecospold1/USLCI_Processes_ecospold1')


uslci_fg_bad = LcResource('test.uslci', '/data/LCI/USLCI/junk.zip', 'ecospold',
                          interfaces='inventory',
                          priority=40,
                          static=False,
                          prefix='USLCI_Processes_ecospold1/USLCI_Processes_ecospold1')


uslci_bg = LcResource('test.uslci.allocated', '/data/GitHub/lca-tools-datafiles/catalogs/uslci_clean_allocated.json.gz',
                      'json',
                      interfaces=READONLY_INTERFACE_TYPES,
                      priority=90,
                      static=True)

work_dir = TEST_ROOT


class LcCatalogFixture(unittest.TestCase):
    work_dir = work_dir

    @classmethod
    def setUpClass(cls):
        os.makedirs(cls.work_dir, exist_ok=True)
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

    def test_add_delete_resource_1(self):
        """
        This adds a resource
        :return:
        """
        r = self._cat.new_resource('test.my.dummy', '/dev/null', 'LcArchive')
        self.assertIn('basic', r.interfaces)
        self.assertIn('test.my.dummy', self._cat.references)
        self.assertNotIn('test.my.doofus', self._cat.references)

    def test_add_delete_resource_2(self):
        """
        This deletes the resource
        :return:
        """
        r = self._cat.get_resource('test.my.dummy')
        self.assertEqual(r.source, '/dev/null')
        self._cat.delete_resource(r)
        self.assertNotIn('test.my.dummy', self._cat.references)
        self.assertFalse(os.path.exists(os.path.join(self._cat.resource_dir, r.reference)))

    def test_has_resource(self):
        """
        If a resource matches one that exists, has_resource should return True
        :return:
        """
        self.assertTrue(self._cat.has_resource(uslci_fg_dup))
        self.assertFalse(self._cat.has_resource(uslci_fg_bad))


if __name__ == '__main__':
    unittest.main()
