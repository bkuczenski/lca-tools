"""
This may be a bit of a protocol violation, but the role of the unittest framework here is to construct a persistent
catalog object.  The clever bit is that the object will be constructed entirely through unit test execution.

The catalog resources are specified in data_sources.local.  This catalog is used by computation unit-testing routines
to store persistent data sets to test against.

Because of naming conventions, this test file should run first, creating all the resources in the CATALOG_ROOT, which
again is a _persistent_ catalog (as opposed to TEST_ROOT, which is made to be blown away).  The later test files will
use the created resources to test functionality.

see ..local.py for resource configuration details.
"""

import unittest
import os
import json

from ..local import CATALOG_ROOT, RESOURCES_CONFIG
from ...catalog import LcCatalog

resource_dir = os.path.join(CATALOG_ROOT, 'resources')


def _check_resource(ref):
    return os.path.exists(os.path.join(resource_dir, ref))


def setUpModule():
    LcCatalog(CATALOG_ROOT)


class LocalCatalog(unittest.TestCase):
    """
    Note hack of alphabetical naming of tests to force order
    """
    @classmethod
    def setUpClass(cls):
        """
        The setUpClass function creates the catalog and unpacks all test-enabled config specifications
        :return:
        """
        cls._configs = dict()
        cls._cat = LcCatalog(CATALOG_ROOT)
        for k, d in RESOURCES_CONFIG.items():
            if d.pop('enable_test', False):
                print('data source %s enabled' % k)
                obj = d.pop('source')
                cls._configs[k] = obj(**d)

    def test_folders(self):
        """
        Catalog creation should lead to the creation of the catalog root, subdirectories, and reference quantities.
        :return:
        """
        self.assertTrue(os.path.isdir(CATALOG_ROOT))
        self.assertTrue(os.path.isdir(resource_dir))
        self.assertTrue(os.path.exists(os.path.join(CATALOG_ROOT, 'reference-quantities.json')))

    def _check_reference(self, ref):
        return ref in self._cat.references

    def _check_interface(self, ref, iface):
        return ':'.join([ref, iface]) in self._cat.interfaces

    def test_a_make_resources(self):
        """
        Installs the resources from the config specifications, but only if the resources do not already exist.
        Something that's needed here is a way to update resources if the DataSource subclasses change-- presently the
        only way to do that is to delete the resources (and also any derived source files like indexes or backgrounds)
        This should simply run without errors, resulting in the creation of any missing resources
        :return:
        """
        # k is resource signifier, s is DataSource subclass
        for k, s in self._configs.items():
            for ref in s.references:
                if not self._check_reference(ref):
                    for res in s.make_resources(ref):
                        self._cat.add_resource(res)

    def test_b_number_of_resources(self):
        """
        This function checks that the number of resources created is correct
        :return:
        """
        # k is resource signifier, s is DataSource subclass
        for k, s in self._configs.items():
            for ref in s.references:
                nres = len([i for i in s.make_resources(ref)])
                with open(os.path.join(resource_dir, ref), 'r') as fp:
                    xres = len(json.load(fp)[ref])
                self.assertEqual(nres, xres, ref)

    def test_c_instantiate_ifaces(self):
        """
        This confirms that each interface specified can be created
        :return:
        """
        for k, s in self._configs.items():
            for ref in s.references:
                for iface in s.interfaces(ref):
                    res = self._cat.get_resource(ref, iface, strict=True)
                    res.check(self._cat)


if __name__ == '__main__':
    unittest.main()
