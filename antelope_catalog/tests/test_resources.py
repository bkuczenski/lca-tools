import unittest
import os

from ..lc_resource import LcResource
from ..catalog_query import READONLY_INTERFACE_TYPES
from lcatools.archives.tests import basic_archive_src


serialized_resource = {
    'dataSource': basic_archive_src,
    'dataSourceType': 'json',
    'interfaces': list(READONLY_INTERFACE_TYPES),
    'priority': 50,
    'static': False,
    'config': {}
}


temp_file = os.path.join(os.path.dirname(__file__), 'index_file.gz')


class LcResourceTest(unittest.TestCase):
    def setUp(self):
        self.res = LcResource('test.basic', basic_archive_src, 'json',
                              interfaces=READONLY_INTERFACE_TYPES)

    @classmethod
    def tearDownClass(cls):
        try:
            os.remove(temp_file)
        except FileNotFoundError:
            pass

    @unittest.skip('dumm')
    def test_serialize(self):
        self.assertEqual(self.res.serialize(), serialized_resource)

    def test_index_preserves_source(self):
        src = self.res.source
        self.assertIsNone(self.res.archive)
        self.res.make_index(temp_file)
        self.assertEqual(self.res.source, src)


if __name__ == '__main__':
    unittest.main()
