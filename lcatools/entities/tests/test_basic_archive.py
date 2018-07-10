import unittest
import os
from uuid import uuid4

from lcatools.entity_store import local_ref
from lcatools.entities.basic_archive import BasicArchive
from lcatools.entity_refs import CatalogRef

WORKING_FILE = os.path.join(os.path.dirname(__file__), 'test-basic-archive.json')
test_ref = 'test.basic'

archive_json = {
  "@context": "https://bkuczenski.github.io/lca-tools-datafiles/context.jsonld",
  "catalogNames": {
    test_ref: [
      WORKING_FILE
    ]
  },
  "dataReference": test_ref,
  "dataSource": WORKING_FILE,
  "dataSourceType": "BasicArchive",
  "flows": [],
  "quantities": []
}


def setUpModule():
    ar = BasicArchive(WORKING_FILE, ref=test_ref)
    ar.write_to_file(WORKING_FILE, gzip=False)


def tearDownModule():
    os.remove(WORKING_FILE)


class BasicArchiveTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ar = BasicArchive(WORKING_FILE)

    def test_rewrite_name(self):
        """
        Discovered-- if we create an archive from an existing file, but without specifying the ref, then the
        EntityStore will convert that file to a local ref and use it.  But if the file contains a ref specification,
        we want that one to win. So we use it instead.
        :return:
        """
        self.assertEqual(self.ar.ref, local_ref(WORKING_FILE))
        self.ar.load_json(archive_json)
        self.assertEqual(self.ar.ref, test_ref)

    def test_catalog_ref(self):
        my_id = str(uuid4())
        self.ar.add(CatalogRef('bogus.origin', my_id, entity_type='flow'))
        self.assertEqual(self.ar[my_id].uuid, my_id)


if __name__ == '__main__':
    unittest.main()
