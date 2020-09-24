"""
This file tests BasicArchive operations that do not involve creating or adding entities-- only basic construction
of the archive and definition of catalog names.

For testing of actual entity-containing archives, look to test_base
"""

import unittest
import os
from uuid import uuid4

from antelope import local_ref, CatalogRef
from ..entity_store import SourceAlreadyKnown
from ..basic_archive import BasicArchive

WORKING_FILE = os.path.join(os.path.dirname(__file__), 'test-basic-archive.json')
conflict_file = '/dummy/conflict/file'
test_ref = 'test.basic'
test_conflict = 'test.conflict'

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
        self.ar.load_from_dict(archive_json)
        self.assertEqual(self.ar.ref, test_ref)

    def test_catalog_ref(self):
        my_id = str(uuid4())
        self.ar.add(CatalogRef('bogus.origin', my_id, entity_type='flow'))
        self.assertEqual(self.ar[my_id].uuid, my_id)

    def test_conflicting_ref(self):
        """
        It's an error to instantiate an existing source with a new reference (why? because the source should know its
        own reference).  If it is desired to load a source without knowing its reference, use BasicArchive.from_file()
        :return:
        """
        a = BasicArchive(WORKING_FILE, ref=test_conflict)
        with self.assertRaises(SourceAlreadyKnown):
            a.load_from_dict(archive_json)

    def test_conflicting_src(self):
        """
        On the other hand, one ref is allowed to have multiple sources so this should not cause any issues
        :return:
        """
        a = BasicArchive(conflict_file, ref=test_ref)
        a.load_from_dict(archive_json)
        self.assertSetEqual(set(k for k in a.get_sources(test_ref)), {conflict_file, WORKING_FILE})


if __name__ == '__main__':
    unittest.main()
