import unittest
from datetime import datetime

from ...entity_store import EntityStore


phony_source = '/path/to/phony/source.gz'
phony_ref = 'local.path.to.phony.source'
phony_ref_bumped = 'local.path.to.phony.source.bumped'


class ArchiveInterfaceTest(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls._ar = EntityStore(phony_source)

    def test_ref(self):
        self.assertEqual(self._ar.ref, phony_ref)
        self.assertEqual(self._ar.get_names()[phony_source], phony_ref)

    def test_new_ref(self):
        now = datetime.now().strftime('%Y%m%d')
        self.assertEqual(self._ar._construct_new_ref('bumped'), '.'.join([phony_ref_bumped, now]))


if __name__ == '__main__':
    unittest.main()
