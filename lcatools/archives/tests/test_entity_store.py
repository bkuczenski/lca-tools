import unittest
from datetime import datetime

from ..entity_store import EntityStore
from lcatools.entities import LcQuantity, LcUnit
from lcatools import from_json
from .test_base import test_file


phony_source = '/path/to/phony/source.gz'
phony_ref = 'local.path.to.phony.source'
phony_ref_bumped = 'local.path.to.phony.source.bumped'


class ArchiveInterfaceTest(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls._ar = EntityStore(phony_source)

    def test_ref(self):
        self.assertEqual(self._ar.ref, phony_ref)
        self.assertEqual(self._ar.names[phony_source], phony_ref)

    def test_new_ref(self):
        now = datetime.now().strftime('%Y%m%d')
        self.assertEqual(self._ar._construct_new_ref('bumped'), '.'.join([phony_ref_bumped, now]))

    def test_nsuuid(self):
        """
        Ensure that an entity with a directly-specified UUID gets its NSUUID assigned.
        :return:
        """
        j = from_json(test_file)
        ar = EntityStore(test_file, ns_uuid=j['initArgs']['ns_uuid'])
        ar._entity_types = ('quantity', )

        q_j = next(k for k in j['quantities'] if k['referenceUnit'] == 'l')
        unit = LcUnit(q_j.pop('referenceUnit'))
        nsuuid = ar._ref_to_nsuuid(unit.unitstring)
        self.assertEqual(nsuuid, '21d34f33-b0af-3d82-9bef-3cf03e0db9dc')
        q = LcQuantity(q_j['externalId'], referenceUnit=unit, Name=q_j['Name'], entity_uuid=q_j['entityId'])
        self.assertNotEqual(q.uuid, nsuuid)
        self.assertTrue(q.validate())
        self.assertNotIn(nsuuid, ar)
        self.assertNotIn(q.uuid, ar)
        ar._add(q, q.external_ref)
        self.assertNotIn(q.uuid, ar)  # this is only done in BasicArchives
        self.assertIn(nsuuid, ar)



if __name__ == '__main__':
    unittest.main()
