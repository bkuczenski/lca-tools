from lcatools.providers.base import LcArchive

import os
import unittest


test_json = {
 'dataReference': 'test.basic',
 'dataSource': None,
 'flows': [
     {
         'CasNumber': '',
         'Comment': '',
         'Compartment': ['Forestry and Logging', 'Logging'],
         'Name': 'Reforesting, average state or private moist cold softwood forest, INW',
         'characterizations': [{
             'entityType': 'characterization',
             'isReference': True,
             'quantity': 'cec3a58d-44c3-31f6-9c75-90e6352f0934',
             'value': 1.0}],
         'entityId': '51d92a19-e550-3000-b49b-e633137eac06',
         'entityType': 'flow',
         'externalId': 37572,
         'origin': 'local.uslci.clean'
     },
     {
         'CasNumber': '',
         'Comment': '',
         'Compartment': ['Forestry and Logging', 'Forest Nurseries, Gathering Forest Prod.'],
         'Name': 'Greenhouse seedling, softwood, INW',
         'characterizations': [{
             'entityType': 'characterization',
             'isReference': True,
             'quantity': '8703965a-7a6b-3e3e-a1cf-d9adf7bf1d9f',
             'value': 1.0}],
         'entityId': '9cc0ccce-8e33-35ca-a3c0-c7bb6c397e95',
         'entityType': 'flow',
         'externalId': 24235,
         'origin': 'local.uslci.clean'
     },
     {
         'CasNumber': '',
         'Comment': '',
         'Compartment': ['Utilities', 'Steam and Air-Conditioning Supply'],
         'Name': 'Gasoline, combusted in equipment',
         'characterizations': [{
             'entityType': 'characterization',
             'isReference': True,
             'quantity': '21d34f33-b0af-3d82-9bef-3cf03e0db9dc',
             'value': 1.0}],
         'entityId': 'c65e7555-7652-377b-b69a-126ba3d9b8b7',
         'entityType': 'flow',
         'externalId': 5233,
         'origin': 'local.uslci.clean'
     }],
 'processes': [
     {
         'Classifications': ['Forestry and Logging', 'Logging'],
         'Comment': 'Important note: although most of the data in the US LCI database has undergone some sort of \
                    review, the database as a whole has not yet undergone a formal validation process. Please email \
                    comments to lci@nrel.gov.',
         'Name': 'Reforesting, average state or private moist cold softwood forest, INW',
         'SpatialScope': 'RNA',
         'TemporalScope': "{'begin': '1989-01-01-07:00', 'end': '1996-01-01-07:00'}",
         'entityId': '78c8b1e5-ca60-38b6-9a94-dde046560a38',
         'entityType': 'process',
         'exchanges': [
             {
                 'direction': 'Input',
                 'entityType': 'exchange',
                 'flow': '9cc0ccce-8e33-35ca-a3c0-c7bb6c397e95',
                 'value': 865.0
             },
             {
                 'direction': 'Input',
                 'entityType': 'exchange',
                 'flow': 'c65e7555-7652-377b-b69a-126ba3d9b8b7',
                 'value': 15.959999999999999
             },
             {
                 'direction': 'Output',
                 'entityType': 'exchange',
                 'flow': '51d92a19-e550-3000-b49b-e633137eac06',
                 'isReference': True,
                 'value': 1.0
             }],
         'externalId': 'Reforesting, average state or private moist cold softwood forest, INW',
         'origin': 'local.uslci.clean'
     }],
 'quantities': [
     {
         'Comment': 'EcoSpold01',
         'Name': 'EcoSpold Quantity l',
         'entityId': '21d34f33-b0af-3d82-9bef-3cf03e0db9dc',
         'entityType': 'quantity',
         'externalId': 'l',
         'origin': 'local.uslci.clean',
         'referenceUnit': 'l'
     },
     {
         'Comment': 'EcoSpold01',
         'Name': 'EcoSpold Quantity Item(s)',
         'entityId': '8703965a-7a6b-3e3e-a1cf-d9adf7bf1d9f',
         'entityType': 'quantity',
         'externalId': 'Item(s)',
         'origin': 'local.uslci.clean',
         'referenceUnit': 'Item(s)'
     },
     {
         'Comment': 'EcoSpold01',
         'Name': 'EcoSpold Quantity ha',
         'entityId': 'cec3a58d-44c3-31f6-9c75-90e6352f0934',
         'entityType': 'quantity',
         'externalId': 'ha',
         'origin': 'local.uslci.clean',
         'referenceUnit': 'ha'
     }]
}


class LcArchiveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._ar = LcArchive.from_dict(test_json)

    def test_get_none(self):
        """
        get None returns None
        :return:
        """
        self.assertIsNone(self._ar[None])

    def test_get_by_id(self):
        """
        the standard getitem: put in an entity key, get back the entity. Also ensure that spurious external IDs were
        not propagated.
        :return:
        """
        uuid = '21d34f33-b0af-3d82-9bef-3cf03e0db9dc'
        ent = self._ar[uuid]
        self.assertEqual(ent.entity_type, 'quantity')
        self.assertNotEqual(ent.external_ref, 'l')
        self.assertEqual(ent.external_ref, uuid)

    def test_get_entity(self):
        """
        An error-preventer: put in an entity itself, get back the entity IF the entity is part of the archive.
        :return:
        """
        ent = self._ar['21d34f33-b0af-3d82-9bef-3cf03e0db9dc']
        self.assertIs(self._ar[ent], ent)

    def test_ents_by_type(self):
        self.assertEqual(len([q for q in self._ar.entities_by_type('q')]), 3)

    def test_recursive_references(self):
        fl_uuid = '9cc0ccce-8e33-35ca-a3c0-c7bb6c397e95'
        q_uuid = '8703965a-7a6b-3e3e-a1cf-d9adf7bf1d9f'
        fl = self._ar[fl_uuid]
        self.assertEqual(fl.external_ref, fl_uuid)
        self.assertIs(self._ar[q_uuid], fl.reference_entity)


if __name__ == '__main__':
    unittest.main()
