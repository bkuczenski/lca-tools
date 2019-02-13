import unittest
import os
from shutil import rmtree
from uuid import uuid4

from lcatools.entities.fragment_editor import FragmentEditor
from .. import LcForeground
from lcatools.entity_refs import CatalogRef  # , EntityRefMergeError  ## merge error no longer!

WORKING_DIR = os.path.join(os.path.dirname(__file__), 'test-foreground')

test_ref = 'test.foreground'
flow_uuid = str(uuid4())
frag_uuid = str(uuid4())
frag_ext_name = 'the bodacious reference fragment'

a_different_frag_uuid = str(uuid4())
a_different_frag_ref = 'a completely separate reference fragment'

flow_json = {
    'externalId': flow_uuid,
    'entityType': 'flow',
    'origin': test_ref,
    'CasNumber': '',
    'Compartment': ['Intermediate Flows']
}

frag_json = [{
    'entityId': a_different_frag_uuid,
    'externalId': a_different_frag_ref,
    'entityType': 'fragment',
    'origin': test_ref,
    'flow': flow_uuid,
    'direction': 'Input',
    'parent': None,
    'isPrivate': False,
    'isBackground': False,
    'isBalanceFlow': False,
    'exchangeValues': {
        '0': 1.0,
        '1': 1.0
    },
    'tags': {
        'Comment': 'this is the fragment the test has made'
    },
    'terminations': {
        'default': {}
    }
}]


"""
Foreground unit testing. What does the foreground do?

on top of a basic archive, it:
 - stores, serializes, deserializes fragments
 - allows to name fragments
 - lists fragments
 - requires all entities to either be local or catalog refs
 * some specialty things like finding terminations nd deleting fragments that are not used
 
For now, the only thing we want to test is the renaming of fragments-- after which either uuid or name should retrieve
the fragment.

Then, this will change somewhat when we upgrade fragments to use links instead of uuids, to allow for different 
instances of the same uuid to be stored in the same foreground.
"""


class LcForegroundTestCase(unittest.TestCase):
    """
    Bucking convention to make this a sequential test, because the operations necessarily depend on one another
    """
    @classmethod
    def setUpClass(cls):
        cls.ed = FragmentEditor()
        cls.fg = LcForeground(WORKING_DIR, ref=test_ref)
        cls.fg.entity_from_json(flow_json)

    def test_1_make_fragment(self):
        myflow = self.fg[flow_uuid]
        frag = self.ed.create_fragment(myflow, 'Output', uuid=frag_uuid, comment='Test Fragment')
        self.fg.add(frag)

    def test_2_retrieve_fragment(self):
        frag = self.fg[frag_uuid]
        self.assertEqual(frag['Comment'], 'Test Fragment')

    def test_3_name_fragment(self):
        frag = self.fg[frag_uuid]
        self.fg.name_fragment(frag, frag_ext_name)
        self.assertIs(frag, self.fg[frag_ext_name])

    def test_4_uuid_still_works(self):
        self.assertIs(self.fg[frag_uuid], self.fg[frag_ext_name])

    def test_5_deserialize_named_fragment(self):
        self.fg._do_load(frag_json)
        self.assertEqual(self.fg[a_different_frag_ref].uuid, a_different_frag_uuid)

    def test_6_save_foreground(self):
        self.fg.save()
        new_fg = LcForeground(WORKING_DIR)
        self.assertEqual(self.fg[flow_uuid], new_fg[flow_uuid])
        self.assertEqual(self.fg[a_different_frag_ref].uuid, new_fg[a_different_frag_uuid].uuid)

    def test_7_different_origins(self):
        my_id = uuid4()
        q_ref_1 = CatalogRef('fictitious.origin.v1', my_id, entity_type='quantity')
        q_ref_2 = CatalogRef('fictitious.origin.v2', my_id, entity_type='quantity')
        self.fg.add(q_ref_1)
        '''
        with self.assertRaises(EntityRefMergeError):
            # this will give an error that CatalogRefs can't merge
            self.fg.add(q_ref_2)
        '''
        self.fg.add(q_ref_2)
        self.assertIs(self.fg[q_ref_2.link], q_ref_2)

    @classmethod
    def tearDownClass(cls):
        rmtree(WORKING_DIR)


if __name__ == '__main__':
    unittest.main()
