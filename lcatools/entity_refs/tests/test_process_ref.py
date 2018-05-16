import unittest

from lcatools import LcCatalog
from lcatools.data_sources.local import CATALOG_ROOT

cat = LcCatalog(CATALOG_ROOT)

test_ref = 'local.ecoinvent.3.2.apos'
blast_furnace = '00420798-e9d1-4de9-8745-09bd85f31db8'
blast_furnace_gas = 'b254bbdf-fb2b-4878-aec9-2a7820f3f32e'


class ProcessRefTest(unittest.TestCase):
    """
    Things to test:
     * default_rx is properly set for processes with one reference exchange
     * default_rx is not set for processes with zero or more than one ref ex
     * RxRef acts like a reference exchange:
       - is_reference is True
       - membership in both process_ref.reference_entity and process.reference_entity
     * is_allocated behaves as expected

    """
    @classmethod
    def setUpClass(cls):
        cls._ar = cat.get_archive(test_ref, 'inventory')

    def test_rxref_is_ref(self):
        """
        The RxRefs that populate a process_ref.reference_entity should test as being contained in the originating
        process.reference_entity.
        :return:
        """
        p = self._ar.retrieve_or_fetch_entity(blast_furnace)
        p_ref = p.make_ref(cat.query(test_ref))
        for rx in p_ref.reference_entity:
            self.assertTrue(p.has_reference(rx))

    @staticmethod
    def _get_matching_ref(rx, p):
        for rxc in p.references():
            if rxc.key == rx.key:
                return rxc
        raise AssertionError('no matching ref found')

    def test_p_ref_access_ref_by_string(self):
        p_ref = cat.query(test_ref).get(blast_furnace)
        rx1 = p_ref.reference(blast_furnace_gas)
        rx2 = p_ref.reference(cat.query(test_ref).get(blast_furnace_gas))
        self.assertIs(rx1, rx2)

    '''
    def test_p_ref_is_allocated(self):
        """
        is_allocated should go away
        :return:
        """
        p = self._ar.retrieve_or_fetch_entity(blast_furnace)
        p_ref = cat.query(test_ref).get(blast_furnace)
        for rxref in p_ref.reference_entity:
            rx = self._get_matching_ref(rxref, p)
            self.assertEqual(p_ref.is_allocated(rx), p_ref.is_allocated(rxref))
    '''


if __name__ == '__main__':
    unittest.main()
