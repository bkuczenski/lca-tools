import unittest


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
    def test_rxref_is_ref(self):
        """
        The RxRefs that populate a process_ref.reference_entity should test as being contained in the originating
        process.reference_entity.
        :return:
        """
        pass


if __name__ == '__main__':
    unittest.main()
