import unittest
from lcatools.entities.tests.base_testclass import BasicEntityTest


class ProcessesTest(BasicEntityTest):
    """
    Things to test:
    """
    def test_reference(self):
        diesel = next(self.A.search('flow', Name='diesel, at refinery'))
        self.assertAlmostEqual(self.petro.reference(diesel).value, 0.000252345)

    def test_single_reference(self):
        """
        For a process with exactly one reference, it should be returned without specification
        :return:
        """
        elec = next(self.A.search('flow', Name='Electricity, at grid, US, 2008'))
        ex = self.grid.reference()
        self.assertIs(elec, ex.flow)

    def test_inventory(self):
        """
        inventory() returns unallocated exchanges and inventory(ref) returns exchanges normalized by ref
        :return:
        """
        ex = self.grid.reference()
        inv = self.grid.inventory()
        inv_dict = {x.flow: x.value for x in inv}
        for x in self.grid.inventory(ex):
            self.assertEqual(x.value, inv_dict[x.flow] / ex.value)


if __name__ == '__main__':
    unittest.main()
