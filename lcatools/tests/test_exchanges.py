import unittest

from lcatools.archives import LcArchive
from lcatools.exchanges import AmbiguousReferenceError, DuplicateExchangeError, ExchangeError
from lcatools.entities.tests import refinery_archive

grid_id = '96bffbb9-b875-36cf-8a11-5723c9d239d9'
petro_id = '0aaf1e13-5d80-37f9-b7bb-81a6b8965c71'
nox_id = '1827a862-ccac-37ac-9ad7-d2dffe71058c'
ocean_id = '455470fa-1a78-344b-ae09-ffa4e22c1f8b'
ng_id = '6cb3e4a8-d7e0-3566-ae66-8022182af846'
diesel_id = 'd939590b-a0d7-310c-8952-9921ed64a078'
resid_id = '562918fe-3ff5-33f4-abaa-d8c615380d25'


class ExchangesTest(unittest.TestCase):
    """
    An exchange requires a process, flow, direction, and optional termination specifier.
    The only things really to test ore setting and querying exchange values

    Expected behavior:
    __setitem__(key, value):
       * key.process must be the same as self.process
       * key must be a reference exchange
       * exchange must not already have an allocation factor specified for the given key
       * if key is self, unallocated value is set (allocated value of reference exchange is always 1 or 0)
       * if self is reference exchange but not key, value must be 0 (and setting it has no effect)
       notes:
       = if process is using allocation by quantity, the specified value will never be returned

    __getitem__(key):
       * if self is a reference item, the allocated exchange value must be 1 if key is self; 0 otherwise
       * if key is a reference item:
         - if process is using allocation by quantity, compute the NORMALIZED factor and return it
         - elif key is in the exchange's value dict, return stored value
         - else return 0
       * else normalize unallocated value by key's value and return it

    """
    @classmethod
    def setUpClass(cls):
        cls.A = LcArchive.from_file(refinery_archive)
        cls.grid = cls.A[grid_id]
        cls.petro = cls.A[petro_id]
        cls.diesel = cls.A[diesel_id]
        cls.petro_nox = next(cls.petro.exchange_values(cls.A[nox_id]))  # pull the NOx exchange
        cls.petro_ocean = next(cls.petro.exchange_values(cls.A[ocean_id]))  # ocean freight exchange
        cls.petro_d = cls.petro.reference(cls.diesel)
        cls.petro_r = cls.petro.reference(cls.A[resid_id])
        cls.grid_ng = next(cls.grid.exchange_values(cls.A[ng_id]))  # electricity from natural gas

    def test_exchange_value(self):
        self.assertAlmostEqual(self.grid.reference().value, 3.6, places=6)

    def test_same_process(self):
        with self.assertRaises(ExchangeError):
            val = self.petro_nox[self.grid.reference()]

        with self.assertRaises(ExchangeError):
            self.petro_nox[self.grid.reference()] = 47

    def test_reference_exchange(self):
        with self.assertRaises(AmbiguousReferenceError):
            self.petro_nox[self.petro_ocean] = 123

        self.assertAlmostEqual(self.petro_d.value, 0.000252345, places=6)
        self.assertIsNotNone(self.petro_nox[self.petro_d])

    def test_reference_alloc(self):
        self.assertEqual(self.petro_d[self.petro_d], 1.0)
        self.assertEqual(self.petro_r[self.petro_r], 1.0)
        self.assertEqual(self.petro_d[self.petro_r], 0.0)

    def test_duplicate_exchange(self):
        with self.assertRaises(DuplicateExchangeError):
            self.petro_d[self.petro_d] = 47

    def test_norm_computation(self):
        self.assertAlmostEqual(self.grid_ng[self.grid.reference()], self.grid_ng.value / self.grid.reference().value,
                               places=10)

    def test_alloc_computation(self):
        # let's spell the whole thing out
        exch = self.petro_ocean
        ref = self.petro_d
        total_req = exch.value  # 4.408 t*km
        total_output = self.petro.alloc_total  # 1.0014 kg
        opt_per_kg = total_req/total_output  # 4.403 t*km / kg

        kg_per_unit_ref = self.petro_d.flow.cf(self.petro.alloc_qty)  # 1 unit diesel = 850 kg

        self.assertAlmostEqual(exch[ref], opt_per_kg * kg_per_unit_ref, places=10)

    def test_alloc_consistency(self):
        for k in self.petro.inventory():
            if k.is_reference:
                continue
            total = sum(k[rx] * rx.value for rx in self.petro.references())
            self.assertAlmostEqual(total, k.value, places=10)

    def test_alloc_equality(self):
        d_cf = self.petro_d.flow.cf(self.petro.alloc_qty)
        r_cf = self.petro_r.flow.cf(self.petro.alloc_qty)
        for k in self.petro.inventory():
            if k.is_reference:
                continue
            self.assertAlmostEqual(k[self.petro_d] / d_cf, k[self.petro_r] / r_cf, places=10)


if __name__ == '__main__':
    unittest.main()
