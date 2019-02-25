from .cas_number import CasNumber, InvalidCasNumber, NotSupported
import unittest


co2_set = {'000124-38-9', '000124389', '124-38-9', '124389'}


class CasNumberTest(unittest.TestCase):
    def test_valid_co2(self):
        g = CasNumber(124389)
        self.assertSetEqual(set([k for k in g.terms]), co2_set)
        self.assertEqual(str(g), '124-38-9')

    def test_invalid(self):
        with self.assertRaises(InvalidCasNumber):
            CasNumber(1234)
        with self.assertRaises(InvalidCasNumber):
            CasNumber(55, 89, 47, 2)
        with self.assertRaises(InvalidCasNumber):
            CasNumber('37205.543.1')

    def test_add_remove(self):
        c = CasNumber(12345)
        with self.assertRaises(NotSupported):
            c.add_term('Floobie')
        self.assertIn('12-34-5', c)
        with self.assertRaises(NotSupported):
            c.remove_term('12-34-5')

    def test_equal(self):
        g = CasNumber(124389)
        h = CasNumber('124-38-9')
        self.assertEqual(g, h)

    def test_pad_input(self):
        g = CasNumber('00124+38+9')
        self.assertSetEqual(set([k for k in g.terms]), co2_set)

    def test_tuple_input(self):
        g = CasNumber(('32768', '4', 1))
        self.assertEqual(str(g), '32768-04-1')
        h = CasNumber((32768, 4, '1'))
        self.assertEqual(h.object, g.object)
        j = CasNumber('32768', 4.0, '1')
        self.assertEqual(h.object, j.object)


if __name__ == '__main__':
    unittest.main()
