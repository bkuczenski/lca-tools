from lcatools.autorange import AutoRange

import unittest


values = [-0.34, 12e9, .0000867]
scales = [1000, 1.0e-9, 1.0e6]
adj_values = [-340, 12, 86.7]
units = ['kg CO2eq', 'mol H+', 'MMT']
correct_units = [['g CO2eq', 'Tg CO2eq', 'mg CO2eq'],
                 ['mmol H+', 'Gmol H+', 'umol H+'],
                 ['kMT', 'PMT', 'MT']]


class AutoRangeTestCase(unittest.TestCase):
    def test_conversion(self):
        for i, val in enumerate(values):
            fm = 'Failed on %g [item #%d]' % (val, i)
            a = AutoRange(val)
            self.assertEqual(a.scale, scales[i], fm)
            self.assertEqual(a.adjust(val), adj_values[i], fm)

    def test_units(self):
        for i, val in enumerate(values):
            fm = 'Failed on %g [item #%d]' % (val, i)
            a = AutoRange(val)
            for j, unit in enumerate(units):
                self.assertEqual(a.adj_unit(unit), correct_units[j][i], fm)


if __name__ == '__main__':
    unittest.main()
