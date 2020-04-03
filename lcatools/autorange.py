"""
autorange.py

Scales numerical values by powers of 1000 to present engineering notation. Also scales metric prefixes to match.
"""
from math import log10


metric_offsets = {
    'a': -6,
    'f': -5,
    'p': -4,
    'n': -3,
    'u': -2,
    'm': -1,
    'k': 1,
    'M': 2,
    'G': 3,
    'T': 4,
    'P': 5,
    'E': 6,
    'Z': 7
}


metric_prefixes = dict([(v, k) for k, v in metric_offsets.items()])
metric_prefixes[0] = ''


disallow_prefix = {'mol', 'MT'}  # units that start with these phrases should NOT use inferred prefix


class AutoRange(object):
    def _set_shift(self):
        self._shift = 0
        r = log10(self._range)
        while True:
            if r < 0:
                r += 3
                self._shift += 1
            elif r > 3:
                r -= 3
                self._shift -= 1
            else:
                return

    def __init__(self, rng):
        """
        creates an object for computing auto-ranged values.  The input argument should be the largest value that
        is expected to appear in the context.  It will be ranged to fall between 0-1000 (absolute value).
        :param rng:
        """
        self._range = abs(rng)
        if self._range == 0:
            print('Warning: autorange initialized with 0 range; using 1')
            self._range = 1
        self._shift = 0
        self._set_shift()

    @property
    def scale(self):
        return 10 ** (3 * self._shift)

    def adjust(self, value):
        return value * self.scale

    def adj_unit(self, unit):
        """
        This is the tricky, ad-hoc part.  Assume the first character in the unit string is a prefix IF it is found
        in the metric prefixes dictionary.  If it is not, assume it is unprefixed.
        :param unit: a unit string
        :return:
        """
        prefix = unit[0]

        if any([unit.startswith(x) for x in disallow_prefix]) or prefix not in metric_offsets:
            pre_shift = 0
            adj = unit
        else:
            pre_shift = metric_offsets[prefix]
            adj = unit[1:]

        post_shift = pre_shift - self._shift

        return metric_prefixes[post_shift] + adj
