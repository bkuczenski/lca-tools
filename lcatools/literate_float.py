
class LiterateFloat(float):
    """
    A class for "fancy" numbers. A literate value is basically a float with a dict- so it operates in computations
    normally with its assigned (immutable) value but can also store "oodles" of additional data, such as, e.g.
    cmutel's uncertainty dictionaries, or tags describing the values, or really anything else.

    Not sure this will ultimately be useful, but here's to trying new things.

    note: serialize as floats- literates are well-read but not outspoken
    """
    def __new__(cls, value, **kwargs):
        return float.__new__(cls, value)

    def __init__(self, value, **kwargs):
        float.__init__(value)
        self._d = dict()
        for k, v in kwargs.items():
            self._d[k] = v

    def __setitem__(self, key, value):
        self._d[key] = value

    def __getitem__(self, item):
        return self._d[item]

    def keys(self):
        return self._d.keys()

    def dict(self):
        return self._d

    def __delitem__(self, key):
        del self._d[key]

    @property
    def mean(self):
        if 'mean' in self._d:
            return self._d['mean']
        else:
            return float(self)

    @mean.setter
    def mean(self, value):
        self._d['mean'] = value

    @property
    def stdev(self):
        if 'stdev' in self._d:
            return self._d['stdev']
        else:
            return 0.0

    @stdev.setter
    def stdev(self, value):
        self._d['stdev'] = value
