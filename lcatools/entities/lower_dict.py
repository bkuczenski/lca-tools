"""
from https://stackoverflow.com/questions/2082152
"""


class LowerDictKeys(object):
    def __init__(self, dict_keys):
        self._orig = dict_keys
        self._dk = [k.lower() for k in dict_keys]

    def __contains__(self, item):
        return item.lower() in self._dk

    def __iter__(self):
        for k in self._orig:
            yield k


# caseinsensitivedict.py
class LowerDict(dict):

    class Key(str):
        def __init__(self, key):
            str.__init__(key)

        def __hash__(self):
            return hash(self.lower())

        def __eq__(self, other):
            return self.lower() == other.lower()

    def __init__(self, *args, data=None, **kwargs):
        super(LowerDict, self).__init__(*args, **kwargs)
        if data is None:
            data = {}
        for key, val in data.items():
            self[key] = val

    def __contains__(self, key):
        key = self.Key(key)
        return super(LowerDict, self).__contains__(key)

    def __setitem__(self, key, value):
        key = self.Key(key)
        super(LowerDict, self).__setitem__(key, value)

    def __getitem__(self, key):
        key = self.Key(key)
        return super(LowerDict, self).__getitem__(key)

    def keys(self):
        return LowerDictKeys(super(LowerDict, self).keys())
