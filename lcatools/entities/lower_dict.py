"""
from https://stackoverflow.com/questions/2082152
(see also https://stackoverflow.com/questions/3387691 -- my requirements are specific enough to justify this approach,
so long as it works)
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

    def __str__(self):
        return self._orig


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

    def pop(self, key, default=None):
        key = self.Key(key)
        return super(LowerDict, self).pop(key, default)

    def items(self):
        # this may be ugly, but.. um.. so's your mother
        return {str(k): v for k, v in super(LowerDict, self).items()}.items()

    def keys(self):
        return {str(k): 0 for k in super(LowerDict, self).keys()}.keys()
