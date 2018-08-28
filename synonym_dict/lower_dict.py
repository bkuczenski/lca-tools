"""
Case-insensitive dict
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


class LowerDict(dict):

    class Key(str):
        def __new__(cls, key):
            # perform the transform at instantiation for performance-- why not?
            return super(LowerDict.Key, cls).__new__(LowerDict.Key, str(key).strip().lower())

        def __hash__(self):
            return super(LowerDict.Key, self).__hash__()

        def __eq__(self, other):
            return self.lower() == other.strip().lower()

    def __init__(self, *args, **kwargs):
        super(LowerDict, self).__init__()
        if len(args) > 0:
            for k, v in args[0]:
                self[k] = v
        for key, val in kwargs.items():
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

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def pop(self, key, default=None):
        key = self.Key(key)
        return super(LowerDict, self).pop(key, default)
