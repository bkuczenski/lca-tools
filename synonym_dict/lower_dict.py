"""
Case-insensitive dict
from https://stackoverflow.com/questions/2082152
(see also https://stackoverflow.com/questions/3387691 -- my requirements are specific enough to justify this approach,
so long as it works)
"""


class LowerDictKeys(object):
    def __init__(self, dict_keys):
        self._orig = dict_keys
        self._dk = [k.lower() for k in dict_keys]

    def __contains__(self, item):
        return item.strip().lower() in self._dk

    def __iter__(self):
        for k in self._orig:
            yield str(k)


class LowerDict(dict):

    class Key(str):
        def __new__(cls, key):
            # perform the transform at instantiation for performance-- why not?
            return super(LowerDict.Key, cls).__new__(LowerDict.Key, str(key).strip())

        def __hash__(self):
            return hash(self.lower())

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

    '''
    # NOTE: as of 2019/02/15, in merge master, I cannot replicate the behavior that inspired this addition
    # So I'm leaving it commented out but present, so that it can be reinstated [and tested] if a test case is found
    # 2019/05/24 done. see test_items(). Solution is also less ugly.
    def items(self):
        # this may be ugly, but.. um.. so's your mother
        return {str(k): v for k, v in super(LowerDict, self).items()}.items()
    '''
    def items(self):
        for k, v in super(LowerDict, self).items():
            yield str(k), v

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def pop(self, key, default=None):
        key = self.Key(key)
        return super(LowerDict, self).pop(key, default)

    def update(self, m, **kwargs):
        for k, v in m.items():
            self.__setitem__(k, v)
        for k, v in kwargs.items():
            self.__setitem__(k, v)
