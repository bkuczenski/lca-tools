class InconsistentIndices(Exception):
    pass


class SynList(object):
    """
    An ordered list of synonym sets.  The SynList has two components:
     * a list of sets (with unique entries)
     * a dict whose keys are the unique entries, and whose values are the indices into the list

    This allows two types of references:
     - put in a string--> get back a set of synonyms
     - put in an index--> get back a set of synonyms

    This gets easily serialized:
     - JSON list of sets of synonyms

    And de-serialized:
     - from that list, construct the list. boo hoo!
    """
    @classmethod
    def from_json(cls, j):
        s = cls()
        for i in j['synList']:
            s.add_set(i)
        return s

    def __init__(self):
        self._list = []
        self._dict = dict()

    def _new_key(self, key, index):
        self._list[index].add(key)
        self._dict[key] = index

    def _new_group(self):
        k = len(self._list)
        self._list.append(set())
        return k

    def add_key(self, key):
        """
        Given a single key--> return its index, either existing or new
        :param key:
        :return:
        """
        if key not in self._dict.keys():
            index = self._new_group()
            self._new_key(key, index)
        return self._dict[key]

    def find_indices(self, it):
        found = set()
        for i in it:
            if i in self._dict.keys():
                found.add(self._dict[i])
        return found

    def add_set(self, it):
        """
        given an iterable of keys:
         - if any of them are found:
          - if they are found in multiple indices, raise an inconsistency error
          - elif they are found in one index, add all to the index
          - elif they are not found at all, add them to a new index
        :param it: an iterable of keys
        :return:
        """
        found = self.find_indices(it)
        if len(found) > 1:
            raise InconsistentIndices('Keys found in indices: %s' % found)
        elif len(found) == 1:
            index = found.pop()
            for i in it:
                self._new_key(i, index)
            return index
        else:
            index = self._new_group()
            for i in it:
                self._new_key(i, index)
            return index

    def _merge(self, merge, into):
        print('Merging\n## %s \ninto synonym set containing\n## %s' % (self._list[merge], self._list[into]))
        self._list[into] = self._list[into].union(self._list[merge])
        for i in self._list[into]:
            self._dict[i] = into
        self._list[merge] = None

    def merge_indices(self, indices):
        """
        merges the synonyms from two or more indices into one set.
        :param indices: list of indices to merge together. the lowest index will receive synonyms from higher indices.
        :return:
        """
        indices = set(indices)
        merge_into = min(indices)
        indices.remove(merge_into)
        for i in indices:
            self._merge(i, merge_into)

    def add_synonym(self, existing, new):
        index = self.add_key(existing)
        self._new_key(new, index)
        return index

    def synonyms_for(self, key):
        return self.synonym_set(self._dict[key])

    def synonym_set(self, index):
        return self._list[index]

    def __getitem__(self, item):
        return self.synonym_set(item)

    def _serialize_set(self, index):
        return [k for k in self._list[index]]

    def serialize(self):
        return {
            'synList': [self._serialize_set(i) for i in range(len(self._list))
                        if self._list[i] is not None]
        }
