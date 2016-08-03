import re


class InconsistentIndices(Exception):
    pass


class ConflictingCas(Exception):
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

    def merge_set_with_index(self, it, index):
        for i in it:
            self._new_key(i, index)

    def new_set(self, it):
        index = self._new_group()
        printed = False
        for i in it:
            '''
            if i in self._dict.keys() and self._dict[i] != index:
                if printed is False:
                    print('\n%s' % i)
                    printed = True
                #print('index %d: Ignoring duplicate term [%s] = %d ' % (index, i, self._dict[i]))
            else:
            '''
            if i not in self._dict.keys():
                self._new_key(i, index)
        return index

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
            self.merge_set_with_index(it, index)
        else:
            index = self.new_set(it)
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


cas_regex = re.compile('^[0-9]{,6}-[0-9]{2}-[0-9]$')


def find_cas(syns):
    found = set()
    for i in syns:
        if bool(cas_regex.match(i)):
            found.add(i)
    if len(found) > 1:
        raise ConflictingCas('Multiple CAS numbers found: %s' % found)
    if len(found) == 0:
        return None
    return found.pop()


class Flowables(SynList):
    """
    A SynList that enforces unique CAS numbers on sets
    """

    @classmethod
    def from_json(cls, j):
        s = cls()
        for i in j['flowables']:
            s.add_set(i)
        return s

    def __init__(self):
        super(Flowables, self).__init__()
        self._cas = []

    def _new_group(self):
        k = super(Flowables, self)._new_group()
        self._cas.append(None)
        return k

    def _new_key(self, key, index):
        if cas_regex.match(key):
            if self._cas[index] is not None and self._cas[index] != key:
                raise ConflictingCas('Index %d already has CAS %s' % (index, self._cas[index]))
            else:
                self._cas[index] = key
        super(Flowables, self)._new_key(key, index)

    def _merge(self, merge, into):
        super(Flowables, self)._merge(merge, into)
        if self._cas[merge] is not None:
            if self._cas[into] is not None:
                raise ConflictingCas('this should not happen')
            self._cas[into] = self._cas[merge]
            self._cas[merge] = None

    def merge_indices(self, indices):
        k = []
        for i in indices:
            if self._cas[i] is not None:
                k.append((i, self._cas[i]))
        if len(k) > 1:
            raise ConflictingCas('Indices have conflicting CAS numbers: %s' % k)
        super(Flowables, self).merge_indices(indices)

    def merge_set_with_index(self, it, index):
        cas = find_cas(it)
        if cas is not None:
            if self._cas[index] is not None and self._cas[index] != cas:
                print('Conflicting CAS: incoming %s; existing [%s] = %d' % (cas, self._cas[index], index))
                raise ConflictingCas('Incoming set has conflicting CAS %s' % cas)
        super(Flowables, self).merge_set_with_index(it, index)

    def serialize(self):
        return {
            'flowables': [self._serialize_set(i) for i in range(len(self._list))
                          if self._list[i] is not None]
        }
