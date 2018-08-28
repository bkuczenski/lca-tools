from synonym_dict.example_compartments import Context
from lcatools.characterizations import Characterization
from collections import defaultdict


class QuantityMismatch(Exception):
    pass


class CLookup(object):
    """
    A CLookup is a kind of fuzzy dictionary that maps context to best-available characterization. A given CLookup is
    associated with a single quantity and a specific flowable.  The query then provides the compartment and returns
    either: a set of best-available characterizations; a single characterization according to a selection rule; or a
    characterization factor (float) depending on which method is used.
    """
    def __init__(self):
        self._dict = defaultdict(set)
        self._qid = None

    def __getitem__(self, item):
        """
        Returns
        :param item:
        :return:
        """
        if not isinstance(item, Context):
            raise TypeError('Supplied CLookup key is not a Context: %s (%s)' % (item, type(item)))
        if item in self._dict:
            return self._dict[item]
        return set()

    def _check_qty(self, value):
        if self._qid is None:
            self._qid = value.quantity.uuid
        else:
            if value.quantity.uuid != self._qid:
                raise QuantityMismatch('Inbound: %s\nCurrent: %s' % (value.quantity, self._qid))

    def add(self, value):
        if not isinstance(value, Characterization):
            raise TypeError('Value is not a Characterization')
        key = value.context
        if isinstance(key, Context):
            self._check_qty(value)
            self._dict[key].add(value)
        else:
            raise ValueError('Context is not valid: %s (%s)' % (key, type(key)))

    def remove(self, value):
        key = value.context
        self._dict[key].remove(value)

    def compartments(self):
        return self._dict.keys()

    def cfs(self):
        for c, cfs in self._dict.items():
            for cf in cfs:
                yield cf

    def find(self, item, dist=1, return_first=True):
        """
        Hunt for a matching compartment. 'dist' param controls the depth of search:
          dist = 0: equivalent to __getitem__
          dist = 1: also check compartment's children (subcompartments), to any depth, returning all CFs encountered
            (unless return_first is True, in which case all CFs from the first nonempty compartment found are returned)
          dist = 2: also check compartment's parent
          dist = 3: also check all compartment's parents until root. Useful for finding unit conversions.
        By default (dist==1), checks compartment self and children. Returns a set.
        :param item: a Compartment
        :param dist: how far to search (with limits) (default: 1= compartment + children)
        :param return_first: stop hunting as soon as a cf is found
        :return: a set of characterization factors that meet the query criteria.
        """
        if not isinstance(item, Context):
            return set()

        def found(res):
            return len(res) > 0 and return_first
        results = self.__getitem__(item)
        if found(results):
            return results

        if dist > 0:
            for s in item.self_and_subcompartments:  # note: this is depth first
                if s is item:
                    continue  # skip self, just recurse subcompartments
                if s in self._dict.keys():
                    results = results.union(self._dict[s])
                    if found(results):
                        return results

        if dist > 1:
            item = item.parent
            if item in self._dict.keys():
                results = results.union(self._dict[item])

        if found(results):
            return results

        if dist > 2:
            while item.parent is not None:
                item = item.parent
                if item in self._dict.keys():
                    results = results.union(self._dict[item])
                if found(results):
                    return results

        return results

    def find_first(self, item, dist=3):
        cfs = self.find(item, dist=dist, return_first=True)
        return list(cfs)[0]


class FactorCollision(Exception):
    pass


class SCLookup(CLookup):
    """
    A Strict CLookup that permits only one CF to be stored per compartment and raises an error if an additional one
    is added.
    """
    def add(self, value):
        if len(self._dict[value.context]) > 0:
            raise FactorCollision('This context already has a CF defined!')
        super(SCLookup, self).add(value)
