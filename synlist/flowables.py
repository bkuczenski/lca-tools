import re
from synlist.synlist import SynList, TermFound, CannotSplitName


class ConflictingCas(Exception):
    pass


class NotACas(Exception):
    pass


cas_regex = re.compile('^[0-9]{,6}-[0-9]{2}-[0-9]$')
cas_strict = re.compile('^[0-9]{6}-[0-9]{2}-[0-9]$')


def find_cas(syns):
    found = set()
    for i in syns:
        if bool(cas_regex.match(i)):
            found.add(trim_cas(i))
    if len(found) > 1:
        raise ConflictingCas('Multiple CAS numbers found: %s' % found)
    if len(found) == 0:
        return None
    return found.pop()


def pad_cas(cas):
    if not bool(cas_regex.match(cas)):
        raise NotACas(cas)
    while not bool(cas_strict.match(cas)):
        cas = '0' + cas
    return cas


def trim_cas(cas):
    return re.sub('^(0*)', '', cas)


class Flowables(SynList):
    """
    A SynList that enforces unique CAS numbers on sets.  Also uses case-insensitive lookup

    The CAS thing requires overloading _new_key and _new_group and just about everything else.
    """

    def __init__(self, ignore_case=None):
        """
        :param ignore_case: this parameter is ignored for flowables
        """
        super(Flowables, self).__init__(ignore_case=True)
        self._cas = []

    def cas(self, term):
        return self._cas[self._get_index(term)]

    def cas_name(self, term):
        """
        returns the [[trimmed???]] cas number if it exists; otherwise the canonical name
        :param term:
        :return:
        """
        ind = self._get_index(term)
        if self._cas[ind] is not None:
            return trim_cas(self._cas[ind])
        return self._name[ind]

    def new_item(self, entity=None):
        k = super(Flowables, self).new_item(entity)
        self._cas.append(None)
        return k

    def _get_index(self, term):
        try:
            return super(Flowables, self)._get_index(term)
        except KeyError:
            if len(term.strip()) > 3:
                return super(Flowables, self)._get_index(term.lower())
            raise

    def _assign_term(self, term, index, force=False):
        lterm = self._sanitize(term)
        if lterm in self._dict:
            if self._dict[lterm] != index:
                if force is False:
                    raise TermFound('%s [%s: %d]' % (term, lterm, self._dict[lterm]))
        self._list[index].add(term)
        self._dict[lterm] = index

    def _new_term(self, term, index):
        if term is None or term == '':
            return
        key = self._sanitize(term)
        if key in self._dict:
            if self._dict[key] == index:
                return  # nothing to do
            raise TermFound(term)
        if cas_regex.match(key):
            if self._cas[index] is not None and trim_cas(self._cas[index]) != trim_cas(key):
                raise ConflictingCas('Index %d already has CAS %s' % (index, self._cas[index]))
            else:
                key = pad_cas(key)
                self._cas[index] = key
                super(Flowables, self)._new_term(trim_cas(key), index)
        self._list[index].add(term)
        self._dict[key] = index
        if self._name[index] is None:
            self._name[index] = term
        elif bool(cas_regex.match(self._name[index])) and not bool(cas_regex.match(term)):
            # override CAS-name with non-CAS name, if one is found
            self._name[index] = term

    def _merge(self, merge, into):
        super(Flowables, self)._merge(merge, into)
        self._cas[merge] = None

    def merge(self, dominant, *terms, multi_cas=False):
        """
        Flowables.merge first checks for conflicting CAS numbers, then merges as normal. the _merge interior function
        deletes cas numbers from merged entries; then this function sets the (non-conflicting) CAS number for the
        dominant entry.
        :param dominant:
        :param terms:
        :param multi_cas: [False] if True, multiple CAS numbers are allowed as synonyms;
         the first one encountered is kept canonical.
        :return:
        """
        dom = self._get_index(dominant)
        cas = [self._cas[dom]]
        for i in terms:
            cas.append(self._cas[self._get_index(i)])
        the_cas = [k for k in filter(None, cas)]
        if len(the_cas) > 1:
            if multi_cas:
                the_cas = the_cas[:1]
            else:
                raise ConflictingCas('Indices have conflicting CAS numbers: %s' % the_cas)
        super(Flowables, self).merge(dominant, *terms)
        if len(the_cas) == 1:
            self._cas[dom] = the_cas[0]

    def _matches(self, term):
        if cas_regex.match(term):
            raise CannotSplitName('Cannot split CAS number')
        return super(Flowables, self)._matches(term)

    def check_cas(self, it):
        """
        Does one of three things:
         if incoming iterable has multiple CAS numbers, raise ConflictingCas (done within find_cas)
         if incoming iterable has no CAS numbers, return None
         if incoming iterable has one CAS number, return a list of indices with conflicting CAS numbers. If no
         conflicts are found, this list will be empty.
        :param it:
        :return:
        """
        incoming_cas = find_cas(it)  # returns a trimmed cas
        if incoming_cas is None:
            return None
        conflicts = set()
        for i in it:
            inx = self._known(i)
            if inx is not None and inx not in conflicts:
                contender = self._cas[inx]
                if contender is not None and trim_cas(contender) != incoming_cas:
                    conflicts.add(inx)
        return sorted(list(conflicts))

    def _merge_set_with_index(self, it, index):
        cas = find_cas(it)
        if cas is not None:
            if self._cas[index] is not None and trim_cas(self._cas[index]) != trim_cas(cas):
                print('Conflicting CAS: incoming %s; existing [%s] = %d' % (cas, self._cas[index], index))
                raise ConflictingCas('Incoming set has conflicting CAS %s' % cas)
        super(Flowables, self)._merge_set_with_index(it, index)
