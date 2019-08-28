from .lower_dict import LowerDict
from .synonym_set import SynonymSet, ChildNotFound

from collections import defaultdict
import json


class TermExists(Exception):
    """
    Thrown when adding a term that is already in the synonym set
    """
    pass


class MergeError(Exception):
    """
    Thrown when a merge would result in merging two separate sets
    """
    pass


class ParentNotFound(Exception):
    pass


class SynonymDict(object):
    """
    A class that allows retrieval of a given object by any of its synonyms.
    """
    _entry_group = 'SynonymSets'
    _syn_type = SynonymSet
    _ignore_case = False

    def _add_from_dict(self, j):
        name = j['name']
        syns = j.pop('synonyms', [])
        self.new_entry(name, *syns, merge=True)

    def load_dict(self, j):
        for ent in j[self._entry_group]:
            self._add_from_dict(ent)

    def load(self, filename=None):
        if filename is None:
            if self._filename is None:
                return
            filename = self._filename
        with open(filename, 'r') as fp:
            fbs = json.load(fp)
        self.load_dict(fbs)

    def _list_entries(self):
        return [f for f in self.entries]

    def serialize(self, entries=None):
        if entries is None:
            entries = self._list_entries()
        return {self._entry_group: [f.serialize() for f in entries]}

    def save(self, filename=None):
        """
        if filename is specified, it overrules any prior filename
        :param filename:
        :return:
        """
        if filename is not None:
            self._filename = filename
        with open(self._filename, 'w') as fp:
            json.dump(self.serialize(), fp, indent=2)

    @property
    def entry_group(self):
        return self._entry_group

    @entry_group.setter
    def entry_group(self, value):
        self._entry_group = str(value)

    def __init__(self, source_file=None, ignore_case=None, entry_group=None):
        """
        Create a synonym dictionary that stores entries of a certain type.
        :param ignore_case: [False] whether the synonyms should be considered case-sensitive. This setting also causes
         the dict to ignore leading and trailing whitespace (calls strip().lower())

        The entry class must support the following API:
          constructor:
            Ent(*args, **kwargs): kwargs not used for the base SynonymSet
          properties:
            ent.terms: generate a list of synonyms
            ent.object: return the payload object (for SynonymSet, this is a str)
          methods:
            ent.add_term(term): add a new synonym
            ent.remove_term(term): remove an existing synonym
            ent.set_name(term): assign the canonical name for the object (by default the first term provided)
            ent.add_child(a): include a's terms with ent's terms
        """
        self._filename = source_file
        if entry_group is not None:
            self.entry_group = str(entry_group)

        ignore_case = ignore_case or self._ignore_case

        if ignore_case:
            self._d = LowerDict()
            self._l = defaultdict(LowerDict)  # reverse mapping
        else:
            self._d = dict()
            self._l = defaultdict(dict)  # reverse mapping

        self.load()

    def _throw_term_exists(self, term):
        raise TermExists('%s -> %s' % (term, self._d[term]))

    def _check_term(self, term, check_obj=None):
        """
        If term is not known to the dict, returns None
        if check_obj is provided and term does NOT map to check_obj, throw a TermExists error
        if otherwise, return the entry that term maps to
        :param term:
        :param check_obj:
        :return:
        """
        if term in self._d:
            if check_obj is not None:
                if self._d[term] is not check_obj:
                    self._throw_term_exists(term)
            return self._d[term]

    def _add_term(self, term, ent):
        if not isinstance(ent, self._syn_type):
            raise TypeError('Entry is not a %s (%s)' % (self._syn_type, type(ent)))
        if len(term.strip()) == 0:
            return
        self._check_term(term, ent)
        self._d[term] = ent
        self._l[ent][term] = term

    def _remove_term(self, term, remove_from_entry=False):
        ent = self._d.pop(term)
        cterm = self._l[ent].pop(term)
        if remove_from_entry:
            ent.remove_term(cterm)
        if len(self._l[ent]) == 0:
            self._l.pop(ent)

    @property
    def entries(self):
        for k in sorted(self._l.keys(), key=str):
            yield k

    @property
    def objects(self):
        """
        Yield all distinct objects in the dict
        :return:
        """
        for k in self.entries:
            yield k.object

    def objects_with_string(self, pattern):
        for ent in self.entries:
            if ent.contains_string(pattern, ignore_case=self._ignore_case):
                yield ent.object

    def new_entry(self, *args, merge=True, create_child=False, prune=False, **kwargs):
        """
        The input arguments are passed directly to the constructor
        :param args: args to pass to constructor
        :param merge: [True] if synonyms are found in an existing entry, merge the new terms according to create_child
        :param create_child: [True] if true, add new entry as a child; otherwise, add terms to the existing entry
        :param prune: [False] If merge is False and a collision exists, simply omit conflicting terms from the new
         entry. Specifying prune=True to new_entry will override merge=True.
        :param kwargs: kwargs to pass to constructor
        :return: the entry that contains the new terms
        """
        ent = self._syn_type(*args, **kwargs)
        if prune:
            merge = False
        return self.add_or_update_entry(ent, merge=merge, create_child=create_child, prune=prune)

    def _match_set(self, terms):
        """
        Returns a set of synonym entries that match terms in the input argument.
        :param terms: iterable
        :return:
        """
        return set(k for k in filter(None, (self._check_term(t) for t in terms)))

    def match_entry(self, *args):
        mg = self._match_set(args)
        if len(mg) > 1:
            raise MergeError('Found terms in multiple sets: %s' % '; '.join(str(k) for k in mg))
        elif len(mg) == 1:
            return mg.pop()
        else:
            return None

    def add_or_update_entry(self, ent, merge=True, create_child=False, prune=False):
        """
        Returns the entry that contains the input argument's terms
        If merge is true, merge incoming entries with existing matching entry. If create_child, the new entry is kept
        intact and added as a child. If more than one existing entry matches, raise MergeError.

        If prune, existing terms are skipped and new terms are added to a new entry.

        merge and prune are mutually inconsistent.  merge wins.
        :param ent:
        :param merge:
        :param create_child:
        :param prune:
        :return: the entry that contains all of ent's terms
        """
        if merge:
            s = self.match_entry(*ent.terms)
            if s is None:
                for t in ent.terms:
                    self._add_term(t, ent)
                return ent
            else:
                if create_child:
                    self._add_child(s, ent)
                    return ent
                else:
                    self._merge(s, ent)
                    return s
        elif prune:
            prune_terms = set()
            for t in ent.terms:
                try:
                    self._check_term(t, ent)
                except TermExists:
                    prune_terms.add(t)
            if ent.name in prune_terms:
                try:
                    ent.set_name(next(k for k in ent.terms if k not in prune_terms))
                except StopIteration:
                    # no non-pruned terms: nothing to add
                    return self._d[ent.name]  # give back the current match
            for k in prune_terms:
                ent.remove_term(k)
            return self.add_or_update_entry(ent, merge=False, prune=False)
        else:
            for t in ent.terms:
                self._check_term(t, ent)
            for t in ent.terms:
                self._add_term(t, ent)
            return ent

    def _add_child(self, existing_entry, child):
        if existing_entry not in self._l.keys():
            raise ParentNotFound('Entry %s not known' % existing_entry)
        existing_entry.add_child(child)
        for t in child.terms:
            self._add_term(t, existing_entry)

    def _merge(self, existing_entry, ent):
        for c in ent.children:
            self._add_child(existing_entry, c)
        for t in ent.base_terms:
            if self._check_term(t) is None:
                self._add_term(t, existing_entry)
                existing_entry.add_term(t)

    def remove_entry(self, ent):
        """
        remove an entry and all of its terms from the dictionary, without altering the entry itself
        :param ent:
        :return:
        """
        remove_terms = [t for t in self._l[ent].keys()]
        for t in remove_terms:
            self._remove_term(t)

    def merge(self, first, second, child=False):
        """
        Merge the sets containing the two terms
        :param first: dominant
        :param second: removed from dict and merged with first
        :param child: [False] if true, preserve the second entry as a child, rather than merging its content
        :return:
        """
        ob1 = self._d[first]
        ob2 = self._d[second]
        self.remove_entry(ob2)
        if child:
            self._add_child(ob1, ob2)
        else:
            self._merge(ob1, ob2)

    def unmerge_child(self, ent):
        """
        The supplied ent must be a child of another entry.  Removes it and establishes it as a separate entry.  This
        will fail [TermExists] if the child and the parent share any terms in common.
        :param ent:
        :return:
        """
        try:
            parent = next(v for v in self.entries if v.has_child(ent))
        except StopIteration:
            raise ChildNotFound('No set found containing child %s' % ent)
        parent.remove_child(ent)
        dups = [k for k in ent.terms if k in parent]
        if len(dups) > 0:
            parent.add_child(ent)
            raise TermExists('Terms duplicated in parent: %s' % '; '.join(dups))
        for k in ent.terms:
            self._remove_term(k)
        return self.add_or_update_entry(ent, merge=False)

    def add_synonym(self, term, syn):
        """
        Add a new term as a synonym to an existing term
        :param term: the existing term
        :param syn: the new synonym
        :return:
        """
        syn = str(syn)
        if isinstance(term, self._syn_type):
            term = term.name
        ent = self._d[term]
        self._add_term(syn, ent)  # checks TermExists
        ent.add_term(syn)

    def del_term(self, term):
        """
        Delete a term from the dictionary and from the entry itself
        :param term:
        :return:
        """
        self._remove_term(term, remove_from_entry=True)

    def set_name(self, term):
        if term not in self:
            raise KeyError('Unknown term %s' % term)
        ent = self._d[term]
        cterm = self._l[ent][term]  # necessary for case-insensitive dictionaries
        ent.set_name(cterm)

    def get(self, term, default=None):
        try:
            return self.__getitem__(term)
        except KeyError:
            return default

    def synonyms(self, term):
        if isinstance(term, SynonymSet):
            ent = term
        else:
            ent = self._d[term]
        for t in sorted(self._l[ent].values()):
            yield t

    def items(self):
        for k, v in self._d.items():
            yield k, v.object

    def __len__(self):
        return len(self._l)

    def __contains__(self, item):
        return item in self._d

    def __getitem__(self, term):
        if isinstance(term, self._syn_type) and term in self._l:
            return term.object
        return self._d[term].object
