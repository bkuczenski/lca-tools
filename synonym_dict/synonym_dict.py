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
        self.new_object(name, *syns, merge=False)

    def load(self, filename=None):
        if filename is None:
            if self._filename is None:
                return
            filename = self._filename
        with open(filename, 'r') as fp:
            fbs = json.load(fp)
        for fb in fbs[self._entry_group]:
            self._add_from_dict(fb)

    def _list_objects(self):
        return [f for f in self.objects]

    def save(self, filename=None):
        """
        if filename is specified, it overrules any prior filename
        :param filename:
        :return:
        """
        if filename is not None:
            self._filename = filename
        fb = self._list_objects()
        with open(filename, 'w') as fp:
            json.dump({self._entry_group: [f.serialize() for f in fb]}, fp, indent=2)

    def __init__(self, source_file=None, ignore_case=None):
        """
        Create a synonym dictionary that stores objects of a certain type.
        :param ignore_case: [False] whether the synonyms should be considered case-sensitive. This setting also causes
         the dict to ignore leading and trailing whitespace (calls strip().lower())

        The object class must support the following API:
          constructor:
            Obj(*args, **kwargs): kwargs not used for the base SynonymSet
          properties:
            obj.terms: generate a list of synonyms
            obj.object: return the payload object (for SynonymSet, this is a str)
          methods:
            obj.add_term(term): add a new synonym
            obj.remove_term(term): remove an existing synonym
            obj.set_name(term): assign the canonical name for the object (by default the first term provided)
            obj.add_child(a): include a's terms with obj's terms
        """
        self._filename = source_file

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
        if otherwise, return the object that term maps to
        :param term:
        :param check_obj:
        :return:
        """
        if term in self._d:
            if check_obj is not None and self._d[term] is not check_obj:
                self._throw_term_exists(term)
            return self._d[term]

    def _add_term(self, term, obj):
        if not isinstance(obj, self._syn_type):
            raise TypeError('Object is not a %s (%s)' % (type(self._syn_type), type(obj)))
        self._check_term(term, obj)
        self._d[term] = obj
        self._l[obj][term] = term

    def _remove_term(self, term, remove_from_object=False):
        obj = self._d.pop(term)
        cterm = self._l[obj].pop(term)
        if remove_from_object:
            obj.remove_term(cterm)
        if len(self._l[obj]) == 0:
            self._l.pop(obj)

    @property
    def objects(self):
        """
        Yield all distinct objects in the dict
        :return:
        """
        for k in sorted(self._l.keys(), key=str):
            yield k

    def new_object(self, *args, merge=True, create_child=False, prune=False, **kwargs):
        """
        The input arguments are passed directly to the constructor
        :param args: args to pass to constructor
        :param merge: [True] if synonyms are found in an existing object, merge the new terms according to create_child
        :param create_child: [True] if true, add new object as a child; otherwise, add terms to the existing object
        :param prune: [False] If merge is False and a collision exists, simply omit conflicting terms from the new
         object. Specifying prune=True to new_object will override merge=True.
        :param kwargs: kwargs to pass to constructor
        :return: the object that contains the new terms
        """
        obj = self._syn_type(*args, **kwargs)
        if prune:
            merge = False
        return self.add_or_update_object(obj, merge=merge, create_child=create_child, prune=prune)

    def _match_set(self, terms):
        """
        Returns a set of synonym objects that match terms in the input argument.
        :param terms: iterable
        :return:
        """
        return set(k for k in filter(None, (self._check_term(t) for t in terms)))

    def match_object(self, *args):
        mg = self._match_set(args)
        if len(mg) > 1:
            raise MergeError('Found terms in multiple sets: %s' % '; '.join(str(k) for k in mg))
        elif len(mg) == 1:
            return mg.pop()
        else:
            return None

    def add_or_update_object(self, obj, merge=True, create_child=False, prune=False):
        """
        Returns the object that contains the input argument's terms
        :param obj:
        :param merge:
        :param create_child:
        :param prune:
        :return: the object that contains all of obj's terms
        """
        if merge:
            s = self.match_object(*obj.terms)
            if s is None:
                for t in obj.terms:
                    self._add_term(t, obj)
                return obj
            else:
                if create_child:
                    self._add_child(s, obj)
                    return obj
                else:
                    self._merge(s, obj)
                    return s
        elif prune:
            prune_terms = []
            for t in obj.terms:
                try:
                    self._check_term(t, obj)
                except TermExists:
                    prune_terms.append(t)
            if obj.name in prune_terms:
                try:
                    obj.set_name(next(k for k in obj.terms if k not in prune_terms))
                except StopIteration:
                    # no non-pruned terms: nothing to add
                    return self._d[obj.name]  # give back the current match
            for k in prune_terms:
                obj.remove_term(k)
            return self.add_or_update_object(obj, merge=False, prune=False)
        else:
            for t in obj.terms:
                self._check_term(t, obj)
            for t in obj.terms:
                self._add_term(t, obj)
            return obj

    def _add_child(self, existing_object, child):
        if existing_object not in self._l.keys():
            raise ParentNotFound('Object %s not known' % existing_object)
        existing_object.add_child(child)
        for t in child.terms:
            self._add_term(t, existing_object)

    def _merge(self, existing_object, obj):
        for c in obj.children:
            self._add_child(existing_object, c)
        for t in obj.base_terms:
            if self._check_term(t) is None:
                self._add_term(t, existing_object)
                existing_object.add_term(t)

    def remove_object(self, obj):
        """
        remove an object and all of its terms from the dictionary, without altering the object itself
        :param obj:
        :return:
        """
        remove_terms = [t for t in self._l[obj].keys()]
        for t in remove_terms:
            self._remove_term(t)

    def merge(self, first, second, child=False):
        """
        Merge the sets containing the two terms
        :param first:
        :param second:
        :param child: [False] if true, preserve the second object as a child, rather than merging its content
        :return:
        """
        ob1 = self._d[first]
        ob2 = self._d[second]
        self.remove_object(ob2)
        if child:
            self._add_child(ob1, ob2)
        else:
            self._merge(ob1, ob2)

    def unmerge_child(self, obj):
        """
        The supplied obj must be a child of another obj.  Removes it and establishes it as a separate object.  This
        will fail [TermExists] if the child and the parent share any terms in common.
        :param obj:
        :return:
        """
        try:
            parent = next(v for v in self.objects if v.has_child(obj))
        except StopIteration:
            raise ChildNotFound('No set found containing child %s' % obj)
        parent.remove_child(obj)
        dups = [k for k in obj.terms if k in parent]
        if len(dups) > 0:
            parent.add_child(obj)
            raise TermExists('Terms duplicated in parent: %s' % '; '.join(dups))
        for k in obj.terms:
            self._remove_term(k)
        return self.add_or_update_object(obj, merge=False)

    def add_synonym(self, term, syn):
        """
        Add a new term as a synonym to an existing term
        :param term: the new term
        :param syn: the existing synonym
        :return:
        """
        obj = self._d[syn]
        self._add_term(term, obj)  # checks TermExists
        obj.add_term(term)

    def del_term(self, term):
        """
        Delete a term from the dictionary and from the object itself
        :param term:
        :return:
        """
        self._remove_term(term, remove_from_object=True)

    def set_name(self, term):
        if term not in self:
            raise KeyError('Unknown term %s' % term)
        obj = self._d[term]
        cterm = self._l[obj][term]  # necessary for case-insensitive dictionaries
        obj.set_name(cterm)

    def get(self, term, default=None):
        try:
            return self.__getitem__(term)
        except KeyError:
            return default

    def synonyms(self, term):
        if isinstance(term, SynonymSet):
            obj = term
        else:
            obj = self._d[term]
        for t in sorted(self._l[obj].values()):
            yield t

    def __contains__(self, item):
        return item in self._d

    def __getitem__(self, term):
        if isinstance(term, self._syn_type) and term in self._l:
            return term.object
        return self._d[term].object
