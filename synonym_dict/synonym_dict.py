from .lower_dict import LowerDict
from .synonym_set import SynonymSet, ChildNotFound


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


class SynonymDict(object):
    """
    A class that allows retrieval of a given object by any of its synonyms.
    """
    def __init__(self, ignore_case=False, syn_type=None):
        """
        Create a synonym dictionary that stores objects of a certain type.
        :param ignore_case: [False] whether the synonyms should be considered case-sensitive
        :param syn_type: What type links the synonyms to the object. Default SynonymSet.

        The object class must support the following API:
          constructor:
            Obj(*args): no kwargs
          properties:
            obj.terms: generate a list of synonyms
            obj.object: return the payload object (for SynonymSet, this is a str)
          methods:
            obj.add_term(term): add a new synonym
            obj.remove_term(term): remove an existing synonym
            obj.set_name(term): assign the canonical name for the object (by default the first term provided)
            obj.add_child(a): include a's terms with obj's terms
        """
        if ignore_case:
            self._d = LowerDict()
        else:
            self._d = dict()

        if syn_type is None:
            syn_type = SynonymSet
        self._syn_type = syn_type

    def _throw_term_exists(self, term):
        raise TermExists('%s -> %s' % (term, self._d[term]))

    def _check_term(self, term, obj=None):
        if term in self._d:
            if obj is not None and self._d[term] is not obj:
                self._throw_term_exists(term)
            return self._d[term]

    def _add_term(self, term, obj):
        if not isinstance(obj, self._syn_type):
            raise TypeError('Object is not a %s (%s)' % (type(self._syn_type), type(obj)))
        self._check_term(term, obj)
        self._d[term] = obj

    def _match_set(self, terms):
        """
        Returns a set of synonym objects that match terms in the input argument.
        :param terms: iterable
        :return:
        """
        return set(k for k in filter(None, (self._check_term(t) for t in terms)))

    def new_object(self, *args, merge=True, create_child=True, **kwargs):
        """
        The input arguments are passed directly to the constructor
        :param args: args to pass to constructor
        :param merge: [True] if synonyms are found in an existing object, merge the new terms according to create_child
        :param create_child: [True] if true, add new object as a child; otherwise, add terms to the existing object
        :param kwargs: kwargs to pass to constructor
        :return:
        """
        obj = self._syn_type(*args, **kwargs)
        self.add_object(obj, merge=merge, create_child=create_child)
        return obj

    def add_object(self, obj, merge=True, create_child=True):
        if merge:
            mg = self._match_set(obj.terms)
            if len(mg) > 1:
                raise MergeError('Found terms in multiple sets: %s' % '; '.join(str(k) for k in mg))
            elif len(mg) == 1:
                s = mg.pop()
                if create_child:
                    s.add_child(obj)
                    for t in obj.terms:
                        self._add_term(t, s)
                else:
                    for t in obj.terms:
                        try:
                            self._add_term(t, s)
                        except TermExists:
                            pass
            else:
                for t in obj.terms:
                    self._add_term(t, obj)
        else:
            for t in obj.terms:
                self._check_term(t, obj)
            for t in obj.terms:
                self._add_term(t, obj)

    def merge(self, first, second):
        """
        Merge the sets containing the two terms, installing the second set as a child of the first.
        :param first:
        :param second:
        :return:
        """
        ob1 = self._d[first]
        ob2 = self._d[second]
        ob1.add_child(ob2)
        for k in ob2.terms:
            self._d[k] = ob1

    def unmerge_child(self, obj):
        """
        The supplied obj must be a child of another obj.  Removes it and establishes it as a separate object.  This
        will fail [TermExists] if the child and the parent share any terms in common.
        :param obj:
        :return:
        """
        try:
            parent = next(v for v in self._d.values() if v.has_child(obj))
        except StopIteration:
            raise ChildNotFound('No set found containing child %s' % obj)
        parent.remove_child(obj)
        dups = [k for k in obj.terms if k in parent]
        if len(dups) > 0:
            parent.add_child(obj)
            raise TermExists('Terms duplicated in parent: %s' % '; '.join(dups))
        for k in obj.terms:
            self._d[k] = obj

    def add_synonym(self, term, syn):
        """
        Add a new term as a synonym to an existing term
        :param term: the new term
        :param syn: the existing synonym
        :return:
        """
        obj = self._d[syn]
        obj.add_term(term)
        self._add_term(term, obj)

    def del_term(self, term):
        obj = self._d.pop(term)
        obj.remove_term(term)

    def set_name(self, term):
        if term not in self._d:
            raise KeyError('Unknown term %s' % term)
        self._d[term].set_name(term)

    def get(self, term):
        return self.__getitem__(term)

    def __getitem__(self, term):
        return self._d[term].object
