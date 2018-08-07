from .lower_dict import LowerDict
from .synonym_set import SynonymSet


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

    def _check_term(self, term, obj):
        if term in self._d and self._d[term] is not obj:
            return self._d[term]

    def _add_term(self, term, obj):
        if not isinstance(obj, self._syn_type):
            raise TypeError('Object is not a %s (%s)' % (type(self._syn_type), type(obj)))
        if self._check_term(term, obj) is not None:
            self._throw_term_exists(term)
        self._d[term] = obj

    def new_object(self, *args, merge=True, **kwargs):
        """
        The input arguments are passed directly to the constructor
        :param args: args to pass to constructor
        :param merge: [True] if synonyms are found in an existing object, install the new object as a child
        :param kwargs: kwargs to pass to constructor
        :return:
        """
        obj = self._syn_type(*args, **kwargs)
        self.add_object(obj, merge=merge)
        return obj

    def add_object(self, obj, merge=True):
        if merge:
            mg = set(k for k in filter(None, (self._check_term(t, obj) for t in obj.terms)))
            if len(mg) > 1:
                raise MergeError('Found terms in multiple sets: %s' % '; '.join(str(k) for k in mg))
            elif len(mg) == 1:
                s = mg.pop()
                s.add_child(obj)
                for t in obj.terms:
                    self._add_term(t, s)
            else:
                for t in obj.terms:
                    self._add_term(t, obj)
        else:
            for t in obj.terms:
                if self._check_term(t, obj) is not None:
                    self._throw_term_exists(t)
            for t in obj.terms:
                self._add_term(t, obj)

    def add_term(self, term, syn):
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
