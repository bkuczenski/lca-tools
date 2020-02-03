"""
SynonymSet
A set of synonyms in a hashable collection
"""
import re
from uuid import uuid4


class RemoveNameError(Exception):
    pass


class ChildNotFound(Exception):
    pass


class DuplicateChild(Exception):
    pass


class SynonymSet(object):
    """

    """
    def __init__(self, *args, **kwargs):
        self._args = kwargs  # ignored
        self._terms = set()
        self._name = None
        self._children = set()

        self._id = str(uuid4())  # a private ID which exists only to hash the set consistently
        for arg in args:
            self.add_term(arg)

    @property
    def terms(self):
        if self.name is None:
            return
        seen = {self.name}
        yield self.name
        for s in self.base_terms:
            if s not in seen:
                yield s
                seen.add(s)
        for c in self._children:
            for t in c.terms:
                if t not in seen:
                    yield t
                    seen.add(t)

    @property
    def base_terms(self):
        for s in sorted(self._terms):
            yield s

    @property
    def children(self):
        for c in self._children:
            yield c

    @property
    def object(self):
        """
        The thing that the synonyms signify.  In the base class, it is just the name.  Create a Subclass to return
        objects of a different type.
        :return:
        """
        return self._name

    @property
    def name(self):
        return self._name

    def add_term(self, term):
        if hasattr(term, 'terms'):
            self.add_child(term)
        else:
            self._add_term(term)

    def _add_term(self, term):
        s = str(term)
        if len(s.strip()) == 0:
            return
        if self._name is None:
            s_s = s.strip()
            self._name = s_s
            self._terms.add(s_s)
        self._terms.add(s)

    def add_child(self, other, force=False):
        """
        This method stores the child as a subsidiary of the current object.  The child's terms will show up after the
        parent's terms.  Repeated terms will not be shown twice.  Child can still be modified.
        :param other:
        :param force: override restriction against having duplicate child sets (since child sets may still be
        individually updated
        :return:
        """
        if hasattr(other, 'terms'):
            if not force:
                if any(t == other for t in self._children) and len(other) > 0:
                    raise DuplicateChild('Child members match existing child')
            self._children.add(other)
            if self._name is None:
                self._name = other.name
        else:
            raise TypeError('Argument is not a synonym set (type %s)' % type(other))

    def remove_child(self, child):
        if child in self._children:
            self._children.remove(child)
        else:
            raise ChildNotFound('%s has no child %s' % (self, child))

    def children_with_term(self, term):
        """
        Returns all child sets having the given term
        :param term:
        :return: a tuple containing all child sets that contain the term
        """
        found = []
        for c in self._children:
            if term in c:
                found.append(c)
        return tuple(found)

    def contains_string(self, pattern, ignore_case=False):
        if ignore_case:
            srch = re.compile(pattern, flags=re.IGNORECASE)
        else:
            srch = re.compile(pattern)
        for t in self.terms:
            if bool(srch.search(t)):
                return t
        return False

    def has_child(self, other):
        return other in self._children

    def remove_term(self, term):
        s = str(term)
        _rmvd = False
        if s == self._name:
            raise RemoveNameError('%s is currently the name of this set.  Assign a different name before removing.' % s)
        if s in self._terms:
            self._terms.remove(s)
            _rmvd = True
        for c in self._children:
            if s in c:
                c.remove_term(s)
                _rmvd = True
        if _rmvd is False:
            raise KeyError('Unknown term %s' % term)

    def set_name(self, name):
        s = str(name)
        if s not in self._terms:
            self.add_term(s)  # is this desirable? Maybe we want to restrict this to names already in the set?
        self._name = s

    def serialize(self):
        """
        Note that this flattens child sets into self- allows for rewrites by assigning children
        :return:
        """
        j = {
            'name': self._name,
            'synonyms': [t for t in sorted(self.terms) if t != self._name]
        }
        if len(j['synonyms']) == 0:
            j.pop('synonyms')
        return j

    def __str__(self):
        return self._name

    def __repr__(self):
        return '%s(%s, %d terms, %d children)' % (self.__class__.__name__, self._name, len(self._terms),
                                                  len(self._children))

    def __len__(self):
        return len([t for t in self.terms])

    def __hash__(self):
        return hash(self._id)

    def __contains__(self, item):
        return item in self.terms

    def __eq__(self, other):
        if hasattr(other, 'terms'):
            otherterms = set(other.terms)
        else:
            otherterms = {other}
        return set(self.terms) >= otherterms  # yes, I know, that's not equal per se
