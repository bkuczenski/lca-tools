"""
A class to keep track of "flowable" objects: substances, materials, products, or services which are exchanged between
activities or between an activity and a context.

Some flowables which are chemicals or substances have CAS numbers.  Others do not.
"""


from .cas_number import CasNumber, cas_regex, InvalidCasNumber
from ..synonym_set import SynonymSet, DuplicateChild


class Flowable(SynonymSet):
    def add_term(self, term):
        """
        Any term that can be coerced into a CAS number is added AS a CAS number
        :param term:
        :return:
        """
        if bool(cas_regex.match(str(term))):
            self.add_child(CasNumber(term))
        else:
            super(Flowable, self).add_term(str(term))

    def remove_term(self, term):
        """
        If term is a CAS number, remove the whole child
        :param term:
        :return:
        """
        term = str(term)
        if bool(cas_regex.match(term)):
            try:
                self.remove_child(next(c for c in self.children if term in c))
            except StopIteration:
                pass
        else:
            super(Flowable, self).remove_term(str(term))

    def add_child(self, other, force=False):
        if isinstance(other, CasNumber):
            try:
                super(Flowable, self).add_child(other)  # duplicate CAS numbers also not allowed
            except DuplicateChild:
                return
        else:
            super(Flowable, self).add_child(other)

    @property
    def cas_numbers(self):
        for c in sorted(self._children, key=str):
            if isinstance(c, CasNumber):
                yield str(c)

    '''
    def set_name(self, name):
        raise NotSupported
    '''

    @property
    def object(self):
        return self

    def serialize(self):
        """
        Keep just names of child sets, since CAS numbers will auto-replicate on add
        :return:
        """
        d = {
            'name': self._name,
            'synonyms': [t for t in sorted(self._terms) if t != self._name]
        }
        for c in self.children:
            if c == self._name:
                continue
            d['synonyms'].append(c.name)
        return d
