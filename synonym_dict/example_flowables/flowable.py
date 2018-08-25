"""
A class to keep track of "flowable" objects: substances, materials, products, or services which are exchanged between
activities or between an activity and a context.

Some flowables which are chemicals or substances have CAS numbers.  Others do not.  For simplicity, the Flowable
class is only permitted to have CAS number children.
"""


from .cas_number import CasNumber, InvalidCasNumber
from ..synonym_set import SynonymSet, NotSupported, DuplicateChild


class Flowable(SynonymSet):
    def add_term(self, term):
        """
        Any term that can be coerced into a CAS number is added AS a CAS number
        :param term:
        :return:
        """
        try:
            cas = CasNumber(term)
        except InvalidCasNumber:
            super(Flowable, self).add_term(str(term))
            return
        super(Flowable, self).add_child(cas)

    def remove_term(self, term):
        try:
            cas = CasNumber(term)
        except InvalidCasNumber:
            super(Flowable, self).remove_term(str(term))
            return
        try:
            self.remove_child(next(c for c in self.children if str(c) == str(cas)))
        except StopIteration:
            pass  # stopgap- we can't remove a CAS multiple times

    def add_child(self, other, force=False):
        if isinstance(other, CasNumber):
            try:
                super(Flowable, self).add_child(other)  # duplicate CAS numbers also not allowed
            except DuplicateChild:
                return
        else:
            raise NotSupported('Flowables are only allowed to have CasNumber children')

    @property
    def cas_numbers(self):
        for c in sorted(self._children, key=str):
            yield str(c)

    def serialize(self):
        """
        Omits child sets to handle manually
        :return:
        """
        d = {
            'name': self._name,
            'synonyms': [t for t in sorted(self._terms) if t != self._name]
        }
        if len(self._children) > 0:
            for c in self.cas_numbers:
                if c == self._name:
                    continue
                d['synonyms'].append(c)
        return d
