from ..synonym_dict import SynonymSet


class InvalidSubCompartment(Exception):
    pass


class NullContext(Exception):
    pass


class Compartment(SynonymSet):
    """
    A compartment is an environmental or social "compartment" that exchanges some "flow" with a technological "activity"
    in a process-flow product system.  Contexts are defined by a hierarchical structure and each instance has an
    optional 'parent' which is a proper superset, if present.

    If 'resources' or 'emissions' match any terms in a compartment, it is considered 'elementary', along with all its
    subcompartments.
    """
    @classmethod
    def null(cls):
        return cls('None')

    @property
    def is_null(self):
        return self._terms == {'None'}

    def add_term(self, term):
        if self.is_null:
            raise NullContext
        return super(Compartment, self).add_term(term)

    def add_child(self, other, force=False):
        if self.is_null:
            raise NullContext
        return super(Compartment, self).add_child(other, force=force)

    def __init__(self, *args, parent=None):
        super(Compartment, self).__init__(*args)
        self._parent = None
        self._subcompartments = set()
        if isinstance(parent, Compartment):
            self.parent = parent  # use setter
        elif parent is not None:
            raise TypeError('Parent must be a Compartment, not %s' % type(parent))

    @property
    def object(self):
        return self

    def top(self):
        if self.parent is None:
            return self
        return self.parent.top()

    def __iter__(self):
        if self.parent is not None:
            for k in self.parent:
                yield k
        yield str(self)

    def as_list(self):
        if self.parent is None:
            return [str(self)]
        else:
            return self.parent.as_list() + [str(self)]
        #return list(self)

    @property
    def base_terms(self):
        yield '; '.join(self.as_list())
        for x in super(Compartment, self).base_terms:
            yield x

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, parent):
        if self._parent is not None:
            self._parent.deregister_subcompartment(self)
        self._parent = parent
        if parent is not None:
            parent.register_subcompartment(self)

    def is_subcompartment(self, comp):
        """
        for simplest semantics, a compartment is considered its own subcompartment. With None I could go either way.
        :param comp:
        :return:
        """
        if comp is self:
            return True
        if self._parent is not None:
            return self._parent.is_subcompartment(comp)
        return False

    @property
    def subcompartments(self):
        for s in self._subcompartments:
            yield s

    def register_subcompartment(self, sub):
        if self.is_null:
            raise NullContext
        if sub.parent is not self:
            raise InvalidSubCompartment('Parent %s: relationship does not exist: %s' % (self, sub))
        self._subcompartments.add(sub)

    def deregister_subcompartment(self, sub):
        if sub not in self._subcompartments:
            raise InvalidSubCompartment('Parent %s: subcompartment is not known: %s' % (self, sub))
        self._subcompartments.remove(sub)  # ??!

    @property
    def self_and_subcompartments(self):
        yield self
        for k in sorted(self._subcompartments, key=str):
            for t in k.self_and_subcompartments:
                yield t

    def serialize(self):
        d = super(Compartment, self).serialize()
        if self._parent is not None:
            d['parent'] = str(self.parent)
        return d
