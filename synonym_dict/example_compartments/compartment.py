from ..synonym_dict import SynonymSet

ELEMENTARY = {'resources', 'emissions'}


class InvalidSense(Exception):
    pass


class InconsistentSense(Exception):
    pass


class InvalidSubCompartment(Exception):
    pass


class NullContext(Exception):
    pass


def valid_sense(sense):
    if sense is None:
        return None
    try:
        v = {'source': 'Source',
             'sink': 'Sink'}[sense.lower()]
    except KeyError:
        raise InvalidSense(sense)
    return v


class Compartment(SynonymSet):
    """
    A context is an environmental or social "compartment" that exchanges some "flow" with a technological "activity"
    in a process-flow product system.  Contexts are defined by a hierarchical structure and each instance has an
    optional 'parent' which is a proper superset, if present.

    A context has a natural directional "sense", which is either 'Source', 'Sink', or None.  A Source context
    generates flows which may be inputs to the activity; a Sink context absorbs flows which are output from the
    activity.

    If a context has a parent, it inherits the sense of the parent- specifying the opposite sense will raise
    an error.

    If 'resources' or 'emissions' match any terms in a context, it is considered 'elementary', along with all its
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

    def __init__(self, *args, parent=None, sense=None):
        super(Compartment, self).__init__(*args)
        self._parent = None
        self._sense = None
        self._subcompartments = set()
        if isinstance(parent, Compartment):
            self.parent = parent  # use setter
        elif parent is not None:
            raise TypeError('Parent must be a Compartment, not %s' % type(parent))
        if sense is not None:
            self.sense = sense

    @property
    def object(self):
        return self

    def as_list(self):
        if self.parent is None:
            return [str(self)]
        else:
            return self.parent.as_list() + [str(self)]

    @property
    def base_terms(self):
        yield '; '.join(self.as_list())
        for x in super(Compartment, self).base_terms:
            yield x

    @property
    def sense(self):
        if self.parent is None:
            return self._sense
        return self.parent.sense

    @sense.setter
    def sense(self, value):
        sense = valid_sense(value)
        if self.sense is not None and self.sense != sense:
            raise InconsistentSense('Value %s conflicts with current sense %s' % (sense, self.sense))
        if self.parent is None:
            self._sense = valid_sense(value)
        else:
            self.parent.sense = value

    @property
    def elementary(self):
        if self.parent is None:
            for t in self.terms:
                if t.strip().lower() in ELEMENTARY:
                    return True
            return False
        else:
            return self.parent.elementary

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, parent):
        if self._parent is not None:
            self._parent.deregister_subcompartment(self)
        self._parent = parent
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
        if self._parent is None:
            if self.sense is not None:
                d['sense'] = self.sense
        else:
            d['parent'] = str(self.parent)
        return d
