from ..synonym_dict import SynonymSet


class InvalidSense(Exception):
    pass


class InconsistentSense(Exception):
    pass


class InvalidSubCompartment(Exception):
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
    A compartment is an environmental or social "context" that exchanges some "flow" with a technological "activity"
    in a process-flow product system.  Contexts are defined by a hierarchical structure and each instance has an
    optional 'parent' which is a proper superset, if present.

    A compartment has a natural directional "sense", which is either 'Source', 'Sink', or None.  A Source compartment
    generates flows which may be inputs to the activity; a Sink context absorbs flows which are output from the
    activity.

    If a compartment has a parent, it inherits the sense of the parent- specifying the opposite sense will raise
    an error.
    """
    def __init__(self, *args, parent=None, sense=None):
        super(Compartment, self).__init__(*args)
        self._parent = None
        self._sense = None
        self._subcompartments = set()
        if isinstance(parent, Compartment):
            self.parent = parent  # use setter
        elif parent is not None:
            raise TypeError('Parent must be a Context, not %s' % type(parent))
        if sense is not None:
            self.sense = sense

    @property
    def object(self):
        return self

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
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, parent):
        if self._parent is not None:
            self._parent.deregister_subcompartment(self)
        self._parent = parent
        parent.register_subcompartment(self)

    @property
    def subcompartments(self):
        for s in self._subcompartments:
            yield s

    def register_subcompartment(self, sub):
        if sub.parent is not self:
            raise InvalidSubCompartment('Parent %s: relationship does not exist: %s' % (self, sub))
        self._subcompartments.add(sub)

    def deregister_subcompartment(self, sub):
        if sub not in self._subcompartments:
            raise InvalidSubCompartment('Parent %s: subcompartment is not known: %s' % (self, sub))

    @property
    def self_and_subcompartments(self):
        yield self
        for k in self._subcompartments:
            for t in k.self_and_subcompartments:
                yield t

    def serialize(self):
        d = {
            'name': self._name,
            'synonyms': [t for t in sorted(self._terms) if t != self._name]
        }
        if self._parent is None:
            if self.sense is not None:
                d['sense'] = self.sense
        else:
            d['parent'] = str(self.parent)
        return d
