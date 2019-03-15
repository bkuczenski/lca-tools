"""
Contexts in this sense are environmental compartments, except they have the added capability to keep lists of origins.
"""

from synonym_dict.example_compartments import Compartment, CompartmentManager


class Context(Compartment):
    _origins = set()
    entity_type = 'context'

    @property
    def external_ref(self):
        return self.name

    def add_origin(self, origin):
        self._origins.add(origin)
        if self.parent is not None:
            self.parent.add_origin(origin)

    def has_origin(self, origin, strict=False):
        try:
            if strict:
                next(x for x in self._origins if x == origin)
            else:
                next(x for x in self._origins if x.startswith(origin))
        except StopIteration:
            return False
        return True

    def __repr__(self):
        return '<Context(%s)>' % ';'.join(self.as_list())


NullContext = Context.null()


class ContextManager(CompartmentManager):
    _entry_group = 'Compartments'  # we keep this so as to access compartment-compatible serializations
    _syn_type = Context
    _ignore_case = True

    _null_entry = NullContext
