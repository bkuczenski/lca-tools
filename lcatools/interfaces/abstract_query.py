"""
Root-level catalog interface
"""


class UnknownOrigin(Exception):
    pass


class ValidationError(Exception):
    pass


class AbstractQuery(object):
    """
    Abstract base class for executing queries
    """
    _debug = False

    def on_debug(self):
        self._debug = True

    def off_debug(self):
        self._debug = False

    def _iface(self, itype, strict=False):
        for i in []:
            yield i

    def _perform_query(self, itype, attrname, exc, *args, strict=False, **kwargs):
        if self._debug:
            print('Performing %s query, iface %s' % (attrname, itype))
        for iface in self._iface(itype, strict=strict):
            try:
                result = getattr(iface, attrname)(*args, **kwargs)
            except NotImplementedError:
                continue
            except type(exc):
                continue
            if result is not None:
                return result
        raise exc

    def _grounded_query(self, origin):
        return self

    def is_elementary(self, f):
        return None

    def make_ref(self, entity):
        if entity is None:
            return None
        if entity.is_entity:
            try:
                return entity.make_ref(self._grounded_query(entity.origin))
            except UnknownOrigin:
                entity.show()
                raise
        else:
            return entity  # already a ref

    def validate(self):
        try:
            self._perform_query(None, 'validate', ValidationError)
            return True
        except ValidationError:
            return False
