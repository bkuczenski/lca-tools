"""
Root-level catalog interface
"""


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
        for arch in self._iface(itype, strict=strict):
            try:
                result = getattr(arch, attrname)(*args, **kwargs)
            except NotImplementedError:
                continue
            except type(exc):
                continue
            if result is not None:
                return result
        raise exc
