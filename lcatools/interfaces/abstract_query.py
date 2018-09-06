"""
Root-level catalog interface
"""


class UnknownOrigin(Exception):
    pass


class ValidationError(Exception):
    pass


class PrivateArchive(Exception):
    pass


class EntityNotFound(Exception):
    pass


class AbstractQuery(object):
    """
    Abstract base class for executing queries
    """
    _debug = False
    _validated = None

    def on_debug(self):
        self._debug = True

    def off_debug(self):
        self._debug = False

    def _iface(self, itype, strict=False):
        """
        Pseudo-abstract method to generate interfaces of the specified type upon demand.  Must be reimplemented
        by user-facing subclasses
        :param itype:
        :param strict:
        :return:
        """
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
            except exc.__class__:
                continue
            if result is not None:
                return result
        raise exc

    def _grounded_query(self, origin):
        """
        Pseudo-abstract method used to construct entity references from a query that is anchored to an actual
        resource.  must be overriden by user-facing subclasses if resources beyond self are required to answer
        the queries (e.g. a catalog).
        :param origin:
        :return:
        """
        return self

    '''
    def is_elementary(self, f):
        """
        Stopgap used to expose access to a catalog's Qdb; in the future, flows will no longer exist and is_elementary
        will be a trivial function of an exchange asking whether its termination is a context or not.
        :param f:
        :return:
        """
        return None
    '''

    def make_ref(self, entity):
        if entity is None:
            return None
        if entity.is_entity:
            try:
                return entity.make_ref(self._grounded_query(entity.origin))
            except UnknownOrigin:
                return entity.make_ref(self._grounded_query(None))  # falls back to self
        else:
            return entity  # already a ref

    def validate(self):
        if self._validated is None:
            try:
                self._perform_query(None, 'validate', ValidationError)
                self._validated = True
            except ValidationError:
                self._validated = False
        return self._validated

    '''# maybe we don't need these?!
    def get_item(self, external_ref, item):
        """
        access an entity's dictionary items
        :param external_ref:
        :param item:
        :return:
        """
        return self._perform_query(None, 'get_item', EntityNotFound('%s/%s' % (self.origin, external_ref)),
                                   external_ref, item)

    def get_reference(self, external_ref):
        return self._perform_query(None, 'get_reference', EntityNotFound('%s/%s' % (self.origin, external_ref)),
                                   external_ref)

    def get_uuid(self, external_ref):
        return self._perform_query(None, 'get_uuid', EntityNotFound('%s/%s' % (self.origin, external_ref)),
                                   external_ref)
    '''
