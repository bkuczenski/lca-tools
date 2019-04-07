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


class NoUuid(Exception):
    pass


class AbstractQuery(object):
    """
    Abstract base class for executing queries

    Query implementation must provide:
     - origin (property)
     - _iface (generator: itype)
     - _tm (property) a TermManager
    """
    _debug = False
    _validated = None

    def on_debug(self):
        self._debug = True

    def off_debug(self):
        self._debug = False

    '''
    Overridde these methods
    '''
    @property
    def origin(self):
        return NotImplemented

    def _iface(self, itype, **kwargs):
        """
        Pseudo-abstract method to generate interfaces of the specified type upon demand.  Must be reimplemented
        :param itype:
        :param kwargs: for use by subclasses
        :return: generate interfaces of the given type
        """
        return NotImplemented

    @property
    def _tm(self):
        return NotImplemented

    '''
    Internal workings
    '''
    def is_elementary(self, context):
        """
        Stopgap used to expose access to a catalog's Qdb; in the future, flows will no longer exist and is_elementary
        will be a trivial function of an exchange asking whether its termination is a context or not.
        :param context:
        :return: bool
        """
        return self._tm[context.fullname].elementary

    def _perform_query(self, itype, attrname, exc, *args, strict=False, **kwargs):
        if self._debug:
            print('Performing %s query, iface %s' % (attrname, itype))
        try:
            for iface in self._iface(itype, strict=strict):
                try:
                    result = getattr(iface, attrname)(*args, **kwargs)
                except exc.__class__:
                    continue
                if result is not None:
                    return result
        except NotImplementedError:
            pass

        raise exc

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

    '''
    Can be overridden
    '''
    def _grounded_query(self, origin):
        """
        Pseudo-abstract method used to construct entity references from a query that is anchored to a metaresource.
        must be overriden by user-facing subclasses if resources beyond self are required to answer
        the queries (e.g. a catalog).
        :param origin:
        :return:
        """
        return self

    def validate(self):
        if self._validated is None:
            try:
                self._perform_query(None, 'validate', ValidationError)
                self._validated = True
            except ValidationError:
                self._validated = False
        return self._validated

    def get(self, eid, **kwargs):
        """
        Basic entity retrieval-- should be supported by all implementations
        :param eid:
        :param kwargs:
        :return:
        """
        return self._perform_query(None, 'get', EntityNotFound('%s/%s' % (self.origin, eid)), eid,
                                   **kwargs)

    def get_item(self, external_ref, item):
        """
        access an entity's dictionary items
        :param external_ref:
        :param item:
        :return:
        """
        return self._perform_query(None, 'get_item', EntityNotFound('%s/%s' % (self.origin, external_ref)),
                                   external_ref, item)

    def get_uuid(self, external_ref):
        return self._perform_query(None, 'get_uuid', NoUuid('%s/%s' % (self.origin, external_ref)),
                                   external_ref)

'''# maybe we don't need these?!
    def get_reference(self, external_ref):
        return self._perform_query(None, 'get_reference', EntityNotFound('%s/%s' % (self.origin, external_ref)),
                                   external_ref)

'''
