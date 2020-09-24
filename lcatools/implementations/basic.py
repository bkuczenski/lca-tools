from antelope import EntityNotFound


class NoAccessToEntity(Exception):
    """
    Used when the actual entity is not accessible
    """
    pass


class BasicImplementation(object):
    def __init__(self, archive, **kwargs):
        """
        Provides common features for an interface implementation: namely, an archive and a privacy setting. Also
        provides access to certain common methods of the archive.  This should be the base class for interface-specific
        implementations.

        Requires an archive with the following attributes:
         - ref - report semantic reference
         - source - report physical data source
         - static - a boolean indicating whether the full contents of the archive are loaded into memory
         - get_uuid() - deprecated - only present for compatibility reasons
         - __getitem__ - retrieve already-loaded entity
         - retrieve_or_fetch_entity() - _fetch abstract method must be implemented

        All of these requirements are met by the standard ArchiveImplementation, with the exception of the _fetch
        abstract method.

        :param archive: an LcArchive
        :param privacy: No longer used. Privacy is enforced at the server and not the resource (where it was phony
        from the beginning)
        """
        self._archive = archive

    def validate(self):
        """
        way to check that a query implementation is valid without querying anything
        :return:
        """
        return self.origin

    @property
    def origin(self):
        return self._archive.ref

    def __str__(self):
        return '%s for %s (%s)' % (self.__class__.__name__, self.origin, self._archive.source)

    def __getitem__(self, item):
        return self._archive[item]

    def get_item(self, external_ref, item):
        """
        In this, we accept either an external_ref or an entity reference itself.  If the latter, we dereference via
        the archive to an actual entity, which we then ask for the item.  If the dereference and the reference are the
        same, throws an error.
        :param external_ref:
        :param item:
        :return:
        """
        if hasattr(external_ref, 'external_ref'):
            eref = external_ref
            external_ref = eref.external_ref
        else:
            eref = None
        entity = self.get(external_ref)
        # if entity:
        if entity is eref:
            raise NoAccessToEntity(entity.link)
        if entity.has_property(item):
            return entity[item]
        raise KeyError('%s: %s [%s]' % (self.origin, external_ref, item))
        # raise EntityNotFound(external_ref)

    def get_reference(self, key):
        entity = self._fetch(key)
        if entity is None:
            return None
        if entity.entity_type == 'process':
            # need to get actual references with exchange values-- not the reference_entity
            return [x for x in entity.references()]
        return entity.reference_entity

    def get_uuid(self, external_ref):
        u = self._archive.get_uuid(external_ref)
        if u is None:
            return False
        return u

    def _fetch(self, external_ref, **kwargs):
        if external_ref is None:
            return None
        if self._archive.static:
            return self._archive[external_ref]
        try:
            return self._archive.retrieve_or_fetch_entity(external_ref, **kwargs)
        except NotImplementedError:
            return None

    def lookup(self, external_ref, **kwargs):
        if self._fetch(external_ref, **kwargs) is not None:
            return True
        return False

    def get(self, external_ref, **kwargs):
        """
        :param external_ref: may also be link, as long as requested origin is equal or lesser in specificity
        :param kwargs:
        :return: entity or None
        """
        e = self._fetch(external_ref, **kwargs)
        if e is not None:
            return e
        er_s = external_ref.split('/')
        if self.origin.startswith(er_s[0]):
            e = self._fetch('/'.join(er_s[1:]), **kwargs)
            if e is not None:
                return e
        raise EntityNotFound(external_ref)
