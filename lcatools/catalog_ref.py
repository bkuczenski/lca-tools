class NoCatalog(Exception):
    pass


class EntityNotKnown(Exception):
    pass


class ForegroundNotKnown(Exception):
    pass


class BackgroundNotKnown(Exception):
    pass


class OriginNotFound(Exception):
    pass


class CatalogRef(object):
    """
    A catalog ref is a resolveable reference to a semantic entity. When paired with an LcCatalog, the reference
    can operate equivalently to an actual entity.
    """
    @classmethod
    def from_json(cls, j, catalog=None):
        if 'entityType' in j:
            etype = j['entityType']
            if etype == 'unknown':
                etype = None
        else:
            etype = None
        return cls(j['origin'], j['externalId'], catalog=catalog, entity_type=etype)

    def __init__(self, origin, ref, catalog=None, entity_type=None):
        """
        A catalog ref is defined by an entity's origin and external reference, which are all that is necessary to
        identify and/or recreate the entity.  A ref can be linked to a catalog, which may be able to resolve the
        reference and retrieve the entity.

        If the reference is linked to a catalog, then the catalog can be used to retrieve the entity and return its
        attributes.  Certain attributes require the entity to be known in a basic ('catalog') sense, while others
        require it to be known in a foreground or background sense.
        :param origin: semantic reference to data source (catalog must resolve to a physical data source)
        :param ref: external reference of entity in semantic data source
        :param catalog: semantic resolver. Must provide the interface:
           catalog.lookup(origin, external_ref) - returns [bool, bool, bool] -> catalog, fg, bg avail.
           catalog.entity_type(CatalogRef) - returns entity type
           catalog.fetch(CatalogRef) - return catalog entity
           catalog.fg_lookup(CatalogRef) - return fg entity
           catalog.bg_lookup(CatalogRef) - return bg entity
        :param entity_type: optional- can be placeholder to enable type validation without retrieving entity
        """
        self._origin = origin
        self._ref = ref

        self._etype = entity_type

        self._catalog = None

        self._known = [False, False, False]

        self._entity = None
        self._fg = None
        self._bg = None

        if catalog is not None:
            self.lookup(catalog)

    @property
    def origin(self):
        return self._origin

    @property
    def external_ref(self):
        return self._ref

    @property
    def entity_type(self):
        if self._etype is None:
            return 'unknown'
        return self._etype

    @property
    def known(self):
        return self._known[0]

    @property
    def entity(self):
        if self.known:
            if self._entity is None:
                self._fetch()
            return self._entity
        else:
            raise EntityNotKnown

    @property
    def fg(self):
        if self._known[1]:
            return self._catalog.fg_lookup(self)
        else:
            raise ForegroundNotKnown

    @property
    def bg(self):
        if self._known[2]:
            return self._catalog.bg_lookup(self)
        else:
            raise BackgroundNotKnown

    def lookup(self, catalog):
        self._known = catalog.lookup(self.origin, self.external_ref)

        if self.known:
            self._catalog = catalog
            self._etype = catalog.entity_type(self)

    def _fetch(self):
        if self.known:
            self._entity = self._catalog.fetch(self)

    def __getattr__(self, item):
        if self._catalog is None:
            raise NoCatalog()
        if self._entity is None:
            self._fetch()
        if self.known:
            return self._entity.__getattr__(item)

    def serialize(self):
        j = {
            'origin': self.origin,
            'externalId': self.external_ref
        }
        if self._etype is not None:
            j['entityType'] = self._etype
        return j
