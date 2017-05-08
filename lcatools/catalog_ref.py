from lcatools.catalog.interfaces import QueryInterface, NoCatalog


class EntityNotKnown(Exception):
    pass


class ForegroundNotKnown(Exception):
    pass


class BackgroundNotKnown(Exception):
    pass


class OriginNotFound(Exception):
    pass


class InvalidQuery(Exception):
    pass


class CatalogRef(object):
    """
    A catalog ref is a resolveable reference to a semantic entity. When paired with an LcCatalog, the reference
    can operate equivalently to an actual entity. The CatalogRef also implements a standard interface for accessing
    LCA information RESTfully.
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
          Automatic - entity information
           catalog.lookup(origin, external_ref) - returns [bool, bool, bool] -> catalog, fg, bg avail.
           catalog.entity_type(origin, external_ref) - returns entity type
           catalog.fetch(origin, external_ref) - return catalog entity (fg preferred; fallback to entity)

          LC Queries:
           catalog.terminate(origin, external_ref, direction)
           catalog.originate(origin, external_ref, direction)
           catalog.mix(origin, external_ref, direction)
           catalog.exchanges(origin, external_ref)
           catalog.exchange_values(origin, external_ref, flow, direction, termination=termination)
           catalog.exchange_relation(origin, external_ref, ref_flow, exch_flow, direction,
                        termination=termination)
           catalog.ad(origin, external_ref, ref_flow)
           catalog.bf(origin, external_ref, ref_flow)
           catalog.lci(origin, external_ref, ref_flow)
           catalog.lcia(origin, external_ref, ref_flow, lcia_qty)

        :param entity_type: optional- can be placeholder to enable type validation without retrieving entity
        """
        self._origin = origin
        self._ref = ref

        self._etype = entity_type

        self._query = None

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

    def lookup(self, catalog):
        _known = catalog.lookup(self.origin, self.external_ref)

        if _known[0]:
            self._known = _known
            self._query = QueryInterface(catalog)
            self._etype = catalog.entity_type(self)
            self._fetch()

    def _fetch(self):
        if self.known:
            self._entity = self._query.fetch(self.external_ref)

    def __getitem__(self, item):
        if self._query is None:
            raise NoCatalog()
        return self.entity.__getitem__(item)

    def serialize(self):
        j = {
            'origin': self.origin,
            'externalId': self.external_ref
        }
        if self._etype is not None:
            j['entityType'] = self._etype
        return j

    """
    Query Methods -- this is the operational version of the API
    """
    def _require_flow(self):
        if self.entity_type != 'flow':
            raise InvalidQuery('This query only applies to flows')

    def _require_process(self):
        if self.entity_type != 'process':
            raise InvalidQuery('This query only applies to processes')

    def terminate(self, direction=None):
        self._require_flow()
        return self._query.terminate(self.external_ref, direction)

    def originate(self, direction=None):
        self._require_flow()
        return self._query.originate(self.external_ref, direction)

    def mix(self, direction):
        self._require_flow()
        return self._catalog.mix(self.origin, self.external_ref, direction)

    def exchanges(self):
        self._require_process()
        return self._catalog.exchanges(self.origin, self.external_ref)

    def exchange_values(self, flow, direction, termination=None):
        self._require_process()
        return self._catalog.exchange_values(self.origin, self.external_ref, flow, direction, termination=termination)

    def exchange_relation(self, ref_flow, exch_flow, direction, termination=None):
        self._require_process()
        return self._catalog.exchange_values(self.origin, self.external_ref, ref_flow, exch_flow, direction,
                                             termination=termination)

    def ad(self, ref_flow=None):
        self._require_process()
        return self._catalog.ad(self.origin, self.external_ref, ref_flow)

    def bf(self, ref_flow=None):
        self._require_process()
        return self._catalog.bf(self.origin, self.external_ref, ref_flow)

    def lci(self, ref_flow=None):
        self._require_process()
        return self._catalog.lci(self.origin, self.external_ref, ref_flow)

    def lcia(self, ref_flow, lcia_qty):
        self._require_process()
        return self._catalog.lcia(self.origin, self.external_ref, ref_flow, lcia_qty)
