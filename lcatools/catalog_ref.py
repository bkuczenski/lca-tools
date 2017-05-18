class NoCatalog(Exception):
    pass


class MultipleOrigins(Exception):
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
        if 'origin' in j:
            origin = j['origin']
        elif 'source' in j:
            origin = j['source']
        else:
            origin = 'foreground'
        return cls(origin, j['externalId'], catalog=catalog, entity_type=etype)

    def __init__(self, origin, ref, catalog=None, entity_type=None):
        """
        A catalog ref is defined by an entity's origin and external reference, which are all that is necessary to
        identify and/or recreate the entity.  A ref can be linked to a catalog, which may be able to resolve the
        reference and retrieve the entity.

        If the reference is linked to a catalog, then the catalog can be used to retrieve the entity and return its
        attributes.  Certain attributes require the entity to be known in a basic ('catalog') sense, while others
        require it to be known in a foreground or background sense.  The catalog can also supply information about
        the entity using a standard interface.  The Catalog Ref can re-implement methods that belong to entities,
        acting as an abstraction layer between the client code and the catalog.

        Implication of this is that the query interface methods should have the same names and signatures as the
        entities' own direct methods.  Finally, a design constraint that dictates my class structures!

        :param origin: semantic reference to data source (catalog must resolve to a physical data source)
        :param ref: external reference of entity in semantic data source
        :param catalog: semantic resolver. Must provide the interfaces that can be used to answer queries
        :param entity_type: optional- can be placeholder to enable type validation without retrieving entity
        """
        self._origin = origin
        self._ref = ref

        self._etype = entity_type

        self._query = None

        self._known = []

        if catalog is not None:
            self.lookup(catalog)

    @property
    def origin(self):
        return self._origin

    @property
    def external_ref(self):
        return self._ref

    @property
    def link(self):
        return '/'.join([self.origin, self.external_ref])

    @property
    def entity_type(self):
        if self._etype is None:
            return 'unknown'
        return self._etype

    @property
    def privacy(self):
        return self._query.privacy()

    @property
    def known(self):
        return self._known

    def lookup(self, catalog):
        org = catalog.lookup(self)

        if len(org) > 0:
            if len(org) > 1:
                raise MultipleOrigins('%s found in:\n%s' % (self.external_ref, '\n'.join(org)))
            self._known = True
            self._origin = org[0]
            self._query = catalog.query(self._origin)
            self._etype = catalog.entity_type(self)

    def __getitem__(self, item):
        if self._query is None:
            raise NoCatalog
        return self._query.get_item(self.external_ref, item)

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

    def references(self):
        return self._query.get_reference(self.external_ref)

    @property
    def reference_entity(self):
        return self._query.get_reference(self.external_ref)

    def mix(self, direction):
        self._require_flow()
        return self._query.mix(self.external_ref, direction)

    def exchanges(self):
        self._require_process()
        return self._query.exchanges(self.external_ref)

    def exchange_values(self, flow, direction, termination=None):
        self._require_process()
        return self._query.exchange_values(self.external_ref, flow.external_ref, direction, termination=termination)

    def inventory(self, ref_flow=None):
        self._require_process()
        return self._query.inventory(self.external_ref, ref_flow=ref_flow)

    def exchange_relation(self, ref_flow, exch_flow, direction, termination=None):
        self._require_process()
        return self._query.exchange_relation(self.origin, self.external_ref, ref_flow.external_ref,
                                             exch_flow.external_ref, direction,
                                             termination=termination)

    def ad(self, ref_flow=None):
        self._require_process()
        return self._query.ad(self.external_ref, ref_flow)

    def bf(self, ref_flow=None):
        self._require_process()
        return self._query.bf(self.external_ref, ref_flow)

    def lci(self, ref_flow=None):
        self._require_process()
        return self._query.lci(self.external_ref, ref_flow)

    def lcia(self, ref_flow, lcia_qty):
        self._require_process()
        return self._query.lcia(self.external_ref, ref_flow, lcia_qty)
