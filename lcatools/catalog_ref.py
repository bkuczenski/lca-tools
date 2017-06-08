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

    def __init__(self, origin, ref, catalog=None, entity_type=None, **kwargs):
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
        self._uuid = None

        self._etype = entity_type

        self._query = None

        self._d = kwargs

        self._known = False

        if catalog is not None:
            self.lookup(catalog)

    @property
    def origin(self):
        return self._origin

    @property
    def external_ref(self):
        return self._ref

    @property
    def uuid(self):
        if self._uuid is None:
            if self._query is not None:
                self._uuid = self._query.get_uuid(self.external_ref)
            else:
                raise NoCatalog('Unable to lookup UUID')
        return self._uuid

    def get_uuid(self):
        """
        DEPRECATED
        :return:
        """
        return self.uuid

    def show(self):
        print('%s CatalogRef (%s)' % (self._etype.title(), self.external_ref))
        print('origin: %s' % self.origin)
        if self._query is None:
            print(' ** UNRESOLVED **')
        else:
            if self._etype == 'process':
                for i in self.references():
                    print('reference: %s' % i)
            else:
                print('reference: %s' % self.reference_entity)
            for i in ('Name', 'Comment'):
                print('%7s: %s' % (i, self._query.get_item(self.external_ref, i)))
        if len(self._d) > 0:
            print('==Local Fields==')
            ml = len(max(self._d.keys(), key=len))
            for k, v in self._d.items():
                print('%*s: %s' % (ml, k, v))

    def validate(self):
        if self._query is None:
            return False
        return True

    def unit(self):
        if self.entity_type == 'quantity':
            return self.reference_entity.unitstring
        print('CatalogRef unit burp')
        return None

    @property
    def is_entity(self):
        return False

    def __str__(self):
        if self._known:
            name = ' ' + self['Name']
        else:
            name = ''
        return '%s/%s%s' % (self.origin, self.external_ref, name)

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
        if item in self._d:
            return self._d[item]
        if 'Local%s' % item in self._d:
            return self._d['Local%s' % item]
        if self._query is None:
            raise NoCatalog
        return self._query.get_item(self.external_ref, item)

    def __setitem__(self, key, value):
        if key in ('Name', 'Comment'):
            key = 'Local%s' % key
        self._d[key] = value

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

    def _require_quantity(self):
        if self.entity_type == 'quantity':
            if self._query.get_item(self.external_ref, 'Indicator') is not None:
                return True
            return False
        raise InvalidQuery('This query only applies to LCIA methods')

    def is_lcia_method(self):
        return self._require_quantity()

    def terminate(self, direction=None):
        self._require_flow()
        return self._query.terminate(self.external_ref, direction)

    def originate(self, direction=None):
        self._require_flow()
        return self._query.originate(self.external_ref, direction)

    def references(self, flow=None):
        self._require_process()
        for x in self._query.get_reference(self.external_ref):
            if flow is None:
                yield x
            else:
                if x.flow == flow:
                    yield x

    def reference(self, flow):
        self._require_process()
        return next(x for x in self.references(flow=flow))

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

    def factors(self):
        self._require_quantity()
        return self._query.factors(self.external_ref)

    def ad(self, ref_flow=None):
        self._require_process()
        return self._query.ad(self.external_ref, ref_flow)

    def bf(self, ref_flow=None):
        self._require_process()
        return self._query.bf(self.external_ref, ref_flow)

    def lci(self, ref_flow=None):
        self._require_process()
        return self._query.lci(self.external_ref, ref_flow)

    def bg_lcia(self, lcia_qty, ref_flow=None):
        self._require_process()
        return self._query.bg_lcia(self.external_ref, lcia_qty, ref_flow=ref_flow)
