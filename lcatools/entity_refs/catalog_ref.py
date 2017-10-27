"""
The main import.
"""


from .process_ref import ProcessRef
from .flow_ref import FlowRef
from .quantity_ref import QuantityRef
from .fragment_ref import FragmentRef

from .base import BaseRef

from lcatools.interfaces.iquery import EntityNotFound


class CatalogRef(BaseRef):
    """
    user-facing entity ref generator

    CatalogRef.from_json(j, catalog=None)

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
    :param _query: if a query is already on hand, set it and skip the catalog lookup
    :param catalog: semantic resolver. Must provide the interfaces that can be used to answer queries
    """

    @classmethod
    def from_json(cls, j, catalog=None):
        external_ref = j.pop('externalId')
        if 'entityType' in j:
            etype = j.pop('entityType', None)
            if etype == 'unknown':
                etype = None
        else:
            etype = None
        if 'origin' in j:
            origin = j.pop('origin')
        elif 'source' in j:
            origin = j['source']
        else:
            origin = 'foreground'
        if catalog is not None:
            org = catalog.lookup(origin, external_ref)
        else:
            return cls(origin, external_ref, entity_type=etype, **j)

    @classmethod
    def from_query(cls, external_ref, query, etype, **kwargs):
        if etype == 'process':
            return ProcessRef(external_ref, query, **kwargs)
        elif etype == 'flow':
            return FlowRef(external_ref, query, **kwargs)
        elif etype == 'quantity':
            return QuantityRef(external_ref, query, **kwargs)
        elif etype == 'fragment':
            return FragmentRef(external_ref, query, **kwargs)
        else:
            return cls(query.origin, external_ref, entity_type=etype, **kwargs)

    def lookup(self, catalog, **kwargs):
        """
        RETURNS a grounded catalogRef matching the current item.  Note that the current item cannot be transformed
        into a grounded ref.
        :param catalog:
        :param kwargs:
        :return: typed EntityRef
        """
        try:
            org = catalog.lookup(self.origin, self.external_ref)
        except EntityNotFound:
            print('Not found: %s/%s' % (self.origin, self.external_ref))
            return None
        query = catalog.query(org, **kwargs)
        ref = query.get(self.external_ref)
        for k, v in self._d.items():
            if not ref.has_property(k):
                ref[k] = v # copy local items
        return ref

    def __init__(self, origin, external_ref, entity_type=None, **kwargs):
        """
        A CatalogRef that is created from scratch will not be active
        :param origin:
        :param external_ref:
        :param entity_type:
        :param kwargs:
        """
        super(CatalogRef, self).__init__(origin, external_ref, **kwargs)

        self._asgn_etype = entity_type

    @property
    def entity_type(self):
        if self._asgn_etype is not None:
            return self._asgn_etype
        return super(CatalogRef, self).entity_type
