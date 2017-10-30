"""
Motivation:
The creation of catalog references is borne out of the idea that I can perform operations on entities without
possessing the actual entities themselves, as long as I have a pointer to a resource that DOES possess them.
All I need to do is specify the entity's origin and name, and find a catalog that knows that origin, and then
I can use the reference as a proxy for the entity itself.

Design principles:
1. References should be lightweight and should not require importing anything. Of course, references are useless
without access to a catalog, and right now the catalog is the heaviest thing in the repo (because it imports
create_archive, which imports all the providers).  An implication is that the next thing I will need to implement
is a web client that will allow me to perform queries to a remote catalog without running my own catalog locally.

2. Un-grounded references are useless except for collecting metadata about entities.

3. Grounded references should behave equivalently to the entities themselves. (except the remote catalogs can act
as gatekeepers to decide what information can be disclosed)

The classes in this file get imported elsewhere; the CatalogRef class imports all the others because it
*instantiates* all the others.
"""


class NoCatalog(Exception):
    pass


class InvalidQuery(Exception):
    pass


class BaseRef(object):
    """
    A base class for defining entity references.
    """
    _etype = None

    def __init__(self, origin, external_ref, **kwargs):
        """

        :param origin:
        :param external_ref:
        :param kwargs:
        """
        self._origin = origin
        self._ref = external_ref

        self._d = kwargs

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
    def is_entity(self):
        return False

    def _localitem(self, item):
        if item in self._d:
            return self._d[item]
        if 'Local%s' % item in self._d:
            return self._d['Local%s' % item]
        return None

    def __getitem__(self, item):
        """
        should be overridden
        :param item:
        :return:
        """
        return self._localitem(item)

    def has_property(self, item):
        return self._localitem(item) is not None

    def __setitem__(self, key, value):
        if not key.startswith('Local'):
            key = 'Local%s' % key
        self._d[key] = value

    @property
    def _name(self):
        if self.has_property('Name'):
            return ' ' + self['Name']
        return ''

    @property
    def _addl(self):
        return ''

    def __str__(self):
        return '%s/%s%s [%s]' % (self.origin, self.external_ref, self._name, self._addl)

    def _show_hook(self):
        """
        Place for subclass-dependent specialization of show()
        :return:
        """
        print(' ** UNRESOLVED **')

    @property
    def resolved(self):
        return False

    def show(self):
        """
        should be finessed in subclasses
        :return:
        """
        print('%s catalog reference (%s)' % (self.__class__.__name__, self.external_ref))
        print('origin: %s' % self.origin)
        self._show_hook()
        if len(self._d) > 0:
            print('==Local Fields==')
            ml = len(max(self._d.keys(), key=len))
            for k, v in self._d.items():
                print('%*s: %s' % (ml, k, v))

    def serialize(self):
        j = {
            'origin': self.origin,
            'externalId': self.external_ref
        }
        if self._etype is not None:
            j['entityType'] = self._etype
        j.update(self._d)
        return j


class EntityRef(BaseRef):
    """
    An EntityRef is a CatalogRef that has been provided a valid catalog query.  the EntityRef is still semi-abstract
    since there is no meaningful reference to an entity that is not typed.
    """
    def __init__(self, external_ref, query, reference_entity, uuid=None, **kwargs):
        """

        :param external_ref:
        :param query:
        :param kwargs:
        """
        super(EntityRef, self).__init__(query.origin, external_ref, **kwargs)
        if not query.validate():
            raise InvalidQuery('Query failed validation')
        self._reference_entity = reference_entity

        self._query = query
        self._uuid = uuid or self._query.get_uuid(self.external_ref)

    def get_reference(self):
        return self._reference_entity

    def _check_query(self, message=''):
        if self._query is None:
            print(self)
            raise NoCatalog(message)

    @property
    def uuid(self):
        return self._uuid

    @property
    def reference_entity(self):
        return self._reference_entity

    def get_uuid(self):
        """
        DEPRECATED
        :return:
        """
        return self.uuid

    def elementary(self, iterable):
        """
        yields flows from iterable that are elementary, using the query's access to qdb
        :param iterable:
        :return:
        """
        self._check_query('elementary')

        for i in iterable:
            if self._query.is_elementary(i):
                yield i

    def intermediate(self, iterable):
        """
        yields flows from iterable that are non-elementary, using the query's access to qdb
        :param iterable:
        :return:
        """
        self._check_query('intermediate')

        for i in iterable:
            if not self._query.is_elementary(i):
                yield i

    @property
    def resolved(self):
        return True

    @property
    def privacy(self):
        return self._query.privacy()

    def _show_ref(self):
        print('reference: %s' % self.reference_entity)

    def _show_hook(self):
        if self.uuid is not None:
            print('UUID: %s' % self.uuid)
        for i in ('Name', 'Comment'):
            print('%7s: %s' % (i, self._query.get_item(self.external_ref, i)))

    def validate(self):
        """
        There should be no way to get through the instantiation without a valid query, so this should probably just
        return True (or be made more useful)
        :return:
        """
        if self._query is None:
            return False
        return True

    def get_item(self, item, force_query=False):
        if not force_query:
            # check local first.  return Localitem if present.
            loc = self._localitem(item)
            if loc is not None:
                return loc
        self._check_query('getitem %s' % item)
        val = self._query.get_item(self.external_ref, item)
        if val is not None:
            self._d[item] = val
            return val
        return None

    def __getitem__(self, item):
        return self.get_item(item)
