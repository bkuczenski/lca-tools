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

    def __init__(self, origin, external_ref, uuid=None, **kwargs):
        """

        :param origin:
        :param external_ref:
        :param kwargs:
        """
        self._origin = origin
        self._ref = external_ref

        self._uuid = uuid

        self._d = kwargs

    @property
    def origin(self):
        return self._origin

    @property
    def external_ref(self):
        return self._ref

    @property
    def uuid(self):
        return self._uuid

    def get_uuid(self):
        """
        DEPRECATED
        :return:
        """
        return self.uuid

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
        if key in ('Name', 'Comment'):
            key = 'Local%s' % key
        self._d[key] = value

    def _show_hook(self):
        """
        Place for subclass-dependent specialization of show()
        :return:
        """
        pass

    def show(self):
        """
        should be finessed in subclasses
        :return:
        """
        print('%s catalog reference (%s)' % (self.__class__.__name__, self.external_ref))
        print('origin: %s' % self.origin)
        if self.uuid is not None:
            print('UUID: %s' % self.uuid)
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
    def __init__(self, origin, external_ref, query, **kwargs):
        """

        :param origin:
        :param external_ref:
        :param query:
        :param kwargs:
        """
        super(EntityRef, self).__init__(origin, external_ref, **kwargs)
        if not query.validate():
            raise InvalidQuery('Query failed validation')
        self._query = query

    def _check_query(self, message=''):
        if self._query is None:
            print(self)
            raise NoCatalog(message)

    def fetch(self):
        return self._query.fetch(self)

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

    def _show_hook(self):
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

    def __getitem__(self, item):
        loc = self._localitem(item)
        if loc is not None:
            return loc
        self._check_query('getitem %s' % item)
        val = self._query.get_item(self.external_ref, item)
        if item in ('Name', 'Comment'):
            self._d['Local%s' % item] = val
        return val
