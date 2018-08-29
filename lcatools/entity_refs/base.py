"""
Motivation:
The creation of catalog references is borne out of the idea that I can perform operations on entities without
possessing the actual entities themselves, as long as I have a pointer to a resource that DOES possess them.
All I need to do is specify the entity's origin and name, and find a catalog that knows that origin, and then
I can use the reference as a proxy for the entity itself.

Design principles:
1. References should be lightweight and should not require importing anything. Of course, references are useless
without access to a query object that can implement the various methods.

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


class EntityRefMergeError(Exception):
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
        return self._uuid or self._ref

    @property
    def link(self):
        return '%s/%s' % (self.origin, self.external_ref)

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
        raise KeyError(item)

    def __getitem__(self, item):
        """
        should be overridden
        :param item:
        :return:
        """
        return self._localitem(item)

    def get(self, item, default=None):
        try:
            self.__getitem__(item)
        except KeyError:
            return default

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

    def __eq__(self, other):
        if other is None:
            return False
        try:
            return (self.entity_type == other.entity_type and self.external_ref == other.external_ref and
                    (self.origin.startswith(other.origin) or other.origin.startswith(self.origin)))
        except AttributeError:
            return False

    def __str__(self):
        return '%s/%s%s [%s]' % (self.origin, self.external_ref, self._name, self._addl)

    def __hash__(self):
        return hash(self.link)

    def _show_hook(self):
        """
        Place for subclass-dependent specialization of show()
        :return:
        """
        print(' ** UNRESOLVED **')

    @property
    def resolved(self):
        return False

    def merge(self, other):
        if self.entity_type != other.entity_type:
            raise EntityRefMergeError('Type mismatch %s vs %s' % (self.entity_type, other.entity_type))
        if self.link != other.link:
            if self.external_ref == other.external_ref:
                if not (self.origin.startswith(other.origin) or other.origin.startswith(self.origin)):
                    raise EntityRefMergeError('Origin mismatch %s vs %s' % (self.origin, other.origin))
            else:
                raise EntityRefMergeError('external_ref mismatch: %s vs %s' % (self.external_ref, other.external_ref))
        # otherwise fine-- left argument is dominant
        self._d.update(other._d)

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

    Must provide a uuid kwarg to avoid a query lookup, though the uuid need not be valid (unless the ref is going
    to be added to an archive that requires it).
    """
    def make_ref(self, *args):
        return self

    def __init__(self, external_ref, query, reference_entity, **kwargs):
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
        if self._uuid is None:
            self._uuid = self._query.get_uuid(self.external_ref)

    def get_reference(self):
        return self._reference_entity

    def _check_query(self, message=''):
        if self._query is None:
            print(self)
            raise NoCatalog(message)

    @property
    def reference_entity(self):
        return self._reference_entity

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

    def _show_ref(self):
        print('reference: %s' % self.reference_entity)

    def _show_hook(self):
        if self.uuid is not None:
            print('UUID: %s' % self.uuid)
        for i in ('Name', 'Comment'):
            print('%7s: %s' % (i, self.get_item(i)))

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
            loc = super(EntityRef, self).get(item)
            if loc is not None:
                return loc
        self._check_query('getitem %s' % item)
        val = self._query.get_item(self.external_ref, item)
        if val is not None and val != '':
            self._d[item] = val
            return val
        raise KeyError(item)

    def __getitem__(self, item):
        return self.get_item(item)

    def serialize(self):
        j = super(EntityRef, self).serialize()
        j['entityId'] = self.uuid
        return j
