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
from synonym_dict import LowerDict
from itertools import chain


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

        self._d = LowerDict()
        self._d.update({k: v for k, v in filter(lambda x: x[1] is not None, kwargs.items())})

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
        if 'local_%s' % item in self._d:
            return self._d['local_%s' % item]

    def __getitem__(self, item):
        """
        should be overridden
        :param item:
        :return:
        """
        return self._localitem(item)

    def properties(self):
        for k in self._d.keys():
            yield k

    def get(self, item, default=None):
        try:
            self.__getitem__(item)
        except KeyError:
            return default

    def has_property(self, item):
        return self._localitem(item) is not None

    def __setitem__(self, key, value):
        key = key.lower()
        if not key.startswith('local_'):
            key = 'local_%s' % key
        self._d[key] = value

    @property
    def _name(self):
        """
        This should be the same as .name for entities; whereas str(ref) prepends origin
        :return:
        """
        if self.has_property('Name'):
            addl = self._addl
            name = self['Name']
            if len(addl) > 0:
                name += ' [%s]' % addl
            return name
        return self.external_ref

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
        return '[%s] %s' % (self.origin, self._name)

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
                pass  # we're going to consider origin mismatches allowable for entity refs
                '''
                if not (self.origin.startswith(other.origin) or other.origin.startswith(self.origin)):
                    raise EntityRefMergeError('Origin mismatch %s vs %s' % (self.origin, other.origin))
                '''
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
            _notprint = True  # only print '==Local Fields==' if there are any
            ml = len(max(self._d.keys(), key=len))
            named = {'comment', 'name'}
            for k, v in self._d.items():
                if k.lower() in named or 'local' + k.lower() in named:
                    continue
                named.add(k.lower())
                if _notprint:
                    print('==Local Fields==')
                    _notprint = False
                print('%*s: %s' % (ml, k, v))

    def serialize(self, **kwargs):
        """

        :param kwargs: 'domesticate' has no effect- refs can't be domesticated
        :return:
        """
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

    UUID is looked up when queried, but not all entities have uuids and I don't want to import the interface just for
    exception checking
    """
    _ref_field = 'referenceEntity'

    @property
    def reference_field(self):
        return self._ref_field

    def make_ref(self, *args):
        return self

    def set_external_ref(self, ref):
        # stopgap because of entity_from_json confusion regarding external refs and foregrounds
        # TODO: rework external ref handling after context refactor
        if ref != self.external_ref:
            raise ValueError('Ref collision! [%s] != [%s]' % (ref, self.external_ref))

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

    @property
    def uuid(self):
        if self._uuid is None:
            self._uuid = self._query.get_uuid(self.external_ref)
        return self._uuid


    def get_reference(self):
        return self._reference_entity

    def _check_query(self, message=''):
        if self._query is None:
            print(self)
            raise NoCatalog(message)

    @property
    def reference_entity(self):
        return self._reference_entity

    @property
    def resolved(self):
        return True

    def signature_fields(self):
        yield self._ref_field

    def _show_ref(self):
        print('reference: %s' % self.reference_entity)

    def _show_hook(self):
        if self.uuid is not None:
            print('UUID: %s' % self.uuid)
        for i in ('Name', 'Comment'):
            try:
                print('%7s: %s' % (i, self.get_item(i)))
            except KeyError:
                pass

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
        """
        This keeps generating recursion errors. Let's think it through.
         - first checks locally. If known, great.
         - if not, need to check remotely by running the query.
         - the query retrieves the entity and asks has_property
           -- which causes recursion error if the query actually gets the entity_ref
           -- so has_property has to not trigger an additional query call, and only asks locally.
         - fine. So when do we raise a key error?
        :param item:
        :param force_query:
        :return:
        """
        if not force_query:
            # check local first.  return Localitem if present.
            loc = self._localitem(item)
            if loc is not None:
                return loc
        self._check_query('getitem %s' % item)
        try:
            val = self._query.get_item(self.external_ref, item)
        except KeyError:
            return None
        if val is not None and val != '':
            self._d[item] = val
            return val
        raise KeyError(item)

    def __getitem__(self, item):
        if item == self._ref_field:
            return self._reference_entity
        val = self.get_item(item)
        if val is None:
            raise KeyError(item)
        return val

    def serialize(self, **kwargs):
        j = super(EntityRef, self).serialize(**kwargs)
        if self.uuid is not None:
            j['uuid'] = self.uuid
        return j
