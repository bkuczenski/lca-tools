from __future__ import print_function, unicode_literals

import uuid
from itertools import chain
from numbers import Number
from lcatools.entity_refs import CatalogRef
from lcatools.interfaces import PropertyExists

from synonym_dict import LowerDict


entity_types = ('process', 'flow', 'quantity', 'fragment')
entity_refs = {
    'process': 'exchange',
    'flow': 'quantity',
    'quantity': 'unit',
    'fragment': 'fragment'
}


def concatenate(*lists):
    return chain(*lists)


class EntityInitializationError(Exception):
    pass


class EntityMergeError(Exception):
    pass


class LcEntity(object):
    """
    All LC entities behave like dicts, but they all have some common properties, defined here.
    """
    _pre_fields = ['Name']
    _new_fields = []
    _ref_field = ''
    _post_fields = ['Comment']

    _origin = None

    def __init__(self, entity_type, external_ref, origin=None, entity_uuid=None, **kwargs):

        if external_ref is None:
            if entity_uuid is None:
                raise EntityInitializationError('At least one of entity_uuid, external_ref must be provided')
            external_ref = str(entity_uuid)

        self._external_ref = str(external_ref)

        self._uuid = None

        if entity_uuid is not None:
            self.uuid = entity_uuid

        self._d = LowerDict()

        self._entity_type = entity_type
        self._reference_entity = None

        if origin is not None:
            self.origin = origin

        self._d['Name'] = self._external_ref
        self._d['Comment'] = ''

        self._query_ref = None  # memoize this

        for k, v in kwargs.items():
            if v is None:
                continue
            self[k] = v

    @property
    def reference_entity(self):
        return self._reference_entity

    def _make_ref_ref(self, query):
        if self.reference_entity is not None:
            return self.reference_entity.make_ref(query)
        return None

    def make_ref(self, query):
        if self._query_ref is None:
            d = dict()
            reference_entity = self._make_ref_ref(query)
            for k in self.signature_fields():
                if k == self._ref_field:
                    continue
                if k in self._d:
                    d[k] = self._d[k]
            self._query_ref = CatalogRef.from_query(self.external_ref, query, self.entity_type, reference_entity,
                                                    uuid=self.uuid, **d)
        return self._query_ref

    @property
    def entity_type(self):
        return self._entity_type

    @property
    def origin(self):
        return self._origin

    @property
    def is_entity(self):
        """
        Used to distinguish between entities and catalog refs (which answer False)
        :return: True for LcEntity subclasses
        """
        return True

    def map_origin(self, omap, fallback=None):
        """
        This is used to propagate a change in origin semantics. Provide a dict that maps old origins to new origins.
        External ref should remain the same with respect to the new origin.
        :param omap: dict mapping old origin to new origin
        :param fallback: if present, use in cases where old origin not found
        :return:
        """
        if self._origin in omap:
            self._origin = omap[self._origin]
        elif fallback is not None:
            self._origin = fallback

    @origin.setter
    def origin(self, value):
        if self._origin is None:
            self._origin = value
        else:
            raise PropertyExists('Origin already set to %s' % self._origin)

    def signature_fields(self):
        return concatenate(self._pre_fields, self._new_fields,
                           [self._ref_field] if self._ref_field is not [] else [], self._post_fields)

    @property
    def reference_field(self):
        return self._ref_field

    @property
    def external_ref(self):
        return self._external_ref

    def get_signature(self):
        k = dict()
        for i in self.signature_fields():
            k[i] = self[i]
        return k

    @property
    def uuid(self):
        return self._uuid

    @uuid.setter
    def uuid(self, key):
        if self._uuid is not None:
            raise PropertyExists('UUID has already been specified! %s' % self._uuid)
        if isinstance(key, uuid.UUID):
            self._uuid = str(key)
        else:
            self._uuid = str(uuid.UUID(key))

    @property
    def link(self):
        return '%s/%s' % (self.origin, self.external_ref)

    def _validate_reference(self, ref_entity):
        if ref_entity is None:
            # raise ValueError('Null reference')
            return False  # allow none references
        if ref_entity.entity_type != entity_refs[self.entity_type]:
            raise TypeError("Type Mismatch on reference entity: expected %s, found %s" % (entity_refs[self.entity_type],
                                                                                          ref_entity.entity_type))
        return True

    def _set_reference(self, ref_entity):
        """
        set the entity's reference value.  Can be overridden
        :param ref_entity:
        :return:
        """
        self._validate_reference(ref_entity)
        self._reference_entity = ref_entity

    def has_property(self, prop):
        return prop in self._d

    def properties(self):
        for i in self._d.keys():
            yield i

    def get_properties(self):
        """
        dict of properties and values for a given entity
        :return:
        """
        d = dict()
        for i in self.properties():
            d[i] = self._d[i]
        return d

    def update(self, d):
        self._d.update(d)

    def validate(self):
        valid = True
        if self.reference_entity is not None:
            try:
                self._validate_reference(self.reference_entity)
            except TypeError:
                print("Reference entity type %s is wrong for %s (%s)" %
                      (self.reference_entity.entity_type,
                       self.entity_type,
                       entity_refs[self.entity_type]))
                valid = False
        for i in self.signature_fields():
            try:
                self[i]
            except KeyError:
                print("Required field %s does not exist" % i)
                valid = False
        return valid

    def _print_ref_field(self):
        if self.reference_entity is None:
            return None
        else:
            return '%s' % self.reference_entity.external_ref

    def serialize(self, domesticate=False, drop_fields=()):
        j = {
            'entityType': self.entity_type,
            'externalId': self.external_ref,
            'origin': self.origin,
            self._ref_field: self._print_ref_field(),
        }
        if domesticate or self._origin is None:
            j.pop('origin')
        for k, v in self._d.items():
            if k in drop_fields:
                continue
            if v is None:
                continue
            elif isinstance(v, list):
                j[k] = v
            elif isinstance(v, set):
                j[k] = sorted(list(v))
            elif isinstance(v, Number):
                j[k] = v
            elif isinstance(v, bool):
                j[k] = v
            elif isinstance(v, LcEntity):
                j[k] = {"origin": v.origin,
                        "externalId": v.external_ref,
                        "entity_type": v.entity_type}
            elif isinstance(v, dict):
                j[k] = v
            else:
                j[k] = str(v)
        return j

    def __getitem__(self, item):
        if item.lower() == self._ref_field.lower():
            return self.reference_entity
        elif item == 'EntityType':
            return self.entity_type
        else:
            # don't catch KeyErrors here-- leave that to subclasses
            return self._d[item]

    def get(self, item):
        try:
            return self.__getitem__(item)
        except KeyError:
            return None

    def __setitem__(self, key, value):
        if key == 'EntityType':
            raise ValueError('Entity Type cannot be changed')
        elif key.lower() in (self._ref_field.lower(), 'reference', 'referenceentity', 'reference_entity'):
            self._set_reference(value)
        elif key.lower() in ('entityid', 'entitytype', 'externalid', 'origin'):
            raise KeyError('Disallowed Keyname %s' % key)
        else:
            self._d[key] = value

    def merge(self, other):
        if False:  # not isinstance(other, LcEntity):  ## This is not a requirement! cf. EntityRefs, Disclosure objs
            raise EntityMergeError('Incoming is not an LcEntity: %s' % other)
        elif self.entity_type != other.entity_type:
            raise EntityMergeError('Incoming entity type %s mismatch with %s' % (other.entity_type, self.entity_type))
        elif self.external_ref != other.external_ref:
            raise EntityMergeError('Incoming External ref %s conflicts with existing %s' % (other.external_ref,
                                                                                            self.external_ref))
        else:
            # if self.origin != other.origin:
            #     print('Merging entities with differing origin: \nnew: %s\nexisting: %s'% (other.origin, self.origin))
            for k in other.properties():
                if k not in self._d.keys():
                    print('Merge: Adding key %s: %s' % (k, other[k]))
                    self[k] = other[k]

    def show(self):
        print('%s Entity (ref %s)' % (self.entity_type.title(), self.external_ref))
        print('origin: %s' % self.origin)
        if self.entity_type == 'process':
            for i in self.reference_entity:
                print('reference: %s' % i)
        else:
            print('reference: %s' % self.reference_entity)
        fix = ['Name', 'Comment']
        postfix = set(str(k) for k in self._d.keys()).difference(fix)
        ml = len(max(self._d.keys(), key=len))
        for k in fix:
            print('%*s: %s' % (ml, k, self._d[k]))
        for k in postfix:
            print('%*s: %s' % (ml, k, self._d[k]))

    def __str__(self):
        return 'LC %s: %s' % (self.entity_type, self._d['Name'])

    @property
    def _name(self):
        return str(self)

    def __hash__(self):
        """
        External ref is set by the end of __init__ and is immutable (except for fragments-- which use uuid for hash)
        :return:
        """
        if self._origin is None:
            raise AttributeError('Origin not set!')
        return hash(self.link)

    def __eq__(self, other):
        """
        two entities are equal if their types, origins, and external references are the same.
        internal refs do not need to be equal; reference entities do not need to be equal
        :return:
        """
        if other is None:
            return False
        # if not isinstance(other, LcEntity):  # taking this out so that CatalogRefs and entities can be compared
        #     return False
        try:
            is_eq = (self.external_ref == other.external_ref
                     and self.origin == other.origin
                     and self.entity_type == other.entity_type)
        except AttributeError:
            is_eq = False
        return is_eq
