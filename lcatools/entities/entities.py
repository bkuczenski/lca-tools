from __future__ import print_function, unicode_literals

import uuid
from itertools import chain
from numbers import Number
from lcatools.entity_refs import CatalogRef


entity_types = ('process', 'flow', 'quantity', 'fragment')
entity_refs = {
    'process': 'exchange',
    'flow': 'quantity',
    'quantity': 'unit',
    'fragment': 'fragment'
}


def concatenate(*lists):
    return chain(*lists)


class PropertyExists(Exception):
    pass


class LcEntity(object):
    """
    All LC entities behave like dicts, but they all have some common properties, defined here.
    """
    _pre_fields = ['Name']
    _new_fields = []
    _ref_field = ''
    _post_fields = ['Comment']

    def __init__(self, entity_type, entity_uuid, origin=None, external_ref=None, **kwargs):

        if isinstance(entity_uuid, uuid.UUID):
            self._uuid = str(entity_uuid)
        else:
            self._uuid = str(uuid.UUID(entity_uuid))
        self._d = dict()

        self._entity_type = entity_type
        self.reference_entity = None
        self._origin = origin

        self._d['Name'] = ''
        self._d['Comment'] = ''

        self._external_ref = external_ref

        self._query_ref = None  # memoize this

        for k, v in kwargs.items():
            self[k] = v

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

    @classmethod
    def signature_fields(cls):
        return concatenate(cls._pre_fields, cls._new_fields,
                           [cls._ref_field] if cls._ref_field is not [] else [], cls._post_fields)

    @property
    def external_ref(self):
        if self._external_ref is None:
            return self._uuid
        return self._external_ref

    @external_ref.setter
    def external_ref(self, ref):
        """
        Specify how the entity is referred to in the source dataset. If this is unset, the UUID is assumed
        to be used externally.
        :param ref:
        :return:
        """
        if self._external_ref is None:
            self._external_ref = ref
        else:
            raise PropertyExists('External Ref already set to %s' % self._external_ref)

    def set_external_ref(self, ref):
        """
        deprecated
        """
        self.external_ref = ref

    def get_external_ref(self):
        """
        deprecated
        """
        return self.external_ref

    def get_signature(self):
        k = dict()
        for i in self.signature_fields():
            k[i] = self[i]
        return k

    def get_uuid(self):
        """
        deprecated.  switch to .uuid property.
        :return:
        """
        return self._uuid

    @property
    def uuid(self):
        return self._uuid

    @property
    def link(self):
        return '%s/%s' % (self.origin, self.get_external_ref())

    def _validate_reference(self, ref_entity):
        if ref_entity is None:
            # raise ValueError('Null reference')
            return False  # allow none references
        if ref_entity.entity_type != entity_refs[self.entity_type]:
            raise TypeError("Type Mismatch on reference entity")
        return True

    def _set_reference(self, ref_entity):
        """
        set the entity's reference value.  Can be overridden
        :param ref_entity:
        :return:
        """
        self._validate_reference(ref_entity)
        self.reference_entity = ref_entity

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
        if self.entity_type not in entity_types:
            print('Entity type %s not valid!' % self.entity_type)
            valid = False
        if self.reference_entity is not None:
            try:
                self._validate_reference(self.reference_entity)
            except TypeError:
                print("Reference entity type %s is wrong for %s (%s)" %
                      (self.reference_entity.entity_type,
                       self.entity_type,
                       entity_types[self.entity_type]))
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
            return '%s' % None
        else:
            return '%s' % self.reference_entity.get_external_ref()

    def serialize(self):
        j = {
            'entityId': self.get_uuid(),
            'entityType': self.entity_type,
            'externalId': self.get_external_ref(),
            'origin': self.origin,
            self._ref_field: self._print_ref_field(),
        }
        for k, v in self._d.items():
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

    def __setitem__(self, key, value):
        if key == 'EntityType':
            raise ValueError('Entity Type cannot be changed')
        elif key.lower() == self._ref_field.lower():
            self._set_reference(value)
        elif key.lower() in ('entityid', 'entitytype', 'externalid', 'origin'):
            raise KeyError('Disallowed Keyname %s' % key)
        else:
            self._d[key] = value

    def merge(self, other):
        if not isinstance(other, LcEntity):
            raise ValueError('Incoming is not an LcEntity: %s' % other)
        elif self.entity_type != other.entity_type:
            raise ValueError('Incoming entity type %s mismatch with %s' % (other.entity_type, self.entity_type))
        elif self.get_external_ref() != other.get_external_ref():
            raise ValueError('Incoming External ref %s conflicts with existing %s' % (other.get_external_ref(),
                                                                                      self.get_external_ref()))
        else:
            # if self.origin != other.origin:
            #     print('Merging entities with differing origin: \nnew: %s\nexisting: %s'% (other.origin, self.origin))
            for k in other.keys():
                if k not in self.keys():
                    print('Merge: Adding key %s: %s' % (k, other[k]))
                    self[k] = other[k]

    def keys(self):
        return self._d.keys()

    def show(self):
        print('%s Entity (ref %s)' % (self.entity_type.title(), self.get_external_ref()))
        print('origin: %s' % self.origin)
        if self.entity_type == 'process':
            for i in self.reference_entity:
                print('reference: %s' % i)
        else:
            print('reference: %s' % self.reference_entity)
        fix = ['Name', 'Comment']
        postfix = set(self._d.keys()).difference(fix)
        ml = len(max(self._d.keys(), key=len))
        for k in fix:
            print('%*s: %s' % (ml, k, self._d[k]))
        for k in postfix:
            print('%*s: %s' % (ml, k, self._d[k]))

    def __str__(self):
        return 'LC %s: %s' % (self.entity_type, self._d['Name'])

    def __hash__(self):
        return hash(self._uuid)

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
