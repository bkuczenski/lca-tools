"""
This file defines a set of abstract classes that act as interfaces for the data providers.

The data providers are expected to inherit from this class, which will reply "not implemented"
for all the interface methods. Then the data providers can override whichever ones do get implemented.

On the calling side, the interface definitions function as documentation-only, since python uses
duck typing and doesn't require strict interface definitions.
"""
from __future__ import print_function, unicode_literals

import uuid
import re
import json
import gzip as gz

from collections import defaultdict


LD_CONTEXT = 'https://bkuczenski.github.io/lca-tools-datafiles/context.jsonld'


# CatalogRef = namedtuple('CatalogRef', ['archive', 'id'])


# import pandas as pd

uuid_regex = re.compile('([0-9a-f]{8}.?([0-9a-f]{4}.?){3}[0-9a-f]{12})')


def to_uuid(_in):
    if isinstance(_in, uuid.UUID):
        return _in
    if _in is None:
        return _in
    try:
        g = uuid_regex.search(_in)
        if g is not None:
            try:
                _out = uuid.UUID(g.groups()[0])
            except ValueError:
                _out = None
        else:
            _out = None
    except TypeError:
        _out = None
    return _out


class ArchiveInterface(object):
    """
    An abstract interface has nothing but a reference

    """

    @classmethod
    def _key_to_id(cls, key):
        """
        in the base class, the key is the uuid-- this can get overridden
        by default, to_uuid just finds a uuid by regex and returns uuid.UUID()
        :param key:
        :return:
        """
        return to_uuid(key)

    def __init__(self, ref, quiet=True, upstream=None):
        self.ref = ref
        self._entities = {}  # uuid-indexed list of known entities

        self._quiet = quiet  # whether to print out a message every time a new entity is added / deleted / modified

        self._serialize_dict = dict()  # this gets added to

        self._counter = defaultdict(int)
        self._upstream = None
        self._upstream_hash = dict()  # for lookup use later

        if upstream is not None:
            self.set_upstream(upstream)

        self.catalog_names = dict()  # this is a place to store *some kind* of upstream reference to be determined

    def entities(self):
        for v in self._entities.values():
            yield v

    def set_upstream(self, upstream):
        assert isinstance(upstream, ArchiveInterface)
        if upstream.ref != self.ref:
            self._serialize_dict['upstreamReference'] = upstream.ref
        self._upstream = upstream

    def query_upstream_ref(self):
        if 'upstreamReference' in self._serialize_dict:
            return self._serialize_dict['upstreamReference']
        return None

    def truncate_upstream(self):
        """
        removes upstream reference and rewrites entity uuids to match current index. note: deprecates the upstream
        upstream_
        :return:
        """
        for k, e in self._entities.items():
            e._uuid = k
        self._upstream = None
        if 'upstreamReference' in self._serialize_dict:
            self._serialize_dict.pop('upstreamReference')

    def _print(self, *args):
        if self._quiet is False:
            print(*args)

    def __str__(self):
        s = '%s with %d entities at %s' % (self.__class__.__name__, len(self._entities), self.ref)
        if self._upstream is not None:
            s += ' [upstream %s]' % self._upstream.__class__.__name__
        return s

    def _get_entity(self, key):
        """
        the fundamental method- retrieve an entity by UUID- either a uuid.UUID or a string that can be
        converted to a valid UUID.

        If the UUID is not found, returns None. handle this case in client code/subclass.
        :param key: something that maps to a literal UUID via _key_to_id
        :return: the LcEntity or None
        """
        if key is None:
            return None
        entity = self._key_to_id(key)
        if entity in self._entities:
            e = self._entities[entity]
            if e.origin is None:
                e.origin = self.ref
            return e
        elif self._upstream is not None:
            e = self._upstream[key]
            if e is not None:
                self.add(e)  # e is just a reference, so this is literally just a dictionary key
            return e
        else:
            return None

    def __getitem__(self, item):
        return self._get_entity(item)

    def add(self, entity):
        if entity.origin is None or entity.origin == self.ref:
            key = entity.get_external_ref()
        else:
            key = entity.get_uuid()
        u = self._key_to_id(key)
        if u is None:
            raise ValueError('Key must be a valid UUID')

        if u in self._entities:
            raise KeyError('Entity already exists: %s' % u)

        if entity.validate():
            if self._quiet is False:
                print('Adding %s entity with %s: %s' % (entity.entity_type, u, entity['Name']))
            if entity.origin is None:
                assert entity.get_uuid() == str(u), 'New entity uuid must match origin repository key!'
                entity.origin = self.ref
            self._entities[u] = entity
            self._counter[entity.entity_type] += 1

        else:
            raise ValueError('Entity fails validation.')

    def check_counter(self, entity_type=None):
        if entity_type is None:
            [self.check_counter(entity_type=k) for k in ('process', 'flow', 'quantity')]
        else:
            print('%d new %s entities added (%d total)' % (self._counter[entity_type], entity_type,
                                                           len([e for e in self._entities_by_type(entity_type)])))
            self._counter[entity_type] = 0

    @staticmethod
    def _narrow_search(result_set, **kwargs):
        """
        Narrows a result set using sequential keyword filtering
        :param result_set:
        :param kwargs:
        :return:
        """
        def _recurse_expand_subtag(tag):
            if tag is None:
                return ''
            elif isinstance(tag, str):
                return tag
            else:
                return ' '.join([_recurse_expand_subtag(t) for t in tag])
        for k, v in kwargs.items():
            if isinstance(v, str):
                v = [v]
            for vv in v:
                result_set = [r for r in result_set if k in r.keys() and
                              bool(re.search(vv, _recurse_expand_subtag(r[k]), flags=re.IGNORECASE))]
        return result_set

    def search(self, *args, upstream=False, **kwargs):
        """
        Find entities by search term, either full or partial uuid or entity property like 'Name', 'CasNumber',
        or so on.
        :param uuid: optional positional argument is a fragmentary (or complete) uuid string. (additional positional
         params are ignored)
        :param upstream: (False) if upstream archive exists, search there too
        :param kwargs: regex search through entities' properties as named in the kw arguments
        :return:
        """
        etype = None
        uid = None if len(args) == 0 else args[0]
        if uid is not None:
            # search on uuids
            result_set = [self._get_entity(k) for k in self._entities.keys()
                          if bool(re.search(uid, str(k), flags=re.IGNORECASE))]
        else:
            if 'entity_type' in kwargs.keys():
                etype = kwargs.pop('entity_type')
                result_set = self._entities_by_type(etype)
            else:
                result_set = [self._get_entity(k) for k in self._entities.keys()]
        result_set = self._narrow_search(result_set, **kwargs)
        if upstream and self._upstream is not None:
            kwargs['entity_type'] = etype  # need to reset for upstream search-- what an awkward interface
            result_set += self._upstream.search(*args, **kwargs)
        return result_set

    def _fetch(self, entity, **kwargs):
        """
        Dummy function to fetch from archive. MUST be overridden.
        Can't fetch from upstream.
        :param entity:
        :return:
        """
        raise NotImplemented

    def retrieve_or_fetch_entity(self, *args, **kwargs):
        """
        Client-facing function to retrieve entity by ID, first locally, then in archive.

        Input is flexible-- could be a UUID or key (partial uuid is just not useful)

        :param args: the identifying string (uuid or partial uuid)
        :param kwargs: used to filter search results on the local archive
        :return:
        """
        if len(args) > 0:
            uid = args[0]
        else:
            uid = None

        entity = self._get_entity(uid)  # this checks upstream if it exists
        if entity is not None:
            # retrieve
            return entity

        '''
        # why would I search locally for partial uuids??? this function is not for retrieve or fetch approximate entity
        result_set = self.search(uid, **kwargs)
        if len(result_set) == 1:
            return result_set[0]
        elif len(result_set) > 1:
            return result_set
        '''

        # fetch
        return self._fetch(uid, **kwargs)

    def validate_entity_list(self):
        count = 0
        for k, v in self._entities.items():
            valid = True
            # 1: confirm key is a UUID
            if not isinstance(k, uuid.UUID):
                print('Key %s is not a valid UUID.' % k)
                valid = False
            if v.origin is None:
                print("%s: No origin!" % k)
                valid = False

            if v.origin == self.ref:
                # 2: confirm entity's external key maps to its uuid
                if self._key_to_id(v.get_external_ref()) != k:
                    print("%s: Key doesn't match UUID in origin!" % v.get_external_ref())
                    valid = False

            # confirm entity is dict-like with keys() and with a set of common keys
            try:
                valid = valid & v.validate()
            except AttributeError:
                print('Key %s: not a valid LcEntity (no validate() method)' % k)
                valid = False

            if valid:
                count += 1
        print('%d entities validated out of %d' % (count, len(self._entities)))
        return count

    def _load_all(self, **kwargs):
        """
        Must be overridden in subclass
        :return:
        """
        raise NotImplemented

    def load_all(self, **kwargs):
        print('Loading %s' % self.ref)
        self._load_all(**kwargs)

    def _entities_by_type(self, entity_type):
        for v in self._entities.values():
            if v.entity_type == entity_type:
                yield v

    '''
    @staticmethod
    def _to_pandas(entities, EntityClass=LcEntity, **kwargs):
        """
        Creates an entity-type-specific DataFrame of entities.  Kind of funky and special purpose.
        :param entities:
        :param EntityClass: LcEntity subclass (used for determining signature fields)
        :param kwargs:
        :return:
        """
        sig = [p.get_signature() for p in entities]
        index = [p.get_uuid() for p in entities]
        df = pd.DataFrame(sig, index=index, columns=[i for i in EntityClass.signature_fields()], **kwargs)
        df.index.name = 'UUID'
        return df
    '''

    def serialize(self, **kwargs):
        j = {
            '@context': LD_CONTEXT,
            'dataSourceType': self.__class__.__name__,
            'dataSourceReference': self.ref,
            'catalogNames': self.catalog_names
        }
        j.update(self._serialize_dict)
        return j

    def write_to_file(self, filename, gzip=False, **kwargs):
        s = self.serialize(**kwargs)
        if gzip is True:
            if not bool(re.search('\.gz$', filename)):
                filename += '.gz'
            try:  # python3
                with gz.open(filename, 'wt') as fp:
                    json.dump(s, fp, indent=2, sort_keys=True)
            except ValueError:  # python2
                with gz.open(filename, 'w') as fp:
                    json.dump(s, fp, indent=2, sort_keys=True)
        else:
            with open(filename, 'w') as fp:
                json.dump(s, fp, indent=2, sort_keys=True)
