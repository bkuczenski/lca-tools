"""
This file defines a set of abstract classes that act as interfaces for the data providers.

The data providers are expected to inherit from this class, which will reply "not implemented"
for all the interface methods. Then the data providers can override whichever ones do get implemented.

On the calling side, the interface definitions function as documentation-only, since python uses
duck typing and doesn't require strict interface definitions.
"""
from __future__ import print_function, unicode_literals

import uuid
from os.path import splitext
import re
import json
import gzip as gz
import os
import six
from datetime import datetime

from collections import defaultdict


LD_CONTEXT = 'https://bkuczenski.github.io/lca-tools-datafiles/context.jsonld'


# CatalogRef = namedtuple('CatalogRef', ['archive', 'id'])


uuid_regex = re.compile('([0-9a-f]{8}.?([0-9a-f]{4}.?){3}[0-9a-f]{12})')


def to_uuid(_in):
    if _in is None:
        return _in
    if isinstance(_in, int):
        return None
    try:
        g = uuid_regex.search(_in)  # using the regexp test is 50% faster than asking the UUID library
    except TypeError:
        if isinstance(_in, uuid.UUID):
            return str(_in)
        g = None
    if g is not None:
        return g.groups()[0]
    # no regex match- let's see if uuid.UUID can handle the input
    try:
        _out = uuid.UUID(_in)
    except ValueError:
        return None
    return str(_out)


def local_ref(source):
    """
    Create a semantic ref for a local filename.  Just uses basename.  what kind of monster would access multiple
    different files with the same basename without specifying ref?

    alternative is splitext(source)[0].translate(maketrans('/\\','..'), ':~') but ugghh...

    Okay, FINE.  I'll use the full path.  WITH leading '.' removed.

    Anyway, to be clear, local semantic references are not supposed to be distributed.
    :param source:
    :return:
    """
    xf = source.translate(str.maketrans('/\\', '..', ':~'))
    while splitext(xf)[1] in {'.gz', '.json', '.zip', '.txt', '.spold', '.7z'}:
        xf = splitext(xf)[0]
    while xf[0] == '.':
        xf = xf[1:]
    while xf[-1] == '.':
        xf = xf[:-1]
    return '.'.join(['local', xf])


class EntityStore(object):
    """
    An abstract interface has nothing but a reference

    """
    _entity_types = ()
    '''
    _ns_uuid_required: specifies whether the archive must be supplied an ns_uuid (generally, archives that are
    expected to generate persistent, deterministic IDs must have an externally specified ns_uuid)
     If False: random ns_uuid generated
     If True: ns_uuid must be supplied as an argument
     If None: ns_uuid forced to None
    '''
    _ns_uuid_required = False

    def _key_to_id(self, key):
        """
        in the base class, the key is the uuid-- this can get overridden
        by default, to_uuid just returns a string matching the regex, or failing that, tries to generate a string
        using uuid.UUID(key)
        :param key:
        :return:
        """
        u = to_uuid(key)  # check if key is already a uuid
        if u is None:
            return self._key_to_nsuuid(key)
        return u

    def _key_to_nsuuid(self, key):
        if self._ns_uuid is None:
            return None
        if isinstance(key, int):
            key = str(key)
        if six.PY2:
            return str(uuid.uuid3(self._ns_uuid, key.encode('utf-8')))
        else:
            return str(uuid.uuid3(self._ns_uuid, key))

    def get_uuid(self, key):
        """
        Deprecated.
        :param key:
        :return:
        """
        return self._key_to_id(key)

    def _set_ns_uuid(self, ns_uuid):
        if self._ns_uuid_required is None:
            if ns_uuid is not None:
                print('Ignoring ns_uuid specification')
            return None
        else:
            if ns_uuid is None:
                if self._ns_uuid_required is True:
                    raise AttributeError('ns_uuid specification required')
                elif self._ns_uuid_required is False:
                    return uuid.uuid4()
            else:
                if isinstance(ns_uuid, uuid.UUID):
                    return ns_uuid
                return uuid.UUID(ns_uuid)

    def __init__(self, source, ref=None, quiet=True, upstream=None, static=False, dataReference=None, ns_uuid=None,
                 **kwargs):
        """
        An archive is a provenance structure for a collection of entities.  Ostensibly, an archive has a single
        source from which entities are collected.  However, archives can also collect entities from multiple sources,
        either by specifying an upstream archive that is subsequently truncated, or by the entity being added
        explicitly.

        The source is a resolvable URI that indicates a data resource from which entities can be extracted.  The
        exact manner of extracting data from resources is left to the subclasses.

        Internally, all entities are stored with UUID keys.  If the external references do not contain UUIDs, it is
        recommended to derive a UUID3 using an archive-specific, stable namespace ID.  The NsUuidArchive subclass
        does this semi-automatically (semi- because the uuid is an input argument to the entity constructor and
        so it has to be known. but maybe we should do away with that.  entities without uuids! amazing!).

        An archive has a single semantic reference that describes the data context from which its native entities
        were gathered.  The reference is given using dot-separated hierarchical terms in order of decreasing
        semantic significance from left to right.  The leftmost specifier should describe the maintainer of the
         resource (which defaults to 'local' when a reference argument is not provided), followed by arbitrarily
         more precise specifications. Some examples are:
         local.lcia.traci.2.1.spreadsheet
         ecoinvent.3.2.undefined

        The purpose for the source / reference distinction is that in principle many different sources can all provide
        the same semantic content: for instance, ecoinvent can be accessed from the website or from a file on the
        user's computer.  In principle, if the semantic reference for two archives is the same, the archives should
        contain excerpts of the same data, even if drawn from different sources.

        An entity is uniquely identified by its origin and a stable reference known as an 'external_ref'.  An entity's
        canonical identifier fits the pattern 'origin/external_ref'.  Examples:

        elcd.3.2/processes/00043bd2-4563-4d73-8df8-b84b5d8902fc
        uslci.ecospold/Acetic acid, at plant

        Note that the inclusion of embedded whitespace, commas, and other delimiters indicate that these semantic
        references are not proper URIs.

        It is hoped that the user community will help develop and maintain a consistent and easily interpreted
        namespace for semantic references.  If this is done, it should be possible to identify any published entity
        with a concise reference.

        When an entity is first added to an archive, it is assigned that archive's *reference* as its origin, following
        the expectation that data about the same reference from different sources is the same data.

        When an entity with a different origin is added to an archive, it is good practice to add a mapping from that
        origin to its source in the receiving archive's "catalog_names" dictionary.  However, since the entity itself
        does not know its archive's source, this cannot be done automatically.

        :param source: physical data source-- where the information is being drawn from
        :param ref: optional semantic reference for the data source. gets added to catalog_names.
        :param quiet:
        :param upstream:
        :param static: [False] whether archive is expected to be unchanging.
        :param dataReference: alternative to ref
        :param ns_uuid: required to store entities by common name.  Used to generate uuid3 from string inputs.
        :param kwargs: any other information that should be serialized with the archive
        """

        self._source = source
        self._entities = {}  # uuid-indexed list of known entities

        self._quiet = quiet  # whether to print out a message every time a new entity is added / deleted / modified

        self._serialize_dict = kwargs  # this gets added to

        self._counter = defaultdict(int)
        self._ents_by_type = defaultdict(set)
        self._upstream = None

        self._loaded = False
        self._static = static

        self._ns_uuid = self._set_ns_uuid(ns_uuid)

        if upstream is not None:
            self.set_upstream(upstream)

        self.catalog_names = dict()  # this is a place to map semantic references to data sources
        if ref is None:
            if dataReference is None:
                ref = local_ref(source)
            else:
                ref = dataReference

        self._serialize_dict['dataReference'] = ref
        if self._ns_uuid is not None:
            self._serialize_dict['nsUuid'] = str(self._ns_uuid)

        self.catalog_names[ref] = source

    def _construct_new_ref(self, signifier):
        new_date = datetime.now().strftime('%Y%m%d')
        old_tail = self.ref.split('.')[-1]
        if signifier is None:
            if old_tail == new_date:
                new_tail = datetime.now().strftime('%Y%m%d-%H%M')
            else:
                new_tail = new_date
        else:
            if not bool(re.match('[A-Za-z0-9_-]+', signifier)):
                raise ValueError('Invalid signifier %s' % signifier)
            new_tail = '.'.join([signifier, new_date])

        if bool(re.match('[0-9-]{6,}', old_tail)):
            # strip trailing date
            new_ref = '.'.join(self.ref.split('.')[:-1])
        else:
            # use current if no date found
            new_ref = self.ref
        new_ref = '.'.join([new_ref, new_tail])
        return new_ref

    def create_descendant(self, archive_path, signifier=None, force=False):
        """
        Saves the archive to a new source with a new semantic reference.  The new semantic ref is derived by
         (a) first removing any trailing ref that matches [0-9]{8+}
         (b) appending the descendant signifier
         (c) appending the current date in YYYYMMDD format

        After that:
         1. The new semantic ref is added to catalog_names,
         2. the source is set to archive_path/semantic.ref.json.gz,
         3. load_all() is executed,
         4. the archive is saved to the new source.

        :param archive_path: where to store the archive
        :param signifier: A nonzero-length string matching [A-Za-z0-9_-]+.  If not supplied, then the semantic ref is
        unchanged except for the date tag.
        :param force: overwrite if file exists
        :return: new semantic ref.
        """
        if not os.path.exists(archive_path):
            os.makedirs(archive_path)

        new_ref = self._construct_new_ref(signifier)
        if new_ref == self.ref:
            raise KeyError('Refs are the same!')  # KeyError bc it's a key in catalog_names

        new_filename = new_ref + '.json.gz'
        new_source = os.path.join(archive_path, new_filename)
        if os.path.exists(new_source):
            if force:
                print('Overwriting existing archive')
            else:
                raise EnvironmentError('File %s exists: force=True to overwrite' % new_source)

        self.catalog_names[new_ref] = new_source
        self._source = new_source
        self.load_all()
        self.write_to_file(new_source, gzip=True, complete=True)
        return new_ref

    @property
    def ref(self):
        return next(k for k, v in self.catalog_names.items() if v == self.source)

    @property
    def static(self):
        return self._static or self._loaded

    '''
    @property
    def ref(self):
        """
        Deprecated.  Archives have a source; catalogs have a ref.
        :return:
        """
        return self._source
    '''

    @property
    def source(self):
        return self._source

    def entities(self):
        for v in self._entities.values():
            yield v

    def set_upstream(self, upstream):
        assert isinstance(upstream, EntityStore)
        if upstream.source != self.source:
            self._serialize_dict['upstreamReference'] = upstream.ref
        self._upstream = upstream

    def get_names(self):
        """
        Return a mapping of data source to semantic reference, based on the catalog_names property.  This is used by
        a catalog interface to convert entity origins from physical to semantic.

        If a single data source has multiple semantic references, only the most-downstream one will be kept.  If there
        are multiple semantic references for the same data source in the same archive, one will be kept at random.
        This should be avoided and I should probably test for it when setting catalog_names.
        :return:
        """
        if self._upstream is None:
            names = dict()
        else:
            names = self._upstream.get_names()

        for k, v in self.catalog_names.items():
            names[v] = k
        return names

    '''
    def truncate_upstream(self):
        """
        BROKEN!
        removes upstream reference and rewrites entity uuids to match current index. note: deprecates the upstream
        upstream_
        :return:
        """
        # TODO: this needs to be fixed: truncate needs localize all upstream entities (retaining their origins)
        for k, e in self._entities.items():
            e._uuid = k
        self._upstream = None
        if 'upstreamReference' in self._serialize_dict:
            self._serialize_dict.pop('upstreamReference')
    '''

    def _print(self, *args):
        if self._quiet is False:
            print(*args)

    def __str__(self):
        s = '%s with %d entities at %s' % (self.__class__.__name__, len(self._entities), self.source)
        if self._upstream is not None:
            s += ' [upstream %s]' % self._upstream.__class__.__name__
        return s

    def _get_entity(self, key):
        """
        the fundamental method- retrieve an entity from LOCAL collection by key, nominally a UUID string.

        If the string is not found, raises KeyError.
        :param key: a uuid
        :return: the LcEntity or None
        """
        if key in self._entities:
            e = self._entities[key]
            if e.origin is None:
                e.origin = self.ref
            return e
        raise KeyError(key)

    def __getitem__(self, item):
        """
        CLient-facing entity retrieval.  item is a key that can be converted to a valid UUID from self._key_to_id()--
         either a literal UUID, or a string containing something matching a naive UUID regex.

        First checks upstream, then local.

        Returns None if nothing is found

        :param item:
        :return:
        """
        if item is None:
            return None
        if self._upstream is not None:
            e = self._upstream[item]
            if e is not None:
                return e
        try:
            if isinstance(item, int) and self._ns_uuid is not None:
                return self._get_entity(self._key_to_nsuuid(item))
            return self._get_entity(self._key_to_id(item))
        except KeyError:
            return None

    def _add(self, entity):
        u = to_uuid(entity.uuid)
        if u is None:
            raise ValueError('Key must be a valid UUID')

        if u in self._entities:
            raise KeyError('Entity already exists: %s' % u)

        if entity.validate():
            if self._quiet is False:
                print('Adding %s entity with %s: %s' % (entity.entity_type, u, entity['Name']))
            if entity.origin is None:
                assert self._key_to_id(entity.external_ref) == u, 'New entity uuid must match origin repository key!'
                entity.origin = self.ref
            self._entities[u] = entity
            self._counter[entity.entity_type] += 1
            self._ents_by_type[entity.entity_type].add(u)  # it's not ok to change an entity's type

        else:
            raise ValueError('Entity fails validation.')

    def check_counter(self, entity_type=None):
        if entity_type is None:
            [self.check_counter(entity_type=k) for k in self._entity_types]
        else:
            print('%d new %s entities added (%d total)' % (self._counter[entity_type], entity_type,
                                                           self.count_by_type(entity_type)))
            self._counter[entity_type] = 0

    def find_partial_id(self, uid, upstream=False, startswith=True):
        """
        :param uid: is a fragmentary (or complete) uuid string.
        :param upstream: [False] whether to look upstream if it exists
        :param startswith: [True] use .startswith instead of full regex
        :return: result set
        """
        if startswith:
            def test(x, y):
                return y.startswith(x)
        else:
            def test(x, y):
                return bool(re.search(x, y))
        result_set = [v for k, v in self._entities.items() if test(uid, k)]
        if upstream and self._upstream is not None:
            result_set += self._upstream.find_partial_id(uid, upstream=upstream, startswith=startswith)
        return result_set

    def _fetch(self, entity, **kwargs):
        """
        Dummy function to fetch from archive. MUST be overridden.
        Can't fetch from upstream.
        :param entity:
        :return:
        """
        raise NotImplementedError

    def retrieve_or_fetch_entity(self, key, **kwargs):
        """
        Client-facing function to retrieve entity by ID, first checking in the archive, then from the source.

        Input is flexible-- could be a UUID or key (partial uuid is just not useful)

        :param key: the identifying string (uuid or external ref)
        :param kwargs: used to pass provider-specific information
        :return:
        """

        if key is not None:
            entity = self.__getitem__(key)  # this checks upstream if it exists
            if entity is not None:
                # retrieve
                return entity

        # fetch
        return self._fetch(key, **kwargs)

    def get(self, key):
        return self.retrieve_or_fetch_entity(key)

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

            if v.origin == self.source:
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
        raise NotImplementedError

    def load_all(self, **kwargs):
        if self._loaded is False:
            print('Loading %s' % self.source)
            self._load_all(**kwargs)
            self._loaded = True

    def entities_by_type(self, entity_type):
        for u in self._ents_by_type[entity_type]:
            yield self._entities[u]

    def count_by_type(self, entity_type):
        return len(self._ents_by_type[entity_type])

    @property
    def init_args(self):
        return self._serialize_dict

    def serialize(self, **kwargs):
        j = {
            '@context': LD_CONTEXT,
            'dataSourceType': self.__class__.__name__,
            'dataSource': self.source,
            'catalogNames': self.catalog_names
        }
        j.update(self._serialize_dict)
        return j

    def _serialize_all(self, **kwargs):
        """
        To be overridden-- specify args necessary to make a complete copy
        :param kwargs:
        :return:
        """
        return self.serialize(**kwargs)

    def write_to_file(self, filename, gzip=False, complete=False, **kwargs):
        if complete:
            s = self._serialize_all(**kwargs)
        else:
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