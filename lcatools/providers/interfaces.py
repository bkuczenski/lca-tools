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

from collections import defaultdict


LD_CONTEXT = 'https://bkuczenski.github.io/lca-tools-datafiles/context.jsonld'


# CatalogRef = namedtuple('CatalogRef', ['archive', 'id'])


uuid_regex = re.compile('([0-9a-f]{8}.?([0-9a-f]{4}.?){3}[0-9a-f]{12})')


def to_uuid(_in):
    if isinstance(_in, uuid.UUID):
        return str(_in)
    if _in is None:
        return _in
    if isinstance(_in, int):
        return None
    try:
        g = uuid_regex.search(_in)
    except TypeError:
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


class ArchiveInterface(object):
    """
    An abstract interface has nothing but a reference

    """
    def _key_to_id(self, key):
        """
        in the base class, the key is the uuid-- this can get overridden
        by default, to_uuid just returns a string matching the regex, or failing that, tries to generate a string
        using uuid.UUID(key)
        :param key:
        :return:
        """
        return to_uuid(key)

    def __init__(self, source, ref=None, quiet=True, upstream=None, static=False, dataReference=None):
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
        """

        self._source = source
        self._entities = {}  # uuid-indexed list of known entities

        self._quiet = quiet  # whether to print out a message every time a new entity is added / deleted / modified

        self._serialize_dict = dict()  # this gets added to

        self._counter = defaultdict(int)
        self._ents_by_type = defaultdict(set)
        self._upstream = None

        self._loaded = False
        self._static = static

        if upstream is not None:
            self.set_upstream(upstream)

        self.catalog_names = dict()  # this is a place to map semantic references to data sources
        if ref is None:
            if dataReference is None:
                ref = local_ref(source)
            else:
                ref = dataReference

        self._serialize_dict['dataReference'] = ref
        self.catalog_names[ref] = source

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
        assert isinstance(upstream, ArchiveInterface)
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
        the fundamental method- retrieve an entity from LOCAL collection by ID- either a uuid.UUID or a key that can
        be converted to a valid UUID from self.key_to_id()

        If the UUID is not found, returns None. handle this case in client code/subclass.
        :param key: something that maps to a literal UUID via _key_to_id
        :return: the LcEntity or None
        """
        entity = self._key_to_id(key)
        if entity in self._entities:
            e = self._entities[entity]
            if e.origin is None:
                e.origin = self.ref
            return e
        return None

    def __getitem__(self, item):
        """
        CLient-facing entity retrieval.

        First checks upstream, then local.

        :param item:
        :return:
        """
        if item is None:
            return None
        if self._upstream is not None:
            e = self._upstream[item]
            if e is not None:
                return e
        return self._get_entity(item)

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
            [self.check_counter(entity_type=k) for k in ('process', 'flow', 'quantity')]
        else:
            print('%d new %s entities added (%d total)' % (self._counter[entity_type], entity_type,
                                                           len([e for e in self.entities_by_type(entity_type)])))
            self._counter[entity_type] = 0

    @staticmethod
    def _narrow_search(entity, **kwargs):
        """
        Narrows a result set using sequential keyword filtering
        :param entity:
        :param kwargs:
        :return: bool
        """
        def _recurse_expand_subtag(tag):
            if tag is None:
                return ''
            elif isinstance(tag, str):
                return tag
            else:
                return ' '.join([_recurse_expand_subtag(t) for t in tag])
        keep = True
        for k, v in kwargs.items():
            if k not in entity.keys():
                return False
            if isinstance(v, str):
                v = [v]
            for vv in v:
                keep = keep and bool(re.search(vv, _recurse_expand_subtag(entity[k]), flags=re.IGNORECASE))
        return keep

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

    def search(self, etype=None, upstream=False, **kwargs):
        """
        Find entities by search term, either full or partial uuid or entity property like 'Name', 'CasNumber',
        or so on.
        :param etype: optional first argument is entity type
        :param upstream: (False) if upstream archive exists, search there too
        :param kwargs: regex search through entities' properties as named in the kw arguments
        :return: result set
        """
        if etype is None:
            if 'entity_type' in kwargs.keys():
                etype = kwargs.pop('entity_type')
        if etype is not None:
            for ent in self.entities_by_type(etype):
                if self._narrow_search(ent, **kwargs):
                    yield ent
        else:
            for ent in self._entities.values():
                if self._narrow_search(ent, **kwargs):
                    yield ent
        if upstream and self._upstream is not None:
            self._upstream.search(etype, upstream=upstream, **kwargs)

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
