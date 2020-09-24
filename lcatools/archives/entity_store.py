"""
A repository of typed entities, retrievable by their external reference

Entity object API:
  entity.entity_type --> string used for groupung
  entity.external_ref --> lookup name
  entity.origin --> one-time settable parameter, set by the entity store
  entity.validate() --> must return True [for valid entities and False for invalid ones]
  entity.name --> printable name

Optional:
  entity.uuid --? used for entity retrieval

"""
from __future__ import print_function, unicode_literals

import uuid
import re
import os
import six
from datetime import datetime

from collections import defaultdict

from antelope import local_ref
from ..from_json import to_json


# CatalogRef = namedtuple('CatalogRef', ['archive', 'id'])


ref_regex = re.compile('[a-z0-9_]+(\.[a-z0-9_]+)*', flags=re.IGNORECASE)


uuid_regex = re.compile('([0-9a-f]{8}-?([0-9a-f]{4}-?){3}[0-9a-f]{12})', flags=re.IGNORECASE)


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


class SourceAlreadyKnown(Exception):
    pass


class EntityExists(Exception):
    pass


class InvalidSemanticReference(Exception):
    pass


class ReferenceCreationError(Exception):
    pass


class EntityStore(object):
    _entity_types = ()  # must be overridden
    '''
    _ns_uuid_required: specifies whether the archive must be supplied an ns_uuid (generally, archives that are
    expected to generate persistent, deterministic IDs must have an externally specified ns_uuid)
     If False: random ns_uuid generated if none is supplied
     If True: ns_uuid must be supplied as an argument, will raise exception if missing
     If None: ns_uuid forced to None - store does not have ns_uuid capabilities
    '''
    _ns_uuid_required = False
    _origin = None  # can be set when a catalog is assigning a ref

    def _ref_to_uuid(self, key):
        """
        This tries to find a UUID from a ref. Not sure what this is good for.
        by default, to_uuid just returns a string matching the regex, or failing that, tries to generate a string
        using uuid.UUID(key)
        :param key:
        :return:
        """
        u = to_uuid(key)  # check if key is already a uuid
        if u is None:
            return self._ref_to_nsuuid(key)
        return u

    def _ref_to_nsuuid(self, key):
        if self._ns_uuid is None:
            return None
        if isinstance(key, int):
            key = str(key)
        if six.PY2:
            return str(uuid.uuid3(self._ns_uuid, key.encode('utf-8')))
        else:
            return str(uuid.uuid3(self._ns_uuid, key))

    def _ref_to_key(self, key):
        """
        This method always returns a valid key into _entities, or None.  May be overridden.

        :param key:
        :return:
        """
        if key in self._entities:
            return key
        uu = self._ref_to_uuid(key)
        if uu is not None:
            if uu in self._entities:
                return uu

    def get_uuid(self, key):
        """
        Deprecated.
        :param key:
        :return:
        """
        return self._ref_to_uuid(key)

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
        An EntityStore is a provenance structure for a collection of entities.  Ostensibly, an EntityStore has a single
        source from which entities are collected.  The source is a resolvable URI that indicates a data resource from
        which data describing the entities can be extracted.  The exact manner of extracting data from resources is
        subclass-dependent.

        Internally, all entities are stored with UUID keys.  If the external references do not contain UUIDs, it is
        recommended to derive a UUID3 using an archive-specific, stable namespace ID.  The class-level
        _ns_uuid_required attribute governs this option:
         - if True, an ns_uuid argument must be provided when the class is instantiated.  This is consistent with a
           use case in which it is desirable to have predictable, fixed UUIDs (i.e. to interface with a data system
           that requires stable UUIDs)

         - if False, a random ns_uuid is generated, and used to create a UUID anytime an entity is given a non-UUID
           external_ref

         - if None, UUID3 are not used and any supplied ns_uuid argument is ignored. external_refs must always be UUIDs.

        There is still some refactoring to be done, to try to eliminate the need for externally visible UUIDs anywhere.

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

        An entity is uniquely identified by its link property, which is made from concatenating the semantic origin and
        a stable reference known as an 'external_ref', as 'origin/external_ref'.  The first slash is the delimiter
        between origin and reference. Examples:

        elcd.3.2/processes/00043bd2-4563-4d73-8df8-b84b5d8902fc
        uslci.ecospold/Acetic acid, at plant

        Note that the inclusion of embedded whitespace, commas, and other characters indicate that these semantic
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
        if ref is None:
            if dataReference is None:
                ref = local_ref(source)
            else:
                ref = dataReference

        self._entities = {}  # uuid-indexed list of known entities

        self._quiet = quiet  # whether to print out a message every time a new entity is added / deleted / modified

        self._serialize_dict = kwargs  # this gets added to

        self._counter = defaultdict(int)
        self._ents_by_type = defaultdict(set)
        self._upstream = None

        self._loaded = False
        self._static = static
        self._descendant = False

        self._ns_uuid = self._set_ns_uuid(ns_uuid)

        if upstream is not None:
            self.set_upstream(upstream)

        self._catalog_names = defaultdict(set)  # this is a place to map semantic references to data sources
        self._add_name(ref, source)
        self._serialize_dict['dataReference'] = ref

        if self._ns_uuid is not None:
            self._serialize_dict['ns_uuid'] = str(self._ns_uuid)

    def _add_name(self, ref, source, rewrite=False):
        """
        A source is not allowed to provide multiple semantic references
        a ref must match the regexp ([A-Za-z0-9_]+(\.[A-Za-z0-9_])*)
        :param ref:
        :param source:
        :param rewrite: [False] if True, if SourceAlreadyKnown, re-assign the source to the new ref. This may result
        in the archive's ref changing, and should only be used when an authoritative source-ref pair is supplied
        (e.g. a JSON file that was loaded into the archive)
        :return:
        """
        if not ref_regex.match(ref):
            raise InvalidSemanticReference('%s' % ref)
        for k, s in self._catalog_names.items():
            if source in s and source is not None:
                if source == self.source and k == local_ref(self.source):
                    '''if we're trying to add our own source and ref to the name index, and the source is currently
                    registered to the default local_ref, then we override it
                    '''
                    self._catalog_names[ref] = self._catalog_names.pop(k)
                    return
                if k == ref:
                    return
                if rewrite:
                    self._catalog_names[k].remove(source)
                    print('%s: <source removed>' % k)
                else:
                    raise SourceAlreadyKnown('Source %s already registered to name %s (vs: %s)' % (source, k, ref))
        print('%s: %s' % (ref, source))
        self._catalog_names[ref].add(source)
        if ref == self.ref and self.source is None and rewrite:
            self._source = source

    def add_new_source(self, new_ref, new_source):
        self._add_name(new_ref, new_source, rewrite=False)

    @property
    def source(self):
        """
        The catalog's original source is the "master descriptor" of the catalog's content. This is required for
        subclass methods to work properly, in the event that the original source is called upon.
        :return:
        """
        return self._source

    def _set_source(self, new_ref, new_source):
        self._source = new_source
        self._add_name(new_ref, new_source)
        self._descendant = True

    def set_origin(self, origin):
        self._serialize_dict['dataReference'] = origin
        self._add_name(origin, self.source, rewrite=True)
        self._origin = origin

    @property
    def ref(self):
        if self._origin is not None:
            return self._origin
        try:
            return next(k for k, s in self._catalog_names.items() if self.source in s)
        except StopIteration:
            return local_ref(self.source)

    @property
    def catalog_names(self):
        for k in self._catalog_names.keys():
            yield k

    @property
    def names(self):
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
            names = self._upstream.names

        for k, s in self._catalog_names.items():
            for v in s:
                names[v] = k
        return names

    def get_sources(self, name):
        s = self._catalog_names[name]
        if len(s) == 0:
            for k, ss in self._catalog_names.items():
                if k.startswith(name):
                    s = s.union(ss)
        for d in s:
            yield d

    def construct_new_ref(self, signifier):
        today = datetime.now().strftime('%Y%m%d')
        if signifier is None:
            new_tail = today
        else:
            if not bool(re.match('[A-Za-z0-9_-]+', signifier)):
                raise ValueError('Invalid signifier %s' % signifier)
            new_tail = '.'.join([signifier, datetime.now().strftime('%Y%m%d')])

        if len(self.ref.split('.')) > 2:  # must be true to be postfixing a postfix
            old_tail = '.'.join(self.ref.split('.')[-2:])
            if old_tail.startswith(new_tail):
                hm = '.' + datetime.now().strftime('-%H%M')
                if old_tail.startswith(new_tail + hm):
                    hm += datetime.now().strftime('%S')
                    if old_tail.startswith(new_tail + hm):
                        raise ReferenceCreationError('HMS? %s', (self.ref, hm))
                new_tail += hm

            elif old_tail.find('.' + today) >= 0 and signifier is not None:
                # don't reprint the date if it already shows up
                new_tail = signifier

        new_ref = '.'.join([self.ref, new_tail])
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

        new_ref = self.construct_new_ref(signifier)
        if new_ref == self.ref:
            raise KeyError('Refs are the same!')  # KeyError bc it's a key in catalog_names

        new_filename = new_ref + '.json.gz'
        new_source = os.path.join(archive_path, new_filename)
        if os.path.exists(new_source):
            if force:
                print('Overwriting existing archive')
            else:
                raise EnvironmentError('File %s exists: force=True to overwrite' % new_source)

        try:
            self.load_all()
        except NotImplementedError:
            pass
        self._set_source(new_ref, new_source)
        self.write_to_file(new_source, gzip=True, complete=True)
        return new_ref

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

    def entities(self):
        for v in self._entities.values():
            yield v

    def set_upstream(self, upstream):
        assert isinstance(upstream, EntityStore)
        if upstream.source != self.source:
            self._serialize_dict['upstreamReference'] = upstream.ref
        self._upstream = upstream

    '''
    def truncate_upstream(self):
        """
        BROKEN! / deprecated
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
        count = sum(len(v) for v in self._ents_by_type.values())
        s = '%s with %d entities at %s' % (self.__class__.__name__, count, self.source)
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
            return self._entities[key]
        raise KeyError(key)

    def __contains__(self, item):
        return item in self._entities

    def __getitem__(self, item):
        """
        CLient-facing entity retrieval.  item is a key that can be converted to a valid UUID from self._ref_to_key()--
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
                return self._get_entity(self._ref_to_nsuuid(item))
            return self._get_entity(self._ref_to_key(item))
        except KeyError:
            return None

    def _ensure_valid_refs(self, entity):
        """
        Hook to validate the incoming entity's references-- namely, to set its uuid
        :param entity:
        :return:
        """
        if hasattr(entity, 'uuid') and entity.uuid is None:
            uu = self._ref_to_uuid(entity.external_ref)
            if uu is not None:
                entity.uuid = uu

    def _add(self, entity, key, quiet=False):
        self._ensure_valid_refs(entity)

        if key is None:
            raise ValueError('Key not allowed to be None')
        if key in self._entities:
            raise EntityExists('Entity already exists: %s' % key)

        if entity.entity_type not in self._entity_types:
            raise TypeError('Entity type %s not valid!' % entity.entity_type)

        if entity.is_entity:
            if not entity.validate():
                raise ValueError('Entity fails validation: %s' % repr(entity))

        if not (self._quiet or quiet):
            print('Adding %s entity with %s: %s' % (entity.entity_type, key, entity['Name']))
        if entity.origin is None:
            # TODO: uncomment / enforce this
            # assert self._ref_to_key(entity.external_ref) == key, 'entity uuid must match origin repository key!'
            entity.origin = self.ref
        self._entities[key] = entity
        if self._ns_uuid is not None:  # ensure UUID3s work even if custom UUIDs are specified
            nsuuid = self._ref_to_uuid(entity.external_ref)
            if nsuuid is not None and nsuuid not in self._entities:
                self._entities[nsuuid] = entity
        self._counter[entity.entity_type] += 1
        self._ents_by_type[entity.entity_type].add(key)  # it's not ok to change an entity's type

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
        """
        This whole thing is crufty and untested and never used and should be abandoned
        :return:
        """
        count = 0
        for k, v in self._entities.items():
            valid = True
            '''
            # 1: confirm key is a UUID
            if not isinstance(k, uuid.UUID):
                print('Key %s is not a valid UUID.' % k)
                valid = False
            '''
            if v.origin is None:
                print("%s: No origin!" % k)
                valid = False

            if v.origin == self.source:
                # 2: confirm entity's external key maps to its uuid
                if self._ref_to_uuid(v.external_ref) != v.uuid:
                    print("%s: Key doesn't match UUID in origin!" % v.external_ref)
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
        for u in sorted(self._ents_by_type[entity_type]):
            yield self._entities[u]

    def count_by_type(self, entity_type):
        return len(self._ents_by_type[entity_type])

    @property
    def init_args(self):
        return self._serialize_dict

    def serialize(self, **kwargs):
        j = {
            'dataSourceType': self.__class__.__name__,
            'dataSource': self.source,
            'catalogNames': {k: sorted(filter(None, s)) for k, s in self._catalog_names.items()},
            'initArgs': self._serialize_dict
        }
        return j

    def _serialize_all(self, **kwargs):
        """
        To be overridden-- specify args necessary to make a complete copy
        :param kwargs:
        :return:
        """
        return self.serialize(**kwargs)

    def write_to_file(self, filename, gzip=False, complete=False, **kwargs):
        """

        :param filename:
        :param gzip:
        :param complete:
        :param kwargs: whatever is required by the subclass's serialize method
        :return:
        """
        if self._source is None:
            self._set_source(self.ref, filename)  # unless there was no source to begin with
        elif filename not in self.names:
            self._add_name(self.ref, filename)
        if complete:
            s = self._serialize_all(**kwargs)
            if self._loaded:
                s['loaded'] = True
        else:
            s = self.serialize(**kwargs)
        to_json(s, filename, gzip=gzip)
