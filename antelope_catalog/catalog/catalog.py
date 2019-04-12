"""
The LcCatalog provides a semantic interface to a collection of (local and remote) read-only LcArchives, which provide
access to physical data.

It is made up of the following components:

  * built on an LciaEngine
  + local, persistent storage of resources, indexes, cache data + etc
  + A resolver, which translates semantic references into resources.  Input: semantic ref. output: CatalogInterface.
  + an interface generator, which creates archive accessors on demand based on resource information from the resolver
  x An internal cache of entities retrieved, by full reference-- this has been cut

From the catalog_ref file, the catalog should meet the following spec:
          Automatic - entity information
           catalog.query(origin) - returns a query interface
           catalog.lookup(origin, external_ref) - returns the origin of the lowest-priority resource resolving the ref
           catalog.fetch(origin, external_ref) - return a reference to the object that can be queried + handled

          LC Queries:
           see lcatools.interfaces.*

"""

import os
import re
from shutil import copy2
import requests
import hashlib
from shutil import rmtree
# from collections import defaultdict

from lcatools.archives import BasicArchive, REF_QTYS
from lcatools.lcia_engine import LciaDb, DEFAULT_CONTEXTS, DEFAULT_FLOWABLES


from lcatools.interfaces import local_ref, EntityNotFound
from ..catalog_query import CatalogQuery, INTERFACE_TYPES
from .lc_resolver import LcCatalogResolver
from ..lc_resource import LcResource
# from lcatools.flowdb.compartments import REFERENCE_INT  # reference intermediate flows
from ..data_sources.local import TEST_ROOT

from lcatools.archives import archive_from_json


class DuplicateEntries(Exception):
    pass


class CatalogError(Exception):
    pass


class LcCatalog(object):

    """
    Provides query-based access to LCI information

    A catalog is stored in the local file system and creates and stores resources relative to its root directory.
    Subfolders (all accessors return absolute paths):
    Public subfolders:
     LcCatalog.resource_dir
     LcCatalog.archive_dir

    Public filenames:
     LcCatalog.cache_file(src) returns a sha1 hash of the source filename in the [absolute] cache dir
     LcCatalog.download_file(src) returns a sha1 hash of the source filename in the [absolute] download dir

    Private folders + files:
     LcCatalog._download_dir
     LcCatalog._index_dir
     LcCatalog._index_file(src) returns a sha1 hash of the source filename in the [absolute] index dir
     LcCatalog._cache_dir
     LcCatalog._entity_cache: local entities file in root
     LcCatalog._reference_qtys: reference quantities file in root
     LcCatalog._compartments: local compartments file (outmoded in Context Refactor)


    """
    @property
    def resource_dir(self):
        return os.path.join(self._rootdir, 'resources')

    @property
    def _download_dir(self):
        return os.path.join(self._rootdir, 'downloads')

    @staticmethod
    def _source_hash_file(source):
        """
        Creates a stable filename from a source argument.  The source is the key found in the _archive dict, and
        corresponds to a single physical data source.  The filename is a sha1 hex-digest, .json.gz
        :param source:
        :return:
        """
        h = hashlib.sha1()
        h.update(source.encode('utf-8'))
        return h.hexdigest()

    @property
    def _index_dir(self):
        return os.path.join(self._rootdir, 'index')

    def _index_file(self, source):
        return os.path.join(self._index_dir, self._source_hash_file(source) + '.json.gz')

    @property
    def _cache_dir(self):
        return os.path.join(self._rootdir, 'cache')

    def cache_file(self, source):
        return os.path.join(self._cache_dir, self._source_hash_file(source) + '.json.gz')

    def download_file(self, url=None, md5sum=None, force=False, localize=True):
        """
        Download a file from a remote location into the catalog and return its local path.  Optionally validate the
        download with an MD5 digest.
        :param url:
        :param md5sum:
        :param force:
        :param localize: whether to return the filename relative to the catalog root
        :return:
        """
        local_file = os.path.join(self._download_dir, self._source_hash_file(url))
        if os.path.exists(local_file):
            if force:
                print('File exists.. re-downloading.')
            else:
                print('File already downloaded.  Force=True to re-download.')
                if localize:
                    return self._localize_source(local_file)
                return local_file

        r = requests.get(url, stream=True)
        md5check = hashlib.md5()
        with open(local_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    md5check.update(chunk)
                    # f.flush() commented by recommendation from J.F.Sebastian
        if md5sum is not None:
            assert md5check.hexdigest() == md5sum, 'MD5 checksum does not match'
        if localize:
            return self._localize_source(local_file)
        return local_file

    @property
    def archive_dir(self):
        return os.path.join(self._rootdir, 'archives')

    '''
    @property
    def _entity_cache(self):
        return os.path.join(self._rootdir, 'entity_cache.json')
    '''

    @property
    def _reference_qtys(self):
        return os.path.join(self._rootdir, 'reference-quantities.json')

    '''
    @property
    def _compartments(self):
        """
        Deprecated
        :return:
        """
        return os.path.join(self._rootdir, 'local-compartments.json')
    '''

    @property
    def _contexts(self):
        return os.path.join(self._rootdir, 'local-contexts.json')

    @property
    def _flowables(self):
        return os.path.join(self._rootdir, 'local-flowables.json')

    def _localize_source(self, source):
        if source.startswith(self._rootdir):
            return re.sub('^%s' % self._rootdir, '$CAT_ROOT', source)
        return source

    def abs_path(self, rel_path):
        if os.path.isabs(rel_path):
            return rel_path
        elif rel_path.startswith('$CAT_ROOT'):
            return re.sub('^\$CAT_ROOT', self.root, rel_path)
        return os.path.join(self.root, rel_path)

    @property
    def root(self):
        return self._rootdir

    @property
    def _dirs(self):
        for x in (self._cache_dir, self._index_dir, self.resource_dir, self.archive_dir, self._download_dir):
            yield x

    def _make_rootdir(self):
        for x in self._dirs:
            os.makedirs(x, exist_ok=True)
        if not os.path.exists(self._contexts):
            copy2(DEFAULT_CONTEXTS, self._contexts)
        if not os.path.exists(self._flowables):
            copy2(DEFAULT_FLOWABLES, self._flowables)
        if not os.path.exists(self._reference_qtys):
            copy2(REF_QTYS, self._reference_qtys)

    @classmethod
    def make_tester(cls):
        rmtree(TEST_ROOT, ignore_errors=True)
        return cls(TEST_ROOT)

    @classmethod
    def load_tester(cls):
        return cls(TEST_ROOT)

    def __init__(self, rootdir, **kwargs):
        """
        Instantiates a catalog based on the resources provided in resource_dir
        :param rootdir: directory storing LcResource files.
        :param kwargs: passed to Qdb
        """
        self._rootdir = os.path.abspath(rootdir)
        self._make_rootdir()  # this will be a git clone / fork;; clones reference quantities
        self._resolver = LcCatalogResolver(self.resource_dir)

        """
        _archives := source -> archive
        _names :=  ref:interface -> source
        _nicknames := nickname -> source
        """
        self._nicknames = dict()  # keep a collection of shorthands for sources

        self._queries = dict()  # keep a collection of CatalogQuery instances for each origin

        '''
        LCIA: 
        '''
        qdb = LciaDb.new(source=self._reference_qtys, contexts=self._contexts, flowables=self._flowables, **kwargs)
        self._qdb = qdb
        self.add_existing_archive(qdb, interfaces=('index', 'quantity'), store=False)

    @property
    def lcia_engine(self):
        return self._qdb.tm

    def register_quantity_ref(self, q_ref):
        print('registering %s' % q_ref.link)
        self._qdb.add(q_ref)

    @property
    def sources(self):
        for k in self._resolver.sources:
            yield k

    @property
    def references(self):
        for ref, ints in self._resolver.references:
            yield ref

    @property
    def interfaces(self):
        for ref, ints in self._resolver.references:
            for i in ints:
                yield ':'.join([ref, i])

    def show_interfaces(self):
        for ref, ints in sorted(self._resolver.references):
            print('%s [%s]' % (ref, ', '.join(ints)))

    '''
    Nicknames
    '''
    @property
    def names(self):
        """
        List known references.
        :return:
        """
        for k, ifaces in self._resolver.references:
            for iface in ifaces:
                yield ':'.join([k, iface])
        for k in self._nicknames.keys():
            yield k

    def add_nickname(self, source, nickname):
        """
        quickly refer to a specific data source already present in the archive
        :param source:
        :param nickname:
        :return:
        """
        if self._resolver.known_source(source):
            self._nicknames[nickname] = source
        else:
            raise KeyError('Source %s not found' % source)

    '''
    Create + Add data resources
    '''

    def new_resource(self, *args, store=True, **kwargs):
        """
        Create a new data resource by specifying its properties directly to the constructor
        :param args: reference, source, ds_type
        :param store: [True] permanently store this resource
        :param kwargs: interfaces=None, priority=0, static=False; **kwargs passed to archive constructor
        :return:
        """
        return self._resolver.new_resource(*args, store=store, **kwargs)  # explicit store= for doc purposes

    def has_resource(self, res):
        return self._resolver.has_resource(res)

    def add_resource(self, resource, store=True):
        """
        Add an existing LcResource to the catalog.
        :param resource:
        :param store: [True] permanently store this resource
        :return:
        """
        self._resolver.add_resource(resource, store=store)
        # self._ensure_resource(resource)

    def delete_resource(self, resource, delete_source=None, delete_cache=True):
        """
        Removes the resource from the resolver and also removes the serialization of the resource. Also deletes the
        resource's source under the following circumstances:
         (resource is internal AND resources_with_source(resource.source) is empty AND resource.source is a file)
        This can be overridden using he delete_source param (see below)
        :param resource: an LcResource
        :param delete_source: [None] If None, follow default behavior. If True, delete the source even if it is
         not internal (source will not be deleted if other resources refer to it OR if it is not a file). If False,
         do not delete the source.
        :param delete_cache: [True] whether to delete cache files (you could keep them around if you expect to need
         them again and you don't think the contents will have changed)
        :return:
        """
        self._resolver.delete_resource(resource)
        abs_src = self.abs_path(resource.source)

        if delete_source is False or resource.source is None or not os.path.isfile(abs_src):
            return
        if len([t for t in self._resolver.resources_with_source(resource.source)]) > 0:
            return
        if resource.internal or delete_source:
            if os.path.isdir(abs_src):
                rmtree(abs_src)
            else:
                os.remove(abs_src)
        if delete_cache:
            if os.path.exists(self.cache_file(resource.source)):
                os.remove(self.cache_file(resource.source))
            if os.path.exists(self.cache_file(abs_src)):
                os.remove(self.cache_file(abs_src))

    def add_existing_archive(self, archive, interfaces=None, store=True, **kwargs):
        """
        Makes a resource record out of an existing archive.  by default, saves it in the catalog's resource dir
        :param archive:
        :param interfaces:
        :param store: [True] if False, don't save the record - use it for this session only
        :param kwargs:
        :return:
        """
        res = LcResource.from_archive(archive, interfaces, source=self._localize_source(archive.source), **kwargs)
        self._resolver.add_resource(res, store=store)

    '''
    Retrieve resources
    '''
    def _find_single_source(self, origin, interface, source=None):
        r = self._resolver.get_resource(ref=origin, iface=interface, source=source, include_internal=False)
        r.check(self)
        return r.source

    def get_resource(self, name, iface=None, source=None, strict=True):
        """
        retrieve a resource by providing enough information to identify it uniquely.  If strict is True (default),
        then parameters are matched exactly and more than one match raises an exception. If strict is False, then
        origins are matched approximately and the first (lowest-priority) match is returned.

        :param name: nickname or origin
        :param iface:
        :param source:
        :param strict:
        :return:
        """
        if name in self._nicknames:
            return self._resolver.get_resource(source=self._nicknames[name], strict=strict)
        return self._resolver.get_resource(ref=name, iface=iface, source=source, strict=strict)

    def get_archive(self, ref, interface=None, strict=False):
        if interface in INTERFACE_TYPES:
            rc = self.get_resource(ref, iface=interface, strict=strict)
        else:
            rc = self.get_resource(ref, strict=strict)
        rc.check(self)
        return rc.archive

    '''
    Manage resources locally
     - index
     - cache
     - static archive (performs load_all())
    '''

    def _index_source(self, source, priority, force=False):
        """
        Instructs the resource to create an index of itself in the specified file; creates a new resource for the
        index
        :param source:
        :param priority:
        :param force:
        :return:
        """
        res = next(r for r in self._resolver.resources_with_source(source))
        res.check(self)
        priority = min([priority, res.priority])
        stored = self._resolver.is_permanent(res)

        inx_file = self._index_file(source)
        inx_local = self._localize_source(inx_file)
        if os.path.exists(inx_file):
            if not force:
                print('Not overwriting existing index. force=True to override.')
                try:
                    ex_res = next(r for r in self._resolver.resources_with_source(inx_local))
                    return ex_res.reference
                except StopIteration:
                    # index file exists, but no matching resource
                    inx = archive_from_json(inx_file)
                    self.new_resource(inx.ref, inx_local, 'json', priority=priority, store=stored,
                                      interfaces='index', _internal=True, static=True, preload_archive=inx)
                    return inx.ref

            print('Re-indexing %s' % source)
        the_index = res.make_index(inx_file, force=force)
        self.new_resource(the_index.ref, inx_local, 'json', priority=priority, store=stored, interfaces='index',
                          _internal=True, static=True, preload_archive=the_index)
        return the_index.ref

    def index_ref(self, origin, interface=None, source=None, priority=60, force=False):
        """
        Creates an index for the specified resource.  'origin' and 'interface' must resolve to one or more LcResources
        that all have the same source specification.  That source archive gets indexed, and index resources are created
        for all the LcResources that were returned.

        Performs load_all() on the source archive, writes the archive to a compressed json file in the local index
        directory, and creates a new LcResource pointing to the JSON file.   Aborts if the index file already exists
        (override with force=True).
        :param origin:
        :param interface: [None]
        :param source: find_single_source input
        :param priority: [60] priority setting for the new index
        :param force: [False] if True, overwrite existing index
        :return:
        """
        source = self._find_single_source(origin, interface, source=source)
        return self._index_source(source, priority, force=force)

    def create_source_cache(self, source, static=False):
        """
        Creates a cache of the named source's current contents, to speed up access to commonly used entities.
        source must be either a key present in self.sources, or a name or nickname found in self.names
        :param source:
        :param static: [False] create archives of a static archive (use to force archival of a complete database)
        :return:
        """
        res = next(r for r in self._resolver.resources_with_source(source))
        if res.static:
            if not static:
                print('Not archiving static resource %s' % res)
                return
            print('Archiving static resource %s' % res)
        res.check(self)
        res.make_cache(self.cache_file(self._localize_source(source)))

    def _background_for_origin(self, ref):
        inx_ref = self.index_ref(ref, interface='inventory')
        bk_file = self._localize_source(os.path.join(self.archive_dir, '%s_background.mat' % inx_ref))
        bk = LcResource(inx_ref, bk_file, 'Background', interfaces='background', priority=99,
                        save_after=True, _internal=True)
        bk.check(self)  # ImportError if antelope_background pkg not found
        self.add_resource(bk)
        return bk.make_interface('background')  # when the interface is returned, it will trigger setup_bm

    '''# deprecated-- background stores itself now
    def create_static_archive(self, archive_file, origin, interface=None, source=None, background=True, priority=90):
        """
        Creates a local replica of a static archive, usually for purposes of improving load time in computing
        background results.  Uses create_source_cache to generate a cache and then renames it to the target
        archive file (which should end in .json.gz) in the catalog's archive directory. Creates resources mapping
        the newly created archive file to the origin with the specified priority.

        The source file can be specified in one of two ways:
         * origin + interface (with interface defaulting to None), as long as it unambiguously identifies one source
         * origin + explicit source, which must correspond

        :param archive_file:
        :param origin:
        :param interface:
        :param source:
        :param background: [True] whether to include the background interface on the created resource
        :param priority: priority of the created resource
        :return:
        """
        source = self._find_single_source(origin, interface, source=source)
        res = next(self._resolver.resources_with_source(source))
        res.check(self)
        res.archive.load_all()

        if not os.path.isabs(archive_file):
            archive_file = os.path.join(self.archive_dir, archive_file)
        self.create_source_cache(source, static=True)
        os.rename(self.cache_file(source), archive_file)
        for res in self._resolver.resources_with_source(source):
            ifaces = set(i for i in res.interfaces)
            ifaces.add('index')  # load_all will satisfy index requirement
            if background:
                ifaces.add('background')
            store = self._resolver.is_permanent(res)
            self.new_resource(res.reference, archive_file, 'JSON', interfaces=ifaces, priority=priority,
                              store=store,
                              _internal=True,
                              static=True)
    '''

    def create_descendant(self, origin, interface=None, source=None, force=False, signifier=None, strict=True,
                          priority=None, **kwargs):
        """

        :param origin:
        :param interface:
        :param source:
        :param force: overwrite if exists
        :param signifier: semantic descriptor for the new descendant (optional)
        :param strict:
        :param priority:
        :param kwargs:
        :return:
        """
        res = self.get_resource(origin, iface=interface, source=source, strict=strict)
        new_ref = res.archive.create_descendant(self.archive_dir, signifier=signifier, force=force)
        print('Created archive with reference %s' % new_ref)
        ar = res.archive
        prio = priority or res.priority
        self.add_existing_archive(ar, interfaces=res.interfaces, priority=prio, **kwargs)
        res.remove_archive()

    '''
    Main data accessor
    '''
    def _sorted_resources(self, origin, interfaces, strict):
        for res in sorted(self._resolver.resolve(origin, interfaces, strict=strict),
                          key=lambda x: (not (x.is_loaded and x.static), x.priority, x.reference != origin)):
            yield res

    def gen_interfaces(self, origin, itype=None, strict=False):
        """
        Generator of interfaces by spec

        :param origin:
        :param itype: single interface or iterable of interfaces
        :param strict: passed to resolver
        :return:
        """
        if itype is None:
            itype = 'basic'  # fetch, get properties, uuid, reference

        # if itype == 'quantity':
        #    yield self._qdb.make_interface(itype)

        for res in self._sorted_resources(origin, itype, strict):
            res.check(self)
            yield res.make_interface(itype)

        if itype == 'background':
            if origin.startswith('local') or origin.startswith('test'):
                yield self._background_for_origin(origin)

        '''
        # no need for this because qdb is (a) listed in the resolver and (b) upstream of everything
        if 'quantity' in itype:
            yield self._qdb  # fallback to our own quantity db for Quantity Interface requests
            '''

    """
    public functions -- should these operate directly on a catalog ref instead? I think so but let's see about usage
    """
    def query(self, origin, strict=False, refresh=False, **kwargs):
        """
        Returns a query using the first interface to match the origin.
        :param origin:
        :param strict: [False] whether the resolver should match the origin exactly, as opposed to returning more highly
         specified matches.  e.g. with strict=False, a request for 'local.traci' could be satisfied by 'local.traci.2.1'
         whereas if strict=True, only a resource matching 'local.traci' exactly will be returned
        :param refresh: [False] by default, the catalog stores a CatalogQuery instance for every requested origin.  With
         refresh=True, any prior instance will be replaced with a fresh one.
        :param kwargs:
        :return:
        """

        next(self._resolver.resolve(origin, strict=strict))  # raises UnknownOrigin
        if refresh or (origin not in self._queries):
            self._queries[origin] = CatalogQuery(origin, catalog=self, **kwargs)
        return self._queries[origin]

    def lookup(self, origin, external_ref):
        """
        Attempts to secure an entity
        :param origin:
        :param external_ref:
        :return: The origin of the lowest-priority resource to match the query
        """
        for i in self.gen_interfaces(origin):
            if i.lookup(external_ref):
                return i.origin
        raise EntityNotFound('%s/%s' % (origin, external_ref))

    def fetch(self, origin, external_ref):
        org = self.lookup(origin, external_ref)
        return self.query(org).get(external_ref)

    def fetch_link(self, link):
        org, ext = link.split('/', maxsplit=1)
        return self.fetch(org, ext)

    def create_foreground(self, path, ref=None, quiet=True):
        """
        Creates or activates a foreground as a sub-folder within the catalog's root directory.  Returns a
        Foreground interface.
        :param path: either an absolute path or a directory name (not an arbitrary relative path) to put the foreground
        :param ref: semantic reference (optional)
        :param quiet: passed to fg archive
        :return:
        """
        if not os.path.isabs(path):
            if path.find(os.path.sep) != -1:
                raise ValueError('Relative path not allowed; use directory name only')
            path = os.path.join(self._rootdir, path)
        local_path = self._localize_source(path)

        if ref is None:
            ref = local_ref(path)

        try:
            res = next(self._resolver.resources_with_source(local_path))
        except StopIteration:
            res = self.new_resource(ref, local_path, 'LcForeground', interfaces=['index', 'foreground'], quiet=quiet)

        res.check(self)
        return res.make_interface('foreground')

    def configure_resource(self, reference, config, *args):
        """
        We must propagate configurations to internal, derived resources. This also begs for testing.
        :param reference:
        :param config:
        :param args:
        :return:
        """
        # TODO: testing??
        for res in self._resolver.resolve(reference, strict=False):
            abs_src = self.abs_path(res.source)
            if res.add_config(config, *args):
                if res.internal:
                    if os.path.dirname(abs_src) == self._index_dir:
                        print('Saving updated index %s' % abs_src)
                        res.archive.write_to_file(abs_src, gzip=True,
                                                  exchanges=False, characterizations=False, values=False)
                else:
                    print('Saving resource configuration for %s' % res.reference)
                    res.save(self)

            else:
                if res.internal:
                    print('Deleting unconfigurable internal resource for %s\nsource: %s' % (res.reference, abs_src))
                    self.delete_resource(res, delete_source=True)
                else:
                    print('Unable to apply configuration to resource for %s\nsource: %s' % (res.reference, res.source))
