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
from shutil import copy2
import requests
import hashlib
# from collections import defaultdict

from lcatools.qdb import LciaEngine

from lcatools.interfaces.catalog_query import CatalogQuery, EntityNotFound, INTERFACE_TYPES
from .lc_resolver import LcCatalogResolver
from .lc_resource import LcResource
from lcatools.qdb import REF_QTYS
from lcatools.flowdb.compartments import REFERENCE_INT  # reference intermediate flows
from lcatools.entity_store import local_ref


class DuplicateEntries(Exception):
    pass


class LcCatalog(LciaEngine):
    """
    Provides REST-style access to LCI information (exclusive of the flow-quantity relation)

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

    def download_file(self, url=None, md5sum=None, force=False):
        """
        Download a file from a remote location into the catalog and return its local path.  Optionally validate the
        download with an MD5 digest.
        :param url:
        :param md5sum:
        :param force:
        :return:
        """
        local_file = os.path.join(self._download_dir, self._source_hash_file(url))
        if os.path.exists(local_file):
            if force:
                print('File exists.. re-downloading.')
            else:
                print('File already downloaded.  Force=True to re-download.')
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
        return local_file

    @property
    def _archive_dir(self):
        return os.path.join(self._rootdir, 'archives')

    @property
    def _entity_cache(self):
        return os.path.join(self._rootdir, 'entity_cache.json')

    @property
    def _reference_qtys(self):
        return os.path.join(self._rootdir, 'reference-quantities.json')

    @property
    def _compartments(self):
        return os.path.join(self._rootdir, 'local-compartments.json')

    @property
    def root(self):
        return self._rootdir

    def _make_rootdir(self):
        for x in (self._cache_dir, self._index_dir, self.resource_dir, self._archive_dir, self._download_dir):
            os.makedirs(x, exist_ok=True)
        if not os.path.exists(self._compartments):
            copy2(REFERENCE_INT, self._compartments)
        if not os.path.exists(self._reference_qtys):
            copy2(REF_QTYS, self._reference_qtys)

    def __init__(self, rootdir, **kwargs):
        """
        Instantiates a catalog based on the resources provided in resource_dir
        :param rootdir: directory storing LcResource files.
        :param kwargs: passed to Qdb
        """
        self._rootdir = rootdir
        self._make_rootdir()  # this will be a git clone / fork
        self._resolver = LcCatalogResolver(self.resource_dir)
        super(LcCatalog, self).__init__(source=self._reference_qtys, compartments=self._compartments, **kwargs)
        """
        _archives := source -> archive
        _names :=  ref:interface -> source
        _nicknames := nickname -> source
        """
        self._nicknames = dict()  # keep a collection of shorthands for sources

        self.add_existing_archive(self._qdb, interfaces=('index', 'quantity'), store=False)

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
        for ref, ints in self._resolver.references:
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

    def add_resource(self, resource, store=True):
        """
        Add an existing LcResource to the catalog.
        :param resource:
        :param store: [True] permanently store this resource
        :return:
        """
        self._resolver.add_resource(resource, store=store)
        # self._ensure_resource(resource)

    def add_existing_archive(self, archive, interfaces=None, store=True, **kwargs):
        """
        Makes a resource record out of an existing archive.  by default, saves it in the catalog's resource dir
        :param archive:
        :param interfaces:
        :param store: [True] if False, don't save the record - use it for this session only
        :param kwargs:
        :return:
        """
        res = LcResource.from_archive(archive, interfaces, **kwargs)
        self._resolver.add_resource(res, store=store)

    '''
    Retrieve resources
    '''
    def _find_single_source(self, origin, interface, source=None):
        r = self._resolver.get_resource(ref=origin, iface=interface, source=source)
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

        inx_file = self._index_file(source)
        if os.path.exists(inx_file):
            if not force:
                print('Not overwriting existing index. force=True to override.')
                self._register_index(source, res.reference, priority, store=self._resolver.is_permanent(res))
                return
            print('Re-indexing %s' % source)
        new_ref = res.make_index(inx_file)
        self._register_index(source, new_ref, priority, store=self._resolver.is_permanent(res), archive=res.archive)

    def _register_index(self, source, new_ref, priority, store=True, archive=None):
        """
        creates a resource entry for an index file, if it exists
        :param source:
        :param new_ref: from index
        :param priority:
        :param store: [True]
        :param archive: [None] if present, pre-load the resource with the specified archive
        :return:
        """
        inx_file = self._index_file(source)
        if os.path.exists(inx_file):
            print('Registering index %s for %s' % (inx_file, source))
            self.new_resource(new_ref, inx_file, 'json', interfaces='index', priority=priority,
                              store=store,
                              _internal=True,
                              static=True,
                              preload_archive=archive)

    def index_resource(self, origin, interface=None, source=None, priority=10, force=False):
        """
        Creates an index for the identified resource.  'origin' and 'interface' must resolve to one or more LcResources
        that all have the same source specification.  That source archive gets indexed, and index resources are created
        for all the LcResources that were returned.

        Performs load_all() on the source archive, writes the archive to a compressed json file in the local index
        directory, and creates a new LcResource pointing to the JSON file.   Aborts if the index file already exists
        (override with force=True).
        :param origin:
        :param interface: [None]
        :param source: find_single_source input
        :param priority: [10] priority setting for the new index
        :param force: [False] if True, create an index even if an interface already exists (will overwrite existing)
        :return:
        """
        source = self._find_single_source(origin, interface, source=source)
        self._index_source(source, priority, force=force)

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
        res.make_cache(self.cache_file(source))

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
        self._index_source(source, 10, force=True)
        if not os.path.isabs(archive_file):
            archive_file = os.path.join(self._archive_dir, archive_file)
        self.create_source_cache(source, static=True)
        os.rename(self.cache_file(source), archive_file)
        for res in self._resolver.resources_with_source(source):
            ifaces = set(i for i in res.interfaces)
            if background:
                ifaces.add('background')
            store = self._resolver.is_permanent(res)
            self.new_resource(res.reference, archive_file, 'JSON', interfaces=ifaces, priority=priority,
                              store=store,
                              _internal=True,
                              static=True)

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
        new_ref = res.archive.create_descendant(self._archive_dir, signifier=signifier, force=force)
        print('Created archive with reference %s' % new_ref)
        ar = res.archive
        prio = priority or res.priority
        self.add_existing_archive(ar, interfaces=res.interfaces, priority=prio, **kwargs)
        res.remove_archive()

    '''
    Main data accessor
    '''

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
        for res in sorted(self._resolver.resolve(origin, interfaces=itype, strict=strict),
                          key=lambda x: (not x.is_loaded, x.reference != origin, x.priority)):
            res.check(self)
            yield res.make_interface(itype)
        '''
        # no need for this because qdb is (a) listed in the resolver and (b) upstream of everything
        if 'quantity' in itype:
            yield self._qdb  # fallback to our own quantity db for Quantity Interface requests
            '''

    """
    public functions -- should these operate directly on a catalog ref instead? I think so but let's see about usage
    """
    def query(self, origin, strict=False, **kwargs):
        """
        Returns a query using the first interface to match the origin.
        :param origin:
        :param strict:
        :param kwargs:
        :return:
        """
        next(self._resolver.resolve(origin, strict=strict))  # raises UnknownOrigin
        return CatalogQuery(origin, catalog=self, **kwargs)

    def lookup(self, origin, external_ref):
        """
        Attempts to secure an entity
        :param origin:
        :param external_ref:
        :return: The origin of the lowest-priority resource to match the query
        """
        for i in self.gen_interfaces(origin, None):
            if i.lookup(external_ref):
                return i.origin
        raise EntityNotFound('%s/%s' % (origin, external_ref))

    def fetch(self, origin, external_ref):
        org = self.lookup(origin, external_ref)
        return self.query(org).get(external_ref)

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

        if ref is None:
            ref = local_ref(path)

        try:
            res = next(self._resolver.resources_with_source(path))
        except StopIteration:
            res = self.new_resource(ref, path, 'LcForeground', interfaces=['index', 'foreground'], quiet=quiet)

        res.check(self)
        return res.make_interface('foreground')
