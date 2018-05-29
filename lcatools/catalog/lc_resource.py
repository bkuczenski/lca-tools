import json
import os
from collections import defaultdict
from datetime import datetime


from lcatools.entity_store import local_ref
from lcatools.interfaces.catalog_query import INTERFACE_TYPES
from lcatools.tools import create_archive, update_archive


new_date = datetime.now().strftime('%Y%m%d')


class LcResource(object):
    """
    This is a record that links a semantic reference to a physical data source, and specifies the capabilities
    (and someday, access limitations) of the data source.

    The LcResource serializes to a json file with the following format:
    { ref: [ { "dataSource": source, "dataSourceType": ds_type, .... }, ... ] }
    where ref is the semantic reference.

    """
    @classmethod
    def from_archive(cls, archive, interfaces, **kwargs):
        source = archive.source
        if source in archive.get_names():
            ref = archive.get_names()[source]
        else:
            ref = local_ref(source)
        ds_type = archive.__class__.__name__  # static flag indicates whether archive is complete
        kwargs.update(archive.init_args)
        static = kwargs.pop('static', archive.static)
        res = cls(ref, source, ds_type, interfaces=interfaces, static=static, preload_archive=archive, **kwargs)

        return res

    @classmethod
    def from_dict(cls, ref, d):
        """
        Returns a single LcResource loaded from a dict.  only required field is 'dataSourceType'.
        other fields are passed to the constructor and either interpreted directly or added as supplemental args

        If 'dataSource' is not present
        :param ref:
        :param d:
        :return:
        """
        source = d.pop('dataSource', None)
        ds_type = d.pop('dataSourceType')
        return cls(ref, source, ds_type, **d)

    @classmethod
    def from_json(cls, file):
        """
        generates LcResources contained in the named file, sorted by increasing priority.  The filename and
        the reference must be the same.
        :param file:
        :return: an ordered list of resources
        """
        ref = os.path.basename(file)
        with open(file, 'r') as fp:
            j = json.load(fp)

        return sorted([cls.from_dict(ref, d) for d in j[ref]], key=lambda x: x.priority)

    def _instantiate(self, catalog):
        if self.source is None:
            if 'download' in self._args:
                print('Downloading from %s' % self._args['download']['url'])
                self._source = catalog.download_file(**self._args['download'])
                self.write_to_file(catalog.resource_dir)  # update resource file
            else:
                raise AttributeError('Resource has no source specified and no download information')
        self._archive = create_archive(self.source, self.ds_type, catalog=catalog, ref=self.reference,
                                       # upstream=catalog.qdb,
                                       **self.init_args)
        if os.path.exists(catalog.cache_file(self.source)):
            update_archive(self._archive, catalog.cache_file(self.source))
        if self.static and self.ds_type.lower() != 'json':
            self._archive.load_all()  # static json archives are by convention saved in complete form
        self.apply_config()

    @property
    def is_loaded(self):
        return self._archive is not None

    def remove_archive(self):
        self._archive = None

    def check(self, catalog):
        if self._archive is None:
            # TODO: try/catch exceptions or return false
            self._instantiate(catalog)
        return True

    def make_index(self, index_file):
        self._archive.load_all()
        suffix = 'index__%s' % new_date
        # note: archive ref is updated by writing index
        self._archive.write_to_file(index_file, gzip=True, ref_suffix=suffix,
                                    exchanges=False, characterizations=False, values=False)
        return self._archive.ref

    def make_cache(self, cache_file):
        # note: do not make descendant
        self._archive.write_to_file(cache_file, complete=True, gzip=True)
        print('Created archive of %s containing:' % self._archive)
        self._archive.check_counter()

    def make_interface(self, iface):
        return self._archive.make_interface(iface)

    def apply_config(self):
        if len(self._config) > 0:
            print('Applying stored configuration')
            self._archive.make_interface('configure').apply_config(self._config)

    def add_interface(self, iface):
        if iface in INTERFACE_TYPES:
            self._interfaces.add(iface)

    def _normalize_interfaces(self, interfaces):
        """
        Ensures that:
         - interfaces spec can be string or list
         - 'basic' appears
        :param interfaces:
        :return:
        """
        self.add_interface('basic')
        if interfaces is None:
            return
        if isinstance(interfaces, str):
            self.add_interface(interfaces)
        else:
            for k in interfaces:
                self.add_interface(k)

    def __init__(self, reference, source, ds_type, interfaces=None, privacy=0, priority=50, static=False,
                 preload_archive=None, **kwargs):
        """

        :param reference: semantic reference
        :param source: physical data source; 'None' allowed if 'downloadLink' argument provided
        :param ds_type: data source type
        :param interfaces: list which can include 'entity', 'foreground', or 'background'. Default 'foreground'
        :param privacy: Ignored / No longer used.
        :param priority: [50] priority level.. numeric (nominally 0-100), lowest priority resource is loaded first
        :param static: [False] if True, load_all() after initializing
        :param preload_archive: [None] use to assign an existing archive
        :param kwargs: additional keyword arguments to constructor. Some interesting ones:
          download: a dict containing 'url' and optional 'md5sum' fields
          prefix: often used when accessing zipped archives


        """
        '''
        if not os.path.exists(source):
            raise EnvironmentError('%s not found' % source)
        '''

        self._archive = preload_archive

        self._ref = reference
        if source is None:
            if 'download' not in kwargs:
                raise KeyError('Resource must be initialized with either source or download')
        self._source = source
        self._type = ds_type
        self._static = static

        self._issaved = False

        self._interfaces = set()
        self._normalize_interfaces(interfaces)

        self._priority = int(priority)

        self._internal = kwargs.pop('_internal', False)

        self._config = defaultdict(set)

        config = kwargs.pop('config', None)
        if config:
            for k, v in config.items():
                for q in v:
                    self.add_config(k, *q)

        self._args = kwargs

    def exists(self, path):
        filename = os.path.join(path, self.reference)
        if os.path.exists(filename):
            with open(filename, 'r') as fp:
                j = json.load(fp)

            if any([self._matches(k) for k in j[self.reference]]):
                return True
        return False

    @property
    def archive(self):
        return self._archive

    @property
    def is_saved(self):
        return self._issaved

    @property
    def reference(self):
        return self._ref

    @property
    def source(self):
        return self._source

    @property
    def ds_type(self):
        return self._type

    @property
    def interfaces(self):
        for k in self._interfaces:
            yield k

    @property
    def internal(self):
        return self._internal

    @property
    def priority(self):
        return self._priority

    @property
    def static(self):
        return self._static

    @property
    def init_args(self):
        return self._args

    def satisfies(self, ifaces):
        if ifaces is None:
            return True
        if isinstance(ifaces, str):
            ifaces = [ifaces]
        for i in ifaces:
            if i == 'basic':
                return True
            if i in self._interfaces:
                return True
        return False

    def add_config(self, config, *args):
        """
        Add a configuration option
        :param config:
        :param args: the arguments, in the proper sequence
        :return:
        """
        self._config[config].add(args)

    def _serialize_config(self):
        j = dict()
        for k, v in self._config.items():
            j[k] = sorted([list(g) for g in v], key=lambda x: x[0])
        return j

    def serialize(self):
        j = {
            "dataSource": self.source,
            "dataSourceType": self.ds_type,
            "interfaces": [k for k in self.interfaces],
            "priority": self.priority,
            "static": self.static
        }
        j.update(self._args)
        if self.internal:
            j['_internal'] = True
        j['config'] = self._serialize_config()
        return j

    def _matches(self, k):
        """
        Pretty cheesy.  When we serialize a set of resources, we need to make sure not to include self twice.
        We were using dataSource as a unique identifier for resource entries; but the introduction of download links
         breaks that because a downloadable resource has no source until it's been downloaded.
         The solution is to fallback to download.url ONLY IF the resource has no source specified.
        :param k:
        :return:
        """
        if 'dataSource' in k and 'dataSource' is not None:
            return k['dataSource'] == self.source
        return k['download']['url'] == self._args['download']['url']

    def write_to_file(self, path):
        """
        Adds the resource to a file whose name is the resource's semantic reference. If the same datasource is
        already present in the file, replace it with the current resource.  otherwise append.
        :param path: directory to store the resource file.
        :return:
        """
        if not os.path.isdir(path):
            if os.path.exists(path):
                raise ValueError('Please provide a directory path')
            os.makedirs(path)

        filename = os.path.join(path, self.reference)
        if os.path.exists(filename):
            with open(filename, 'r') as fp:
                j = json.load(fp)

            resources = [k for k in j[self.reference] if not self._matches(k)]
            resources.append(self.serialize())
        else:
            resources = [self.serialize()]
        with open(os.path.join(path, self.reference), 'w') as fp:
            json.dump({self.reference: resources}, fp, indent=2)
        self._issaved = True
