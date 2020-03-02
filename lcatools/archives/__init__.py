from .entity_store import EntityStore, SourceAlreadyKnown, EntityExists, uuid_regex
from .basic_archive import BasicArchive, BASIC_ENTITY_TYPES, InterfaceError, ArchiveError
from .archive_index import index_archive, BasicIndex, LcIndex
from .term_manager import TermManager
from .lc_archive import LcArchive, LC_ENTITY_TYPES
from ..from_json import from_json
from ..interfaces import UnknownOrigin

import os
import importlib

REF_QTYS = os.path.join(os.path.dirname(__file__), 'data', 'elcd_reference_quantities.json')


class Qdb(BasicArchive):
    """
    A simple archive that just contains the 25-odd reference (non-LCIA) quantities of the ELCD database circa v3.2
    """
    @classmethod
    def new(cls, ref='local.qdb'):
        """
        Create a Quantity database containing the ILCD reference quantities.  Specify a ref if desired.
        :param ref: ['local.qdb']
        """
        return cls.from_file(REF_QTYS, ref=ref)

    def _fetch(self, entity, **kwargs):
        return self.__getitem__(entity)

    def _load_all(self, **kwargs):
        self.load_from_dict(from_json(self.source))


def create_archive(source, ds_type, catalog=None, **kwargs):
    """
    Create an archive from a source and type specification.
    :param source:
    :param ds_type:
    :param catalog: required to identify upstream archives, if specified
    :param kwargs:
    :return:
    """
    if ds_type.lower() == 'json':
        a = archive_from_json(source, catalog=catalog, **kwargs)
    else:
        cls = archive_factory(ds_type)
        if ds_type.lower() == 'ecoinventlcia':
            # this is a GIANT HACK
            ei_ref = '.'.join(['local', 'ecoinvent', kwargs['version']])
            try:
                res = catalog.get_resource(ei_ref, iface='inventory', strict=False)
                res.check(catalog)
                if hasattr(res.archive, 'load_flows'):
                    res.archive.load_flows()
                ar = res.archive
            except UnknownOrigin:
                ar = None

            a = cls(source, ei_archive=ar, **kwargs)
        else:
            a = cls(source, **kwargs)
    return a


def update_archive(archive, json_file):
    archive.load_from_dict(from_json(json_file), jsonfile=json_file)


def archive_factory(ds_type):
    """
    Returns an archive class
    :param ds_type:
    :return:
    """
    dsl = ds_type.lower()
    init_map = {
        'basicarchive': BasicArchive,
        'basicindex': BasicIndex,
        'lcarchive': LcArchive,
        'lcindex': LcIndex
    }
    try:
        init_fcn = init_map[dsl]
        return init_fcn
#        'foregroundarchive': ForegroundArchive.load,
#        'foreground': ForegroundArchive.load
    except KeyError:
        try:
            mod = importlib.import_module('.providers', package='antelope_catalog')
            return getattr(mod, ds_type)
        except AttributeError:
            try:
                mod = importlib.import_module('.%s' % dsl, package='antelope_%s' % dsl)
                return mod.init_fcn
            except ImportError as e:
                raise ArchiveError(e)  # what is going on here?


def archive_from_json(fname, factory=archive_factory, catalog=None, **archive_kwargs):
    """
    :param fname: JSON filename
    :param factory: function returning a class
    :param catalog: [None] necessary to retrieve upstream archives, if specified
    :param static: [True]
    :return: an ArchiveInterface
    """
    j = from_json(fname)

    upstream = None
    if 'upstreamReference' in j:
        print('**Upstream reference encountered: %s\n' % j['upstreamReference'])
        if catalog is not None:
            try:
                upstream = catalog.get_archive(j['upstreamReference'])  # this doesn't even make sense anymore.
            except KeyError:
                print('Upstream reference not found in catalog!')
                archive_kwargs['upstreamReference'] = j['upstreamReference']
            except ValueError:
                print('Upstream reference is ambiguous!')
                archive_kwargs['upstreamReference'] = j['upstreamReference']

    cls = factory(j.pop('dataSourceType', 'LcArchive'))
    return cls.from_already_open_file(j, fname, quiet=True, upstream=upstream, **archive_kwargs)
