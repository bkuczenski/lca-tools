
from .entity_store import EntityStore, SourceAlreadyKnown, EntityExists, uuid_regex
from .basic_archive import BasicArchive, BASIC_ENTITY_TYPES, InterfaceError, ArchiveError
from .archive_index import index_archive, BasicIndex, LcIndex
from .term_manager import TermManager
from .lc_archive import LcArchive, LC_ENTITY_TYPES
from ..from_json import from_json

from pathlib import Path
# import pkgutil

REF_QTYS = str(Path(__file__).parent / 'data' / 'elcd_reference_quantities.json')


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


def update_archive(archive, json_file):
    archive.load_from_dict(from_json(json_file), jsonfile=json_file)


# find antelope providers
init_map = {
    'basicarchive': BasicArchive,
    'basicindex': BasicIndex,
    'lcarchive': LcArchive,
    'lcindex': LcIndex
}

def archive_factory(ds_type):
    """
    Returns an archive class
    :param ds_type:
    :return:
    """
    dsl = ds_type.lower()
    if dsl in init_map:
        return init_map[dsl]
    raise ArchiveError('No provider found for %s' % ds_type)


def archive_from_json(fname, factory=archive_factory, catalog=None, **archive_kwargs):
    """
    :param fname: JSON filename
    :param factory: function returning a class
    :param catalog: [None] necessary to retrieve upstream archives, if specified
    :return: an ArchiveInterface
    """
    j = from_json(fname)

    if 'upstreamReference' in j or catalog is not None:
        print('**Upstream reference encountered: %s' % j['upstreamReference'])
        print('**XX Upstream is gone; catalog argument is deprecated\n')
    cls = factory(j.pop('dataSourceType', 'LcArchive'))
    return cls.from_already_open_file(j, fname, quiet=True, **archive_kwargs)


def create_archive(source, ds_type, factory=archive_factory, **kwargs):
    """
    Create an archive from a source and type specification.
    :param source:
    :param ds_type:
    :param factory: override archive factory with fancier version
    :param kwargs:
    :return:
    """
    if ds_type.lower() == 'json':
        a = archive_from_json(source, factory=factory, **kwargs)
    else:
        cls = factory(ds_type)
        a = cls(source, **kwargs)
    return a
