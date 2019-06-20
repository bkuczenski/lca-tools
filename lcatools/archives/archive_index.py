import os
import re
from collections import defaultdict

from .basic_archive import BasicArchive, BASIC_ENTITY_TYPES, InterfaceError, ArchiveError
from .lc_archive import LcArchive, LC_ENTITY_TYPES
from ..implementations import BasicImplementation, IndexImplementation


class AbstractIndex(object):
    def make_interface(self, iface):
        if iface == 'index':
            return IndexImplementation(self)
        elif iface == 'basic':
            return BasicImplementation(self)
        else:
            raise InterfaceError('Unable to create interface %s' % iface)


class BasicIndex(AbstractIndex, BasicArchive):
    def serialize(self, characterizations=False, values=False, domesticate=False):
        return super(BasicIndex, self).serialize(characterizations=False, values=False, domesticate=False)


class LcIndex(AbstractIndex, LcArchive):
    def serialize(self, exchanges=False, characterizations=False, values=False, domesticate=False):
        return super(LcIndex, self).serialize(exchanges=False, characterizations=False, values=False, domesticate=False)


def index_archive(archive, source, ref=None, signifier='index', force=False):
    if source is None:
        raise AssertionError('Source is required')
    if not bool(re.search('\.gz$', source)):
        source += '.gz'
    if source == archive.source:
        raise ArchiveError('Index must have a different source from original archive')
    if os.path.exists(source):
        if force:
            print('File exists: %s. Overwriting..' % source)
        else:
            raise FileExistsError(source)

    if ref is None or ref == archive.ref:
        ref = archive.construct_new_ref(signifier=signifier)

    if isinstance(archive, LcArchive):
        index = LcIndex(source, ref=ref, static=True, **archive.init_args)
        types = LC_ENTITY_TYPES
    else:
        index = BasicIndex(source, ref=ref, static=True, **archive.init_args)
        types = BASIC_ENTITY_TYPES
    for t in types:
        for e in archive.entities_by_type(t):
            index.add(e)

    # import original archives list of names
    names = defaultdict(list)
    for s, r in archive.names.items():
        names[r].append(s)

    index.load_from_dict({'catalogNames': names})

    index.write_to_file(source, gzip=True)
    return index
