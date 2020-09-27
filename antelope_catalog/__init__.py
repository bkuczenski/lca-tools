from synonym_dict import LowerDict
from antelope import antelope_herd

import importlib

from lcatools.archives import archive_factory, ArchiveError


FOUND_PROVIDERS = LowerDict()
for ant in antelope_herd:
    p = importlib.import_module('.providers', package=ant)
    """
    try:
        p = importlib.import_module('.providers', package=ant)
    except ModuleNotFoundError:
        print('Module .providers not found: %s' % ant)
        continue
    except ImportError:
        print('failed to import providers for %s' % ant)
        continue
    """
    try:
        inits = getattr(p, 'PROVIDERS')
    except AttributeError:
        print('No PROVIDERS found in %s' % ant)
        continue
    for mod in inits:
        FOUND_PROVIDERS[mod] = p

print('Found Antelope providers:' )
for k, v in FOUND_PROVIDERS.items():
    print('%s:%s' % (v.__name__, k))


def herd_factory(ds_type):
    try:
        return archive_factory(ds_type)
    except ArchiveError:
        if ds_type in FOUND_PROVIDERS:
            prov = FOUND_PROVIDERS[ds_type]
            try:
                return getattr(prov, ds_type)
            except AttributeError:
                raise ArchiveError('ds_type %s not found in %s' % (ds_type, prov.__name__))
    print('# LENGTH OF PROVIDERS: %d' % len(FOUND_PROVIDERS))
    raise ArchiveError('Even the herd cannot save you with %s' % ds_type)


from .catalog_query import CatalogQuery, UnknownOrigin
from .catalog import LcCatalog
from .lc_resource import LcResource
from .data_sources.local import CATALOG_ROOT, make_config
