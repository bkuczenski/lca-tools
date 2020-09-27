# from .archive import Archive
# from .ecospold import EcospoldArchive
# from .ilcd import IlcdArchive


# import importlib

# from lcatools.archives import LcArchive, archive_from_json

from .ilcd import IlcdArchive, IlcdLcia
from .ecospold2 import EcospoldV2Archive
from .ecoinvent_spreadsheet import EcoinventSpreadsheet
from .ecospold import EcospoldV1Archive
from .ecoinvent_lcia import EcoinventLcia
from .openlca_jsonld import OpenLcaJsonLdArchive
from .traci import Traci21Factors

PROVIDERS = ['IlcdArchive', 'IlcdLcia', 'EcospoldV2Archive', 'EcospoldV1Archive',
             'EcoinventLcia', 'OpenLcaJsonLdArchive', 'Traci21Factors']

''' # this has all been folded into archive.__init__
class ArchiveError(Exception):
    pass


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
        a = archive_from_json(source, factory=_provider_factory, catalog=catalog, **kwargs)
    else:
        cls = _provider_factory(ds_type)
        a = cls(source, **kwargs)
    return a


def _provider_factory(ds_type):
    """
    Returns an archive class
    :param ds_type:
    :return:
    """
    ds_type = ds_type.lower()
    init_map = {
        'lcarchive': LcArchive,
        'ilcdarchive': IlcdArchive,
        'ilcd': IlcdArchive,
        'ilcdlcia': IlcdLcia,
        'ilcd_lcia': IlcdLcia,
        'ecospoldv1archive': EcospoldV1Archive,
        'ecospold': EcospoldV1Archive,
        'ecospoldv2archive': EcospoldV2Archive,
        'ecospold2': EcospoldV2Archive,
        'ecoinventspreadsheet': EcoinventSpreadsheet,
        'ecoinventlcia': EcoinventLcia,
        'ecoinvent_lcia': EcoinventLcia,
        'openlcajsonldarchive': OpenLcaJsonLdArchive,
        'openlca': OpenLcaJsonLdArchive,
        'openlca_jsonld': OpenLcaJsonLdArchive,
        'olca': OpenLcaJsonLdArchive,
        'olca_jsonld': OpenLcaJsonLdArchive,
        'traci2': Traci21Factors,
        'traci': Traci21Factors,
        'traci21factors': Traci21Factors,
        'v1_client': AntelopeV1Client,
        'antelope_v1_client': AntelopeV1Client,
        'antelopev1client': AntelopeV1Client,
        'antelopev1': AntelopeV1Client
    }
    try:
        init_fcn = init_map[ds_type]
        return init_fcn
#        'foregroundarchive': ForegroundArchive.load,
#        'foreground': ForegroundArchive.load
    except KeyError:
        try:
            mod = importlib.import_module('.%s' % ds_type, package='antelope_%s' % ds_type)
        except ImportError as e:
            raise ArchiveError(e)
        return mod.init_fcn


def archive_from_json(fname, static=True, catalog=None, **archive_kwargs):
    """
    :param fname: JSON filename
    :param catalog: [None] necessary to retrieve upstream archives, if specified
    :param static: [True]
    :return: an ArchiveInterface
    """
    j = from_json(fname)
    archive_kwargs['quiet'] = True
    archive_kwargs['static'] = static

    if 'prefix' in j.keys():
        archive_kwargs['prefix'] = j['prefix']

    if 'nsUuid' in j.keys():
        archive_kwargs['ns_uuid'] = j['nsUuid']

    if j['dataSourceType'] == 'EcoinventSpreadsheet':
        archive_kwargs['internal'] = bool(j['internal'])
        archive_kwargs['version'] = j['version']

    if 'dataSourceReference' in j:
        # old style
        source = j['dataSourceReference']
    else:
        # new style
        source = j['dataSource']

    try:
        a = archive_factory(source, j['dataSourceType'], **archive_kwargs)
    except KeyError:
        raise ValueError('Unknown dataSourceType %s' % j['dataSourceType'])

    if 'upstreamReference' in j:
        print('**Upstream reference encountered: %s\n' % j['upstreamReference'])
        if catalog is not None:
            try:
                upstream = catalog.get_archive(j['upstreamReference'])
                a.set_upstream(upstream)
            except KeyError:
                print('Upstream reference not found in catalog!')
                a.init_args['upstreamReference'] = j['upstreamReference']
            except ValueError:
                print('Upstream reference is ambiguous!')
                a.init_args['upstreamReference'] = j['upstreamReference']
        else:
            a.init_args['upstreamReference'] = j['upstreamReference']

    a.load_from_dict(j, jsonfile=fname)
    return a
'''
