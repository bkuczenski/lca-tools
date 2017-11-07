from lcatools.providers.interfaces import ArchiveInterface
from lcatools.interfaces import IndexInterface, InventoryInterface, QuantityInterface
from lcatools.entity_refs.catalog_ref import CatalogRef
import json
from collections import defaultdict

try:
    from urllib.request import urlopen, urljoin
    from urllib.parse import urlparse
except ImportError:
    from urllib2 import urlopen
    from urlparse import urlparse, urljoin


class AntelopeV1Error(Exception):
    pass


def remote_ref(url):
    p = urlparse(url)
    ref = p.netloc
    for k in p.path.split('/'):
        if len(k) > 0:
            ref = '.'.join([ref, k])
    return ref


class AntelopeV1Client(ArchiveInterface, IndexInterface, InventoryInterface, QuantityInterface):
    """
    Provider class for .NET-era Antelope servers.  The basic function is to rely on the interface whenever possible
    but also to cache inventory and LCIA results locally as catalog refs.
    """
    def __init__(self, source, ref=None, catalog=None, **kwargs):
        """

        :param source:
        :param catalog:
        :param kwargs:
        """
        if ref is None:
            ref = remote_ref(source)

        super(AntelopeV1Client, self).__init__(source, ref=ref, **kwargs)

        if catalog is None:
            raise AntelopeV1Error('A Catalog is required for AntelopeV1Client')
        self._query = catalog.query(self.ref)

        # a set of dicts where the key is a string-formatted integer and the value is an entity_ref
        self._endpoints = dict()
        self._cached = defaultdict(dict)

        self._fetched_all = {
            'process': False,
            'flow': False,
            'flowproperty': False,
            'lciamethod': False,
            'fragment': False
        }

    def entities_by_type(self, entity_type):
        if entity_type == 'process':
            endp = 'processes'
        elif entity_type == 'flow':
            endp = 'flows'
        elif entity_type == 'flowproperty':
            endp = 'flowproperties'
        elif entity_type == 'lciamethod':
            endp = 'lciamethods'
        elif entity_type == 'fragment':
            endp = 'fragments'
        else:
            raise ValueError('Unknown entity type %s' % entity_type)

        if self._fetched_all[entity_type]:
            for k, v in sorted(self._cached[endp].items()):
                if entity_type == 'flowproperty':
                    if v.is_lcia_method():
                        continue
                elif entity_type == 'lciamethod':
                    if not v.is_lcia_method():
                        continue
                yield v
        else:
            for j in self._get_endpoint(endp, cache=False):
                yield self._parse_and_save_entity(j)
            self._fetched_all[entity_type] = True

    def _load_all(self, **kwargs):
        raise AntelopeV1Error('Cannot load all entities from remote Antelope server')

    def _get_endpoint(self, endpoint, cache=True):
        if endpoint in self._endpoints:
            return self._endpoints[endpoint]
        with urlopen(urljoin(self.source, endpoint)) as response:
            j = json.loads(response.read())
        if cache:
            self._endpoints[endpoint] = j
        return j

    def _fetch(self, entity, **kwargs):
        self._print('Fetching %s from remote server' % entity)
        j = self._get_endpoint(entity)
        return self._parse_and_save_entity(j)

    def _get_comment(self, processId):
        target = '/'.join(['processes', processId, 'comment'])
        j = self._get_endpoint(target)
        return j['comment']

    def _get_impact_category(self, cat_id):
        cats = self._get_endpoint('impactcategories')
        try:
            return next(j['name'] for j in cats if j['impactCategoryID'] == int(cat_id))
        except StopIteration:
            raise ValueError('Unknown impact category ID %d' % int(cat_id))

    '''
    Entity handling
    '''
    def retrieve_or_fetch_entity(self, key, **kwargs):
        """
        A key in v1 antelope is 'datatype/index' where index is a sequential number starting from 1.  The archive also
        caches references locally by their uuid, so retrieving an entity by uuid will work if the client has already
        come across it.
        :param key:
        :param kwargs:
        :return:
        """
        ent = None

        if key in self._entities:
            ent = self._entities[key]

        parts = key.split('/')
        if parts[1] in self._cached[parts[0]]:
            ent = self._cached[parts[0]][parts[1]]
        if ent is not None:
            return ent
        else:
            return self._fetch(key, **kwargs)

    def _parse_and_save_entity(self, j):
        """
        Take a json object obtained from a query and use it to create a
        :param j:
        :return:
        """
        func = {'process': self._parse_and_save_process,
                'flow': self._parse_and_save_flow,
                'lciamethod': self._parse_and_save_lcia,
                'flowproperty': self._parse_and_save_fp,
                'fragment': self._parse_and_save_fragment}[j.pop('resourceType').lower()]
        j.pop('links')
        return func(j)

    def _parse_and_save_process(self, j):
        process_id = str(j.pop('processID'))
        ext_ref = 'processes/%s' % process_id
        j['Name'] = j.pop('name')
        j['SpatialScope'] = j.pop('geography')
        j['TemporalScope'] = j.pop('referenceYear')
        j['Comment'] = self._get_comment(process_id)
        ref = CatalogRef.from_query(ext_ref, self._query, 'process', [], **j)
        self._cached['processes'][process_id] = ref
        return ref

    def _parse_and_save_flow(self, j):
        flow_id = str(j.pop('flowID'))
        ext_ref = 'flows/%s' % flow_id
        j['Name'] = j.pop('name')
        j['CasNumber'] = j.pop('casNumber', '')
        j['Compartment'] = [j.pop('category')]
        ref_qty = self.retrieve_or_fetch_entity('flowproperties/%s' % j.pop('referenceFlowPropertyID'))
        ref = CatalogRef.from_query(ext_ref, self._query, 'flow', ref_qty, **j)
        self._cached['flows'][flow_id] = ref
        return ref

    def _parse_and_save_lcia(self, j):
        lm_id = str(j.pop('lciaMethodID'))
        ext_ref = 'lciamethods/%s' % lm_id
        j['Name'] = j.pop('name')
        j['Method'] = j.pop('methodology')
        rfp = j.pop('referenceFlowProperty')
        ref_unit = rfp['referenceUnit']
        j.pop('referenceFlowPropertyID')
        j['Indicator'] = rfp['name']
        j['Category'] = self._get_impact_category(j.pop('impactCategoryID'))
        ref = CatalogRef.from_query(ext_ref, self._query, 'quantity', ref_unit, **j)
        self._cached['lciamethods'][lm_id] = ref
        return ref

    def _parse_and_save_fp(self, j):
        fp_id = str(j.pop('flowPropertyID'))
        ext_ref = 'flowproperties/%s' % fp_id
        j['Name'] = j.pop('name')
        ref_unit = j.pop('referenceUnit')
        ref = CatalogRef.from_query(ext_ref, self._query, 'quantity', ref_unit, **j)
        self._cached['flowproperties'][fp_id] = ref
        return ref

    def _parse_and_save_fragment(self, j):
        frag_id = str(j.pop('fragmentID'))
        ext_ref = 'fragments/%s' % frag_id
        j['Name'] = j.pop('name')
        dir = j.pop('direction')
        flow = self.retrieve_or_fetch_entity('flows/%s' % j.pop('termFlowID'))
        ref = CatalogRef.from_query(ext_ref, self._query, 'fragment', None, **j)
        ref.set_config(flow, dir)
        self._cached['fragments'][frag_id] = ref
        return ref

    '''
    Interface implementations
    '''
    def exchanges(self, process, **kwargs):
        pass

    def exchange_values(self, process, flow, direction, termination=None, **kwargs):
        pass

    def inventory(self, process, ref_flow=None, **kwargs):
        pass

    def traverse(self, fragment, scenario=None, **kwargs):
        pass

    def lcia(self, process, ref_flow, quantity_ref, refresh=False, **kwargs):
        pass

    def fragment_lcia(self, fragment, quantity_ref, scenario=None, refresh=False, **kwargs):
        pass

    def factors(self, quantity, flowable=None, compartment=None, **kwargs):
        pass

