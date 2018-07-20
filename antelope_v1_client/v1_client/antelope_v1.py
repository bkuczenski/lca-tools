import json
from collections import defaultdict

from lcatools.entities import BasicArchive
from lcatools.interfaces import IndexInterface, InventoryInterface, QuantityInterface
from lcatools.entity_refs import CatalogRef
from lcatools.fragment_flows import FragmentFlow


from .inventory import AntelopeInventoryImplementation
from .quantity import AntelopeQuantityImplementation
from .exceptions import AntelopeV1Error

try:
    from urllib.request import urlopen, urljoin
    from urllib.parse import urlparse
except ImportError:
    from urllib2 import urlopen
    from urlparse import urlparse, urljoin


def remote_ref(url):
    p = urlparse(url)
    ref = p.netloc
    for k in p.path.split('/'):
        if len(k) > 0:
            ref = '.'.join([ref, k])
    return ref


class AntelopeV1Client(BasicArchive, IndexInterface, InventoryInterface, QuantityInterface):
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

    def make_interface(self, iface):
        if iface == 'inventory':
            return AntelopeInventoryImplementation(self)
        elif iface == 'quantity':
            return AntelopeQuantityImplementation(self)
        else:
            return super(AntelopeV1Client, self).make_interface(iface)

    def _make_ref(self, external_ref, entity_type, reference_entity, **kwargs):
        return CatalogRef.from_query(external_ref, self._query, entity_type, reference_entity, **kwargs)

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
            for j in self.get_endpoint(endp, cache=False):
                yield self._parse_and_save_entity(j)
            self._fetched_all[entity_type] = True

    def get_uuid(self, ext_ref):
        ent = self.retrieve_or_fetch_entity(ext_ref)
        return ent.uuid

    def _load_all(self, **kwargs):
        raise AntelopeV1Error('Cannot load all entities from remote Antelope server')

    def get_endpoint(self, endpoint, cache=True):
        """
        Generally we do not want to cache responses that get parsed_and_saved because that process pop()s
        all the relevant information out of them.
        :param endpoint:
        :param cache:
        :return:
        """
        if endpoint in self._endpoints:
            return self._endpoints[endpoint]
        with urlopen(urljoin(self.source, endpoint)) as response:
            self._print('Fetching %s from remote server' % endpoint)
            j = json.loads(response.read())
        if cache:
            self._endpoints[endpoint] = j
        return j

    def _fetch(self, entity, **kwargs):
        j = self.get_endpoint(entity, cache=False)[0]
        return self._parse_and_save_entity(j)

    def _get_comment(self, processId):
        target = '/'.join(['processes', processId, 'comment'])
        j = self.get_endpoint(target)
        return j['comment']

    def _get_impact_category(self, cat_id):
        cats = self.get_endpoint('impactcategories')
        cat_id = int(cat_id)
        if cats[cat_id - 1]['impactCategoryID'] == cat_id:
            return cats[cat_id - 1]['name']
        try:
            return next(j['name'] for j in cats if j['impactCategoryID'] == cat_id)
        except StopIteration:
            raise ValueError('Unknown impact category ID %d' % cat_id)

    def make_fragment_flow(self, ff):
        """
        This needs to be in-house because it uses the query mechanism
        :param ff:
        :return:
        """
        return FragmentFlow.from_antelope_v1(ff, self._make_ref)

    '''
    Entity handling
    '''
    def retrieve_or_fetch_entity(self, key, **kwargs):
        """
        A key in v1 antelopev1 is 'datatype/index' where index is a sequential number starting from 1.  The archive also
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
        Take a json object obtained from a query and use it to create an entity ref
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
        ref = self._make_ref(ext_ref, 'process', [], **j)
        self._cached['processes'][process_id] = ref
        return ref

    def _parse_and_save_flow(self, j):
        flow_id = str(j.pop('flowID'))
        ext_ref = 'flows/%s' % flow_id
        j['Name'] = j.pop('name')
        j['CasNumber'] = j.pop('casNumber', '')
        j['Compartment'] = [j.pop('category')]
        ref_qty = self.retrieve_or_fetch_entity('flowproperties/%s' % j.pop('referenceFlowPropertyID'))
        ref = self._make_ref(ext_ref, 'flow', ref_qty, **j)
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
        ref = self._make_ref(ext_ref, 'quantity', ref_unit, **j)
        self._cached['lciamethods'][lm_id] = ref
        return ref

    def _parse_and_save_fp(self, j):
        fp_id = str(j.pop('flowPropertyID'))
        ext_ref = 'flowproperties/%s' % fp_id
        j['Name'] = j.pop('name')
        ref_unit = j.pop('referenceUnit')
        ref = self._make_ref(ext_ref, 'quantity', ref_unit, **j)
        self._cached['flowproperties'][fp_id] = ref
        return ref

    def _parse_and_save_fragment(self, j):
        frag_id = str(j.pop('fragmentID'))
        ext_ref = 'fragments/%s' % frag_id
        j['Name'] = j.pop('name')
        dirn = j.pop('direction')
        flow = self.retrieve_or_fetch_entity('flows/%s' % j.pop('termFlowID'))
        ref = self._make_ref(ext_ref, 'fragment', None, **j)
        ref.set_config(flow, dirn)
        self._cached['fragments'][frag_id] = ref
        return ref
