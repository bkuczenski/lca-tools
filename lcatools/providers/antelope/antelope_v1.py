from lcatools.providers.interfaces import ArchiveInterface
from lcatools.interfaces import IndexInterface, InventoryInterface, QuantityInterface, EntityNotFound
from lcatools.entity_refs import CatalogRef
from lcatools.fragment_flows import FragmentFlow
from lcatools.lcia_results import LciaResult
from lcatools.exchanges import ExchangeValue
from lcatools.characterizations import Characterization
import json
from collections import defaultdict
from math import isclose

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

    def get_uuid(self, ext_ref):
        ent = self.retrieve_or_fetch_entity(ext_ref)
        return ent.uuid

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
        j = self._get_endpoint(entity)[0]
        return self._parse_and_save_entity(j)

    def _get_comment(self, processId):
        target = '/'.join(['processes', processId, 'comment'])
        j = self._get_endpoint(target)
        return j['comment']

    def _get_impact_category(self, cat_id):
        cats = self._get_endpoint('impactcategories')
        cat_id = int(cat_id)
        if cats[cat_id - 1]['impactCategoryID'] == cat_id:
            return cats[cat_id - 1]['name']
        try:
            return next(j['name'] for j in cats if j['impactCategoryID'] == cat_id)
        except StopIteration:
            raise ValueError('Unknown impact category ID %d' % cat_id)

    def _get_stage_name(self, stage_id):
        stgs = self._get_endpoint('stages')
        stage_id = int(stage_id)
        if stgs[stage_id - 1]['fragmentStageID'] == stage_id:
            return stgs[stage_id - 1]['name']
        try:
            return next(j['name'] for j in stgs if j['fragmentStageID'] == stage_id)
        except StopIteration:
            raise ValueError('Unknown fragment stage ID %d' % stage_id)

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
        dirn = j.pop('direction')
        flow = self.retrieve_or_fetch_entity('flows/%s' % j.pop('termFlowID'))
        ref = CatalogRef.from_query(ext_ref, self._query, 'fragment', None, **j)
        ref.set_config(flow, dirn)
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
        if scenario is not None:
            endpoint = 'scenarios/%s/%s/fragmentflows' % (scenario, fragment)
        else:
            endpoint = '%s/fragmentflows' % fragment
        ffs = self._get_endpoint(endpoint)
        for ff in ffs:
            if 'fragmentStageID' in ff:
                ff['StageName'] = self._get_stage_name(ff['fragmentStageID'])
        return [FragmentFlow.from_antelope_v1(ff, self._query) for ff in ffs]

    def lcia(self, process, ref_flow, quantity_ref, refresh=False, **kwargs):
        """
        Antelope v1 doesn't support or even have any knowledge of process reference-flows. this is a somewhat
        significant design flaw.  well, no matter.  each antelope process must therefore represent an allocated single
        operation process that has an unambiguous reference flow.  This is a problem to solve on the server side;
        for now we just ignore the ref_flow argument.

        If the quantity ref is one of the ones natively known by the antelope server-- i.e. if it is a catalog ref whose
        origin matches the origin of the current archive-- then it is trivially used.  Otherwise, the lcia call reduces
        to obtaining the inventory and computing LCIA locally.
        :param process:
        :param ref_flow:
        :param quantity_ref:
        :param refresh:
        :param kwargs:
        :return:
        """
        if isinstance(quantity_ref, str):
            lcia_q = self.retrieve_or_fetch_entity(quantity_ref)
        elif quantity_ref.origin == self.ref:
            lcia_q = self.retrieve_or_fetch_entity(quantity_ref.external_ref)
        else:
            raise EntityNotFound
        endpoint = '%s/%s/lciaresults' % (process, lcia_q.external_ref)
        lcia_r = self._get_endpoint(endpoint, cache=False)
        lcia_r['origin'] = self.ref

        res = LciaResult(lcia_q, scenario=lcia_r.pop('scenarioID'))
        total = lcia_r.pop('total')
        if len(lcia_r['lciaScore']) > 1:
            raise AntelopeV1Error('Process LCIA result contains too many components\n%s' % process)
        l = lcia_r['lciaScore'][0]
        cum = l['cumulativeResult']
        if not isclose(cum, total, rel_tol=1e-8):
            raise AntelopeV1Error('Total and Cumulative Result do not match! %g / %g' % (total, cum))
        if 'processes/%s' % l['processID'] != process:
            raise AntelopeV1Error('Reference mismatch: %s begat %s' % (process, l['processID']))
        ent = self.retrieve_or_fetch_entity(process)
        loc = ent['SpatialScope']
        if len(l['lciaDetail']) == 0:
            res.add_summary(ent.external_ref, ent, 1.0, cum)
        else:
            res.add_component(ent.external_ref, ent)
            for d in l['lciaDetail']:
                flow = self.retrieve_or_fetch_entity('flows/%s' % d['flowID'])
                exch = ExchangeValue(ent, flow, d['direction'], value=d['quantity'])
                fact = Characterization(flow, lcia_q, value=d['factor'], location=loc)
                res.add_score(ent.external_ref, exch, fact, loc)

        if not isclose(res.total(), total, rel_tol=1e-8):
            raise AntelopeV1Error('Total and computed result do not match! %g / %g' % (total, res.total()))

        return res

    def fragment_lcia(self, fragment, quantity_ref, scenario=None, refresh=False, **kwargs):
        pass

    def factors(self, quantity, flowable=None, compartment=None, **kwargs):
        pass

