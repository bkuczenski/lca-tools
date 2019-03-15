from math import isclose

from lcatools.implementations import InventoryImplementation
from lcatools.interfaces import EntityNotFound

from lcatools.exchanges import ExchangeValue
from lcatools.characterizations import QRResult
from lcatools.lcia_results import LciaResult

from .exceptions import AntelopeV1Error


class FragmentFlowProxy(object):
    def __init__(self, entity_type, external_ref):
        self._etype = entity_type
        self._eref = external_ref

    @property
    def entity_type(self):
        return self._etype

    @property
    def external_ref(self):
        return self._eref

    def __str__(self):
        return '%s %s' % (self.external_ref, self.entity_type)


class AntelopeInventoryImplementation(InventoryImplementation):
    """
    Overrides the default implementation to handle the AntelopeV1 case
    """

    def get_stage_name(self, stage_id):
        stgs = self._archive.get_endpoint('stages')
        stage_id = int(stage_id)
        if stgs[stage_id - 1]['fragmentStageID'] == stage_id:
            return stgs[stage_id - 1]['name']
        try:
            return next(j['name'] for j in stgs if j['fragmentStageID'] == stage_id)
        except StopIteration:
            raise ValueError('Unknown fragment stage ID %d' % stage_id)

    '''
    LCIA handling
    '''
    def add_lcia_component(self, res, component):
        if 'processID' in component:
            entity = self._archive.retrieve_or_fetch_entity('processes/%s' % component['processID'])
            loc = entity['SpatialScope']
        elif 'fragmentFlowID' in component:  # not currently implemented
            entity = FragmentFlowProxy('FragmentFlow', 'fragmentflows/%s' % component['fragmentFlowID'])
            loc = 'GLO'
        elif 'fragmentStageID' in component:  # currently the default
            entity = FragmentFlowProxy('Stage', self.get_stage_name(component['fragmentStageID']))
            loc = 'GLO'
        else:
            raise ValueError('Unable to handle unrecognized LCIA Result Type\n%s' % component)

        if len(component['lciaDetail']) == 0:
            res.add_summary(entity.external_ref, entity, 1.0, component['cumulativeResult'])
        else:
            if entity.entity_type != 'process':
                raise TypeError('Antelope v1 should not have details for non-process entities\n%s' % entity)
            res.add_component(entity.external_ref, entity)
            for d in component['lciaDetail']:
                self._add_lcia_detail(res, entity, d, loc=loc)

    def _add_lcia_detail(self, res, entity, detail, loc='GLO'):
        flow = self._archive.retrieve_or_fetch_entity('flows/%s' % detail['flowID'])
        cx = self._archive.tm[flow.context]
        exch = ExchangeValue(entity, flow, detail['direction'], termination=cx, value=detail['quantity'])
        qrr = QRResult(flow.flowable, flow.reference_entity, res.quantity, cx, loc, self.origin, detail['factor'])
        res.add_score(entity.external_ref, exch, qrr)

    @staticmethod
    def check_total(res, check):
        if not isclose(res, check, rel_tol=1e-8):
            raise AntelopeV1Error('Total and Check do not match! %g / %g' % (res, check))

    def get_lcia_quantity(self, quantity_ref):
        if isinstance(quantity_ref, str):
            return self._archive.retrieve_or_fetch_entity(quantity_ref)
        elif quantity_ref.origin == self._archive.ref:
            return self._archive.retrieve_or_fetch_entity(quantity_ref.external_ref)
        else:
            raise EntityNotFound

    '''
    Implementation Method Overrides
    Unimplemented overrides will simply fallthrough to default:

    def exchanges(self, process, **kwargs):
        pass

    def exchange_values(self, process, flow, direction, termination=None, **kwargs):
        pass

    def inventory(self, process, ref_flow=None, **kwargs):
        pass
    '''

    def lcia(self, process, ref_flow, quantity_ref, refresh=False, **kwargs):
        """
        Antelope v1 doesn't support or even have any knowledge of process reference-flows. this is a somewhat
        significant design flaw.  well, no matter.  each antelopev1 process must therefore represent an allocated single
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
        lcia_q = self.get_lcia_quantity(quantity_ref)
        endpoint = '%s/%s/lciaresults' % (process, lcia_q.external_ref)
        lcia_r = self._archive.get_endpoint(endpoint, cache=False)

        res = LciaResult(lcia_q, scenario=lcia_r.pop('scenarioID'))
        total = lcia_r.pop('total')

        if len(lcia_r['lciaScore']) > 1:
            raise AntelopeV1Error('Process LCIA result contains too many components\n%s' % process)

        component = lcia_r['lciaScore'][0]
        cum = component['cumulativeResult']
        self.check_total(cum, total)

        if 'processes/%s' % component['processID'] != process:
            raise AntelopeV1Error('Reference mismatch: %s begat %s' % (process, component['processID']))

        self.add_lcia_component(res, component)

        self.check_total(res.total(), total)
        return res

    def traverse(self, fragment, scenario=None, **kwargs):
        if scenario is not None:
            endpoint = 'scenarios/%s/%s/fragmentflows' % (scenario, fragment)
        else:
            endpoint = '%s/fragmentflows' % fragment

        self._archive.fetch_flows(fragment)

        ffs = self._archive.get_endpoint(endpoint)
        for ff in ffs:
            if 'fragmentStageID' in ff:
                ff['StageName'] = self.get_stage_name(ff['fragmentStageID'])
        return [self._archive.make_fragment_flow(ff)
                for ff in sorted(ffs, key=lambda x: ('parentFragmentFlowID' in x, x['fragmentFlowID']))]

    def fragment_lcia(self, fragment, quantity_ref, scenario=None, refresh=False, **kwargs):
        if scenario is None:
            scenario = '1'
        lcia_q = self.get_lcia_quantity(quantity_ref)
        endpoint = 'scenarios/%s/%s/%s/lciaresults' % (scenario, fragment, lcia_q.external_ref)
        lcia_r = self._archive.get_endpoint(endpoint, cache=False)
        if lcia_r is None or (isinstance(lcia_r, list) and all(i is None for i in lcia_r)):
            res = LciaResult(lcia_q, scenario=scenario)
            return res

        res = LciaResult(lcia_q, scenario=lcia_r.pop('scenarioID'))
        total = lcia_r.pop('total')

        for component in lcia_r['lciaScore']:
            self.add_lcia_component(res, component)

        self.check_total(res.total(), total)

        return res
