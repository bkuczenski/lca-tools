from lcatools.implementations import ForegroundImplementation
from lcatools.lcia_results import LciaResult


class AntelopeForegroundImplementation(ForegroundImplementation):
    def traverse(self, fragment, scenario=None, **kwargs):
        if scenario is not None:
            endpoint = 'scenarios/%s/%s/fragmentflows' % (scenario, fragment)
        else:
            endpoint = '%s/fragmentflows' % fragment
        ffs = self._archive.get_endpoint(endpoint)
        for ff in ffs:
            if 'fragmentStageID' in ff:
                ff['StageName'] = self._archive.get_stage_name(ff['fragmentStageID'])
        return [self._archive.make_fragment_flow(ff) for ff in ffs]

    def fragment_lcia(self, fragment, quantity_ref, scenario=None, refresh=False, **kwargs):
        if scenario is None:
            scenario = '1'
        lcia_q = self._archive.get_lcia_quantity(quantity_ref)
        endpoint = 'scenarios/%s/%s/%s/lciaresults' % (scenario, fragment, lcia_q.external_ref)
        lcia_r = self._archive.get_endpoint(endpoint, cache=False)

        res = LciaResult(lcia_q, scenario=lcia_r.pop('scenarioID'))
        total = lcia_r.pop('total')

        for component in lcia_r['lciaScore']:
            self._archive.add_lcia_component(res, component)

        self._archive.check_total(res.total(), total)

        return res
