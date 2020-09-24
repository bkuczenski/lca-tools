
from lcatools.implementations import ExchangeImplementation


class AntelopeExchangeImplementation(ExchangeImplementation):
    """
    Overrides the default implementation to handle the AntelopeV1 case
    """

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

    def traverse(self, fragment, scenario=None, **kwargs):
        if scenario is not None:
            endpoint = 'scenarios/%s/%s/fragmentflows' % (scenario, fragment)
        else:
            endpoint = '%s/fragmentflows' % fragment

        self._archive.fetch_flows(fragment)

        ffs = self._archive.get_endpoint(endpoint)
        for ff in ffs:
            if 'fragmentStageID' in ff:
                ff['StageName'] = self._archive.get_stage_name(ff['fragmentStageID'])
        return [self._archive.make_fragment_flow(ff)
                for ff in sorted(ffs, key=lambda x: ('parentFragmentFlowID' in x, x['fragmentFlowID']))]
