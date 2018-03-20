from lcatools.implementations import InventoryImplementation
from lcatools.lcia_results import LciaResult

from .exceptions import AntelopeV1Error


class AntelopeInventoryImplementation(InventoryImplementation):
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
        lcia_q = self._archive.get_lcia_quantity(quantity_ref)
        endpoint = '%s/%s/lciaresults' % (process, lcia_q.external_ref)
        lcia_r = self._archive.get_endpoint(endpoint, cache=False)

        res = LciaResult(lcia_q, scenario=lcia_r.pop('scenarioID'))
        total = lcia_r.pop('total')

        if len(lcia_r['lciaScore']) > 1:
            raise AntelopeV1Error('Process LCIA result contains too many components\n%s' % process)

        component = lcia_r['lciaScore'][0]
        cum = component['cumulativeResult']
        self._archive.check_total(cum, total)

        if 'processes/%s' % component['processID'] != process:
            raise AntelopeV1Error('Reference mismatch: %s begat %s' % (process, component['processID']))

        self._archive.add_lcia_component(res, component)

        self._archive.check_total(res.total(), total)
        return res
