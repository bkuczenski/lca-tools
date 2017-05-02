from lcatools.background.background import BackgroundEngine
from lcatools.exchanges import ExchangeValue


class TerminationNotFound(Exception):
    pass


class BackgroundManager(object):
    """
    Provides Lc access to matrix inversion results from a BackgroundEngine.  Acts as the content generator for
    BackgroundInterface.

    Abstracts the actual entities? do we pass entities or external_refs? TBD.

    BackgroundEngine needs to figure out the following:
     - list of foreground and background ProductFlows (terminations), and cutoff exterior flows (flow + direction)
    """
    def __init__(self, fg_interface):
        self._be = BackgroundEngine(fg_interface)
        self._be.add_all_ref_products()

    @property
    def background_flows(self):
        for k in self._be.background_flows():
            yield k

    @property
    def foreground_flows(self):
        for k in self._be.foreground_flows():
            yield k

    @property
    def emissions(self):
        for k in self._be.emissions:
            yield k

    def _get_product_flow(self, process, ref_flow):
        rx = process.reference(flow=ref_flow)
        pf = self._be.check_product_flow(rx.flow, process)
        if pf is None:
            raise TerminationNotFound
        return pf

    def foreground(self, process, ref_flow=None):
        product_flow = self._get_product_flow(process, ref_flow)

    def inventory(self, process, ref_flow=None, show=None):
        """
        Report the direct dependencies and exterior flows for the named product flow.  If the second argument is
        non-None, print the inventory instead of returning it.
        This should be identical to product_flow.process.inventory() so WHY DID I WRITE IT??????
        ans: because it exposes the allocated matrix model. so it's not the same for allocated processes.
        :param process:
        :param ref_flow: required for multi-product case
        :param show: [None] if present, show rather than return the inventory.
        :return: a list of exchanges.
        """
        product_flow = self._get_product_flow(process, ref_flow)
        interior = [ExchangeValue(product_flow.process, product_flow.flow, product_flow.direction, value=1.0)]
        exterior = []
        if self._be.is_background(product_flow):
            # need to index into background matrix
            _af, _ad, _bf = self._be.make_foreground(product_flow)
            for i, row in enumerate(_ad.nonzero()[0]):
                dep = self._be.tstack.bg_node(row)
                dat = _ad.data[i]
                if dat < 0:
                    dirn = 'Output'
                else:
                    dirn = 'Input'
                interior.append(ExchangeValue(product_flow.process, dep.flow, dirn, value=abs(dat)))
            for i, row in enumerate(_bf.nonzero()[0]):
                ems = self._be.emissions[row]
                dat = _bf.data[i]
                exterior.append(ExchangeValue(product_flow.process, ems.emission.flow, ems.emission.direction,
                                              value=dat))
        else:
            # need to simply access the sparse matrix entries
            for fg in self._be.foreground_dependencies(product_flow):
                dat = fg.value
                if dat < 0:
                    dirn = 'Output'
                else:
                    dirn = 'Input'
                interior.append(ExchangeValue(product_flow.process, fg.term.flow, dirn, value=dat))
            for em in self._be.foreground_emissions(product_flow):
                exterior.append(ExchangeValue(product_flow.process, em.emission.flow, em.emission.direction,
                                              value=em.value))
        if show is None:
            return interior + exterior
        else:
            for x in interior:
                print('%s' % x)
            print('Exterior')
            for x in exterior:
                print('%s' % x)

    def lci(self, process, ref_flow=None, **kwargs):
        """
        Wrapper for compute_lci, returns exchanges with flows (and characterizations) drawn from self.archive
        :param process:
        :param ref_flow: required for multi-product case
        :param kwargs: passed to iterative solver: threshold=1e-8, count=100
        :return: list of exchanges.
        """
        product_flow = self._get_product_flow(process, ref_flow)
        b = self._be.compute_lci(product_flow, **kwargs)  # comes back as a sparse vector
        exchanges = []
        for i, em in enumerate(self._be.emissions):
            if b[i, 0] != 0:
                exchanges.append(ExchangeValue(product_flow.process, em.flow, em.direction, value=b[i, 0]))
        return exchanges
