from lcatools.implementations import QuantityImplementation


class AntelopeQuantityImplementation(QuantityImplementation):
    def factors(self, quantity, flowable=None, compartment=None, **kwargs):
        """
        This needs to be done
        :param quantity:
        :param flowable:
        :param compartment:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def profile(self, flow, **kwargs):
        f = self._archive.retrieve_or_fetch_entity(flow)
        endpoint = '%s/flowpropertymagnitudes' % flow
        fpms = self._archive.get_endpoint(endpoint)
        for cf in fpms:
            q = self._archive.retrieve_or_fetch_entity('flowproperties/%s' % cf['flowProperty']['flowPropertyID'])
            if 'location' in cf:
                location = cf['location']
            else:
                location = 'GLO'
            self.characterize(f.link, f.reference_entity, q, cf['magnitude'], context=f.context, location=location)
        for cf in self._archive.tm.factors_for_flowable(f.flowable):
            yield cf

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

