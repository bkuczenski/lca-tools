from antelope_interface import QuantityImplementation


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
        pass

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
            if not f.has_characterization(q):
                f.add_characterization(q, value=cf['magnitude'], location=location)
        for cf in f.characterizations():
            yield cf
