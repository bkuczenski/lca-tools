class LcaModelInterface(object):

    def transporters(self):
        """
        List the transporters in the model as name, link tuples
        :return: generate name, link tuples or EntitySpec namedtuples
        """
        raise NotImplementedError

    def products(self):
        """
        List the MFA products in the model as name, link tuples
        :return: generate name, link tuples or EntitySpec namedtuples
        """
        raise NotImplementedError

    @property
    def scope(self):
        raise NotImplementedError

    @property
    def ref(self):
        raise NotImplementedError

    @property
    def reference_flow(self):
        raise NotImplementedError

    @property
    def default_term_map(self):
        return dict()
