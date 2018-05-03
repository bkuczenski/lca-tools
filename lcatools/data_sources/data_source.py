from lcatools.catalog.lc_resource import LcResource


class DataSource(object):
    """
    An abstract class that defines how data sources are handled.
    """
    _ds_type = None

    def _make_resource(self, ref, source, ds_type=None, **kwargs):
        if ds_type is None:
            ds_type = self._ds_type
        return LcResource(ref, source, ds_type, **kwargs)

    @property
    def references(self):
        """
        Generates a list of semantic references the DataSource knows how to instantiate
        :return:
        """
        raise NotImplementedError

    def interfaces(self, ref):
        """
        Generates a list of interfaces known for the given reference. the reference must be in the list of references.
        :param ref:
        :return:
        """
        raise NotImplementedError

    def make_resources(self, ref):
        """
        Generates an exhaustive sequence of LcResource objects for a given reference.
        :param ref:
        :return:
        """
        raise NotImplementedError

    def test_params(self, ref, interface):
        """
        User supplies a reference and interface.
        This function returns a 2-tuple:
          - kwargs to pass as input arguments
          - expected output

        This function can't be written until the tests are designed.
        :param ref:
        :param interface:
        :return:
        """
        fun = {
            'index': self._index_test_params,
            'inven': self._inventory_test_params,
            'backg': self._background_test_params,
            'quant': self._quantity_test_params
        }[interface[:5]]
        return fun(ref)

    def _index_test_params(self, ref):
        pass

    def _inventory_test_params(self, ref):
        pass

    def _background_test_params(self, ref):
        pass

    def _quantity_test_params(self, ref):
        pass
