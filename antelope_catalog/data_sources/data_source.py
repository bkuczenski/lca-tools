from ..lc_resource import LcResource
from collections import namedtuple

ResourceInfo = namedtuple('ResourceInfo',
                          ('source', 'ds_type', 'download_url', 'download_md5sum', 'config', 'init_args'))


class DataSourceError(Exception):
    pass


class DataSource(object):
    """
    An abstract class that defines how data sources are handled.
    """
    _ds_type = None

    def __init__(self, data_root=None, **kwargs):
        self._root = data_root
        self._kwargs = kwargs

    @property
    def root(self):
        return self._root

    def _make_resource(self, ref, source=None, ds_type=None, info=None, **kwargs):
        if isinstance(info, ResourceInfo):
            source = info.source
            ds_type = info.ds_type
            if info.download_url is not None:
                kwargs['download'] = {'url': info.download_url, 'md5sum': info.download_md5sum}
            if info.config is not None:
                kwargs['config'] = info.config
            if info.init_args is not None:
                kwargs.update(info.init_args)
        if ds_type is None:
            ds_type = self._ds_type
        return LcResource(ref, source, ds_type, **kwargs)

    def register_all_resources(self, cat):
        for ref in self.references:
            for res in self.make_resources(ref):
                cat.add_resource(res)

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


class DataCollection(DataSource):
    """
    A container for a number of related data sources.  Uses a factory to generate the data sources, and then spools
    out their results.
    """
    def factory(self, *args, **kwargs):
        raise NotImplementedError

    def __init__(self, data_root, **kwargs):
        super(DataCollection, self).__init__(data_root, **kwargs)
        self._sources = dict()
        for b in self.factory(data_root, **kwargs):
            for r in b.references:
                if r in self._sources:
                    raise KeyError('Duplicate reference %s' % r)
                self._sources[r] = b

    @property
    def references(self):
        for s in self._sources.keys():
            yield s

    def interfaces(self, ref):
        b = self._sources[ref]
        for i in b.interfaces(ref):
            yield i

    def make_resources(self, ref):
        b = self._sources[ref]
        for m in b.make_resources(ref):
            yield m
