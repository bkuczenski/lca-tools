from .data_source import DataSource
import os


EI_LCIA_SPREADSHEETS = {
    '3.1': 'LCIA implementation v3.1 2014_08_13.xlsx',
    '3.4': 'LCIA_implementation_3.4.xlsx',
    '3.5': 'LCIA_implementation_3.5.xlsx',
    '3.6': 'LCIA_implementation_3.6.xlsx',
}


E_CFG = {'hints': [  # cover elementary contexts that need directional hints
    ['context', 'air', 'to air'],
    ['context', 'water', 'to water']
]}


class EcoinventLciaConfig(DataSource):
    _ifaces = ('index', 'quantity')
    _ds_type = 'EcoinventLcia'

    def __init__(self, data_root, version, **kwargs):
        self._version = str(version)
        try:
            self._sourcefile = EI_LCIA_SPREADSHEETS[self._version]
        except KeyError:
            self._sourcefile = None
        super(EcoinventLciaConfig, self).__init__(data_root, **kwargs)

    @property
    def references(self):
        if self._sourcefile is not None:
            if os.path.exists(os.path.join(self._root, self._sourcefile)):
                yield '.'.join(['local', 'lcia', 'ecoinvent', self._version])

    def interfaces(self, ref):
        for k in self._ifaces:
            yield k

    def make_resources(self, ref):
        if ref not in self.references:
            raise ValueError('Unknown reference %s' % ref)
        if self._sourcefile is None:
            raise AttributeError('This exception should never occur')  # because self.references screens self._info
        source = os.path.join(self._root, self._sourcefile)
        yield self._make_resource(ref, source=source, interfaces=self._ifaces, version=self._version, static=True,
                                  config=E_CFG)
