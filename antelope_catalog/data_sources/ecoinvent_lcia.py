from .data_source import DataSource
from collections import namedtuple
import os


EiLciaInfo = namedtuple('EiLciaInfo', ('filename', 'suffix', 'ns_uuid'))


EI_LCIA_SPREADSHEETS = {
    '3.1': EiLciaInfo('LCIA implementation v3.1 2014_08_13.xlsx', '2014_08_13', '46802ca5-8b25-398c-af10-2376adaa4623')
}


class EcoinventLciaConfig(DataSource):
    _ifaces = ('index', 'quantity')
    _ds_type = 'EcoinventLcia'

    def __init__(self, data_root, version, **kwargs):
        self._version = str(version)
        self._info = EI_LCIA_SPREADSHEETS[self._version]
        super(EcoinventLciaConfig, self).__init__(data_root, **kwargs)

    @property
    def references(self):
        yield '.'.join(['local', 'lcia', 'ecoinvent', self._version, self._info.suffix])

    def interfaces(self, ref):
        for k in self._ifaces:
            yield k

    def make_resources(self, ref):
        if ref not in self.references:
            raise ValueError('Unknown reference %s' % ref)
        source = os.path.join(self._root, self._info.filename)
        yield self._make_resource(ref, source=source, interfaces=self._ifaces, ns_uuid=self._info.ns_uuid)
