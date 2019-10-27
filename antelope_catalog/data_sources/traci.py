import os

from .data_source import DataSource


TRACI_VERSIONS = {
    '2.1' : {
        'ds_type': 'Traci21Factors',
        'filename': 'traci_2_1_2014_dec_10_0.xlsx'
    }
}  # would be great for this to be downloadable somewhere


E_CFG = {'hints': [  # cover elementary contexts that need directional hints
    ['context', 'air', 'to air'],
    ['context', 'water', 'to water']
]}


class TraciConfig(DataSource):

    _prefix = 'local.lcia.traci'
    _ifaces = ('index', 'quantity')

    @property
    def references(self):
        for k in TRACI_VERSIONS.keys():
            yield '.'.join([self._prefix, k])

    def interfaces(self, ref):
        if ref in self.references:
            for k in self._ifaces:
                yield k

    def _trim_origin(self, origin):
        gen = (_part for _part in origin.split('.'))
        for z in self._prefix.split('.'):
            if z != next(gen):
                raise ValueError
        return '.'.join(gen)

    def make_resources(self, ref):
        """

        :param ref: can be a full reference OR a version (e.g. '2.1')
        :return:
        """
        if ref in TRACI_VERSIONS:
            ver = ref
            ref = '.'.join([self._prefix, ver])
        else:
            try:
                ver = self._trim_origin(ref)
            except ValueError:
                raise KeyError('Not a valid ref %s' % ref)
        try:
            info = TRACI_VERSIONS[ver]
        except KeyError:
            raise KeyError('Unknown version %s' % ver)
        yield self._make_resource(ref, os.path.join(self._root, info['filename']), ds_type=info['ds_type'],
                                  interfaces=self._ifaces, config=E_CFG)
