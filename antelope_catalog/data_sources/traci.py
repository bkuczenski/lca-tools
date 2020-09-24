import os

from .data_source import DataSource, ResourceInfo, DataSourceError


TRACI_VERSIONS = {
    # Each entry must have ds_type and either a filename that exists or a download_url (optional download_md5sum)
    '2.1' : {
        'ds_type': 'Traci21Factors',
        'filename': 'traci_2_1_2014_dec_10_0.xlsx',
        'download_url': 'https://www.dropbox.com/s/p8w72o8iik5gzb6/traci_2_1_2014_dec_10_0.xlsx?dl=1',
        'download_md5sum': '4a16a40c453ab9084b610da7ded5e9c8'
    }  # would be great for this to be downloadable somewhere
}


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
                raise ValueError('Invalid ref %s' % origin)
        return '.'.join(gen)

    def _check_info(self, info):
        if 'filename' in info:
            path = os.path.join(self._root, info['filename'])
            if os.path.exists(path):
                return ResourceInfo(path, info['ds_type'], None, None, E_CFG, None)
            else:
                print('Missing local source file: %s' % path)
            if 'download_url' not in info:
                raise DataSourceError('Missing source and no download link')

        return ResourceInfo(None, info['ds_type'],
                            info['download_url'], info.get('download_md5sum'), E_CFG, None)

    def make_resources(self, ref):
        """

        :param ref: can be a full reference OR a version (e.g. '2.1')
        :return:
        """
        if ref in TRACI_VERSIONS:
            ver = ref
            ref = '.'.join([self._prefix, ver])
        else:
            ver = self._trim_origin(ref)
            if ver not in TRACI_VERSIONS:
                raise KeyError('Unknown version %s' % ver)

        info = self._check_info(TRACI_VERSIONS[ver])
        yield self._make_resource(ref, info=info, interfaces=self._ifaces)
