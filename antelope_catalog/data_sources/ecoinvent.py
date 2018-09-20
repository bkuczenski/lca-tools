import os

from .data_source import DataSource, DataCollection

FILE_PREFIX = ('current_Version_', 'ecoinvent ')
FILE_EXT = ('7z', 'zip')
ECOINVENT_VERSIONS = ('3.2', '3.4')
ECOINVENT_SYS_MODELS = ('apos', 'conseq', 'cutoff')
MODELMAP = {
    'apos': ('apos',),
    'conseq': ('consequential', 'consequential_longterm'),
    'cutoff': ('cutoff',)
}


class Ecoinvent3Base(DataSource):

    _ds_type = 'EcospoldV2Archive'

    def __init__(self, data_root, version, model, **kwargs):
        assert version in ECOINVENT_VERSIONS
        assert model in ECOINVENT_SYS_MODELS
        super(Ecoinvent3Base, self).__init__(data_root=data_root, **kwargs)
        self._version = version
        self._model = model

    @property
    def _lci_ref(self):
        if self.lci_source is not None:
            yield 'local.ecoinvent.lci.%s.%s' % (self._version, self._model)

    @property
    def _inv_ref(self):
        if self.inv_source is not None:
            yield 'local.ecoinvent.%s.%s' % (self._version, self._model)

    @property
    def references(self):
        for x in self._lci_ref:
            yield x
        for x in self._inv_ref:
            yield x

    def interfaces(self, ref):
        yield 'inventory'

    def make_resources(self, ref):
        if ref in self._lci_ref:
            yield self._make_resource(ref, self.lci_source, interfaces='inventory', prefix='datasets')
        elif ref in self._inv_ref:
            yield self._make_resource(ref, self.inv_source, interfaces=('index', 'inventory'), prefix='datasets')

    def _fname(self, ftype=None):
        for pf in FILE_PREFIX:
            for mod in MODELMAP[self._model]:
                if ftype is None:
                    pname = '%s%s_%s_ecoSpold02' % (pf, self._version, mod)
                else:
                    pname = '%s%s_%s_%s_ecoSpold02' % (pf, self._version, mod, ftype)
                dsource = os.path.join(self.root, self._version, pname)
                if os.path.isdir(dsource):
                    return dsource
                for ext in FILE_EXT:
                    fname = '%s.%s' % (pname, ext)
                    source = os.path.join(self.root, self._version, fname)
                    if os.path.exists(source):
                        return source

    @property
    def inv_source(self):
        return self._fname()

    @property
    def lci_source(self):
        return self._fname('lci')


class EcoinventConfig(DataCollection):
    def factory(self, data_root, **kwargs):
        for v in ECOINVENT_VERSIONS:
            for m in ECOINVENT_SYS_MODELS:
                yield Ecoinvent3Base(data_root, v, m, **kwargs)
