"""
This function performs the (surpassingly slow) task of extracting reduced aggregated LCI results from the large bundled
ecoinvent 7z LCI archives.  (The operation is slow on RAM-limited machines because the 7z algorithm requires tremendous
memory)

The routine requires the process inventory test case to be selected manually.

The selected inventory is loaded, and then one hundred exchanges are selected at random and the rest are removed.  This
reduces the file size (and load time) of the generated archives without sacrificing the representativeness of the
computation.
"""

import os

from .data_source import DataSource


ECOINVENT_VERSIONS = ('3.2', )
ECOINVENT_SYS_MODELS = ('apos', 'conseq', 'cutoff')
MODELMAP = {
    'apos': 'apos',
    'conseq': 'consequential_longterm',
    'cutoff': 'cutoff'
}


class Ecoinvent3Base(DataSource):

    _ds_type = 'EcospoldV2Archive'

    def __init__(self, ecoinvent_root, version, model, inv_ext='7z', lci_ext='7z'):
        assert version in ECOINVENT_VERSIONS
        assert model in ECOINVENT_SYS_MODELS
        self._exts = {'inv': inv_ext,
                      'lci': lci_ext}
        self._root = ecoinvent_root
        self._version = version
        self._model = model

    @property
    def _lci_ref(self):
        if os.path.exists(self.lci_source):
            yield 'local.ecoinvent.lci.%s.%s' % (self._version, self._model)

    @property
    def _inv_ref(self):
        if os.path.exists(self.inv_source):
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

    @property
    def inv_file(self):
        return 'current_Version_%s_%s_ecoSpold02.%s' % (self._version, MODELMAP[self._model], self._exts['inv'])

    @property
    def lci_file(self):
        return 'current_Version_%s_%s_lci_ecoSpold02.%s' % (self._version, MODELMAP[self._model], self._exts['lci'])

    @property
    def inv_source(self):
        return os.path.join(self._root, self._version, self.inv_file)

    @property
    def lci_source(self):
        return os.path.join(self._root, self._version, self.lci_file)


class EcoinventConfig(DataSource):
    def __init__(self, ecoinvent_root, **kwargs):
        self._sources = dict()
        for v in ECOINVENT_VERSIONS:
            for m in ECOINVENT_SYS_MODELS:
                b = Ecoinvent3Base(ecoinvent_root, v, m, **kwargs)
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
