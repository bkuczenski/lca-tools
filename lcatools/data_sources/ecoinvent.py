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

    def __init__(self, ecoinvent_root, version, model, **kwargs):
        assert version in ECOINVENT_VERSIONS
        assert model in ECOINVENT_SYS_MODELS
        self._root = ecoinvent_root
        self._version = version
        self._model = model
        self._kwargs = kwargs

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
                for ext in FILE_EXT:
                    if ftype is None:
                        fname = '%s%s_%s_ecoSpold02.%s' % (pf, self._version, mod, ext)
                    else:
                        fname = '%s%s_%s_%s_ecoSpold02.%s' % (pf, self._version, mod, ftype, ext)
                    source = os.path.join(self._root, self._version, fname)
                    if os.path.exists(source):
                        return source

    @property
    def inv_source(self):
        return self._fname()

    @property
    def lci_source(self):
        return self._fname('lci')


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
