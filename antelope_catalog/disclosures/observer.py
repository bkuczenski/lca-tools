from lca_disclosures import ForegroundFlow, BackgroundFlow, EmissionFlow


class UnobservedFragmentFlow(Exception):
    pass


class ObservedFlow(object):
    _parent = None

    @property
    def parent(self):
        if self._parent is None:
            raise UnobservedFragmentFlow
        return self._parent

    @property
    def key(self):
        raise NotImplemented

    @property
    def bg_key(self):
        """
        OF subclass must be further subclassed to define bg_key
        :return:
        """
        raise TypeError

    @property
    def flow(self):
        raise NotImplemented

    @property
    def direction(self):
        raise NotImplemented

    @property
    def locale(self):
        raise NotImplemented

    @property
    def value(self):
        raise NotImplemented

    def observe(self, parent):
        self._parent = parent


class ReferenceFlow(ObservedFlow):
    @property
    def key(self):
        return None

    @property
    def value(self):
        return 0.0


RX = ReferenceFlow()


class SeqList(object):
    def __init__(self):
        self._l = []
        self._d = {}

    def index(self, key):
        if key not in self._d:
            ix = len(self._l)
            self._l.append(key)
            self._d[key] = ix

        return self._d[key]

    def __len__(self):
        return len(self._l)

    def __getitem__(self, key):
        try:
            return self._l[key]
        except TypeError:
            return self._d[key]

    def to_list(self):
        return self._l


class SeqDict(object):
    def __init__(self):
        self._l = []
        self._d = {}
        self._ix = {}

    def __setitem__(self, key, value):
        if key in self._d:
            raise KeyError('Value for %s already set!' % key)
        ix = len(self._l)
        self._l.append(key)
        self._d[key] = value
        self._ix[value] = ix
        self._ix[key] = ix

    def __len__(self):
        return len(self._l)

    def __getitem__(self, key):
        try:
            return self._d[key]
        except KeyError:
            return self._d[self._l[key]]

    def index(self, item):
        return self._ix[item]

    def to_list(self):
        return [self._d[x] for x in self._l]


class Observer(object):
    def __init__(self):
        """
        These are really 'list-dicts' where they have a sequence but also a reverse-lookup capability.
        functionality tbd
        """
        self._fg = SeqDict()  # log the flow entities we encounter; map key to OFF
        self._co = SeqList()  # map index to (flow, direction)
        self._bg = SeqList()  # map index to (process_ref, term_flow)
        self._em = SeqList()  # map index to (flow, direction)

        self._Af = []  # list of OFFs
        self._Ac = []  # list of cutoffs  (get appended to Af)
        self._Ad = []  # list of OBGs
        self._Bf = []  # list of emissions

        self._key_lookup = dict()  # map ff key to type-specific (map, key)

    def __iter__(self):
        return self

    def __next__(self):
        raise NotImplemented

    def __getitem__(self, key):
        _map, _key = self._key_lookup[key]
        return _map[_key]

    def _show_mtx(self, mtx):
        tag = {'Af': 'Foreground',
               'Ac': 'Cutoffs',
               'Ad': 'Background',
               'Bf': 'Emissions'}[mtx]  # KeyError saves
        if mtx != 'Af':
            print('\n')
        print('%s - %s' % (mtx, tag))
        for k in getattr(self, '_' + mtx):
            print(k)

    def show(self):
        self._show_mtx('Af')
        self._show_mtx('Ac')
        self._show_mtx('Ad')
        self._show_mtx('Bf')

    def _add_foreground(self, off):
        """
        Creates a new node from the most recent OFF
        :param off: the observed fragment flow
        :return:
        """
        print('Handling as FG')
        self._fg[off.key] = off
        self._key_lookup[off.key] = (self._fg, off.key)
        self._Af.append(off)

    def _add_background(self, obg):
        """

        :param obg: an Observed background
        :return:
        """
        print('Handling as BG')
        ix = self._bg.index(obg.bg_key)
        self._key_lookup[obg.key] = (self._bg, ix)
        self._Ad.append(obg)

    def _add_cutoff(self, oco, extra=''):
        """

        :param oco: the Observed Cutoff
        :return:
        """
        print('Adding Cutoff %s' % extra)
        ix = self._co.index(oco.key)
        self._key_lookup[oco.key] = (self._co, ix)
        self._Ac.append(oco)

    def _add_emission(self, oco):
        """

        :param oco: the Observed Fragment Flow
        :return:
        """
        print('Adding Emission')
        ix = self._em.index(oco.key)
        self._key_lookup[oco.key] = (self._em, ix)
        self._Bf.append(oco)

    @property
    def functional_unit(self):
        return self._fg[0]

    def generate_disclosure(self):
        _ = [x for x in self]  # ensure fully iterated
        p = len(self._fg)

        d_i = [ForegroundFlow(off.flow['Name'], off.direction, off.unit(), location=off.locale)
               for off in self._fg.to_list()]  # this returns an ObservedForegroundFlow
        d_i += [ForegroundFlow(flow['Name'], dirn, flow.unit(), location=locale)
                for flow, dirn, locale in self._co.to_list()]

        d_ii = [BackgroundFlow(node.origin, flow['Name'], dirn, flow.unit(),
                               activity=node.external_ref,
                               location=node['SpatialScope'],
                               external_ref=flow.external_ref)
                for node, flow, dirn in self._bg.to_list()]

        d_iii = [EmissionFlow(flow.origin, flow['Name'], dirn, flow.unit(),
                              context=flow.context,
                              location=locale,
                              external_ref=flow.external_ref)
                 for flow, dirn, locale in self._em.to_list()]

        d_iv = []
        d_v = []
        d_vi = []

        for off in self._Af:
            if off.parent is RX:
                continue
            d_iv.append([self._fg.index(off.key), self._fg.index(off.parent.key), off.value])
        for oco in self._Ac:
            d_iv.append([p + self._co.index(oco.key), self._fg.index(oco.parent.key), oco.value])

        for obg in self._Ad:
            d_v.append([self._bg.index(obg.bg_key), self._fg.index(obg.parent.key), obg.value])

        for oem in self._Bf:
            d_vi.append([self._em.index(oem.key), self._fg.index(oem.parent.key), oem.value])

        return d_i, d_ii, d_iii, d_iv, d_v, d_vi
