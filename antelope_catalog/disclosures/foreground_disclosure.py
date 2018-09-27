from .disclosure import ObservedFlow, RX, Disclosure


class ObservedForegroundFlow(ObservedFlow):
    def __init__(self, exch, key, locale):
        self._exch = exch
        self._key = key
        self._locale = locale

    @property
    def key(self):
        return self._key

    @property
    def value(self):
        return self._exch.value

    @property
    def flow(self):
        return self._exch.flow

    @property
    def direction(self):
        return self._exch.direction

    @property
    def locale(self):
        return self._locale

    def __repr__(self):
        return 'ObservedForegroundFlow(Parent: %s, Term: %s: Value: %g)' % (self.parent.key,
                                                                            self.key,
                                                                            self.value)

    def __str__(self):
        return str(self._exch)


class ObservedBackgroundFlow(ObservedForegroundFlow):
    def __init__(self, exch, query):
        self._term = query.get(exch.termination)
        super(ObservedBackgroundFlow, self).__init__(exch, exch.key, self._term['SpatialScope'])

    @property
    def bg_key(self):
        return self._term, self._exch.flow


class ObservedEmissionFlow(ObservedForegroundFlow):
    def __init__(self, exch):
        super(ObservedEmissionFlow, self).__init__(exch, exch.key, exch.process['SpatialScope'])

    @property
    def key(self):
        return self.flow, self.direction


class ForegroundDisclosure(Disclosure):
    def __init__(self, query, *refs):
        super(ForegroundDisclosure, self).__init__()

        self._query = query

        self._exchs_gen = (x for ref in refs for x in query.foreground(ref.external_ref))
        self._exchs = []

        self._parent_map = dict()

    def __next__(self):
        return self.next_exch()

    def _add_foreground_deps_ems(self, parent, term):
        self._parent_map[term] = parent
        for x in self._query.dependencies(term):
            bg = ObservedBackgroundFlow(x, self._query)
            bg.observe(parent)
            self._add_background(bg)

        for x in self._query.emissions(term):
            em = ObservedEmissionFlow(x)
            em.observe(parent)
            self._add_emission(em)

    def next_exch(self):
        x = next(self._exchs_gen)
        self._exchs.append(x)

        if x.process.external_ref in self._parent_map:
            ss = self._query.get_item(x.termination, 'SpatialScope')
            off = ObservedForegroundFlow(x, x.termination, ss)
            parent = self._parent_map[x.process.external_ref]
            off.observe(parent)

            if x.termination in self._parent_map:
                self._Af.append(off)
            else:
                self._add_foreground(off)
                self._add_foreground_deps_ems(off, x.termination)

        else:
            off = ObservedForegroundFlow(x, x.process.external_ref, x.process['SpatialScope'])
            print('Adding rx')
            off.observe(RX)
            self._add_foreground(off)
            self._add_foreground_deps_ems(off, off.key)
