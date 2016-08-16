from collections import defaultdict


class BackgroundError(Exception):
    pass


class BgLciaCache(object):
    def __init__(self):
        self._ref_flow = None
        self._exchange_ref = None

    @property
    def exchange(self):
        return self._exchange_ref

    @exchange.setter
    def exchange(self, value):
        if self._exchange_ref is not None:
            raise BackgroundError('Exchange already set!')
        else:
            self._exchange_ref = value
            self._ref_flow = value.exchange.flow

    def bg_lookup(self, q, location=None, flowdb=None):
        if self._ref_flow.factor(q) is not None:
            return self._ref_flow.factor(q)[location]
        else:
            if location is None:
                location = self._exchange_ref.exchange.process['SpatialScope']
            archive = self._exchange_ref.catalog[self._exchange_ref.index]
            result = archive.bg_lookup(self._exchange_ref.exchange.process,
                                       ref_flow=self._ref_flow,
                                       quantities=[q],
                                       location=location,
                                       flowdb=flowdb)
            factor = result.factor(q)
            if factor is not None:
                self._ref_flow.add_characterization(q, value=factor[location], location=location)
                return factor[location]
            return None

    def serialize(self):
        return {
            "source": self._exchange_ref.catalog.source_for_index(self._exchange_ref.index),
            "process": self._exchange_ref.exchange.process.get_uuid(),
            "flow": self._exchange_ref.exchange.flow.get_uuid(),
            "direction": self._exchange_ref.exchange.direction
        }

    def __str__(self):
        return '(%s) %s %s' % (self._exchange_ref.catalog.name(self._exchange_ref.index),
                               self._exchange_ref.exchange.direction,
                               self._exchange_ref.exchange.process)


class BgReference(object):
    """
    A BG Reference is a dict that translates geography to exchange.
    The way it works is:
     - a flow is specified as a background flow- so it's added to the foreground's _background dict.
     - once identified as a background, the BgReference can be given different terminations for different
       geographies.  The termination is created based on the incoming flow's geography.
     - A termination is a CatalogRef and an exchange
     - The BgReference computes LCIA scores by catalog lookup using the bg_lookup method. It can cache these if they
       turn out to be slow.
     - if the background flow instance's direction is opposite the stored ExchangeRef's direction, then the sign of
       the LCIA result is inverted.

    This could turn out to be a tremendous error.
    """
    def __init__(self):
        self._geog = defaultdict(BgLciaCache)

    def add_bg_termination(self, location, exchange):
        if location in self._geog.keys():
            raise BackgroundError('Location already terminated for this background flow')
        self._geog[location].exchange = exchange

    def lookup_bg_lcia(self, location, q, flowdb=None):
        return self._geog[location].bg_lookup(q, location=location, flowdb=flowdb)

    def serialize(self):
        return {k: v.serialize() for k, v in self._geog.items()}

    def __str__(self):
        return '\n'.join(['%s: %s' % (k, v) for k, v in self._geog.items()])


