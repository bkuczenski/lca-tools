from ..interfaces import check_direction


class ExchangeRef(object):
    """
    Codifies the information required to define an exchange.  The supplied information could be either object or
    reference/link; it isn't specified.
    """
    is_reference = False

    def __init__(self, process, flow, direction, value=0, unit=None, termination=None, reference=None, **kwargs):
        """

        :param process:
        :param flow:
        :param direction:
        :param value:
        :param unit:
        :param termination:
        """
        self._node = process
        self._flow = flow
        self._dir = check_direction(direction)
        self._val = value
        if unit is None:
            if hasattr(self._flow, 'unit'):
                unit = self._flow.unit
            else:
                unit = ''
        self._unit = unit
        self._term = termination
        self.args = kwargs
        if reference is not None:
            self.is_reference = bool(reference)

    @property
    def process(self):
        return self._node

    @property
    def flow(self):
        return self._flow

    @property
    def direction(self):
        return self._dir

    @property
    def value(self):
        return self._val

    @property
    def termination(self):
        return self._term

    @property
    def unit(self):
        return self._unit

    def __getitem__(self, item):
        if isinstance(self._val, dict):
            return self._val[item]
        else:
            return 0.0

    def __str__(self):
        ds = {'Input': '<--',
              'Output': '==>'}[self._dir]
        if self._term is None:
            tt = ''
        else:
            tt = ' %s' % self._term
        if isinstance(self._val, dict):
            v = '{ #%d# }' % len(self._val)
        else:
            v = '%.3g' % self.value
        return '[ %s ] %s %s (%s) %s%s' % (self.process, ds, v, self.flow, self.unit, tt)
