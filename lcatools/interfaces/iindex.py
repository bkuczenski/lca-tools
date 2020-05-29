from .abstract_query import AbstractQuery


class IndexRequired(Exception):
    pass


directions = ('Input', 'Output')


class InvalidDirection(Exception):
    pass


def check_direction(dirn):
    _dir = dirn  # needed only in case of exception
    if isinstance(dirn, str):
        dirn = dirn[0].lower()
    try:
        return {0: 'Input',
                1: 'Output',
                'i': 'Input',
                'o': 'Output'}[dirn]
    except KeyError:
        raise InvalidDirection(_dir)


class InvalidSense(Exception):
    pass


def valid_sense(sense):
    if sense is None:
        return None
    try:
        v = {'source': 'Source',
             'sink': 'Sink'}[sense.lower()]
    except KeyError:
        raise InvalidSense(sense)
    return v


def comp_dir(direction):
    if direction is None:
        return None
    try:
        _dirn = check_direction(direction)
    except InvalidDirection:
        try:
            return {'Source': 'Input',
                    'Sink': 'Output'}[valid_sense(direction)]
        except InvalidSense:
            raise InvalidDirection('%s' % direction)
    return next(k for k in directions if k != _dirn)


_interface = 'index'


class IndexInterface(AbstractQuery):
    """
    CatalogInterface core methods
    These are the main tools for describing information about the contents of the archive
    """
    def count(self, entity_type, **kwargs):
        """
        Return a count of the number of entities of the named type
        :param entity_type:
        :param kwargs:
        :return: int
        """
        return self._perform_query(_interface, 'count', IndexRequired, entity_type, **kwargs)

    def processes(self, **kwargs):
        """
        Generate process entities (reference exchanges only)
        :param kwargs: keyword search
        :return:
        """
        for i in self._perform_query(_interface, 'processes', IndexRequired, **kwargs):
            yield self.make_ref(i)

    def flows(self, **kwargs):
        """
        Generate flow entities (reference quantity only)
        :param kwargs: keyword search
        :return:
        """
        for i in self._perform_query(_interface, 'flows', IndexRequired, **kwargs):
            yield self.make_ref(i)

    def synonyms(self, item, **kwargs):
        """
        Return a list of synonyms for the object -- quantity, flowable, or compartment
        :param item:
        :return: list of strings
        """
        return self._perform_query(_interface, 'synonyms', IndexRequired, item,
                                   ** kwargs)

    def flowables(self, **kwargs):
        """
        Generate known flowables by their canonical name
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'flowables', IndexRequired,
                                   ** kwargs)

    def contexts(self, **kwargs):
        """
        Generate known contexts as tuples of canonical names
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'contexts', IndexRequired,
                                   ** kwargs)

    def get_context(self, term, **kwargs):
        """
        Return the context matching the specified term
        :param term:
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'get_context', IndexRequired,
                                   term, ** kwargs)

    def quantities(self, **kwargs):
        """
        Generate quantities
        :param kwargs: keyword search
        :return:
        """
        for i in self._perform_query(_interface, 'quantities', IndexRequired, **kwargs):
            yield self.make_ref(i)

    def lcia_methods(self, **kwargs):
        """
        Generate LCIA Methods-- which are quantities that have defined indicators
        :param kwargs:
        :return:
        """
        indicator = kwargs.pop('Indicator', '')
        return self.quantities(Indicator=indicator, **kwargs)

    """
    API functions- entity-specific -- get accessed by catalog ref
    index interface
    """
    def unmatched_flows(self, flows, **kwargs):
        """
        Takes in a list of flowable terms and generates a sublist of flows that were not recognized as synonyms to any
        local flows.
        :param flows: iterable
        :param kwargs:
        :return:
        """
        return self._perform_query(_interface, 'unmatched_flows', IndexRequired,
                                   flows, **kwargs)

    def terminate(self, flow, direction=None, **kwargs):
        """
        Find processes that match the given flow and have a complementary direction
        :param flow:
        :param direction: if omitted, return all processes having the given flow as reference, regardless of direction
        :return:
        """
        for i in self._perform_query(_interface, 'terminate', IndexRequired,
                                     flow, direction=direction, **kwargs):
            yield self.make_ref(i)

    def originate(self, flow, direction=None, **kwargs):
        """
        Find processes that match the given flow and have the same direction
        :param flow:
        :param direction: if omitted, return all processes having the given flow as reference, regardless of direction
        :return:
        """
        '''
        for i in self._perform_query(_interface, 'originate', IndexRequired('Index access required'),
                                     flow, direction=direction, **kwargs):
            yield self.make_ref(i)
        '''
        for i in self.terminate(flow, comp_dir(direction), **kwargs):  # just gets flipped back again in terminate()
            yield i
    '''
    def mix(self, flow, direction, **kwargs):
        """
        Create a mixer process whose inputs are all processes that terminate the given flow and direction
        :param flow:
        :param direction:
        :return:
        """
        return self._perform_query(_interface, 'mix', IndexRequired('Index access required'),
                                   flow, direction, **kwargs)
    '''
