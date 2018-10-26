from .abstract_query import AbstractQuery


class IndexRequired(Exception):
    pass


directions = ('Input', 'Output')


def check_direction(dirn):
    if isinstance(dirn, str):
        dirn = dirn[0].lower()
    return {0: 'Input',
            1: 'Output',
            'i': 'Input',
            'o': 'Output'}[dirn]


class InvalidDirection(Exception):
    pass


def comp_dir(direction):
    if direction is None:
        return None
    try:
        _dirn = check_direction(direction)
    except KeyError:
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
        return self._perform_query(_interface, 'count', IndexRequired('Index access required'), entity_type, **kwargs)

    def processes(self, **kwargs):
        """
        Generate process entities (reference exchanges only)
        :param kwargs: keyword search
        :return:
        """
        for i in self._perform_query(_interface, 'processes', IndexRequired('Index access required'), **kwargs):
            yield self.make_ref(i)

    def flows(self, **kwargs):
        """
        Generate flow entities (reference quantity only)
        :param kwargs: keyword search
        :return:
        """
        for i in self._perform_query(_interface, 'flows', IndexRequired('Index access required'), **kwargs):
            yield self.make_ref(i)

    def quantities(self, **kwargs):
        """
        Generate quantities
        :param kwargs: keyword search
        :return:
        """
        '''
        # we're abandoning this behavior of dubious utility
        try:
            return self._perform_query(self.interface, 'quantities', CatalogRequired('Catalog access required'),
                                       **kwargs)
        except CatalogRequired:
            return self._perform_query('quantity', 'quantities', CatalogRequired('Catalog or Quantity access required'),
                                       **kwargs)
        '''
        for i in self._perform_query(_interface, 'quantities', IndexRequired('Index access required'), **kwargs):
            yield self.make_ref(i)

    def lcia_methods(self, **kwargs):
        """
        Generate LCIA Methods-- which are quantities that have defined indicators
        :param kwargs:
        :return:
        """
        indicator = kwargs.pop('Indicator', '')
        return self.quantities(Indicator=indicator, **kwargs)

    def fragments(self, show_all=False, **kwargs):
        if show_all:
            raise ValueError('Cannot retrieve non-parent fragments via interface')
        for i in self._perform_query(_interface, 'fragments', IndexRequired('Index access required'), **kwargs):
            yield self.make_ref(i)

    """
    API functions- entity-specific -- get accessed by catalog ref
    index interface
    """
    def terminate(self, flow, direction=None, **kwargs):
        """
        Find processes that match the given flow and have a complementary direction
        :param flow:
        :param direction: if omitted, return all processes having the given flow as reference, regardless of direction
        :return:
        """
        for i in self._perform_query(_interface, 'terminate', IndexRequired('Index access required'),
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
