from .abstract_query import AbstractQuery


class IndexRequired(Exception):
    pass


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
        return self._perform_query(_interface, 'count', IndexRequired('Index access required'), **kwargs)

    def processes(self, **kwargs):
        """
        Generate process entities (reference exchanges only)
        :param kwargs: keyword search
        :return:
        """
        return self._perform_query(_interface, 'processes', IndexRequired('Index access required'), **kwargs)

    def flows(self, **kwargs):
        """
        Generate flow entities (reference quantity only)
        :param kwargs: keyword search
        :return:
        """
        return self._perform_query(_interface, 'flows', IndexRequired('Index access required'), **kwargs)

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
        return self._perform_query(_interface, 'quantities', IndexRequired('Index access required'), **kwargs)

    def fragments(self, show_all=False, **kwargs):
        if show_all:
            raise ValueError('Cannot retrieve non-parent fragments via interface')
        return self._perform_query(_interface, 'fragments', IndexRequired('Index access required'), **kwargs)

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
        return self._perform_query(_interface, 'terminate', IndexRequired('Index access required'),
                                   flow, direction=direction, **kwargs)

    def originate(self, flow, direction=None, **kwargs):
        """
        Find processes that match the given flow and have the same direction
        :param flow:
        :param direction: if omitted, return all processes having the given flow as reference, regardless of direction
        :return:
        """
        return self._perform_query(_interface, 'originate', IndexRequired('Index access required'),
                                   flow, direction=direction, **kwargs)

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
