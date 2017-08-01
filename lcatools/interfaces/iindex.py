from .abstract_query import AbstractQuery


class IndexRequired(Exception):
    pass


class IndexInterface(AbstractQuery):
    _interface = 'index'
    """
    CatalogInterface core methods
    These are the main tools for describing information about the contents of the archive
    """
    def processes(self, **kwargs):
        """
        Generate process entities (reference exchanges only)
        :param kwargs: keyword search
        :return:
        """
        return self._perform_query(self.interface, 'processes', IndexRequired('Index access required'), **kwargs)

    def flows(self, **kwargs):
        """
        Generate flow entities (reference quantity only)
        :param kwargs: keyword search
        :return:
        """
        return self._perform_query(self.interface, 'flows', IndexRequired('Index access required'), **kwargs)

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
        return self._perform_query(self.interface, 'quantities', IndexRequired('Index access required'), **kwargs)

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
        return self._perform_query(self.interface, 'terminate', IndexRequired('Index access required'),
                                   flow, direction=direction, **kwargs)

    def originate(self, flow, direction=None, **kwargs):
        """
        Find processes that match the given flow and have the same direction
        :param flow:
        :param direction: if omitted, return all processes having the given flow as reference, regardless of direction
        :return:
        """
        return self._perform_query(self.interface, 'originate', IndexRequired('Index access required'),
                                   flow, direction=direction, **kwargs)

    def mix(self, flow, direction, **kwargs):
        """
        Create a mixer process whose inputs are all processes that terminate the given flow and direction
        :param flow:
        :param direction:
        :return:
        """
        return self._perform_query(self.interface, 'mix', IndexRequired('Index access required'),
                                   flow, direction, **kwargs)
