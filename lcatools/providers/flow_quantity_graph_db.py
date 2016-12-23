column_mapping = {
    'method': ('Tag', 'Quantity'),
    'category': ('Tag', 'Quantity'),
    'indicator': ('Tag', 'Quantity'),
    'name': ('Property', 'Flowable'),
    'compartment': ('Compartment', 'Flow'),
    'subcompartment': ('Compartment', 'Flow'),
    'CF 3.01': ('Tag', 'Factor'),
    'CF 3.1': ('Tag', 'Factor'),
    'Known issue': ('Tag', 'Factor')
}

from lcatools.interface import FlowQuantityInterface
from py2neo import Graph, Schema, Node, Relationship


class FlowQuantityGraphDb(FlowQuantityInterface):
    """
    A F-Q interface using a neo4j backend for graph-mazement
    """
    def __init__(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:
        """
        graph = Graph(*args, **kwargs)
        super(FlowQuantityGraphDb, self).__init__(graph)
