

class Emission(object):
    """
    Class for storing exchange information about a single row in the exterior (cutoff) matrix.

    """
    def __init__(self, index, flow, direction, context):
        """
        Initialize a row+column in the technology matrix.  Each row corresponds to a reference exchange in the database,
        and thus represents a particular process generating / consuming a particular flow.  A ProductFlow entry is
        akin to a fragment termination.

        :param flow: the LcFlow entity that represents the commodity (term_flow in the fragment sense)
        :param direction: the direction of the exchange
        """
        self._index = index
        self._flow = flow
        self._direction = direction
        self._context = context

        self._hash = (flow.external_ref, direction, self._context)

    def __eq__(self, other):
        """
        shortcut-- allow comparisons without dummy creation
        :param other:
        :return:
        """
        return hash(self) == hash(other)
        # if not isinstance(other, ProductFlow):
        #    return False
        # return self.flow == other.flow and self.process == other.process

    def __hash__(self):
        return hash(self._hash)

    @property
    def entity_type(self):
        return 'emission'

    @property
    def index(self):
        return self._index

    @property
    def key(self):
        """
        Key is (uuid of flow, direction relative to 'emitting' process)
        :return:
        """
        return self._hash

    @property
    def flow(self):
        return self._flow

    @property
    def context(self):
        if self._context is None:
            return None
        return self._context.name

    @property
    def direction(self):
        return self._direction

    def __str__(self):
        return '%s: %s [%s]' % (self._direction, self._flow['Name'], self.context)

    def table_label(self, concise=False):
        """

        :param concise: [False] omit the compartment
        :return:
        """
        if concise:
            return '%s: %s (%s)' % (self._direction, self._flow['Name'], self._flow.unit)
        return str(self)
