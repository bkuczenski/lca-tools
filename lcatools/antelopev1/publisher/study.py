


class StudyPublication(object):
    """
    This is a data store that contains all the entities used in the foreground study and also keeps track of the
    scenarios and parameters specified by users.  As this matures, it may make sense to tie it to some kind of
    non-volatile RDBMS backend, but for now our objective is to get up and running in memory

    Later on there is plenty of thinking to do re: sql vs mongo vs neo4j vs redis.

    but given that our objective is to make something distributed, the backend architecture is inherently beside the
    point.

    the priority is to get the API running and tested.
    """
    def __init__(self, flowproperties, flows, processes, fragments, **kwargs):
        """
        A study publication is mostly static-- i.e. the sets of entities used in the study cannot be enlarged.

        The set of characterized LCIA methods, however, can be lengthened (though not shortened)

        For now, the study also stores a list of scenarios and parameters, but NOT users. The study doesn't handle
        authentication or authorization at all.  The AntelopeV1Server is responsible for authenticating users and
        mapping them to scenarios they are authorized to see / edit.
        """

        # define the different core entities:
        self._flows = list()
        self._flowproperties = list()
        self._processes = list()
        self._fragments = list()

        self._lciamethods = list()


ccccc



















































>?/////'' \
       '?{{{{{{{{{{{{{{{{{{{{{[[[[[[[[[[;.l,,,,,,,,,,,,,,'