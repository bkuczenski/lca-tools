"""
Classes for handling metadata about LCA entities.

Ultimately this will be the storehouse for Semantic Web functions to
dereference search terms or IDs to data sets

"""

import pandas as pd
from collections import namedtuple


class LcInstance(object):
    """
    A class for defining instances of entities represented in Fig1, namely:
     - activities
     - flows
     - quantities

    Class instances can be queried as to their properties.  All entities are defined by a UUID
    and an internal reference to a data source.  Typology of the data source is open for discussion;
    the internal reference will get passed to a resolver that is yet to be implemented.

    The reference is interpreted as a URI or pseudo-URI based on the prefix:path model
    using regex '^(\w*:)?(.*)$'. Default prefix is 'file:'

    Anyway, to create the object, you pass the data reference- then it queries the data source and implements
    a uniform interface to the data contained therein.

    The common elements of the interface are:
     - a UUID that the data source will recognize / can be recognized in the data source
     - a structured non-unique name to use for identification
     - a specification of some reference entity that is type-dependent
     - a comment
     - a set of properties that is instance-dependent

    Different types will be created that subclass LcInstance

    """
    def __init__(self, reference, *args, **kwargs):
        self._ref = reference
        self.UUID, self.Name = res







class ProcessTable(object):
    """
    A ProcessTable is a collection of unit process definitions that share a common database origin.
    The process table instance provides an interface to list process details, retrieve data sets or exchanges
    (if locally available), and export a static version of the table to CSV

    The ref
    """

