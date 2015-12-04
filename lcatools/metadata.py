"""
Classes for handling metadata about LCA entities.

Ultimately this will be the storehouse for Semantic Web functions to
dereference search terms or IDs to data sets

"""

import pandas as pd

class ProcessTable(pd.DataFrame):
    """
    A ProcessTable is a pandas DataFrame that stores a list of references
    to Process Instance datasets.

    The ref
    """
