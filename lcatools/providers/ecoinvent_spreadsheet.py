from lcatools.interfaces import ProcessFlow
from lcatools.entities import LcProcess, LcFlow


class EcoinventSpreadsheet(ProcessFlow):
    """
    A class for implementing the ProcessFlow interface based on the contents of an ecoinvent
    "activity overview" spreadsheet
    """
