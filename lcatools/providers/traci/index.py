from lcatools.implementations import IndexImplementation


class Traci21IndexImplementation(IndexImplementation):
    """
    The main purpose of this is to enable iteration over elements without requiring load_all()
    No need to override lcia methods or quantities- since those all get initialized with the constructor--
    only flowables needs to be init from scratch-- but not right now
    """
    pass
