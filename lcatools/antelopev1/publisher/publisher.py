"""
Antelope Publisher

This creates a StudyPublication which is required to seed the AntelopeV1Server.
"""

class AntelopePublisher(object):
    """

    """

    def __init__(self, fragment, hints=None):
        """

        :param fragment: an LcFragment entity or FragmentRef that forms the top-level study fragment
        :param hints: dict that maps entity types to ordered lists of UUIDs-- these lists are then used to map UUIDs to
         sequential indices
        """
