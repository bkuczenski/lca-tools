
from lcatools.providers.base import LcArchive
from lcatools.entities import LcFlow
import json
import os


class ForegroundError(Exception):
    pass


FG_TEMPLATE = os.path.join(os.path.dirname(__file__), 'data', 'foreground_template.json')


class ForegroundArchive(LcArchive):
    """
    A foreground archive stores entities used within the foreground of a fragment model.  Its main useful
    trick is to allow the user to easily create new flows.

    Foreground archives are not supposed to have upstreams-- instead, the user finds entities in the catalog and
     adds them to the foreground archive.  If those entities are found "correctly", they should arrive with their
     characterizations / exchanges intact.

    When a foreground is used in an antelope instance, only the contents of the foreground archive are exposed.
     This means that the foreground archive should include all flows that are used in fragment flows, and all
     processes that terminate fragment flows.
    """
    @classmethod
    def new(cls, directory):
        """
        Create a new foreground and store it in the specified directory. The foreground is pre-loaded with about
        20 quantities (drawn from the ILCD collection) for easy use.
        :param directory:
        :return:
        """
        if not os.path.isdir(directory):
            try:
                os.makedirs(directory)
            except PermissionError:
                print('Permission denied.')

        if not os.path.isdir(directory):
            raise ForegroundError('Must provide a working directory.')

        c = cls(directory)
        with open(FG_TEMPLATE, 'r') as fp:
            j = json.load(fp)

        for q in j['quantities']:
            c.entity_from_json(q)
        return c

    def __init__(self, ref, upstream=None, quiet=False, **kwargs):
        if upstream is not None:
            raise ForegroundError('Foreground archive not supposed to have an upstream')
        super(ForegroundArchive, self).__init__(ref, quiet=quiet, **kwargs)

    def create_flow(self, flowname, ref_quantity):
        pass
