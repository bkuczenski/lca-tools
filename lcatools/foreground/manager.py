import os
import json


MANIFEST = ('catalog.json', 'entities.json', 'fragments.json', 'flows.json')

from lcatools.catalog import CatalogInterface
from lcatools.foreground.flow_database import LcFlows


class ForegroundManager(object):
    """
    The foreground manager is the one-liner that you load to start building and editing LCI foreground models.

    It consists of:
     * a catalog of LcArchives, of which the 0th one is a ForegroundArchive to store fragments;

     * a logical database of flows, which tracks observed exchange values, quantities, and characterization factors.

    A foreground is constructed from scratch by giving a directory specification. The directory is used for
    serializing the foreground; the same serialization can be used to invoke an Antelope instance.

    The directory contains:
      - catalog.json: a serialization of the catalog
      - entities.json: the foreground archive
      - fragments.json: serialized FragmentFlows
      - flows.json: a list of logical flows defined as a list of sets of synonymous catalog references

    The foreground manager directs the serialization process and writes the files, but the components serialize
    and de-serialize themselves.
    """

    def _create_new_catalog(self):
        pass

    def _load(self, item):
        with open(os.path.join(self._folder, item)) as fp:
            return json.load(fp)

    def __init__(self, folder):
        """

        :param folder: directory to store the foreground.
        """
        self._folder = folder
        if not os.path.isdir(folder):
            os.makedirs(folder)

        self._validate_catalog()  # results in 4 valid json files

        self._catalog = CatalogInterface.from_json(self._load('catalog.json'))
        self._flowdb = LcFlows.from_json(self._catalog, self._load('flows.json'))
