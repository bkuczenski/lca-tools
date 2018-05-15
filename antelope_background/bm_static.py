"""
LcArchive subclass that supports rich background computations by providing a FlatBackground implementation.
"""

import os

from lcatools.providers import LcArchive
from .flat_background import FlatBackground
from .implementation import TarjanBackgroundImplementation


class TarjanBackground(LcArchive):
    def __init__(self, source, ref=None, **kwargs):
        super(TarjanBackground, self).__init__(source, ref=ref, **kwargs)

        if os.path.exists(source):  # flat background already stored
            self._flat = FlatBackground.from_file(source)
        else:
            self._flat = None

    def make_interface(self, iface, privacy=None):
        if iface == 'background':
            return TarjanBackgroundImplementation(self)
        else:
            raise AttributeError('This class can only implement the background interface')

    def create_flat_background(self, index):
        """
        Create an ordered background, save it, and instantiate it as a flat background
        :param index: index interface to use for the engine
        :return:
        """
        if self._flat is None:
            self._flat = FlatBackground.from_index(index)
            self._add_name(index.origin, self.source, rewrite=True)
        return self._flat

    def write_to_file(self, filename=None, gzip=False, complete=False, **kwargs):
        if filename is None:
            filename = self.source
        self._flat.write_to_file(filename, complete=complete, **kwargs)
