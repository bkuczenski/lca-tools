"""
LcArchive subclass that supports rich background computations by providing a FlatBackground implementation.
"""

import os

from lcatools.archives import LcArchive, InterfaceError
from .flat_background import FlatBackground, SUPPORTED_FILETYPES
from .implementation import TarjanBackgroundImplementation


class TarjanBackground(LcArchive):

    def __init__(self, source, save_after=False, **kwargs):
        self._save_after = save_after
        filetype = os.path.splitext(source)[1]
        if filetype not in SUPPORTED_FILETYPES:
            raise ValueError('Unsupported filetype %s' % filetype)

        '''
        if not source.endswith(self._filetype):
            source += self._filetype
        '''

        super(TarjanBackground, self).__init__(source, **kwargs)

        if os.path.exists(source):  # flat background already stored
            self._flat = FlatBackground.from_file(source)
        else:
            self._flat = None

    def make_interface(self, iface, privacy=None):
        if iface == 'background':
            return TarjanBackgroundImplementation(self)
        else:
            raise NotImplementedError('%s: This class can only implement the background interface' % iface)

    def create_flat_background(self, index):
        """
        Create an ordered background, save it, and instantiate it as a flat background
        :param index: index interface to use for the engine
        :return:
        """
        if self._flat is None:
            self._flat = FlatBackground.from_index(index)
            self._add_name(index.origin, self.source, rewrite=True)
            if self._save_after:
                self.write_to_file()  # otherwise, the user / catalog must explicitly request it
        return self._flat

    def write_to_file(self, filename=None, gzip=False, complete=True, **kwargs):
        if filename is None:
            filename = self.source
        self._flat.write_to_file(filename, complete=complete, **kwargs)
