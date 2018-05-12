"""
Replacement for Background Manager that uses static, flat backgrounds
"""

from .flat_background import FlatBackground


class BackgroundManager(object):
    def __init__(self, index_interface, flat_file, quiet=True):
        self._be = FlatBackground.from_matfile(flat_file, quiet=quiet)
        self._ix = index_interface
