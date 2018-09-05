from .bm_static import TarjanBackground
from .flat_background import FlatBackground, TermRef
from .implementation import TarjanBackgroundImplementation

import os


__all__ = ['init_fcn', 'tarjan_background']


def init_fcn(source, ref=None, **kwargs):
    if ref is None:
        ref = 'test.free.background'
    return TarjanBackground(source, ref=ref, **kwargs)


def tarjan_background(index, source=None, **kwargs):
    """
    Create a standalone Background Implementation based on a supplied resource.  In order to construct the technology
    matrix, full inventory access is required, and the index implementation must provide 'processes', 'get', 'count'
    (processes), and 'terminate'.

    If a source file is specified, the background will be serialized to the named file after it is created.  If the
    source file already exists, then it will be restored from the saved file.

    :param index: data resource providing index interface.  If the background has not been generated, then the index
     must return entity references that can provide full inventory information.
    :param source: optional file to load/save ordered matrices (if present and exists, will load; if present and not
     exists, will save after construction)
    :return:
    """
    if os.path.exists(source):
        return TarjanBackgroundImplementation.from_file(index, source, **kwargs)
    else:
        tb = TarjanBackground(source, save_after=True, **kwargs)
        bg = tb.make_interface('background')
        bg.setup_bm(index.make_interface('index'))
