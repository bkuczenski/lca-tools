from .providers import TarjanBackground
from .background import TarjanBackgroundImplementation
import os

__all__ = ['init_fcn', 'tarjan_background']


def init_fcn(source, ref=None, **kwargs):
    """
    Returns a TarjanBackground archive which can produce a TarjanBackgroundImplementation using
    make_interface('background').  Note that setup_bm() must be called on the resulting implementation- this will
    trigger the Tarjan traversal if it has not been already stored.
    :param source: Filename to store the serialized A and B matrices in numpy [matlab] format.
    :param ref: semantic reference for the archive
    :param kwargs:
    :return: a TarjanBackground archive.
    """
    if ref is None:
        ref = 'test.free.background'
    return TarjanBackground(source, ref=ref, **kwargs)  # make_interface('background') to generate/access flat bg


def tarjan_background(res, source=None, **kwargs):
    """
    Create a standalone Background Implementation based on a supplied resource.  In order to construct the technology
    matrix, full inventory access is required, and the index implementation must provide 'processes', 'get', 'count'
    (processes), and 'terminate'.

    If a source file is specified, the background will be serialized to the named file after it is created.  If the
    source file already exists, then it will be restored from the saved file.

    :param res: data resource providing index interface.  If the background has not been generated, then the index
     must return entity references that can also provide full inventory information.
    :param source: optional file to load/save ordered matrices (if present and exists, will load; if present and not
     exists, will save after construction)
    :return:
    """
    if os.path.exists(source):
        return TarjanBackgroundImplementation.from_file(res, source, **kwargs)
    else:
        tb = TarjanBackground(source, save_after=True, **kwargs)
        bg = tb.make_interface('background')
        bg.setup_bm(res.make_interface('index'))
        return bg
