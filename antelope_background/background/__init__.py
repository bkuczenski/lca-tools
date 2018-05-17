from .bm_static import TarjanBackground
from .flat_background import FlatBackground, TermRef


def init_fcn(source, ref=None, **kwargs):
    if ref is None:
        ref = 'test.free.background'
    return TarjanBackground(source, ref=ref, **kwargs)
