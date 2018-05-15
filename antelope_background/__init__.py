import os

from .engine import Background


def init_fcn(source, **kwargs):
    if os.path.exists(source):
        print('Loading static background from %s' % source)
        return Background.from_file(source, **kwargs)
    else:
        return Background(source, **kwargs)
