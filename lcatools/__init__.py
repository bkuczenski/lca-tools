from .from_json import from_json, to_json
from .interfaces import directions, comp_dir
from .archives import archive_from_json, archive_factory
import re


__all__ = ['archive_factory', 'archive_from_json', 'directions', 'comp_dir', 'from_json',
           'to_json', 'enum']


def enum(_listss, filt=None, invert=True):
    """
    Enumerate an iterable for interactive use. return it as a list. Optional negative filter supplied as regex
    :param _listss:
    :param filt:
    :param invert: [True] sense of filter. note default is negative i.e. to screen *out* matches
     (the thinking is that input is already positive-filtered)
    :return:
    """
    ret = []
    if filt is not None:
        if invert:
            _iter = filter(lambda x: not bool(re.search(filt, str(x), flags=re.I)), _listss)
        else:
            _iter = filter(lambda x: bool(re.search(filt, str(x), flags=re.I)), _listss)
    else:
        _iter = _listss
    for k, v in enumerate(_iter):
        print(' [%02d] %s' % (k, v))
        ret.append(v)
    return ret
