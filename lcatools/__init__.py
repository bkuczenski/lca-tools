from lcatools.from_json import from_json
from lcatools.catalog import LcCatalog


def enum(_listss):
    ret = []
    for k, v in enumerate(_listss):
        print(' [%02d] %s' % (k, v))
        ret.append(v)
    return ret
