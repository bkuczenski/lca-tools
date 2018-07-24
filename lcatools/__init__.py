from .from_json import from_json, to_json
from .basic_query import BasicQuery, LcQuery
from .interfaces import directions, comp_dir


def enum(_listss):
    ret = []
    for k, v in enumerate(_listss):
        print(' [%02d] %s' % (k, v))
        ret.append(v)
    return ret
