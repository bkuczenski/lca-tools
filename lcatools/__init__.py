from .from_json import from_json, to_json
from .basic_query import BasicQuery, LcQuery
from .interfaces import directions, comp_dir
from .archives import archive_from_json, archive_factory


__all__ = ['archive_factory', 'archive_from_json', 'BasicQuery', 'LcQuery', 'directions', 'comp_dir', 'from_json',
           'to_json', 'enum']


def enum(_listss):
    ret = []
    for k, v in enumerate(_listss):
        print(' [%02d] %s' % (k, v))
        ret.append(v)
    return ret
