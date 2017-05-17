import re
import gzip
import json

from eight import USING_PYTHON2


def from_json(fname):
    """
    Routine to extract the contents of a json file.
    :param fname: json file, optionally gzipped
    :return: a json-derived dict
    """
    print('Loading JSON data from %s:' % fname)
    if bool(re.search('\.gz$', fname)):
        if USING_PYTHON2:
            with gzip.open(fname, 'r') as fp:
                j = json.load(fp)
        else:
            with gzip.open(fname, 'rt') as fp:
                j = json.load(fp)
    else:
        with open(fname, 'r') as fp:
            j = json.load(fp)
    return j
