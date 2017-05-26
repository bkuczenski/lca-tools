"""
Deals with ILCD namespaces

"""

import re


def find_ns(nsmap, dtype):
    return next((k for k, v in nsmap.items() if re.search(dtype + '$', v)))


def find_tag(o, tag, ns=None):
    """
    :param o: objectified element
    :param tag:
    :return:
    """
    found = o.find('.//{0}{1}'.format('{' + o.nsmap[ns] + '}', tag))
    return '' if found is None else found


def find_tags(o, tag, ns=None):
    """
    :param o: objectified element
    :param tag:
    :return:
    """
    found = o.findall('.//{0}{1}'.format('{' + o.nsmap[ns] + '}', tag))
    return [''] if len(found) == 0 else found


def find_common(o, tag):
    return find_tag(o, tag, ns='common')
