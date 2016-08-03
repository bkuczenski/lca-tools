import re


def find_ns(nsmap, dtype):
    return next((k for k, v in nsmap.items() if re.search(dtype + '$', v)))


def find_tag(o, tag, ns=None):
    """
    Deals with the fuckin' ILCD namespace shit
    :param o: objectified element
    :param tag:
    :return:
    """
    found = o.findall('.//{0}{1}'.format('{' + o.nsmap[ns] + '}', tag))
    return [''] if len(found) == 0 else found


def find_common(o, tag):
    return find_tag(o, tag, ns='common')
