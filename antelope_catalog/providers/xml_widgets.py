"""
Deals with ILCD namespaces

"""

import re
from collections import defaultdict


class XmlWidgetError(Exception):
    pass


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


def _convert_variable_ref(el):
    el = re.sub('%', '%%', str(el))
    return re.sub('{{([A-Za-z0-9_]+)}}', '%(\\1)s', el)


def render_text_block(el, ns=None):
    """
    Created for EcoSpold2.  Take all the children of an objectified element and render them as a text block. Implement
    variable substitution (!!!)
    :param el: Element with tags including 'text' and 'variable'
    :param ns: [None] namespace
    :return:
    """
    vs = find_tags(el, 'variable', ns=ns)
    variables = defaultdict(str)
    if vs != ['']:
        try:
            for a in vs:
                variables[a.attrib['name']] = a.text
            # variables = {a.attrib['name']: a.text for a in vs}
        except AttributeError:
            print(vs)
            raise
    ts = find_tags(el, 'text', ns=ns)
    texts = [_convert_variable_ref(t) % variables
             for t in sorted(ts, key=lambda _a: _a.attrib['index'])]
    return '\n'.join(texts)
