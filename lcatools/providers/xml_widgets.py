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


def _convert_variable_ref(el):
    return re.sub('{{([\w]+)}}', '%(\\1)s', str(el))


def render_text_block(el, ns=None):
    """
    Created for EcoSpold2.  Take all the children of an objectified element and render them as a text block. Implement
    variable substitution (!!!)
    :param el: Element with tags including 'text' and 'variable'
    :param ns: [None] namespace
    :return:
    """
    variables = {a.attrib['name']: a.text for a in find_tags(el, 'variable', ns=ns)}
    texts = [_convert_variable_ref(t) % variables
             for t in sorted(find_tags(el, 'text', ns=ns), key=lambda a: a.attrib['index'])]
    return '\n'.join(texts)
