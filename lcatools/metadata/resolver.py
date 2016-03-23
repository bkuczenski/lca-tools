
import re
uri_reg = re.compile('^(\w*:)?(.*)$')


def uri_re(uri):
    """
    Get prefix and path from a URI (leaving out the ':'.

    :param uri:
    :return:
    """
    p, q = uri_reg.match(uri).groups()
    if p is not None:
        p = p[:-1].lower()
    return p, q


class LcResolve(object):
    """
    Reference resolver for LC data.  This tool returns an object that implements the requested
    interface.  This tool would also be an ideal place to locate an interface validator.

    This could conceivably be extended with an archiving resolver, i.e. a catalog
    """

    def __init__(self, root='/' ):
        """

        :param root: data jail
        :return:
        """
        self.data_jail = root

    def resolve_provider(self, uri):
        """
        The eponymous resolver.  The URI format should match the following regex:

        '^(\w*):(\w*:)?(.*)$'

        Where subexpression 1 is the interface format,
              subexpression 2 is the access mdoe (defaults to file)
              subexpression 3 is the path

        Returns a local provider name. The provider then implements the providers on its
        own.  The providers themselves have yet to be defined...

        valid interface  are files (modules) in the providers directory

        :param uri:
        :return:000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
        """
        prefix, path = uri_re(uri)
        if prefix is None:
            prefix = 'file'
