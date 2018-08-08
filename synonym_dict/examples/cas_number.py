"""
accept a variety of inputs; store synonyms for a CAS number of the canonical form xxxxxx-xx-x
"""

import re
from ..synonym_set import SynonymSet


cas_regex = re.compile('^0*([0-9]{2,6})-?([0-9]{2})-?([0-9])$')


class InvalidCasNumber(Exception):
    """
    raised when input cannot be deciphered as a valid CasNumber
    """


class NotSupported(Exception):
    pass


def _validate_numeric_input(cas):
    if cas < 10**4 or cas >= 10**9:
        raise InvalidCasNumber('Numeric input out of range')
    tup2 = cas % 10
    cas = int((cas - tup2) / 10)

    tup1 = cas % 100

    tup0 = int((cas - tup1) / 100)
    return str(tup0), str(tup1), str(tup2)


def _validate_string_input(cas):
    match = cas_regex.match(cas)
    if match is None:
        raise InvalidCasNumber('String does not match regex')
    return match.groups()


def _validate_tuple_input(tup):
    x = [int(k) for k in tup]
    y = '%d' % x[0], '%02d' % x[1], '%d' % x[2]
    if not bool(cas_regex.match('-'.join(y))):
        raise InvalidCasNumber('Tuple input does not match regex')
    return y


def _generate_cas_formats(tup):
    pad_tup = ('%06d' % int(float(tup[0])), tup[1], tup[2])
    for k in (tup, pad_tup):
        yield '-'.join(k)
        yield ''.join(k)


class CasNumber(SynonymSet):
    def __init__(self, *args):
        """
        Accept the following forms:
         - integer satisfying 10**4 <= x <= 10**8 (124389 = 124-38-9 Carbon Dioxide)
         - string matching the cas_regex '^0*([0-9]{2,6})-?([0-9]{2})-?([0-9])$'
         - 3-tuple containing either number or string having (6 or fewer digits, 2 digits, 1 digit)
         - 3 positional arguments that make up a 3-tuple according to above

        Produces a synonym set containing both zero-padded and non-zero-padded CAS number strings, both with and without
        hyphens.  e.g. above input would yield {'000124-38-9', '000124389', '124-38-9', '124389'}
        :param cas:
        """
        if len(args) == 1:
            cas = args[0]
        else:
            cas = tuple(args)

        try:
            casnumber = int(cas)
            _tup = _validate_numeric_input(casnumber)
        except (TypeError, ValueError):
            if isinstance(cas, str):
                _tup = _validate_string_input(cas)
            else:
                _tup = _validate_tuple_input(cas)

        super(CasNumber, self).__init__(*_generate_cas_formats(_tup))

    def set_name(self, name):
        raise NotSupported('May not change canonical name for CAS numbers')
