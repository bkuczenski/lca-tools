"""
Antelope Interface Definitions

The abstract classes in this sub-package define what information is made available via a stateless query to an Antelope
resource of some kind.  The interfaces must be instantiated in order to be used.  In the core package
"""

from .abstract_query import UnknownOrigin, PrivateArchive, EntityNotFound

from .iconfigure import ConfigureInterface
from .iexchange import ExchangeInterface, ExchangeRequired
from .iindex import IndexInterface, IndexRequired, directions, comp_dir, check_direction, valid_sense
from .ibackground import BackgroundInterface, BackgroundRequired
from .iquantity import QuantityInterface, QuantityRequired, NoFactorsFound, ConversionReferenceMismatch, FlowableMismatch

from .iforeground import ForegroundInterface, ForegroundRequired

from .flow_interface import FlowInterface

from os.path import splitext

from collections import namedtuple


class PropertyExists(Exception):
    pass


'''
Query classes
'''

class BasicQuery(IndexInterface, ExchangeInterface, QuantityInterface):
    def __init__(self, archive, debug=False):
        self._archive = archive
        self._dbg = debug

    def _iface(self, itype, **kwargs):
        if itype is None:
            itype = 'basic'
        yield self._archive.make_interface(itype)

    @property
    def origin(self):
        return self._archive.ref

    @property
    def _tm(self):
        return self._archive.tm

    '''
    I think that's all I need to do!
    '''


class LcQuery(BasicQuery, BackgroundInterface, ConfigureInterface):
    pass


'''
Utilities

'''
def local_ref(source, prefix=None):
    """
    Create a semantic ref for a local filename.  Just uses basename.  what kind of monster would access multiple
    different files with the same basename without specifying ref?

    alternative is splitext(source)[0].translate(maketrans('/\\','..'), ':~') but ugghh...

    Okay, FINE.  I'll use the full path.  WITH leading '.' removed.

    Anyway, to be clear, local semantic references are not supposed to be distributed.
    :param source:
    :param prefix: [None] default 'local'
    :return:
    """
    if prefix is None:
        prefix = 'local'
    xf = source.translate(str.maketrans('/\\', '..', ':~'))
    while splitext(xf)[1] in {'.gz', '.json', '.zip', '.txt', '.spold', '.7z'}:
        xf = splitext(xf)[0]
    while xf[0] == '.':
        xf = xf[1:]
    while xf[-1] == '.':
        xf = xf[:-1]
    return '.'.join([prefix, xf])


def q_node_activity(fg):
    """
    A reference quantity for dimensionless node activity. This should be part of Qdb reference quantities (but isn't)
    :param fg:
    :return:
    """
    try:
        return fg.get_canonical('node activity')
    except EntityNotFound:
        fg.new_quantity('Node Activity', ref_unit='activity', external_ref='node activity', comment='MFA metric')
        return fg.get_canonical('node activity')


"""
In most LCA software, including the current operational version of lca-tools, a 'flow' is a composite entity
that is made up of a 'flowable' (substance, product, intervention, or service) and a 'context', which is 
synonymous with an environmental compartment.

We are planning a major shift to a data model where 'context' is a standalone entity, belonging to BasicArchive,
the context of an exchange is stored in its termination, and 'flows' are context-free. This will have the 
advantages of normalizing the definition of the exchange, reducing the number of flow entities in a database, and
simplifying the process of seeking compatibility between databases.

The new Flat Background already implements context-as-termination, but the mainline code has not yet been 
changed. So we introduce this flag CONTEXT_STATUS_ to express to client code which one to do. It should take
either of the two values: 'compat' means "old style" (flows have Compartments) and 'new' means use the new data
model (exchange terminations are contexts) 
"""
CONTEXT_STATUS_ = 'new'  # 'compat': context = flow['Compartment']; 'new': context = exch.termination


# Containers of information about linked exchanges.  Direction is given with respect to the termination.
ExteriorFlow = namedtuple('ExteriorFlow', ('origin', 'flow', 'direction', 'termination'))
ProductFlow = namedtuple('ProductFlow', ('origin', 'flow', 'direction', 'termination', 'component_id'))

EntitySpec = namedtuple('EntitySpec', ('link', 'ref', 'name', 'group'), defaults=(None,))
