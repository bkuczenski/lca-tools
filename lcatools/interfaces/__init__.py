"""
Antelope Interface Definitions

The abstract classes in this sub-package define what information is made available via a stateless query to an Antelope
resource of some kind.  The interfaces must be instantiated in order to be used.  In the core package
"""

from .abstract_query import UnknownOrigin, PrivateArchive, EntityNotFound

from .iconfigure import ConfigureInterface
from .iinventory import InventoryInterface, InventoryRequired
from .iindex import IndexInterface, IndexRequired, directions, comp_dir, check_direction
from .ibackground import BackgroundInterface, BackgroundRequired
from .iquantity import QuantityInterface, QuantityRequired, NoFactorsFound, ConversionReferenceMismatch, FlowableMismatch

from .iforeground import ForegroundInterface, ForegroundRequired

import re
import uuid
from os.path import splitext

from collections import namedtuple


uuid_regex = re.compile('([0-9a-f]{8}-?([0-9a-f]{4}-?){3}[0-9a-f]{12})', flags=re.IGNORECASE)



def to_uuid(_in):
    if _in is None:
        return _in
    if isinstance(_in, int):
        return None
    try:
        g = uuid_regex.search(_in)  # using the regexp test is 50% faster than asking the UUID library
    except TypeError:
        if isinstance(_in, uuid.UUID):
            return str(_in)
        g = None
    if g is not None:
        return g.groups()[0]
    # no regex match- let's see if uuid.UUID can handle the input
    try:
        _out = uuid.UUID(_in)
    except ValueError:
        return None
    return str(_out)


def local_ref(source):
    """
    Create a semantic ref for a local filename.  Just uses basename.  what kind of monster would access multiple
    different files with the same basename without specifying ref?

    alternative is splitext(source)[0].translate(maketrans('/\\','..'), ':~') but ugghh...

    Okay, FINE.  I'll use the full path.  WITH leading '.' removed.

    Anyway, to be clear, local semantic references are not supposed to be distributed.
    :param source:
    :return:
    """
    xf = source.translate(str.maketrans('/\\', '..', ':~'))
    while splitext(xf)[1] in {'.gz', '.json', '.zip', '.txt', '.spold', '.7z'}:
        xf = splitext(xf)[0]
    while xf[0] == '.':
        xf = xf[1:]
    while xf[-1] == '.':
        xf = xf[:-1]
    return '.'.join(['local', xf])




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
