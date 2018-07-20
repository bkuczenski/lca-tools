"""
Antelope Interface Definitions

The abstract classes in this sub-package define what information is made available via a stateless query to an Antelope
resource of some kind.  The interfaces must be instantiated in order to be used.  In the core package
"""

from .abstract_query import UnknownOrigin, PrivateArchive, EntityNotFound

from .iconfigure import ConfigureInterface
from .iinventory import InventoryInterface, InventoryRequired
from .iindex import IndexInterface, IndexRequired, comp_dir, directions
from .ibackground import BackgroundInterface, BackgroundRequired
from .iquantity import QuantityInterface, QuantityRequired
from .iforeground import ForegroundInterface, ForegroundRequired

import re
from collections import namedtuple


"""
In most LCA software, including the current operational version of lca-tools, a 'flow' is a composite entity
that is made up of a 'flowable' (substance, product, intervention, or service) and a 'context', which is 
synonymous with an environmental compartment.

We are planning a major shift to a data model where 'context' is a standalone entity, belonging to BasicArchive,
the context of an exchange is stored in its termination, and 'flows' are context-free. This will have the 
advantages of normalizing the definition of the exchange, reducing the number of flow entities in a database, and
simplifying the process of seeking compatibility between databases.

The new Flat Background already implements context-as-termination, but the mainline code has not yet been 
changed. So we introduce this flag _CONTEXT_STATUS_ to express to client code which one to do. It should take
either of the two values: 'compat' means "old style" (flows have Compartments) and 'new' means use the new data
model (exchange terminations are contexts) 
"""
_CONTEXT_STATUS_ = 'compat'  # 'compat': context = flow['Compartment']; 'new': context = exch.termination


ExteriorFlow = namedtuple('ExteriorFlow', ('origin', 'flow', 'direction', 'termination'))
ProductFlow = namedtuple('ProductFlow', ('origin', 'flow', 'direction', 'termination', 'component_id'))


def trim_cas(cas):
    try:
        return re.sub('^(0*)', '', cas)
    except TypeError:
        print('%s %s' % (cas, type(cas)))
        return ''
