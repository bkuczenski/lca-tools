"""
Entity References

There are two main subclasses of entity references:
 * subclasses of the EntityRef class are typed and can act as local representations of remotely-situated data.  These
   all require a query object that is somehow grounded in that remote archive.

 * CatalogRef instances are un-grounded references, useable as local "stand-ins" for remote data that may not be
   available.  A CatalogRef is a hollow shell of an entity and can only supply information that it was given.  However,
   it can be supplied with a query to return a grounded Entity reference.

   For simplicity, the best way to make a

"""

from .catalog_ref import CatalogRef, QuantityRef
from .base import NoCatalog, EntityRefMergeError
from .flow_interface import FlowInterface
