"""
This file contains the re-imagined LCIA engine as an outgrowth of the Term Manager.  The plan:
 - Term Manager retains its core functionality, but introduce a new synonym lookup for quantities
 - NO archive subclass
 - YES query subclass
 - implements is_elementary
 - make_ref should set context and install self as term manager
   = thereby, factor lookup in the quantity implementation should work directly
 - do_lcia should be unchanged from quantity implementation


"""