"""
Code for generating a catalog populated with local data sources.  This is where active workflow development can
occur.  See GitHub root / workflow.txt for thoughts.

MAIN IDEA:
 local.py:
  The local config file. specifies source locations and resource-specific configurations for data sources.
  It must define the following variables:
   CATALOG_ROOT = location to build a permanent data catalog
   TEST_ROOT = location to store temporary test materials (blown away after every use)
   RESOURCES_CONFIG = a dict mapping source nicknames to instantiation parameters.
    The instantiation parameters have two required keys:
     'source': key whose value is an executable DataSource subclass
     'enable_test': key whose absence (or falsity) will suppress tests.
    All other kv pairs in the dict are passed as input arguments to the DataSource.

 data_source.py:
  provides an abstract class for specifying data sources.  Each data source should be defined as a
  subclass of DataSource that implements its interface.

 tests/test_local.py:
  uses local.py to generate and validate the persistent catalog at the location specified in CATALOG_ROOT.

TO ADD A NEW DATA SOURCE:

 1. Produce a resource factory for your data source.

 1a. Create or locate an appropriate DataSource subclass, and implement its abstract methods:
     references: generate a list of semantic references the class knows how to instantiate
     interfaces(ref): generate a list of interfaces known for the given ref (ref must be in references)
     make_resources(ref): generate an exhaustive sequence of LcResource objects for a given ref

 1b. Make sure the resources created by your DataSource subclass include any necessary configuration information

 2. Provide configuration info for your data source

 2a. Import your DataSource subclass here.

 2b. Create an entry in RESOURCES_CONFIG with at minimum the 'source' and 'enable_test' keys, as well as any
     necessary input arguments.

 3. Run python -m unittest lcatools/data_sources/tests/test_local.py

"""

import os
import warnings

from .gwp_ipcc_2007 import GwpIpcc2007
from .traci import TraciConfig
from .uslci import UsLciConfig

# local resources
#from .ecoinvent import EcoinventConfig
#from .ecoinvent_lcia import EcoinventLciaConfig
#from .calrecycle_lca import CalRecycleConfig


'''CATALOG_ROOT specifies the local folder that stores the reference catalog
It must either:
 - be specified in an environment variable os.environ['ANTELOPE_CATALOG_ROOT'], 
 - or as a file in the local directory named antelope_catalog_root  
'''

try:
    CATALOG_ROOT = os.environ['ANTELOPE_CATALOG_ROOT']
except KeyError:
    CATALOG_ROOT = '/data/LCI/cat-demo/'  # persistent
    warnings.warn('environ ANTELOPE_CATALOG_ROOT missing; using BK default: %s' % CATALOG_ROOT)

'''RESOURCES_CONFIG 
provides enabling and configuration information to the various data resource
classes defined elsewhere in this package.  Each key should match a python module name, and each value should
be a dict.
 
The contents of the module dict are resource-specific but must include a 'source' field which maps to a config object,
and a 'data_root' which maps to the _root variable in the DataSource.
The remaining args are passed as init args to the object.

The RESOURCES_CONFIG dict by default includes all the resources the catalog can make without any site-specific
configuration.  Use RESOURCES_CONFIG_LOCAL to add and configure local resources.  Still figuring out the best way 
to actually do all this.  Community input welcome. 
'''

RESOURCES_CONFIG = {
    'ipcc2007': {
        'source': GwpIpcc2007,
        'enable_test': True
    },
    'uslci': {
        'source': UsLciConfig,
        'enable_test': True
    },
    'traci': {
        'source': TraciConfig,
        'data_root': '/data/LCI/TRACI/',
        'enable_test': True
    }
}


"""
RESOURCES_CONFIG_LOCAL = {
    'ecoinvent': {
        'source': EcoinventConfig,
        'data_root': '/data/LCI/Ecoinvent/',
        'enable_test': False
    },
    'calrecycle': {
        'source': CalRecycleConfig,
        'data_root': '/data/GitHub/CalRecycle/LCA_Data/',
        'enable_test': False
    },
    'ecoinvent_lcia': {
        'source': EcoinventLciaConfig,
        'version': '3.1',
        'data_root': '/data/LCI/Ecoinvent/LCIA/',
        'enable_test': False
    }
}

RESOURCES_CONFIG.update(RESOURCES_CONFIG_LOCAL)  # still trying to figure out the best way to do this
"""

'''OPERATIONAL CONFIG
Code below this line is used by the local init machinery to setup catalogs / testing
'''


def make_config(resource=None):
    if resource is None:
        print('Configurable Resources:')
        for k, v in RESOURCES_CONFIG.items():
            print('%s: %s' % (k, v['source'].__name__))
        return None
    d = RESOURCES_CONFIG[resource]
    return d['source'](**{k: v for k, v in d.items() if k != 'source'})


def check_enabled(resource):
    if resource in RESOURCES_CONFIG:
        if 'enable_test' in RESOURCES_CONFIG[resource]:
            return RESOURCES_CONFIG[resource]['enable_test']
    return False
