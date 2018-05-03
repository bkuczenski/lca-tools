"""
Code for generating a catalog populated with local data sources.  This is where active workflow development can
occur.  See GitHub root / workflow.txt for thoughts.
"""

from .ecoinvent import EcoinventConfig

'''CATALOG_ROOT specifies the local folder that stores the reference catalog
'''
CATALOG_ROOT = '/data/LCI/cat-test/'

'''RESOURCES_CONFIG provides enabling and configuration information to the various data resource
classes defined elsewhere in this package.  Each key should match a python module name, and each value should
be a dict.
 
The contents of the module dict are resource-specific but must include a 'source' field which maps to a config object.
The remaining args are passed as init args to the object.
'''

RESOURCES_CONFIG = {
    'ecoinvent': {
        'source': EcoinventConfig,
        'ecoinvent_root': '/data/LCI/Ecoinvent/',
        'inv_ext': 'zip'
    }
}
