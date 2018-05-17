"""
Code for generating a catalog populated with local data sources.  This is where active workflow development can
occur.  See GitHub root / workflow.txt for thoughts.
"""

from .ecoinvent import EcoinventConfig


def _check_enabled(resource):
    if resource in RESOURCES_CONFIG:
        if 'enable_test' in RESOURCES_CONFIG[resource]:
            return RESOURCES_CONFIG[resource]['enable_test']
    return False


'''CATALOG_ROOT specifies the local folder that stores the reference catalog
'''
CATALOG_ROOT = '/data/LCI/cat-test/'  # persistent
TEST_ROOT = '/data/LCI/cat-demo/'  # volatile

'''RESOURCES_CONFIG 
provides enabling and configuration information to the various data resource
classes defined elsewhere in this package.  Each key should match a python module name, and each value should
be a dict.
 
The contents of the module dict are resource-specific but must include a 'source' field which maps to a config object.
The remaining args are passed as init args to the object.
'''

RESOURCES_CONFIG = {
    'ecoinvent': {
        'source': EcoinventConfig,
        'ecoinvent_root': '/data/LCI/Ecoinvent/',
        'inv_ext': 'zip',
        'enable_test': False
    }
}

run_ecoinvent = _check_enabled('ecoinvent')
