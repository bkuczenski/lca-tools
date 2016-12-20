# lca-tools
A Python package for doing stuff with LCA data

## Exchange

Module for handling exchange value computation.  An exchange value is a quantitative relationship between two exchanges that belong to the same process instance.

## Quantity

Module for handling flow conversion factor computation.  A factor is a quantitative relationship between two flow quantities that both pertain to the same flow instance.

## Metadata

Module for handling instance lookups and metadata management. Future home for Semantic Web tools.

## Projects

catch-all collection of scripts and functions that have project-specific purposes but aren't especially reusable.

`gabi_package_list(url)`
Function for scraping names, types, and geographies of GaBi process data sets from the GaBi website. Something similar is planned for Ecoinvent. 

Usage:

    >>> from projects import *
    >>> P = gabi_package_list(GABI_URLS[PROFESSIONAL])
    Found 2 processListTables
    Processing 
    Adding DataFrame with 4 columns and 2608 rows
    Processing 
    Adding DataFrame with 4 columns and 41 rows
    
    
    >>>


## Testing

The python `unittest` framework is used to implement unit tests.  The tests should be automatically discovered and run by running `python -m unittest` from the root directory of the repo.

In order to ensure that tests are discoverable, the test files must match the shell pattern `'test_*.py'` and the directory containing the files must have a `__init__.py`.

Currently tested modules:

 * `flowdb.compartments`

