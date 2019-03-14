"""
This package performs the tarjan ordering of a well-defined unit process inventory database, and the engine object
created can be used to populate a static, matrix-based FlatBackground for fast LCI calculations.

No support for parametric uncertainty at present. use bw2 if you want that.
"""

from .background_engine import BackgroundEngine
