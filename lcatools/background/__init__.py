"""
This package is a mess, but it works.

The main code is contained in background.py, which performs Tarjan's algorithm to detect strongly connected
components and order them.  The module is 650 loc and definitely performs multiple jobs.  It needs to be refactored to
separate the traversal operation from the query operation, and to use the new interface architecture.

But for now, we will just roll with it.
"""

from .background_manager import BackgroundManager
