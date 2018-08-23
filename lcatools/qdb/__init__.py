from .qdb import Qdb, REF_QTYS
from .lcia_engine import LciaEngine

import os

IPCC_2007_GWP = os.path.join(os.path.dirname(__file__), 'data', 'ipcc_2007_gwp.json')
