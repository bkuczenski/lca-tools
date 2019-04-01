from .lcia_engine import LciaEngine, DEFAULT_CONTEXTS, DEFAULT_FLOWABLES

import os

IPCC_2007_GWP = os.path.join(os.path.dirname(__file__), 'data', 'ipcc_2007_traci.json.gz')
REF_QTYS = os.path.join(os.path.dirname(__file__), 'data', 'elcd_reference_quantities.json')
