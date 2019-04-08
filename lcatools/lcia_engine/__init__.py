from lcatools.archives import Qdb, REF_QTYS
from .lcia_engine import LciaEngine, DEFAULT_CONTEXTS, DEFAULT_FLOWABLES

import os

IPCC_2007_GWP = os.path.join(os.path.dirname(__file__), 'data', 'ipcc_2007_traci.json.gz')


class LciaDb(Qdb):
    """
    Augments the Qdb with an LciaEngine instead of a TermManager
    """
    @classmethod
    def new(cls, source=REF_QTYS, ref='local.lciadb', **kwargs):
        lcia = LciaEngine(**kwargs)
        qdb = cls.from_file(source, ref=ref, term_manager=lcia, quiet=True)
        return qdb
