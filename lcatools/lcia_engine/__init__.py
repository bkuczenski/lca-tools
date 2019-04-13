from lcatools.archives import Qdb, REF_QTYS, EntityExists
from .lcia_engine import LciaEngine, DEFAULT_CONTEXTS, DEFAULT_FLOWABLES
from lcatools.entity_refs import QuantityRef, FlowInterface

import os

IPCC_2007_GWP = os.path.join(os.path.dirname(__file__), 'data', 'ipcc_2007_gwp.json')


class LciaDb(Qdb):
    """
    Augments the Qdb with an LciaEngine instead of a TermManager
    """
    @classmethod
    def new(cls, source=REF_QTYS, **kwargs):
        lcia = LciaEngine(**kwargs)
        qdb = cls.from_file(source, term_manager=lcia, quiet=True)
        return qdb

    def _ref_to_key(self, key):
        """
        LciaDb uses links as keys so as to store different-sourced versions of the same quantity. But we also want
        to find local entities by external ref- so if they come up empty we try prepending local origin.
        of course that won't work basically ever, since none of the canonical quantities have local origin.
        so this may require some tuning.
        :param key:
        :return:
        """
        key = super(LciaDb, self)._ref_to_key(key)
        if key is None:
            key = super(LciaDb, self)._ref_to_key('%s/%s' % (self.ref, key))
        return key

    def __getitem__(self, item):
        """
        Note: this user-friendliness check adds 20% to the execution time of getitem-- so avoid it if possible
        (use _get_entity directly -- especially now that upstream is now deprecated)
        (note that _get_entity does not get contexts)

        :param item:
        :return:
        """
        if hasattr(item, 'link'):
            item = item.link
        return super(LciaDb, self).__getitem__(item)

    def add(self, entity):
        """
        Add entity to archive.  If entity is a quantity ref, add a masquerade to the lcia engine
        :param entity:
        :return:
        """
        if entity.origin is None:
            raise AttributeError('Origin not set! %s' % entity)
        self._ensure_valid_refs(entity)
        try:
            self._add(entity, entity.link)
        except EntityExists:
            # merge incoming entity's properties with existing entity
            current = self[entity.link]
            current.merge(entity)

        if entity.entity_type == 'quantity':
            if entity.is_entity:  # not ref
                print('Adding real entity %s' % entity.link)
                self.tm.add_quantity(entity)
                entity.set_qi(self.make_interface('quantity'))
            else:
                print('Adding qty ref %s' % entity)
                q_masq = QuantityRef(entity.external_ref, self.query, entity.reference_entity, Name=entity['Name'])
                print('Adding masquerade %s' % q_masq)
                self.tm.add_quantity(q_masq)
                self.tm.add_quantity(entity)  # should turn up as a child
                print('Importing factors')
                self.tm.import_cfs(entity)
                assert self.tm.get_canonical(entity) is q_masq
        elif isinstance(entity, FlowInterface):
            self.tm.add_flow(entity)
