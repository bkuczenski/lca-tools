"""
A Flow Database is the core of a foreground builder. A foreground literally consists of a collection of flows.

and a set of fragments, and a list of entity records; dereferenced by a set of catalogs.

The flow database contains a list of flow observations, which are crucially made by the analyst. So every flow
in the database has to be added.

When a flow is added, a logical flow is created, which indexes its name and uuid to a logical flow-> which I guess
should be given a uuid?
no, the fragment is built from catalog refs- naturally including 0

so the user does one thing- the user selects a flow to be added to the foreground. That flow is queried and added by
reference to the foreground archive, and that flow is added to the logical flow database.  Logical flows can be
added together to express synonymy: characterizations are concatenated for queries by name or by id. In this case
the

The flow database interface should include creating new flows, and linking flows to quantities.

It operates on CatalogRefs, which can dereference themselves

"""
from lcatools.logical_flows import LogicalFlow, LogicalQuantity, LogicalSet
from lcatools.catalog import CatalogRef
from lcatools.foreground.compartments import Compartment, load_compartments


class LcFlows(object):

    @classmethod
    def from_json(cls, catalog, j):
        db = cls()
        db._compartments = Compartment.from_json(j['compartments'])

        def to_ref(x):
            return CatalogRef(catalog, int(x['index']), x['entity'])

        for f in j['flows']:
            flow_ref = to_ref(f[0])
            db.add_flow(flow_ref)
            if len(f) > 1:
                for i in range(1, len(f)):
                    db.add_ref(f, to_ref(f[i]))
        for q in j['quantities']:
            q_ref = to_ref(q[0])
            db.add_quantity(q_ref)
            if len(q) > 1:
                for i in range(1, len(q)):
                    db.add_ref(q_ref, to_ref([i]))
        return db

    def __init__(self):
        self._compartments = Compartment.from_json(load_compartments())
        self._flows = LogicalSet(LogicalFlow)
        self._quantities = LogicalSet(LogicalQuantity)

    def compartments(self, cat_ref):
        c = cat_ref.entity()['Compartment']
        return self._compartments.traverse(c)

    def add_compartments(self, cat_ref):
        c = cat_ref.entity()['Compartment']
        return self._compartments.add_subs(c, verbose=True)

    def is_elementary(self, cat_ref):
        comps = self.compartments(cat_ref)
        return comps[-1].elementary

    def list_subcompartments(self, comp_list):
        """
        lists subcompartments of a given compartment string
        :param comp_list:
        :return:
        """
        comps = self._compartments.traverse(comp_list)
        return [x.name for x in comps[-1].subcompartments()]

    def add_flow(self, cat_ref):
        self._flows.add(LogicalFlow.create(cat_ref))
        self._add_flow_cfs(cat_ref)
        try:
            self.compartments(cat_ref)
        except KeyError:
            print('New compartments added!')
            self.add_compartments(cat_ref)

    def _add_flow_cfs(self, cat_ref):
        """
        We only want to add CFs for quantities already IN the database-
        the workflow is first curate the set of quantities of interest; then add flows of interest
        :param cat_ref:
        :return:
        """
        for cf in cat_ref.entity().characterizations():
            q_ref = CatalogRef(cat_ref.catalog, cat_ref.index, cf.quantity)
            if q_ref in self._quantities.keys():
                self.add_cf(q_ref, cf)

    def add_quantity(self, cat_ref):
        self._quantities.add(LogicalQuantity.create(cat_ref))

    def add_exchange(self, flow_ref, exchange):
        """
        exchanges are normally stored with processes; here we want to associate one with a flow
        :param flow_ref: catalog ref of a flow.
        Not sure what this is for, exactly.
        :param exchange:
        :return:
        """
        self._flows[flow_ref].add_exchange(flow_ref, exchange)

    def add_cf(self, q_ref, cf):
        self._quantities[q_ref].add_cf(q_ref, cf)

    def add_ref(self, existing_ref, new_ref):
        if existing_ref in self._flows.keys():
            self._flows.add_ref(existing_ref, new_ref)
            self._add_flow_cfs(new_ref)
        elif existing_ref in self._quantities.keys():
            self._quantities.add_ref(existing_ref, new_ref)
        else:
            raise KeyError("Can't find existing ref %s" % existing_ref)

    def synonyms(self, ref_a, ref_b):
        """
        if two different refs already in the DB should be merged.
        :param ref_a:
        :param ref_b:
        :return:
        """
        if ref_a in self._flows.keys():
            self._flows.mergewith(ref_a, self._flows[ref_b])
        elif ref_a in self._quantities.keys():
            self._quantities.mergewith(ref_a, self._quantities[ref_b])
        else:
            raise KeyError("Can't find existing ref %s" % ref_a)

    def flows(self):
        for f in self._flows.items():
            yield f

    def quantities(self):
        for q in self._quantities.items():
            yield q

    def flow(self, f):
        """
        f can be anything in cat_ref.names()
        :param f:
        :return:
        """
        return self._flows[f]

    def quantity(self, q):
        """
        q can be anything in cat_ref.names()
        :param q:
        :return:
        """
        return self._quantities[q]

    def lcia_methods(self):
        """
        Quantities with an Indicator property are judged to be LCIA methods. We are yielding logical quantities so we
        don't need to worry if there are repeats or inconsistencies- the point of the logical db is to be inclusive
        :return:
        """
        for q in self.quantities():
            for e in q:
                if 'Indicator' in e.entity().keys():
                    yield q
                    break

    def serialize(self):
        self._flows.check()
        self._quantities.check()
        return {
            'compartments': self._compartments.serialize(),
            'flows': self._flows.serialize(),
            'quantities': self._quantities.serialize()
        }
