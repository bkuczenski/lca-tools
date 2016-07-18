import os
import json


COMPARTMENTS = os.path.join(os.path.dirname(__file__), 'compartments.json')


def load_compartments():
    with open(COMPARTMENTS, 'r') as fp:
        return json.load(fp)


def _ensure_list(var):
    if isinstance(var, str):
        return [var]
    return var



class Compartment(object):
    """
    A hierarchical listing of compartments.  A compartment contains subcompartments, which are themselves compartments.
    """
    @classmethod
    def from_json(cls, j):
        root = cls(j['name'], elementary=False)
        root._add_subs_from_json(j['subcompartments'])
        return root

    def _add_subs_from_json(self, subs):
        for sub in subs:
            if 'elementary' in sub.keys():
                elementary = bool(sub['elementary'])
            else:
                elementary = False
            sc = self.add_sub(sub['name'], elementary=elementary)
            sc._add_subs_from_json(sub['subcompartments'])

    def __init__(self, name, elementary=False):
        self.name = name
        self._elementary = elementary
        self._subcompartments = set()

    @property
    def elementary(self):
        return self._elementary

    def subcompartments(self):
        for i in self._subcompartments:
            yield i

    def show(self):
        print('%s\nElementary: %s' % (self.name, self._elementary))
        for i in self.subcompartments():
            print('  sub: %s' % i.name)

    def set_elementary(self):
        self._elementary = True
        for i in self.subcompartments():
            i.set_elementary()

    def unset_elementary(self):
        self._elementary = False

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return (self.name == other.name) and (self._elementary == other.elementary)

    def __getitem__(self, item):
        for x in self._subcompartments:
            if x.name == item:
                return x
        raise KeyError('No subcompartment found')

    def add_sub(self, name, elementary=None):
        try:
            sub = self[name]
        except KeyError:
            if elementary is None:
                elementary = self._elementary
            print('New compartment %s [elementary: %s]' % (name, elementary))
            sub = Compartment(name, elementary=elementary)
        self._subcompartments.add(sub)
        return sub

    def add_subs(self, subs):
        """
        input is a list of subcompartments to be added recursively
        :param subs:
        :return:
        """
        subs = _ensure_list(subs)
        if len(subs) == 1:
            self.add_sub(subs[0])
        else:
            sub = self.add_sub(subs[0])
            sub.add_subs(subs[1:])

    def traverse(self, subs):
        """
        turns a list of compartment strings into a list of Compartment objects
        :param subs:
        :return:
        """
        subs = _ensure_list(subs)
        comps = [self[subs[0]]]
        if len(subs) == 1:
            return comps
        comps.extend(comps[0].traverse(subs[1:]))
        return comps

    def serialize(self):
        j = {
            "name": self.name,
            "subcompartments": [x.serialize() for x in self._subcompartments]
        }
        if self._elementary:
            j['elementary'] = True
        return j


