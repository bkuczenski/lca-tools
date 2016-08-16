import os
import json
import re
from uuid import uuid4


COMPARTMENTS = os.path.join(os.path.dirname(__file__), 'compartments.json')


def load_compartments(file=COMPARTMENTS):
    with open(file, 'r') as fp:
        return Compartment.from_json(json.load(fp))


def save_compartments(compartments, file=COMPARTMENTS):
    with open(file, 'w') as fp:
        json.dump(compartments.serialize(), fp, indent=2, sort_keys=True)


def _ensure_list(var):
    if isinstance(var, str):
        return [var]
    return var


def traverse_compartments(compartment, clist):
    """
    This is necessary because of pop()
    :param compartment:
    :param clist:
    :return:
    """
    my_clist = []
    my_clist.extend(clist)
    return _traverse_compartments(compartment, my_clist)


def _traverse_compartments(compartment, clist):
    while len(clist) > 0 and clist[0] in compartment:
        clist.pop(0)
    while len(clist) > 0 and clist[0] is None:
        clist.pop(0)
    if len(clist) > 0:
        if bool(re.search('unspecified$', clist[0], flags=re.IGNORECASE)):
            clist.pop(0)
    if len(clist) == 0:
        return compartment
    else:
        for s in compartment.subcompartments():
            n = _traverse_compartments(s, clist)
            if n is not None:
                return n
        return None


class Compartment(object):
    """
    A hierarchical listing of compartments.  A compartment contains subcompartments, which are themselves compartments.
    """
    @classmethod
    def from_json(cls, j):
        rootname = j['name']
        if not isinstance(rootname, list):
            rootname = [rootname]
        root = cls(rootname[0], elementary=False)
        root._add_subs_from_json(j['subcompartments'])
        return root

    def _add_subs_from_json(self, subs):
        for sub in subs:
            if 'elementary' in sub.keys():
                elementary = bool(sub['elementary'])
            else:
                elementary = False
            syns = sub['name']
            if not isinstance(syns, list):
                if syns is None or syns.lower() == 'unspecified':
                    continue
                syns = [syns]

            sc = self.add_sub(syns[0], elementary=elementary)
            for syn in syns[1:]:
                sc.add_syn(syn)
            sc._add_subs_from_json(sub['subcompartments'])

    def __init__(self, name, parent=None, elementary=False):
        self.name = name
        self.synonyms = {name}
        self._elementary = elementary
        self._subcompartments = set()
        self._id = uuid4()
        self.parent = parent

    def add_syn(self, syn):
        self.synonyms.add(syn)

    def _ensure_comp(self, item):
        if isinstance(item, Compartment):
            return item
        return self.__getitem__(item)

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

    def to_list(self):
        if self.parent is None:
            return []  # non-parent node is root
        l = self.parent.to_list()
        l.append(self.name)
        return l

    def print_tree(self, up=''):
        s = self.name
        if s is None:
            s = '##NONE##'
        ls = self._names()
        print(up + '; '.join(ls))
        up += s
        for x in self.subcompartments():
            ls += x.print_tree(up=up + ' -- ')
        return ls

    def set_elementary(self):
        self._elementary = True
        for i in self.subcompartments():
            i.set_elementary()

    def unset_elementary(self):
        self._elementary = False

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(self._id)

    def __eq__(self, other):
        return (self.name in other.synonyms) and (self.parent == other.parent)

    def __contains__(self, item):
        if item in self.synonyms:
            return True
        return False

    def __getitem__(self, item):
        for x in self._subcompartments:
            if item in x.synonyms:
                return x
        raise KeyError('No subcompartment found')

    def delete(self, item):
        s1 = self._ensure_comp(item)
        if len(s1._subcompartments) > 0:
            raise ValueError('Subcompartment not empty')
        self._subcompartments.remove(s1)

    def merge_subs(self, n1, n2):
        """
        merge n1 into n2; n2 dominant
        :param n1:
        :param n2:
        :return:
        """
        s1 = self._ensure_comp(n1)
        s2 = self._ensure_comp(n2)

        if s1 is s2:
            print('Compartments are the same')
            return

        if s1._elementary != s2._elementary:
            raise ValueError('elementary flag must match')
        for i in s1.synonyms:
            s2.add_syn(i)
        for i in s1._subcompartments:
            s2._merge_into(i)

        if s1 in self._subcompartments:
            self._subcompartments.remove(s1)

    def _merge_into(self, comp):
        """
        take an existing compartment and make it a subcompartment of self
        :param comp: existing compartment
        :return:
        """
        merge = False
        for i in self._subcompartments:
            if i.synonyms.intersection(comp.synonyms):
                i.synonyms = i.synonyms.union(comp.synonyms)
                for j in comp._subcompartments:
                    i._merge_into(j)
                merge = True
                break
        if merge is False:
            self._subcompartments.add(comp)

    def merge(self, merged, merge_into):
        """
        Merge a subcompartment into another one downstream
        :param merged: must be a subcompartment
        :param merge_into:
        :return:
        """
        s1 = self._ensure_comp(merged)
        merge_into._merge_into(s1)
        self._subcompartments.remove(s1)

    def _collapse(self, subcompartment):
        """
        recursive collapse omits the removal to avoid 'set changed size' error
        :param subcompartment:
        :return:
        """
        for sub in subcompartment.subcompartments():
            subcompartment._collapse(sub)
        self.synonyms = self.synonyms.union(subcompartment.synonyms)

    def collapse(self, subcompartment):
        """
        Collapse a subcompartment (and all its subcompartments recursively) into a set of synonyms for the
        current compartment. This is written to deal with GaBi categories like "Group NMVOC to air" that are
        not compartments.
        :param subcompartment:
        :return:
        """
        self._collapse(subcompartment)
        self._subcompartments.remove(subcompartment)

    def add_sub(self, name, elementary=None, verbose=False):
        """
        make a new subcompartment based on name only
        :param name:
        :param elementary:
        :param verbose:
        :return:
        """
        try:
            sub = self[name]
        except KeyError:
            if elementary is None:
                elementary = self._elementary
            if verbose:
                print('New compartment %s [elementary: %s]' % (name, elementary))
            sub = Compartment(name, parent=self, elementary=elementary)
        self._subcompartments.add(sub)
        return sub

    def add_subs(self, subs, verbose=False):
        """
        input is a list of subcompartments to be added recursively
        :param subs:
        :param verbose:
        :return:
        """
        subs = _ensure_list(subs)
        if len(subs) == 1:
            self.add_sub(subs[0], verbose=verbose)
        else:
            sub = self.add_sub(subs[0], verbose=verbose)
            sub.add_subs(subs[1:], verbose=verbose)

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

    def _names(self):
        a = [self.name]
        a.extend(list(self.synonyms.difference({self.name})))
        return a

    def serialize(self):
        j = {
            "name": self._names(),
            "subcompartments": [x.serialize() for x in self._subcompartments]
        }
        if self._elementary:
            j['elementary'] = True
        return j
