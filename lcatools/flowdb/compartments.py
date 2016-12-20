import os
import json
import re

from lcatools.interact import _pick_list


COMPARTMENTS = os.path.join(os.path.dirname(__file__), 'compartments.json')


class MissingCompartment(Exception):
    pass


def _ensure_list(var):
    if isinstance(var, str):
        return [var]
    return var


class CompartmentManager(object):
    """
    This class stores a hierarchy of compartments and exposes tools to allow client code to query and manipulate
    it.
    """
    def __init__(self, file=COMPARTMENTS):
        self._compartments_file = file
        self.compartments = self._load_compartments()  # this is a single compartment at the root of the hierarchy

    def _load_compartments(self):
        with open(self._compartments_file, 'r') as fp:
            return Compartment.from_json(json.load(fp))  # the use of classmethod ensures that this returns typesafe

    def save(self):
        with open(self._compartments_file, 'w') as fp:
            json.dump(self.compartments.serialize(), fp, indent=2, sort_keys=True)

    def crawl(self, clist):
        """
        THIS is used to crawl an existing compartment hierarchy and find the one that matches a supplied compartment
        name. Compartment names are allowed to be sloppy (e.g. to omit steps, as long as they are monotonic in the
        hierarchy).

        The hard work is done by a static recursive function.  This wrapper is necessary because of the centrality of
        pop() to the underlying recursion.
        :param clist:
        :return:
        """
        my_clist = []
        my_clist.extend(clist)
        return _crawl_compartments(self.compartments, my_clist)

    def is_elementary(self, flow):
        comp = self.find_matching(flow['Compartment'], check_elem=True)
        return comp.elementary

    # inspection methods
    def filter_exch(self, process_ref, elem=True, **kwargs):
        return [x for x in process_ref.archive.fg_lookup(process_ref.id, **kwargs)
                if self.is_elementary(x.flow) is elem]

    def find_matching(self, compartment_name, interact=True, check_elem=False):
        """

        :param compartment_name:  a monotonic list of compartment names (as stored in an archive)
        :param interact: whether to interactively add / merge missing compartments
        :param check_elem: whether to bail out as soon as the compartment is determined to be elementary
        :return:
        """
        match = self.crawl(compartment_name)
        if match is None:
            if interact:
                c = self.merge_compartment(compartment_name)
                match = _crawl_compartments(self.compartments, compartment_name, check_elem=check_elem)
                if c is match and c is not None:
                    print('match: %s' % match.to_list())
                    print('Updating compartments...')
                    self.save()
                    return c
                # else - MissingCompartment
            raise MissingCompartment('%s' % compartment_name)

    def merge_compartment(self, missing):
        """

        :param missing:
        :return:
        """
        compartment = self.compartments
        my_missing = []
        my_missing.extend(missing)
        while len(my_missing) > 0:
            sub = _crawl_compartments(compartment, my_missing[:1])
            if sub is not None:
                my_missing.pop(0)
                compartment = sub
            else:
                print('Missing compartment: %s' % my_missing[0])
                subs = sorted(s.name for s in compartment.subcompartments())
                print('subcompartments of %s:' % compartment)
                c = _pick_list(subs, 'Merge "%s" into %s' % (my_missing[0], compartment),
                               'Create new Subcompartment of %s' % compartment)
                if c == (None, 0):
                    compartment.add_syn(my_missing.pop(0))
                elif c == (None, 1):  # now we add all remaining compartments
                    while len(my_missing) > 0:
                        new_sub = my_missing.pop(0)
                        compartment.add_sub(new_sub)
                        compartment = compartment[new_sub]
                elif c == (None, None):
                    raise ValueError('User break')
                else:
                    compartment = compartment[subs[c[0]]]
        return compartment


def _crawl_compartments(compartment, clist, check_elem=False):
    """

    :param compartment:
    :param clist:
    :return:
    """
    while len(clist) > 0 and clist[0] in compartment:
        clist.pop(0)
    while len(clist) > 0 and clist[0] is None:
        clist.pop(0)
    if len(clist) > 0:
        if bool(re.search('unspecified$', clist[0], flags=re.IGNORECASE)):
            clist.pop(0)
    if len(clist) == 0:
        return compartment
    if check_elem and compartment.elementary:
        # compartments never switch back from elementary, so if we're just checking elem, bail out here
        return compartment
    else:
        for s in compartment.subcompartments():
            n = _crawl_compartments(s, clist)
            if n is not None:
                return n
        return None


class Compartment(object):
    """
    A hierarchical listing of compartments.  A compartment contains subcompartments, which are themselves compartments.

    Each compartment has a canonical name and a set of synonyms.
    """
    @classmethod
    def from_json(cls, j):
        """
        Classmethod to build compartment hierarchy from serialization
        :param j:
        :return:
        """
        rootname = j['name']
        if not isinstance(rootname, list):
            rootname = [rootname]
        root = cls(rootname[0], elementary=False)  # root compartment is not elementary
        for i in rootname[1:]:
            root.add_syn(i)
        root._add_subs_from_json(j['subcompartments'])
        return root

    def add_branch_from_json(self, j):
        branch = Compartment.from_json(j)
        self._subcompartments.add(branch)

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
        self._synonyms = set()
        self._elementary = elementary
        self._subcompartments = set()
        self.parent = parent
        self._id = '; '.join(self.to_list())

    @property
    def synonyms(self):
        return {self.name}.union(self._synonyms)

    def add_syn(self, syn):
        if syn != self.name:
            self._synonyms.add(syn)

    def add_syns(self, syns):
        for i in syns:
            self.add_syn(i)

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
            return []  # node with no parent is root-- does not show up in compartment name list
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
        Merge two subcompartments of the same parent compartment.
        The first is absorbed into the second and then removed.
        merge n1 into n2; n2 dominant
        :param n1: compartment / name to be subsumed
        :param n2: compartment / name to be enlarged
        :return:
        """
        s1 = self._ensure_comp(n1)
        s2 = self._ensure_comp(n2)

        if s1 is s2:
            print('Compartments are the same')
            return

        if s1._elementary != s2._elementary:
            raise ValueError('elementary flag must match')
        s2.add_syns(s1.synonyms)
        for i in s1._subcompartments:
            s2.merge_sub(i)

        if s1 in self._subcompartments:
            self._subcompartments.remove(s1)

    def merge_sub(self, comp):
        """
        take an existing compartment and make it a subcompartment of self.  If self already has a subcompartment
        whose synonyms match the argument, then the argument is combined with the already-matching subcompartment.
        This merge is performed recursively, with the incoming subcompartment's subcompartments being merged with
        the existing subcompartment.
        Otherwise, the argument is introduced as a new subcompartment.
        :param comp: existing compartment
        :return:
        """
        merge = False
        for i in self._subcompartments:
            if i.synonyms.intersection(comp.synonyms):
                i.add_syns(comp.synonyms)
                for j in comp._subcompartments:
                    i.merge_sub(j)
                merge = True
                break
        if merge is False:
            self._subcompartments.add(comp)

    def uproot(self, merged, new_parent):
        """
        Uproot a subcompartment from self and install it as a subcompartment of a new parent.
        :param merged: must be a subcompartment
        :param new_parent:
        :return:
        """
        s1 = self._ensure_comp(merged)
        new_parent.merge_sub(s1)
        self._subcompartments.remove(s1)

    def _collapse(self, subcompartment):
        """
        recursive collapse omits the removal to avoid 'set changed size' error
        :param subcompartment:
        :return:
        """
        for sub in subcompartment.subcompartments():
            subcompartment._collapse(sub)
        self.add_syns(subcompartment.synonyms)

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
        a.extend(sorted(list(self._synonyms)))
        return a

    def serialize(self):
        j = {
            "name": self._names(),
            "subcompartments": sorted([x.serialize() for x in self._subcompartments], key=lambda x: x['name'][0])
        }
        if self._elementary:
            j['elementary'] = True
        return j
