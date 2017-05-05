import os
import json
import re

from lcatools.interact import _pick_list


REFERENCE_EFLOWS = os.path.join(os.path.dirname(__file__), 'compartments.json')


class MissingCompartment(Exception):
    pass


class ProtectedReferenceFile(Exception):
    pass


def _ensure_list(var):
    if isinstance(var, str):
        return [var]
    return var


def compartment_string(compartment_name):
    """
    Defined locally because it is relevant only to the FlowDB's compartment-lookup dictionary
    :param compartment_name: an iterable of monotonically nested compartments (though nonconsecutive is allowed)
    :return:
    """
    return '; '.join(list(filter(None, compartment_name)))


class CompartmentManager(object):
    """
    This class stores a hierarchy of compartments and exposes tools to allow client code to query and manipulate
    it.
    """

    def __init__(self, file=None):
        with open(REFERENCE_EFLOWS, 'r') as fp:
            self.compartments = Compartment.from_json(json.load(fp))
        self._local_file = None
        if file is not None:
            self.set_local(file)
        self._c_dict = dict()  # dict of '; '.join(compartments) to Compartment -- an example of premature optimization

    @property
    def known_names(self):
        return self.compartments.known_names()

    @property
    def writeable(self):
        if self._local_file is None:
            return False
        return os.access(self._local_file, os.W_OK)

    def set_local(self, file):
        self._local_file = file
        if os.path.exists(file):
            self._load_compartments()
        else:
            self.save()

    def _load_compartments(self):
        """
        Merge compartments specified in the local file into the reference hierarchy.
        Four steps here:
        * load the local json
        * add top-level synonyms
        * merge intermediate compartments
        * merge elementary compartments

        This ensures that (1) user can make local modifications to both intermediate and elementary compartments, and
        (2) no other compartment types are permitted. This may be unduly dogmatic. but I think it's OK.
        :return:
        """
        if self._local_file is not None:
            with open(self._local_file, 'r') as fp:
                local = Compartment.from_json(json.load(fp))
                self.compartments.add_syns(local.synonyms)
                local.uproot('Intermediate Flows', self.compartments)
                local.uproot('Elementary Flows', self.compartments)

    def save(self, force=False):
        if self._local_file is None:
            if force is False:
                raise ProtectedReferenceFile('Use force=True to force a rewrite of the reference file')
            print('Overwriting reference elementary flow compartments')
            file = REFERENCE_EFLOWS
        else:
            print('Updating local compartment file')
            file = self._local_file

        with open(file, 'w') as fp:
            json.dump(self.compartments.serialize(), fp, indent=2, sort_keys=True)

    def _crawl(self, clist, check_elem=False):
        """
        THIS is used to crawl an existing compartment hierarchy and find the one that matches a supplied compartment
        name. Compartment names are allowed to be sloppy (e.g. to omit steps, as long as they are monotonic in the
        hierarchy).

        The hard work is done by a static recursive function.  This wrapper is necessary because of the centrality of
        pop() to the underlying recursion.
        :param clist:
        :param check_elem: return as soon as compartment is determined to be elementary (without finishing the crawl)
        :return:
        """
        my_clist = []
        my_clist.extend(clist)
        return _crawl_compartments(self.compartments, my_clist, check_elem=check_elem)

    def is_elementary(self, flow):
        comp = self.find_matching(flow['Compartment'], check_elem=True, interact=False)
        if comp is None:
            return False
            # raise MissingCompartment('Cannot check if unknown compartment %s is_elementary' % flow['Compartment'])
        return comp.elementary

    # inspection methods
    def filter_exch(self, process_ref, elem=True, **kwargs):
        """
        Given a process reference, return a list of exchanges that match the specification
        :param process_ref:
        :param elem: [True] whether to return elementary [if True] or intermediate [if False] exchanges
        :param kwargs: passed to fg_lookup
        :return:
        """
        return [x for x in process_ref.archive.fg_lookup(process_ref.id, **kwargs)
                if self.is_elementary(x.flow) is elem]

    def find_matching(self, compartment_name, interact=True, check_elem=False, force=False):
        """
        :param compartment_name: a monotonic list of compartment names (as stored in an archive)
        :param interact: whether to interactively add / merge missing compartments
        :param check_elem: whether to bail out as soon as the compartment is determined to be elementary
        :param force:

        :return: matching or newly merged compartment; or None if check_elem is True and no compartment found
        """
        if compartment_name is None:
            return None
        compartment_name = _ensure_list(compartment_name)
        cs = compartment_string(compartment_name)
        if cs in self._c_dict.keys():
            return self._c_dict[cs]

        match = self._crawl(compartment_name, check_elem=check_elem)
        if match is None and check_elem is False:
            if interact:
                try:
                    c = self._merge_compartment(compartment_name, force=force)
                    match = self._crawl(compartment_name)
                    if c is match and c is not None:
                        print('match: %s' % match.to_list())
                        self._c_dict[cs] = match
                        return match
                    else:
                        raise MissingCompartment('Merge failed: %s' % c)
                except ProtectedReferenceFile:
                    pass
            return None
            # raise MissingCompartment('%s' % compartment_name)
        return match

    def add_compartment(self, compartment_name, parent='Intermediate Flows', force=False):
        """
        Add a compartment recursively to a named parent.
        :param compartment_name:
        :param parent:
        :param force:
        :return:
        """
        if not self.writeable and not force:
            raise ProtectedReferenceFile('set_local(file) to store a local compartment hierarchy (or force=True)')
        if not isinstance(parent, Compartment):
            parent = self._crawl(_ensure_list(parent))
        sub = parent.add_subs(compartment_name)
        return sub

    def _merge_compartment(self, compartment_name, force=False):
        """
        Recursively and interactively merge a compartment specifier into the existing hierarchy.

        crawls the compartment hierarchy until it runs out of matches. Beginning with the last match, successively
        prompts the user to pick a subcompartment to descend into, to add the current term as a synonym, or to add
        the current term as a subcompartment.

        :param compartment_name:
        :param force: [False] set to True in order to modify the reference eflows
        :return:
        """
        if not self.writeable and not force:
            raise ProtectedReferenceFile('Specify a target file to add to the compartment hierarchy (or force=True)')
        compartment = self.compartments
        my_missing = []
        my_missing.extend(compartment_name)
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
                    return compartment.add_subs(my_missing)
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
        if check_elem and compartment.elementary:
            # compartments never switch back from elementary, so if we're just checking elem, bail out here
            return compartment
    while len(clist) > 0 and clist[0] is None:
        clist.pop(0)
    if len(clist) > 0:
        if bool(re.search('unspecified$', clist[0], flags=re.IGNORECASE)):
            clist.pop(0)
    if len(clist) == 0:
        return compartment
    else:
        for s in compartment.subcompartments():
            n = _crawl_compartments(s, clist, check_elem=check_elem)
            if n is not None:
                return n
        return None


class Compartment(object):
    """
    A hierarchical listing of compartments.  A compartment contains subcompartments, which are themselves compartments.

    Each compartment has a canonical name and a set of synonyms.
    """
    @classmethod
    def from_json(cls, j, elementary=False, **kwargs):
        """
        Classmethod to build compartment hierarchy from serialization
        :param j:
        :param elementary: [False] root compartment should not be elementary unless
        :return:
        """
        rootname = j['name']
        if not isinstance(rootname, list):
            rootname = [rootname]
        root = cls(rootname[0], elementary=elementary, **kwargs)  # root compartment is not elementary
        for i in rootname[1:]:
            root.add_syn(i)
        root._add_subs_from_json(j['subcompartments'], elementary=elementary)
        return root

    def add_branch_from_json(self, j):
        branch = Compartment.from_json(j, parent=self)
        self._merge_sub(branch)

    def _add_subs_from_json(self, subs, elementary=False):
        for sub in subs:
            if elementary:
                elementary_sub = True
            elif 'elementary' in sub.keys():
                elementary_sub = bool(sub['elementary'])
            else:
                elementary_sub = False
            syns = sub['name']
            if not isinstance(syns, list):
                if syns is None or syns.lower() == 'unspecified':
                    continue
                syns = [syns]

            sc = self.add_sub(syns[0], elementary=elementary_sub)
            for syn in syns[1:]:
                sc.add_syn(syn)
            sc._add_subs_from_json(sub['subcompartments'], elementary=elementary_sub)

    def __init__(self, name, parent=None, elementary=False):
        self._name = name
        self._synonyms = set()
        assert isinstance(elementary, bool)
        self._elementary = elementary
        self._subcompartments = set()
        if parent is not None:
            assert isinstance(parent, Compartment)
        self.parent = parent
        self._id = '; '.join(self.to_list())

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._synonyms.add(self._name)
        if value in self._synonyms:
            self._synonyms.remove(value)
        self._name = value

    @property
    def synonyms(self):
        return {self.name}.union(self._synonyms)

    def add_syn(self, syn):
        if syn != self.name:
            self._synonyms.add(syn)

    def add_syns(self, syns):
        for i in syns:
            self.add_syn(i)

    def add_sub(self, name, elementary=None, verbose=False):
        """
        make a new subcompartment based on name only.  if parent is elementary, child will be as well.
        :param name:
        :param elementary: force elementary with arg.
        :param verbose:
        :return:
        """
        try:
            sub = self[name]
        except KeyError:
            if elementary is None or self._elementary is True:
                elementary = self._elementary
            if verbose:
                print('New compartment %s [elementary: %s]' % (name, elementary))
            sub = Compartment(name, parent=self, elementary=elementary)
        self._merge_sub(sub)
        return sub

    def add_subs(self, subs, verbose=False):
        """
        input is a list of nested subcompartments to be added recursively
        :param subs:
        :param verbose:
        :return:
        """
        subs = _ensure_list(subs)
        if len(subs) == 1:
            return self.add_sub(subs[0], verbose=verbose)
        else:
            sub = self.add_sub(subs[0], verbose=verbose)
            return sub.add_subs(subs[1:], verbose=verbose)

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

    def known_names(self, up='', print_tree=False):
        s = self.name
        if s is None:
            s = '##NONE##'
        ls = self._names()
        if print_tree is True:
            print(up + '; '.join(ls))
        up += s
        for x in sorted(self.subcompartments(), key=lambda z: z.name):
            ls += x.known_names(up=up + ' -- ', print_tree=print_tree)
        return ls

    def set_elementary(self):
        self._elementary = True
        for i in self.subcompartments():
            i.set_elementary()

    def unset_elementary(self, unset_children=False):
        self._elementary = False

        if unset_children is True:
            for i in self.subcompartments():
                i.set_elementary()

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(self._id)

    def __eq__(self, other):
        return (self.name in other.synonyms) and (self.parent == other.parent)

    def __contains__(self, item):
        """
        Tests whether item is a name or synonym for self
        :param item:
        :return:
        """
        if item in self.synonyms:
            return True
        return False

    def __getitem__(self, item):
        """
        subcompartment accessor; returns a subcompartment that contains item
        :param item:
        :return:
        """
        for x in self._subcompartments:
            if item in x.synonyms:
                return x
        raise KeyError('No subcompartment found')

    def delete(self, item):
        s1 = self._ensure_comp(item)
        if len(s1._subcompartments) > 0:
            raise ValueError('Subcompartment not empty')
        self._subcompartments.remove(s1)

    def _merge_sub(self, comp):
        """
        take an existing compartment and make it a subcompartment of self.  If self already has a subcompartment
        whose synonyms match the argument (first encountered), then the argument is combined with the already-matching
        subcompartment. This merge is performed recursively, with the incoming subcompartment's subcompartments being
        merged with the existing subcompartment.
        Otherwise, the argument is introduced as a new subcompartment.
        :param comp: existing unattached compartment
        :return:
        """
        for i in self._subcompartments.union({self}):
            if i.synonyms.intersection(comp.synonyms):
                # if an existing match is found, merge subcompartments recursively
                i.add_syns(comp.synonyms)
                for j in comp.subcompartments():
                    i._merge_sub(j)
                return

        # still around?
        self._subcompartments.add(comp)
        comp.parent = self

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
            s2._merge_sub(i)

        if s1 in self._subcompartments:
            self._subcompartments.remove(s1)

    def uproot(self, merged, new_parent):
        """
        Uproot a subcompartment from self and install it as a subcompartment of a new parent.
        :param merged: must be a subcompartment
        :param new_parent:
        :return:
        """
        s1 = self._ensure_comp(merged)
        s2 = self._ensure_comp(new_parent)
        s2._merge_sub(s1)
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
        s1 = self._ensure_comp(subcompartment)
        self._collapse(s1)
        self._subcompartments.remove(s1)

    '''
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
    '''

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
