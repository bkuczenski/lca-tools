"""
Used to generate disclosures of the sort required by lca_disclosures - mainly derived from traversal results.

Every distinct fragment.reference_entity (including distinct None reference flows) should be a column in Af .
By extension, every FragmentFlow that terminates in a fragment should be an entry in Af.  Every FragmentFlow that
terminates in a bg should be an Ad entry, and every null termination should be a cutoff (note that emissions are
currently modeled as foreground-terminated flows with no children, but after ContextRefactor they will be terminated
to compartments;; until that time, only unobserved_exchanges will be included in emissions)

When a flow terminates in a subfragment, the treatment is different for descend=True and descend=False.

In the former case, the subfragment's flows will be duplicated for every instance of the subfragment.  Thus, to ensure
uniqueness, the fg node's "name" should be the fragment's external ref SUFFIXED BY its sequential FFID.  This should
be true for every fragment flow whose top() is not the reference fragment.  For descended-into fragments, their IO
flows will be forwarded out and will appear to emanate from the parent fragment, as distinct fg flows.

In the non-descend case, the subfragment will need to be replicated explicitly in the disclosure-- add it to the
foreground and enqueue the term's subfragments (already cached) to be traversed later.  Any cutoff children of the
parent fragment will thus have emanated from the subfragment and should be discarded.  On the other hand, any non-
cutoff children (i.e. child fragments that have stuff going on) need to be *subtracted* from the cutoff flows, as they
will get fed back in during the traversal) and added equally-and-oppositely as new fg flows to continue following.

Still TODO: figure out how to log these things.  An OFF should be created upon processing the FragmentFlow... when we
get an ff, say we're in a subfragment- we know the most recent parent from the _parents list-- which starts out None,
for the overall reference flow.   So if the end of the _parents list is not None, we are in a subfragment, in which
case the fragment name should be uniquified.-- should be the parent FF's FFID-- so an OFF also needs to know its own
FFID- which is just its position in the sequence of encountered FFIDs.  In fact, that duple - parent, index- SHOULD BE
the unique key-- no, has to be the full tuple of all parents up to None-- but fine, that's the key- and retrieving the
key should get us the OFF, which tells us the fragment entity and the FFID; and trimming the last entry from the key
gives us the key to the most recent parent, which gives us the context we need to determine descent.

Then the only reverse mapping we need is from ff to key, so that given a fragmentflow from the sequence we can retrieve
its OFF without knowing its FFID.


here we go:

To start off, add ffs[0].fragment to running tops key, add None to running parents key

OFF has ff, ffid, parent, observed node_weight

input: FragmentFlow object
known: key for current parent

while ff.fragment.top() is not tops[-1]:
    try:
        tops.pop()
        parents.pop()
    except IndexError:
        if ff.fragment.reference_entity is not None:
            raise SomeKindaError
        print('starting new traversal\n%s' % ff
        tops = [ff.fragment]
        parents = None
        break



send it to list, get back its FFID.  {FF -> FFID}
create OFF

exchange value is off.value / parent_off.value

if null: it's a cutoff->
  if parent OFF.term.is_subfrag and OFF.term.descend is False, continue
  else, add it to cutoffs, column = parent
elif fg emission (detectable??)->
  add it to emissions, column = parent
elif process ref->
  if fg:
    for term's unobserved exchanges, add to emissions, column = parent
  elif bg:
    add to background, column = parent
elif frag:
  if descending subfrag->
    add self to parents
    add term to tops
    add OFF to foreground
    add Af entry, column = parent, row = self
  elif non-descending subfrag->
    enqueue term's cached subfragments in deque
    add term's term_node-- somehow--

more to this than meets the eye.. FUCK




"""

from collections import deque

from lcatools.interfaces import comp_dir

'''
from lca_disclosures import BaseExporter


class LcaDisclosure(BaseExporter):

    def _prepare_efn(self):
        return self.filename or self.[0].fragment.external_ref

    def __init__(self, foreground, background, emissions, Af, Ad, Bf, filename=None):
        self._ffs = fragment_flows
        self.filename = filename

    def _prepare_disclosure(self):
        pass
'''

# ff is the actual FragmentFlow; ffid is its position in the sequence;
# parent is the key to the parent off
# pnw is the node weight of the parent node
# off.ff.node_weight / off.pnw = the exchange value to put in the matrix
#
# ObservedFragmentFlow = namedtuple('ObservedFragmentFlow', ('ff', 'key', 'ffid', 'pnw'))


class UnobservedFragmentFlow(Exception):
    pass


class EmptyFragQueue(Exception):
    pass


class ProxyParent(object):
    def __init__(self, off):
        self.fragment = off.term.term_node
        self.term = off.term


class ObservedFragmentFlow(object):
    def __init__(self, ff, key):
        """

        :param ff: the fragmentflow from the traversal
        :param key: the fg key for the fragment
        """
        self.ff = ff
        self.key = key
        self._parent = None

    def observe(self, parent):
        self._parent = parent

    @property
    def parent(self):
        if self._parent is None:
            raise UnobservedFragmentFlow
        return self._parent

    @property
    def ffid(self):
        return self.key[-1]

    @property
    def value(self):
        if self.parent is RX:
            return self.ff.magnitude
        return self.ff.node_weight / self.parent.magnitude

    @property
    def magnitude(self):
        if self.parent is RX:
            return self.ff.magnitude
        return self.ff.node_weight

    @property
    def fragment(self):
        return self.ff.fragment

    @property
    def term(self):
        return self.ff.term

    @property
    def flow_key(self):
        return self.ff.fragment.flow, self.ff.fragment.direction

    def observe_bg_flow(self):
        bg = ObservedBgFlow(self.ff, self.ffid)
        bg.observe(self.parent)
        return bg

    def __repr__(self):
        return str(self)

    def __str__(self):
        return 'ObservedFragmentFlow(Parent: %s, Term: %s, Magnitude: %g)' % (self.parent.key,
                                                                              self.key,
                                                                              self.magnitude)


RX = ObservedFragmentFlow(None, None)


class ObservedBgFlow(ObservedFragmentFlow):
    @property
    def bg_key(self):
        return self.ff.term.term_node, self.ff.term.term_flow

    def __str__(self):
        return 'ObservedBg(Parent: %s, Term: %s, Magnitude: %g)' % (self.parent.key, self.bg_key, self.magnitude)


class ObservedCutoff(object):
    @classmethod
    def from_off(cls, off, negate=False):
        return cls(off.parent, off.fragment.flow, off.fragment.direction, off.ff.magnitude, negate=negate)

    @classmethod
    def from_exchange(cls, parent, exch):
        mag = parent.magnitude * exch.value
        return cls(parent, exch.flow, exch.direction, mag)

    def __init__(self, parent, flow, direction, magnitude, negate=False):
        """

        :param parent:
        :param flow:
        :param direction:
        :param magnitude:
        :param negate: [False]
        """
        self.parent = parent
        self.flow = flow
        self.direction = direction
        self.magnitude = magnitude
        self.negate = negate

    @property
    def value(self):
        val = self.magnitude / self.parent.magnitude
        if self.negate:
            val *= -1
        return val

    @property
    def key(self):
        return self.flow, self.direction

    def __repr__(self):
        return str(self)

    def __str__(self):
        return 'ObservedCutoff(Parent: %s, %s: %s, Magnitude: %g)' % (self.parent.key, self.direction,
                                                                      self.flow, self.magnitude)


class SeqList(object):
    def __init__(self):
        self._l = []
        self._d = {}

    def index(self, key):
        if key not in self._d:
            ix = len(self._l)
            self._l.append(key)
            self._d[key] = ix

        return self._d[key]

    def __len__(self):
        return len(self._l)

    def __getitem__(self, key):
        try:
            return self._l[key]
        except TypeError:
            return self._d[key]

    def to_list(self):
        return self._l


class SeqDict(object):
    def __init__(self):
        self._l = []
        self._d = {}
        self._ix = {}

    def __setitem__(self, key, value):
        if key in self._d:
            raise KeyError('Value for %s already set!' % key)
        ix = len(self._l)
        self._l.append(key)
        self._d[key] = value
        self._ix[value] = ix
        self._ix[key] = ix

    def __len__(self):
        return len(self._l)

    def __getitem__(self, key):
        try:
            return self._d[key]
        except KeyError:
            return self._d[self._l[key]]

    def index(self, item):
        return self._ix[item]

    def to_list(self):
        return [self._d[x] for x in self._l]


class TraversalDisclosure(object):
    """
    Take a sequence of fragmentflows and processes them into an Af, Ad, and Bf
    """

    def __init__(self, fragment_flows, ):
        """
        Each incoming fragment flow implies the existence of:
         - a distinct foreground flow (i.e. column of af) as the ff's parent - always known or None
         - a distinct row in Af, Ad, or Bf, as the term - always determinable from the fragment

        so we ensure
        from that we create an observed fragment flow
        :param fragment_flows:
        """

        self._ffqueue = deque(fragment_flows)  # once thru

        self._frags_seen = dict()  # maps to key of ffid

        self._deferred_frag_queue = deque()  # for non-descend subfragments, for later

        self._ffs = []

        self._descents = []  # add ffids
        self._parents = []  # keep a stack of OFFs

        """
        These are really 'list-dicts' where they have a sequence but also a reverse-lookup capability.
        functionality tbd
        """
        self._fg = SeqDict()  # log the flow entities we encounter; map key to OFF
        self._co = SeqList()  # map index to (flow, direction)
        self._bg = SeqList()  # map index to (process_ref, term_flow)
        self._em = SeqList()  # map index to (flow, direction)

        self._Af = []  # list of OFFs
        self._Ac = []  # list of cutoffs  (get appended to Af)
        self._Ad = []  # list of OBGs
        self._Bf = []  # list of emissions

        self._key_lookup = dict()  # map ff key to type-specific (map, key)

    def __iter__(self):
        return self

    def __next__(self):
        return self.next_ff()

    def _make_key(self, ffid):
        return tuple(self._descents + [ffid])

    def __getitem__(self, key):
        _map, _key = self._key_lookup[key]
        return _map[_key]

    def _show_mtx(self, mtx):
        tag = {'Af': 'Foreground',
               'Ac': 'Cutoffs',
               'Ad': 'Background',
               'Bf': 'Emissions'}[mtx]  # KeyError saves
        if mtx != 'Af':
            print('\n')
        print('%s - %s' % (mtx, tag))
        for k in getattr(self, '_' + mtx):
            print(k)

    def show(self):
        self._show_mtx('Af')
        self._show_mtx('Ac')
        self._show_mtx('Ad')
        self._show_mtx('Bf')

    def _add_parent(self, parent):
        print('Adding parent %s' % parent)
        self._parents.append(parent)

    def _pop_parent(self):
        pop = self._parents.pop()
        print('Popped parent %s' % pop)
        return pop

    @property
    def _current_parent(self):
        try:
            return self._parents[-1]
        except IndexError:
            return RX

    def _add_deferred_dependency(self, off, tgt):
        """
        The off is the parent, the off.term.term_node is the term
        :param off:
        :return:
        """
        dep = ObservedFragmentFlow(off.ff, tgt)
        dep.observe(off)
        self._Af.append(dep)

    def _feed_deferred_frag(self):
        """
        Processes the deferred fragment queue-
         - if the queue is empty, returns False
         - if the queue is nonempty:
            = if the target frag has already been seen, add the deferred dependency and then go back to the queue
            = otherwise, send the cached subfragments into the ff queue and return the deferred off
              (deferred dependency must be added after next ffid has been created)
\        """
        while 1:
            try:
                # deferred is an off whose term is a non-descend subfrag
                deferred = self._deferred_frag_queue.popleft()
            except IndexError:
                return False

            if deferred.term.term_node in self._frags_seen:
                self._add_deferred_dependency(deferred, self._frags_seen[deferred.term.term_node])
            else:
                # re-traversal is necessary to properly locate cutoffs
                ffs = deferred.term.term_node.traverse(**deferred.ff.subfragment_params)
                self._ffqueue.extend(ffs)
                return deferred

    def _get_next_fragment_flow(self):
        deferred = None
        try:
            ff = self._ffqueue.popleft()
        except IndexError:
            deferred = self._feed_deferred_frag()
            if deferred:
                ff = self._ffqueue.popleft()
            else:
                raise EmptyFragQueue

        ffid = len(self._ffs)
        self._ffs.append(ff)

        new_off = ObservedFragmentFlow(self._ffs[ffid], self._make_key(ffid))

        if deferred:
            self._add_deferred_dependency(deferred, new_off.key)

        return new_off

    def _traverse_node(self, off):
        """
        Creates a new node from the most recent OFF
        :param off: the observed fragment flow
        :return:
        """
        print('Handling as FG')
        self._fg[off.key] = off
        self._key_lookup[off.key] = (self._fg, off.key)
        self._Af.append(off)
        self._add_parent(off)

    def _add_background(self, off):
        """

        :param off: an OFF
        :return:
        """
        print('Handling as BG')
        obg = off.observe_bg_flow()
        ix = self._bg.index(obg.bg_key)
        self._key_lookup[off.key] = (self._bg, ix)
        self._Ad.append(obg)

    def _add_cutoff(self, oco, extra=''):
        """

        :param oco: the Observed Cutoff
        :return:
        """
        print('Adding Cutoff %s' % extra)
        ix = self._co.index(oco.key)
        self._key_lookup[oco.key] = (self._co, ix)
        self._Ac.append(oco)

    def _add_emission(self, oco):
        """

        :param oco: the Observed Fragment Flow
        :return:
        """
        print('Adding Emission')
        ix = self._em.index(oco.key)
        self._key_lookup[oco.key] = (self._em, ix)
        self._Bf.append(oco)

    def next_ff(self):
        try:
            off = self._get_next_fragment_flow()
        except EmptyFragQueue:
            raise StopIteration
        print(off.ff)

        if off.ff.magnitude == 0:
            print('Dropping zero-weighted node')
            return

        # new fragment--
        if off.ff.fragment.reference_entity is None:
            assert off.ff.fragment not in self._frags_seen
            off.observe(RX)
            self._frags_seen[off.ff.fragment] = off.key
            # self._traverse_node(off)

        else:
            # upon descent, we load up the descender and then the subfrag on the stack
            while off.ff.fragment.reference_entity is not self._current_parent.fragment:
                try:
                    oldparent = self._pop_parent()
                except IndexError:
                    print('Ran out of parents to pop!')
                    print(off)
                    print(off.ff)
                    raise

                if len(self._descents) > 0:
                    if self._ffs[self._descents[-1]].fragment is oldparent:
                        self._descents.pop()

            parent = self._current_parent  # last-seen fragment matching parent is ours!

            off.observe(parent)

            if parent.term.is_subfrag and not parent.term.descend:
                if off.ff.term.is_null:
                    # drop cutoffs from nondescend subfrags because they get traversed later
                    print('Dropping enclosed cutoff')
                    return
                # otherwise we need to "borrow" from their cutoffs to continue our current op

                self._handle_term(off)  # off gets observed here

                oco = ObservedCutoff.from_off(off, negate=True)
                self._add_cutoff(oco, ' * negated')
                return off

        return self._handle_term(off)

    def _handle_term(self, off):
        """
        Upon arrival here, the last entry in self._parents should be our column; our ff determines the rest

        the term can be any of the following:
         * null -> cutoff (parent stays same)
         * bg -> add ad (parent stays same)

         * fg process -> traverse, add unobserved exchanges as emissions

         * fg -> traverse
         * nondescending subfrag -> traverse, add to deferred list
         * descending subfrag -> traverse, add to _descents, add term to _parents

        :param off: the observed fragment flow
        :return:
        """
        if off.ff.term.is_null:
            self._add_cutoff(ObservedCutoff.from_off(off))
        elif off.ff.term.term_is_bg or off.fragment.is_background:
            self._add_background(off)
        elif off.ff.term.is_emission:
            self._add_emission(ObservedCutoff.from_off(off))
        else:
            self._traverse_node(off)  # make it the parent
            if off.ff.term.is_subfrag:
                self._add_parent(ProxyParent(off))
                if off.ff.term.descend:
                    self._descents.append(off.ffid)
                else:
                    self._deferred_frag_queue.append(off)
            else:
                # add unobserved exchanges--
                for x in off.ff.term._unobserved_exchanges():
                    emf = ObservedCutoff.from_exchange(off, x)
                    self._add_emission(emf)

        return off

    @property
    def functional_unit(self):
        return self._fg[0]

    def generate_disclosure(self):
        _ = [x for x in self]  # ensure fully iterated
        p = len(self._fg)

        d_i = [(off.fragment.flow, comp_dir(off.fragment.direction)) if off.parent is RX
               else (off.fragment.flow, off.fragment.direction) for off in self._fg.to_list()]
        d_i += self._co.to_list()

        d_ii = self._bg.to_list()

        d_iii = self._em.to_list()

        d_iv = []
        d_v = []
        d_vi = []

        for off in self._Af:
            if off.parent is RX:
                continue
            d_iv.append([self._fg.index(off.key), self._fg.index(off.parent.key), off.value])
        for oco in self._Ac:
            d_iv.append([p + self._co.index(oco.key), self._fg.index(oco.parent.key), oco.value])

        for obg in self._Ad:
            d_v.append([self._bg.index(obg.bg_key), self._fg.index(obg.parent.key), obg.value])

        for oem in self._Bf:
            d_vi.append([self._em.index(oem.key), self._fg.index(oem.parent.key), oem.value])

        return d_i, d_ii, d_iii, d_iv, d_v, d_vi
