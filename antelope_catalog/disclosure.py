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

from collections import namedtuple, defaultdict, deque

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


DeferredSubFragment = namedtuple('DeferredSubFragment', ('parent_key', 'cached_subfrags', 'value'))


class ObservedFragmentFlow(object):
    def __init__(self, ff, ffid, parent):
        """

        :param ff: the fragmentflow from the traversal
        :param ffid: the position of the fragment flow in the traversal sequence
        :param parent: an OFF, or None for ref exch
        """
        self.ff = ff
        self.ffid = ffid
        self.parent = parent

    @property
    def value(self):
        if self.parent is None:
            return self.ff.magnitude
        return self.ff.node_weight / self.parent.value

    @property
    def fragment(self):
        return self.ff.fragment

    @property
    def flow_key(self):
        return self.ff.fragment.flow, self.ff.fragment.direction


class ObservedBgFlow(ObservedFragmentFlow):
    @property
    def bg_key(self):
        return self.ff.term.term_node, self.ff.term.term_flow


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
        val = self.magnitude / self.parent.value
        if self.negate:
            val *= -1
        return val

    @property
    def flow_key(self):
        return self.flow, self.direction


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

    def __getitem__(self, key):
        return self._d[key]

    def index(self, item):
        ix = self._ix[item]
        return self._l[ix]  # give back the key (???)


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

        self._frag_queue = []  # for non-descend subfragments, for later

        self._ffs = []

        self._descents = []
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

    def _make_key(self, ffid):
        return tuple(self._descents + [ffid])

    def _new_fg_node(self, ffid, parent):
        """
        Creates a new node from the most recent FFID
        :param ffid: the index into the ffs array
        :param parent: the OFF of the parent node
        :return:
        """
        off = ObservedFragmentFlow(self._ffs[ffid], ffid, parent)
        self._fg[self._make_key(ffid)] = off
        self._Af.append(off)
        self._parents.append(off)
        return off

    def _add_background(self, ffid, parent):
        """

        :param off: an OFF
        :return:
        """
        obg = ObservedBgFlow(self._ffs[ffid], ffid, parent)
        self._bg.index(obg.bg_key)
        self._Ad.append(obg)

    def _add_cutoff(self, off, negate=False):
        """

        :param off: the Observed Fragment Flow
        :return:
        """
        oco = ObservedCutoff.from_off(off, negate=negate)
        self._co.index(oco.flow_key)
        self._Ac.append(oco)

    def next_ff(self):
        ff = self._ffqueue.popleft()

        ffid = len(self._ffs)
        self._ffs.append(ff)

        # new fragment--
        if ff.fragment.reference_entity is None:
            self._new_fg_node(ffid, None)

        else:
            try:
                while ff.fragment.reference_entity is not self._parents[-1].fragment:
                    self._parents.pop()
            except IndexError:
                print('Ran out of parents to pop!')
                raise
            parent = self._parents[-1]  # last-seen fragment matching parent is ours!

            if parent.term.is_subfrag and not parent.term.descend:
                if ff.term.is_null:
                    # drop cutoffs from nondescend subfrags because they get traversed later
                    return
                # otherwise we need to "borrow" from their cutoffs to continue our current op

                off = self._handle_term(ffid)

                self._add_cutoff(off, negate=True)
                return off

        return self._handle_term(ffid)

    def _handle_term(self, ffid):
        """
        Upon arrival here, the last entry in self._parents should be our column; our ff determines the rest

        the term can be any of the following:
         * null -> cutoff (parent stays same)
         * bg -> add ad (parent stays same)

         * fg process -> add unobserved exchanges as emissions (parent stays the same)

         * fg -> create a new off
         * nondescending subfrag -> add to deferred list, create a new off
         * descending subfrag -> add to _descents, create a new off

        :param key: 1:1 to columns, lookup into _fg, gives us OFF for off.ff.term and off.value
        :return:
        """








    @property
    def functional_unit(self):
        return self._fg[0]



