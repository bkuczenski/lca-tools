WHAT INTERFACES?

Antelope is [supposed to be] an API specification.  My thesis is that performing LCA computations requires access to different types of information.

First there's a basic interface, which basically retrieves things that exist and exposes the properties all entities have:
 - semantic origin
 - privacy status
 - uuid
 - arbitrary key-value content
 - reference entity

the type of the reference entity is as follows:

 | Type | Reference Entity Type |
 |----|-----|
 | quantity | unit |
 | context | natural direction |
 | flow | quantity |
 | process | set of exchanges |
 | fragment | fragment |

All object types are also permitted to have `None` as a reference, though some operations require a reference to be present.

Then there are operations performed on the entities.  I grouped these somewhat arbitrarily into index, inventory, background, and quantity; then recently I added foreground (for authoring + editing models) and configure (for manipulating reference flows and allocation)

`*` indicates a feature in development
`#` indicates a possible new feature / interface

## Read-only interfaces

INDEX

Handles data discovery and reference and metadata retrieval. "Termination" is a search for processes that can act as a complementary source or sink for a given flow or exchange, so that is a potentially rich / contextual query.

 - list, count, and filter entities by type
 - retrieve properties
 - terminate flows or exchanges
 - get synonyms for entity descriptors

QUANTITY

Handle quantitative information about physical measurements or characterizations of flowable entities.

 - retrieve canonical quantity [grounded in a semantic reference]
 - retrieve a flow profile (list of characterizations)
 - retrieve characterization factors by quantity
 - convert a given flow between quantities
 - compute the quantity relation given flowable, context, reference and query quantity, and optional location (always returns a float)

INVENTORY

Handle quantitative information about exchanges of flows by processes.

 - retrieve exchanges or exchange values by process specification
 - retrieve inventory list, either allocated or unallocated
 - compute the exchange relation given a process, reference and exchange flow, exchange direction, and optional termination (always returns a float)
 - perform foreground LCIA using local quantity resources

BACKGROUND

Handle connectivity / adjacency of processes; perform partial ordering, foreground identification, and lci computation.  A background engine is required in order to compute a strongly connected database.  Both index and inventory access to a resource must be available to compute the background.

 - distinguish foreground from background flows
 - distinguish interior from exterior flows
 - compute lci
 `*` retrieve foreground as weighted adjacency list (list of ExchangeValues)
 `*` retrieve dependencies for a given node (fg or bg)
 `*` retrieve emissions (exterior exchanges- both elementary and intermediate) for a given node
 - perform background LCIA using local quantity resources

## Writable (local-only?) interfaces

FOREGROUND

Construct fragments of product system models from entity references.  Flows are created locally and don't really fit into an interface, but remote communication of flows would happen here.

 - create flows and fragments by specification
 - find or create a fragment that terminates a given exchange
 `*` specify a termination for a fragment
 `*` create a fragment from a given reference node- requires inventory access
 `*` create a foreground forest for a given reference node- requires background access
 - copy fragments
 `*` split / join subtree or subfragment
 - traverse a fragment
 - terminate a fragment
 - perform fragment LCIA
 `#` edit fragment observational data and metadata
 `#` edit fragment scenario specifications


CONFIGURE

Created to manipulate allocation + references.  Basically this should be replaced by ocelot.

 - set or unset a given exchange as a reference
 - add a characterization value to a given flow
 - specify a quantity for partitioning allocation

There is not currently an interface for specifying an arbitrary allocation. All manner of system expansion or otherwise resolving multifunctionality can be performed through foreground construction.




==========
Tue Mar 06 22:13:30 -0800 2018



Re-evaluation of the BackgroundInterface

BackgroundInterface 




==========
Fri Jan 19 15:29:56 -0800 2018

This is a bit misplaced... but the 'interfaces.py' file for ArchiveInterface needs to be moved to an EntityStore.  The NsUuidArchive functionality needs to be integrated, and some of the basic functionality needs to be subclassed to BasicArchive.



ArchiveInterface:
    _key_to_id <- get_uuid, __getitem__, _add, validate_entity_list
    	       <- Basic.entity_from_json
	       <- Foreground.name_fragment
    get_uuid [deprecated]

    make_interface [overloads]

    _construct_new_ref <- create_descendant

    entities [0]

    set_upstream <- __init__
    get_names <- LcResource

    _print

    _get_entity
    __getitem__

    _add [overloads]

    check_counter [many]

    _narrow_search <- search

    find_partial_id [0]

    

    @properties
    ref
    static
    source






==========
Mon Nov 27 23:04:41 -0800 2017

We ARE progressing. We just need to get sorted the whole reference() references() find_reference() nonsense.

What is it we want to accomplish?

 - We need to know whether a given flow is listed in a reference exchange for a process
 - we need to know whether a given exchange is a reference exchange
 - we need to know whether a given exchange shows up as an allocation key

What is the architecture?

An Exchange is distinguished by its key == hash == (process.uuid, flow.external_ref, direction, termination.external_ref or None)

A process has:
 a dict of _exchanges that maps key to ExchangeValue
 a set of reference exchange keys, which is a subset of the keys in _exchanges

==========
Wed Nov 29 12:08:05 -0800 2017

Thinking strictly from the interfaces perspective: we want to accept the following reference specifications:
 * an exchange-like entity (Exchange or subclass or RxRef) (( maybe those should have a common ancestor ?? )) that matches a reference
 * an external ID for a flow that is present in a reference
 * None
 * a literal flow or flow_ref that matches a flow in a reference

There is also the terribly fuzzy _find_reference_by_string that also does regexp comparisons against name, UUID, and external_ref.  This may well be a case of "too nice to the user" but it's hard to say.





==========
Wed Nov 22 13:54:38 -0800 2017

Fragment interface

The unit_inventory() method returns two sets of FragmentFlows: interior and exterior, with the exterior flows being grouped together by group_ios.

Wed 2017-11-22 14:59:23 -0800
Numerous snags here... first off, couldn't import group_ios because FragmentFlow depended on CatalogRef... so I masked CatalogRef inside of antelope...
then we have this peculiar problem with making catalog refs for reference entities that have different origins-- they are failing their lookups with the original query, for obvious reasons. Worst, this problem is apparently nondeterministic, because it only occurs sometimes.
Now, a big challenge with unit_inventory-- as it requires to be run on self.top() and not self, and apparently I actually make connections to non-reference flows in the AUOMA study, so this is implicated.  I suppose there isn't really a good reason not to just make a real-live unit_inventory method in the interface... the main reason why not is that Antelope v1 doesn't support it.

I seem to remember thinking at some point about requiring the term_node to be the actual reference fragment, and using the linked flow as term_flow-- that would be analogous to how it works with processes, I suppose.
But to implement that I would actually need to modify the AUOMA study, which is not desirable.
ok, let's pause and eat lunch.

Wed 2017-11-22 16:40:58 -0800

So there's an easy answer-- just force the fragment ref to only traverse top fragments. This WILL require updates to the AUOMA model when we try to implement remote calrecycle.

The bigger problem is with the reference entities failing their lookups.  This is weird and it seems like the problem has to do with adding an entity from an NsUuidArchive to an IlcdArchive, where it seems like the entity retains its old externalId even though that is no longer applicable.

Let's test that.

Wed 2017-11-22 21:55:52 -0800

Current status:

 * figured a workaround for the spurious external refs: simply disallow them if they don't validate
 * figured out a fix for unit conversions: str(q) was not what I wanted- q_name was
 * unit_inventory still not working as expected-- I imagine this is because of equality test. more debugging to come.

Sun 2017-11-26 14:28:44 -0800

Update on the equality test.

Number one: entity refs had no equality test, so this was primed to fail eventually. Now they do and the problem persisted.

Number two: the problem comes about because of mismatched origins between the parent fragment flow (calrecycle.uolca.core) and the termination (foreground.calrecycle.uolca).  This reveals two essential issues:

 * semantically, 'foreground' should NOT be a leading origin term.  It should be calrecycle.uolca.foreground or even calrecycle.uolca.core.foreground.  The latter would have masked this issue completely, with the current entity_ref equality test.

 * operationally, when traversing a remote fragment, the fragment's own reference flow should ... what? the origin mismatch issue is kind of major.

I see two different approaches.  Possibly both are required.

(1) The bizarre practice, originating from basically when the original catalog was first deployed for the AUOMA study, of naming 'foreground' as comparable to 'local' in terms of semantic dominance, needs to be reversed, meaning that all the AUOMA fragment records need to be corrected so that the origins read 'xxx.yyy.foreground' and not 'foreground.xxx.yyy'

(2) it's possible that fragments should not serialize their origins AT ALL, in which case we need a better way to specify the fragments' origins.  Let's think this through.

    One idea would be to substitute the 'fragments' directory with a 'semantic.origin' directory, and omit the origin specification in serialized fragments.  This also solves the problem of LcForegrounds not really knowing their origins endogenously-- they would be encoded into the fragment definitions.

    Entities still have their origins encoded, because entities don't change origins just because they change locations.  But fragments .. are different?  a fragment's origin is whatever reference will tell you about the fragment- which means whatever reference deserializes the fragment- which means

Sun 2017-11-26 15:27:14 -0800

I'm getting sketched out about this, making design decisions in the middle of a refactor that is historically long to complete and against a non-working master branch.

There's lots of things going on here- de-emphasizing uuids in favor of links, of external refs; changing norms and protocols for local and remote fragment terminations; without any spec document to speak of.

What's the problem we're encountering? Let's spell it out.

1. AUOMA foreground fragment terminates in a CalRecycle UO-LCA fragment.
2. Recursively asks fragment ref for a unit_inventory()
3. CalRecycle fragment traverses; prepares unit inventory.
   3a. Per unit_inventory, adds fragment's reference flow as another I/O flow
   3b. Because this is being done remotely, using the fragment ref, the flow is a flow ref constructed with the same origin as fragment. This ignores the actual origin of the flow entity.
   3c. THIS MEANS THAT THE FLOW HAS CHANGED ORIGINS during the referencing process
   3d. Essentially, this means the remote archive is masquerading as the origin of the flow.
   3e. Therefore, had this fragment ref been constructed using the modern protocol, the reference and the de-reference would be self-consistent
4. Because the fragment reference was constructed in the old-skool, it made reference to the flow's literal origin, rather than its masqueraded origin
5. Therefore there is an origin mismatch and the equality test fails.

From this, IF we are okay with remote archives masquerading fragments' flows' origins, then the correct thing to do is to update the termination flows to have the same origins as the remote fragment refs

But there's a snag-- it's not the termination that's the basis of comparison-- it's the local fragment's flow itself.  THAT flow was collected from a search and would NOT have gotten masqueraded.

Interesting.... I don't have index access to calrecycle.uolca.core. That means I could not have gotten the flow from a search against calrecycle.uolca.core.  

No, there's some other logic that is failing here. I think the failure is 


Sun 2017-11-26 21:32:00 -0800

Some serious progress AND cleanup.

The failure in logic from above was indeed with the flow references:: the fragment's set_config() method knew its captive flow's true origin but was misrepresenting it.  Fix: use CatalogQuery.cascade() to set the proper origin for the flow ref.

THEN-- ran into troubles because fragments didn't know how to do LCIA, only fragment_refs.  But of course it turns out that the reason for this is that do_lcia() ultimately needed qdb as an argument.  But do_lcia() doesn't need qdb as an explicit argument if it's given a quantity ref-- quantity refs already have a query object, and therefore a catalog, and therefore a qdb.  The rather elegant solution was to make do_lcia() a quantity_ref method, with a pass-through to the query that invokes it on the Qdb.

This is tremendously freeing-- suddenly we don't need qdb exposed ANYWHERE! suddenly frag_flow_lcia is once again a self-contained recursive function and can be located anywhere (so-- back to fragment_flows).  LcProcess.lcia() also no longer needs a qdb if it has a quantity_ref.  And as mentioned- score_cache and compute_unit_score no longer need a qdb around.  This is very much closer to realizing the original objective of the catalog.

The only remaining hiccup is, with the new change, we no longer have a terminate() method on all LcArchives, which means that the background engine no longer works.  Need to have a think about this, but we are now very close to reproducing AUOMA. (there is also a minor matter of stage names that don't seem to be working quite properly)

And it looks like there is a double counting issue with sub fragment aggregation.

Sun 2017-11-26 22:17:30 -0800

double counting issue: if the term is marked descend, then we want to skip it!

OK, taken care of in terminations.py

Sun 2017-11-26 22:34:44 -0800

==========
Mon Nov 27 12:49:41 -0800 2017

Now the issue is with the background engine, which was always a bit of a sloppy mess.  But it is clearly trying to do too much.  Since its implementation we have removed mix() from the index interface, and also we do not provide the capability for process refs to allocate (though: why not?).

In the rejiggering, we pass an index interface instead of an archive-- and then the index interface provides process refs instead of processes themselves.  They should function identically (although who knows whether performance will suffer).  But now that we are getting process refs we can't do allocation on the fly.

Presently we are not allocating on the fly, I believe.  But if we go the index interface route, then we can't go back unless we provide allocation capabilities in the process refs.

Mon 2017-11-27 16:48:47 -0800

Fuck all. we're in allocation hell.

You know what the problem is here? a complete lack of a specification.  No testing.  Nothing that works for certain.

I think the next thing to do is to focus on testing, 100%.  Get USLCI up to snuff, get background up to snuff, WITHOUT bothering with any interfaces.

Mon 2017-11-27 17:18:53 -0800

Well- some consolation (ish)- it doesn't work on master either :(

Mon 2017-11-27 23:04:26 -0800

rejiggering process reference machinery at the top of this file.


==========
Wed Nov 01 10:43:46 -0700 2017

Hard to believe how long this project has been going and how piss poor it all is.

The best thing going right now is the process_ref interface, which provides all the information required about nodes in a process-flow network.

The process API doesn't even match the process_ref API though.  and fragments are just utterly confused.

Here's what we should do:
 - rearrange the process API to match the process_ref
   * filter that through usage
 - make the fragment API match the relevant aspects of process_ref
 - simply extend fragment to fragment_ref

We also wind up back at the same quandary regarding specifying LCIA quantities over REST. I guess they should be query args.  e.g. API_ROOT/semantic.origin/p_entity_id/lcia?quantity=semantic.origin/q_entity_id&locale=x
with the default locale of course being the process's SpatialScope


I keep getting stuck on "how should this work REALLY?"

How it should work is this:
 - the interface specification should be the master of all things
 - fragments should behave like bg processes if they are remote, and like foregrounds if they are local
 x except we don't want remote fragments to behave like processes because we still want to enable deep stage aggregation, a la the used oil study, which is required for AUOMA.
 - we can pre-aggregate on the remote side.  presumably we would want to allow the remote authors to specify the aggregation.  But they can do that by setting the descend flag during subfragment construction.
 - I think we need to implement unit_inventory and then derive inventory + exchanges from that
 - unit_inventory delivers two sets of fragmentflows: exterior, interior
   = inventory + exchanges convert the exterior ones to exchanges and return the list
   = exchange relation can work the same
   = fg_lcia--> not sure we want to implement this but we could conceivably do fragment_lcia only for non-background ffs
   = foreground--> same as traversal
   = is_in_background--> false for all fragments
   = ad, bf--> can conceivably construct these from interior ffs
   = lci--> to be implemented
   = bg_lcia--> this is just fragment_lcia
   = fragment lcia -> score_cache -> if term is remote, just recurse at query time. NOPE NOPE NOPE
     == that just means that remote queries will result in repeated traversals. that is maybe not efficient.
     == plus we will need to serialize the entire LciaResult and then deserialize it.
     == plus that prevents local referents from overriding stage names
   = fragment_lcia -> score_cache -> if term is remote, DO NOTHING.
     == we DO need to extend fragment_refs to include a 'private' flag indicating whether agg is mandatory
     = given that, if descend is allowed, we just receive and deserialize the interior ffs as current
     = if descend is false locally, we receive and deserialize interior ffs as current and store them
       == when fragment lcia comes around, we recurse on the list of ffs per usual and locally compute unit scores
     = if descend is not allowed remotely, we receive a single agg interior ff
       == when fragment lcia comes around, we recurse on the single agg interior ff

OK this is going well.  We DO need a mechanism to mark fragments as private. But that's not urgent. For now we just need to implement unit_inventory.

and refactor process to match process_ref
and continue fragment and fragment_ref refactor
and work through the traversal snarl!
and implement frag_flow_lcia in LcFragment (or make a dedicated class?)

fuck this is all a big mess.

Another problem is, all process queries expect/require a ref_flow specification, but all fragment queries have a single reference flow but expect/require a scenario specification.  So the traversal machinery still must distinguish between them.

On that note, let's review the traversal machinery.

Three major blocks:
 1. determine exchange value w.r.t. node balance
 2. recurse over children, process case
 3. recurse over children, fragment case

Ultimately we want 2 and 3 to align. but they do need to be different.  processes are static but frags are not.
Can we / do we want to implement conservation flows on fragment children?
 

==========
Thu Apr 20 13:01:43 -0700 2017

Notes on current de-facto and future idealized interfaces between components.

###########################################################################
PART 1: EXISTING INTERFACES, empirical
###########################################################################


--- Data File Storage and Retrieval


Archive: ('physical' layer)
 + init: path, query string (remotes: append to request), cache (remotes: dl local copy)
   + cache is a recursive physical archive
 - listfiles(in_prefix) (local only)
 - countfiles (local only)
 - writefile (local uncompressed only)
 - readfile

ArchiveInterface: caching digest of contents of an archive
 - add
 - [getitem] by key or UUID
 + upstream-- look first
   + truncate upstream: TODO
 - retrieve or fetch
 - search
 - validate entity list
 - serialize, write to file

 - load all (must be implemented in subclass)
 - fetch (must be implemented in subclass)

LcArchive(ArchiveInterface):
 - deserialize process, flow, quantity from json
 - add entity and children
 - processes, flows, quantities, lcia_methods
 - terminate
 : serialize p, f, q

NsUuidArchive(LcArchive):
 + create a UUID3 namespace for translating keys to ID



--- Data Entities

LcEntity:
 - origin, uuid, external_ref, entity_type
   + entity's true id should be origin + external_ref
 - signature_fields -- type-specific list of mandatory properties
 - properties -- exclusive of signature fields (why? never used)
 - update (properties dict)
 - validate (called by archive to validate all entities):
   + valid entity_type
   + valid reference (None allowed)
   + signature fields all defined
 - serialize
 + show() - pretty print
 - eq: origin, entity type, external ref all the same
 - merge:
   - differing origins allowed! (only because origins right now are not semantic)
   + add missing properties; do not overwrite existing properties

LcQuantity:
 + reference entity is LcUnit -- which is just a dummy: unitstring with an external ref
 - unit : return self.reference_entity.unitstring()
 - is_lcia_method (has_propoerty 'Indicator')
 - convert: use existing 'UnitConversion' property as spec'd

LcFlow:
 + reference_entity is LcQuantity
 - add_characterization (quantity, reference=False, value=None, ...)
   + ... passed to characterization.add_value()
 + has_characterization
 - del_characterization
 - characterizations (generator)
 - factor, cf (backwards??) 
 - convert (to=quantity, fr=quantity)  both default to reference_entity
 + match: check if uuids or names or cas numbers or external refs are equal
 + profile: list of characterizations
 + serialize: include characterizations
 - merge
   - add characterizations, only if missing

LcProcess:
 + reference_entity is set of Exchanges.
 + inventory(ref): print + return list of exchanges
 - exchange(flow, direction): generate matching exchanges
 - exchanges(reference) generate allocated exchanges
 + has_exchange(flow, dir)
 - find_reference(ref) flexible input
 - add_reference(flow, dir) *both required*
 - remove_reference(ref_exch)
 + references: generator
 - reference(flow): return one; error if ambiguous
 - allocate_by_quantity(quantity): perform automatic allocation
 - is_allocated(ref) - determine whether ref appears as allocation key in any exchanges
 - remove allocation(ref)
 - add_exchange(flow, dirn, ...)
 + lcias(quantities): return LciaResults
 - lcia(quantity): return LciaResult (flowdb????) should only use flows' native cfs
 - serialize: include exchanges
 x merge not implemented











ForegroundQuery:
 - requires ForegroundManager, which provides:
   = retrieve quantities from foreground archive
   = retrieve LcFragment or LcProcess by reference
   = perform fragment_lcia of fragment (really owned by the fragment)
     = this routine must accept the following keyword arguments:
       * scenario
       * observed
       * scale
       * normalized
     = this routine must return an LciaResults object
     = this requires unit scores to be computed in advance
   = perform fg_lcia of the process
     = this routine must return an LciaResults object

 - user provides fragment references, which the foreground manager must resolve
 - user provides quantity references, which the foreground manager must resolve
 - user provides LciaWeightings, which must be constructed by the user

LciaResults:
 - implement dict of quantity uuid to LciaResult having that quantity
 - input to self._do_stages

LciaResult: (this is the big one) (need testing!)
 - aggregate(self, key=lambda) returns an LciaResult having the same total, with entries grouped by key
 - components(self) returns a list of entries in the result
 - dict of component key to AggregateLciaScore 
   - each component has a cumulative_result property
 
lots of muddiness regarding the LciaResult / AggregateLciaScore / DetailedLciaResult interface

In particular: I want to dispense with explicit factors wherever possible: the factor can be queried from the exchange's flow.  To do otherwise would imply 





###########################################################################
PART 2: NEW INTERFACES, conceptual spec
###########################################################################

What are the computations that we need to do? refer to workflows spec:

 * list process instances, flows, quantities
 * find process instances that terminate an exchange
 * get inventory (list of exchanges w.r.t. reference)
 * get exchange value for a given exchange
 * compute life cycle inventory
 * [compute life cycle impact assessment]
 * get characterization

SO-- from a functional perspective, if we break this into four groups:

 QuantityInterface:
q0 - get characterization for flowable | compartment | locale | ref qty
      Input: ref qty, flowable, compartment, query quantity, [optional locale]
      Output: value
q1 - show quantity metadata
      Input: quantity entity
      Output: quantity metadata document (JSON serialization)
q2 - show flowables characterized for qty + compartment
      Input: query quantity, [optional compartment string]
      Output: list of flowable strings
q3 - show compartments characterized for qty + flowable
      Input: query quantity, [optional flowable string]
      Output: compartment strings
q4 - show quantities characterized for flowable + compartment
      Input: flowable string, [optional compartment string]
      Output: list of quantities

 CatalogInterface:
c0 - list process instances, flows, quantities known to an archive
      Input: semantic ref, entity type, [optional search params?]
      Output: list of entities
c1 - show metadata for one process instance
      Input: one process
      Output: process metadata document (JSON serialization)
c2 - show metadata for one flow instance
      Input: one flow
      Output: flow metadata document (JSON serialization)
c3 - show reference entity for process instance
      Input: one process
      Output: list of exchanges
c4 - show reference entity for flow instance
      Input: flow
      Output: quantity
c5 - show processes that terminate an exchange (directions complement; reference only)
      Input: exchange, or flow + direction
      Output: list of processes
c6 - show processes that originate an exchange (directions match; reference only)
      Input: exchange, or flow + direction
      Output: list of processes
..... other semantic services

 ForegroundInterface:
f0 - list process exchanges
      Input: process
      Output: list of valueless exchanges
f1 - get exchange values for a given exchange w.r.t. reference
      Input: exchange + opt. reference
      Output: exchange value
f2 - compute foreground LCIA
      Input: process, query quantity
      Output: LciaResult for quantity
f6 - show processes that originate an exchange (directions match; reference or non-reference)
    (enclosed access to flow db)

 BackgroundInterface:
b0 - list foreground processes
b1 - list background processes
b2 - list cutoff flows
b3 - get lci for process (bx~)
b4 - get agg background dependencies for process (ad~)
b5 - get agg cutoffs for process (bf~)
b6 - get background LCIA
    (enclosed access to flow db)


 Study Construction:
s0 - create reference fragment with flow + direction
s1 - terminate fragment with:
     Null -> IO
     0 -> FG
     uuid -> 


Activities we want to do:

When building a model:
 - choose or create a flow
 - add characterizations to that flow
 - terminate the flow => to foreground, to background
 - get inventory

When traversing a model:
 - get characterizations for foreground emissions
 - get exchange values of listed exchanges
 - compute LCIA of non-listed exchanges (fg lcia)
 - compute LCIA of background nodes (bg lcia)



How will it work?

We have an LcCatalog, which dereferences origins + external ids to entities stored in LcArchives.  
