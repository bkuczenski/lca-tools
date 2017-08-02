# API Routes

This is a list of queryable API routes for the catalog interface.  All these routes are read-only.  The only writeable routes are for study construction.

## Catalog queries

General queries of what collections are available within the catalog

    /references -> return list of semantic refs known to the catalog
    /index/references -> list of semantic refs with index interface
    /inventory/references -> list of semantic refs with inventory interface
    /background/references -> list of semantic refs with background interface
    /foreground/references -> list of semantic refs with foreground interface

## IndexInterface

See the contents of the catalogs (J Cleaner paper)
    
    /[semantic.ref]/processes -> return list of processes (R1)
    /[semantic.ref]/flows -> "" 
    /[semantic.ref]/quantities -> ""

    /[semantic.ref]/{entity type}?[property]=[string]&... -> perform a search (R1)
    /[semantic.ref]/[id] -> return entity (without numerical data)  (R1)
    /[semantic.ref]/[id]/reference -> return reference entity (R1)-(R2)
    /[semantic.ref]/terminate/[flow-id]/{direction} -> return list of processes (R1)
    /[semantic.ref]/originate/[flow-id]/{direction} -> return list of processes (ref only) (R1)
    /[semantic.ref]/mix/[flow-id]/{direction} -> return mixer process [what, with unit weights? that's stupid]

## QuantityInterface

Store information about properties and quantifications of flows.

    /qdb/quantities -> return list of quantities known to the qdb
    /qdb/lcia-methods -> list of quantities with an 'Indicator' property

Qualitative information / scope retrieval

    /qdb/[id] -> return matching quantity. (R1)
    /qdb/[id]/synonyms -> return synonyms for quantity (R0)
    /qdb/[id]/flowables -> return flowables characterized by qty (R0)
    /qdb/[id]/compartments -> return nonempty compartments (R0)
    /qdb/[id]/[flowable]/synonyms -> return synonyms for flowable (R0)
    /qdb/[id]/[flowable]/compartments -> return nonempty compartments for flowable (R0)
    /qdb/[id]/[compartment]/synonyms -> return synonyms for compartment (R0)
    /qdb/[id]/[compartment]/flowables -> return flowables in compartment (R1)
    /qdb/[id]/[flowable]/[compartment] -> return locales of characterization (R0)
    /qdb/[id]/[compartment]/[flowable] -> return locales of characterization (R0)

Quantitative data - flow-quantity relation

    /qdb/convert/[ref-qty]/[flowable]/[compartment]/[q-qty] -> return dict of locale:cf (R3)
    /qdb/convert/[ref-qty]/[compartment]/[flowable]/[q-qty] -> return dict of locale:cf (R3)
    /qdb/convert/[ref-qty]/[flowable]/[compartment]/[q-qty]/[locale] -> return value (R4)
    /qdb/convert/[ref-qty]/[compartment]/[flowable]/[q-qty]/[locale] -> return value (R4)

## InventoryInterface

    % /[semantic.ref]/[id] -> same as /catalog/[s.r.]/[id], but include numeric data (R1*)?? -- DEFERRED

Qualitative information

    /[semantic.ref]/[process-id]/exchanges -> return direct exchanges without values (R2)

Quantitative data - process-flow relation -- this needs some work to fully achieve access to ecoinvent-style terminated flows

    /[semantic.ref]/[process-id]/[exch-flow-id]/{direction} -> (single-ref) return list of exch values (R2*)
    /[semantic.ref]/[process-id]/[exch-flow-id]/{direction} -> (multi-ref) return list of alloc exchs (R2*)
    /[semantic.ref]/[process-id]/[exch-flow-id]/{direction}/[termination] -> return single alloc exch (R2*)
    /[semantic.ref]/[process-id]/[ref-flow]/[exch-flow-id]/{direction} -> (single result) return norm. exch val (R4)
    /[semantic.ref]/[process-id]/[ref-flow]/[exch-flow-id]/{direction} -> (mult. terminations) return norm. sum exch val (R4)
    /[semantic.ref]/[process-id]/[ref-flow]/[exch-flow-id]/{direction}/[termination] return norm. exch val (R4)

Foreground LCIA using the local Qdb

    /[semantic.ref]/[process-id]/lcia/[q-qty] -> (single ref) return qty fg LciaResult (R5)
    /[semantic.ref]/[process-id]/lcia/[q-qty] -> (multi ref) return array qty fg LciaResult (R5)
    /[semantic.ref]/[process-id]/[ref-flow]/lcia/[q-qty] -> return norm. qty fg LciaResult (R5)
    
    % /[semantic.ref]/originate/[flow-id]/{direction} -> return list of processes (ref or non) (R1) -- DEFERRED
    % /[semantic.ref]/terminate/[flow-id]/{direction} -> return list of processes (ref or non) (R1) -- DEFERRED

IF the foreground is ALLOCATED (or even if it isn't.... ) then it can be used as input to a Background Manager which solves for LCI.  


## BackgroundInterface

NOTE: BG reference processes must be fully allocated in order to be operable. or maybe we introduce an allocation structure to catalog serializations to allow them to be automagically allocated.

Qualitative / scoping information

    /[semantic.ref]/foreground -> list fg products (R6)
    /[semantic.ref]/background -> list bg products (R6)
    /[semantic.ref]/exterior -> list all B matrix rows (R7)
    /[semantic.ref]/cutoff -> list intermediate B matrix rows (requires qdb or compartments) (R7)
    /[semantic.ref]/emissions -> list elementary B matrix rows (requires qdb or compartments) (R7)

Let the user deal with disambiguation. Always return arrays unless the query is fully specified.

First, LCI queries -- B*inv(I-A)*u 
    
    /[semantic.ref]/[process-id]/lci -> array of (R2) arrays
    /[semantic.ref]/[process-id]/[ref flow]/lci -> return LCI (R2) array

Dependencies: Ad * x~

    /[semantic.ref]/[process-id]/ad -> array of (R2) arrays
    /[semantic.ref]/[process-id]/[ref flow]/ad -> return agg background dependencies (R2) array

Foreground emissions: Bf * x~

    /[semantic.ref]/[process-id]/bf ->   array of (R2) arrays
    /[semantic.ref]/[process-id]/[ref flow]/bf -> (R2) array

LCIA using the native Qdb-- here if the process has multiple reference flows, compute all of them (should I do that by default?) (just always return a list)

    /[semantic.ref]/[process-id]/lcia/[q-qty] -> array of qty bg LCIA (R5)
    /[semantic.ref]/[process-id]/[ref flow]/lcia/[q-qty] -> qty bg LCIA (R5)

Deferred:
This is the heavy lifter- I think right now it just returns...?? 

    /[semantic.ref]/[flow-id]/[process-id]/foreground -> pf's foreground (Af only, no Ad or Bf) as a list of terminated exchanges (R2)


# Operational form

Here is a list of queries: {} = controlled vocabulary. () = optional. [] = argument. '' = literal.

Archive-level queries (not applicable for individual entities); all support search arguments to filter results

 * `{entity_types}` 
 * `foreground_flows`
 * `background_flows`
 * `exterior_flows`
 * `cutoffs`
 * `emissions`

Catalog Ref provides:
 * `fetch` - [id] * automatic for catalog-refs if foreground available
 * `get` - [id] * automatic for catalog-refs if entity available

Entity queries:

 * `terminate` - [flow-id] ({direction})
 * `originate` - [flow-id] ({direction})
 * `mix` - [flow-id] {direction}

Foreground queries:

 * `exchanges` - [process-id]
 * `exchange_values` - [process-id] [flow-id] {direction} ([termination])
 * `exchange_relation` - [process-id] [ref-flow-id] [query-flow-id] {direction} ([termination])

Background queries:

 * `foreground` - [process-id] ([ref-flow-id])
 * `ad` - [process-id] ([ref-flow-id])
 * `bf` - [process-id] ([ref-flow-id])
 * `lci` - [process-id] ([ref-flow-id])
 * `lcia` - [process-id] ([ref-flow-id]) [query-qty] 

 


# Return Data Formats

There are 8 different types of return objects.  In each case, the internal (python) API returns the objects themselves, while the web-based API returns JSON serializations of the objects.

## R0 - strings

The query returns a list of valid query strings.  The strings here can be used directly in constructing subsequent queries.

## R1 - entities

The query returns a list of (serialized) entities (`process`, `flow`, `quantity`).  R1* indicates a list of entities with quantitative data included -- for flows, the profile; for processes, the exchanges. 

## R2 - exchanges

The query returns a list of (serialized) exchanges.  R2* indicates a list of exchanges with numeric data or allocation data.  The embedded entities are included as links rather than serializations.

## R3 - characterizations

The query returns a list of (serialized) characterizations, with numeric data.  The embedded entities are included as links rather than serializations.

## R4 - raw values

The query returns a raw float.  Someday the query may return a value with uncertainty information, but that is not today.

## R5 - LCIA Results

The query returns one or a list of LciaResult objects, whose serializations are identical to those of the Antelope spec.

## R6 - Flow Terminations or Product Flows

The query returns a list of processes and reference flows and directions.  These are highly similar to R2, but they are relevant in background systems because they provide reference products to upstream processes.  They also will be adapted to include fragment-flow terminations, for queries involving study models.  

## R7 - Exterior flows

The query returns a list of flows with directions.  The flows are included as links rather than serialized entities.
