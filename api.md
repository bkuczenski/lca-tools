# API Routes

This is a list of queryable API routes for the catalog interface.  All these routes are read-only.  The only writeable routes are for study construction.

## CatalogInterface

/catalog/references -> return list of semantic refs known to the catalog interface (R0)

/catalog/[semantic.ref]/processes -> return list of processes (R1)
/catalog/[semantic.ref]/flows -> "" 
/catalog/[semantic.ref]/quantities -> ""
/catalog/[semantic.ref]/{entity type}?[property]=[string]&... -> perform a search (R1)
/catalog/[semantic.ref]/[id] -> return entity (without numerical data)  (R1)
/catalog/[semantic.ref]/[id]/reference -> return reference entity (R1)-(R2)
/catalog/[semantic.ref]/terminate/[flow-id]/{direction} -> return list of processes (R1)
/catalog/[semantic.ref]/originate/[flow-id]/{direction} -> return list of processes (ref only) (R1)

## QuantityInterface

/qdb/quantities -> return list of quantities known to the qdb

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

/qdb/convert/[ref-qty]/[flowable]/[compartment]/[q-qty] -> return dict of locale:cf (R3)
/qdb/convert/[ref-qty]/[compartment]/[flowable]/[q-qty] -> return dict of locale:cf (R3)
/qdb/convert/[ref-qty]/[flowable]/[compartment]/[q-qty]/[locale] -> return value (R4)
/qdb/convert/[ref-qty]/[compartment]/[flowable]/[q-qty]/[locale] -> return value (R4)

## ForegroundInterface

/fg/references -> return list of semantic refs known to the fg interface (R0)

/fg/[semantic.ref]/{entity-type} -> same as /catalog/[s.r.]/{etype} (R1)
/fg/[semantic.ref]/{entity-type}?... -> same as /catalog/[s.r.]/{etype}?... (R1)
/fg/[semantic.ref]/[id] -> same as /catalog/[s.r.]/[id], but include numeric data (R1*)

/fg/[semantic.ref]/[process-id]/exchanges -> return direct exchanges without values (R2)
/fg/[semantic.ref]/[process-id]/[exch-flow-id]/{direction} -> (single-ref) return exch value (R2*)
/fg/[semantic.ref]/[process-id]/[exch-flow-id]/{direction} -> (multi-ref) return alloc exch (R2*)
/fg/[semantic.ref]/[process-id]/[ref-flow]/[exch-flow-id]/{direction} -> return norm. exch val (R4)

/fg/[semantic.ref]/[process-id]/lcia/[q-qty] -> (single ref) return qty fg LciaResult (R5)
/fg/[semantic.ref]/[process-id]/lcia/[q-qty] -> (multi ref) return array qty fg LciaResult (R5)
/fg/[semantic.ref]/[process-id]/[ref-flow]/lcia/[q-qty] -> return norm. qty fg LciaResult (R5)

/fg/[semantic.ref]/originate/[flow-id]/{direction} -> return list of processes (ref or non) (R1)

## BackgroundInterface

/bg/references -> return list of semantic refs known to the bg interface (R0)
NOTE: BG references must be fully allocated in order to be operable. or maybe we introduce an allocation structure to catalog serializations to allow them to be automagically allocated.

/bg/[semantic.ref]/{entity-type} -> same as /catalog/[s.r.]/{etype} (R1)
/bg/[semantic.ref]/{entity-type}?... -> same as /catalog/[s.r.]/{etype}?... (R1)
/bg/[semantic.ref]/[id] -> same as /catalog/[s.r.]/[id] (R1)

/bg/[semantic.ref]/foreground -> list fg products (R6)
/bg/[semantic.ref]/background -> list bg products (R6)
/bg/[semantic.ref]/exterior -> list all B matrix rows (R7)
/bg/[semantic.ref]/cutoff -> list intermediate B matrix rows (requires qdb or compartments) (R7)
/bg/[semantic.ref]/emissions -> list elementary B matrix rows (requires qdb or compartments) (R7)

/bg/[semantic.ref]/[process-id]/lci -> (single ref) return LCI (R2)
/bg/[semantic.ref]/[process-id]/lci -> (multi ref) error
/bg/[semantic.ref]/[process-id]/[ref flow]/lci -> return LCI (R2)

/bg/[semantic.ref]/[process-id]/ad -> (single ref) return agg background dependencies (R2)
/bg/[semantic.ref]/[process-id]/ad -> (multi ref) error
/bg/[semantic.ref]/[process-id]/[ref flow]/ad -> return agg background dependencies (R2)

/bg/[semantic.ref]/[process-id]/bf -> (single ref) return agg fg emissions (R2)
/bg/[semantic.ref]/[process-id]/bf -> (multi ref) error
/bg/[semantic.ref]/[process-id]/[ref flow]/bf -> return agg fg emissions (R2)

/bg/[semantic.ref]/[process-id]/lcia/[q-qty] -> (single ref) return qty bg LCIA (R5)
/bg/[semantic.ref]/[process-id]/lcia/[q-qty] -> (multi ref) return array qty bg LCIA (R5)
/bg/[semantic.ref]/[process-id]/[ref flow]/lcia/[q-qty] -> return qty bg LCIA (R5)

# Return Data Formats

There are 8 different types of return objects.  In each case, the internal (python) API returns the objects themselves, while the web-based API returns JSON serializations of the objects.

## R0 - strings

The query returns a list of valid query strings.  The strings here can be used directly in constructing subsequent queries.

## R1 - entities

The query returns a list of (serialized) entities (`process`, `flow`, `quantity`).  R1* indicates a list of entities with quantitative data included.

## R2 - exchanges

The query returns a list of (serialized) exchanges.  R2* indicates a list of exchanges with numeric data or allocation data.  The embedded entities are included as links rather than serializations.

## R3 - characterizations

The query returns a list of (serialized) characterizations, with numeric data.  The embedded entities are included as links rather than serializations.

## R4 - raw values

The query returns a raw float.  Someday the query may return a value with uncertainty information, but that is not today.

## R5 - LCIA Results

The query returns one or a list of LciaResult objects, whose serializations are identical to those of the Antelope spec.

## R6 - Flow Terminations

The query returns a list of processes and reference flows and directions.  These are highly similar to R2, but they are relevant in background systems because they provide reference products to upstream processes.  They also will be adapted to include fragment-flow terminations, for queries involving study models.  

## R7 - External flows

The query returns a list of flows with directions.  The flows are included as links rather than serialized entities.
