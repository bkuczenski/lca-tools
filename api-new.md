# API Routes

This is a list of valid queries for LcArchives.  The document has three parts:

 * Mapping from REST endpoint -> Catalog Query
 * Mapping from Catalog Query -> REST endpoint
 * List of controlled vocabularies
 * List of return types and their properties

In the endpoint syntax, `()` indicates a parameter value; `{}` indicates a controlled vocabulary; `[]` indicates optional query arguments; and plain text indicates literal text.

## REST Endpoint -> Catalog Query



## Catalog Query -> REST Endpoint

### Base Interface

 * `get_item(external_ref, item)` -> `/(external_ref)/get_item/(item)` [R0]
 * `get_reference(external_ref)` -> `/(external_ref)/reference` [R1]
 * `get_uuid(external_ref)` -> `/(external_ref)/uuid` [R0]
 * `get(external_ref)` -> `/(external_ref)` [R1] of length 1

Note: `cf()` and `do_lcia()` both use the local Qdb and do not result in Antelope requests.

### Index Interface

 * `count(entity_type)` -> `{entity_types}/count` [R4]
 * `{entity_types}(**kwargs) -> `{entity_types}?kwarg=val&...` [R1 or R1a, depending on number]
 * `terminate(flow_ref, direction)` -> `/terminate/(flow_ref)[?direction=dir]` [R1]
 * `originate(flow_ref, direction)` -> `/originate/(flow_ref)[?direction=dir]` [R1]

### Inventory Interface

 * `exchanges(process_ref)` -> `/(process_ref)/exchanges`
 * `exchange_values(process_ref, flow_ref, dirn, term)` -> `(process_ref)/exchanges/{direction}/(flow_ref)[?termination=term]` [R2*] 
 * `inventory(process_ref, ref_flow)` -> `/(process_ref)/inventory[?ref_flow=(ref_flow)] [R2*]
 * `exchange_relation(process_ref, ref_flow, exch_flow, direction, termination=None)` ->
   `/(process_ref)/exchange/{direction}/(exch_flow)[?termination=term&ref_flow=ref_flow]`

Note: `exchange_values` endpoint possibly spuriously omits the ability to specify a reference flow in the case of multifunctional processes.  Will result in a value_dict.  Maybe this is not desirable.

## Controlled Vocabularies

Valid Entries for controlled vocabularies:

 * `entity_types`:
   - `processes`
   - `flows` | `flowables`
   - `fragments`
   - `contexts`
   - `quantities`
   - `lcia_methods` (subset of quantities)

 * `direction`:
   - `input`
   - `output`


## Return Types and Properties

There are 8 different types of return objects.  In each case, the internal (python) API returns the objects themselves, while the web-based API returns JSON serializations of the objects.

All JSON responses have the fields `links` and `data`, with the contents of the `data` field being as specified below.

### R0 - string

The query returns a literal string.

 * Python Type: `str`
 * JSON Fields: none; content is plain text

### R0a - list of strings

list of valid query strings.  The strings here can be used directly in constructing subsequent queries.

 * Python Type: `list` of `str`
 * JSON Field: none; content is list of strings

### R1 - entity list

The query returns a list of (serialized) entities (`process`, `flow`, `quantity`).  R1* indicates a list of entities with quantitative data included -- for flows, the profile; for processes, the exchanges.

 * Python Type: `EntityRef` subclass
 * JSON Fields: used to make operable CatalogRef.from_query() (origin and query known)
   - `entityType`
   - `externalId`
   - `reference_entity` - used to make inoperable CatalogRef() that can subsequently be lookup()ed
     - `entityType`
     - `externalId`
   - `uuid`
   - Entity type's signature fields

### R1a - Abbreviated entity list

For queries with large [configurable on the server side] number of results, the query returns a list of entities with a single [specifyable; default 'Name'] identifying property instead of the completely serialized entity.  Entities which lack the specified property are not included in the list.  Entity Type must be known to the query.  Server reserves the right to truncate or refuse to answer (probably with a 400) queries with a very large number of results.

 * Python Type: `CatalogRef`
 * JSON Fields: used to make inoperable CatalogRef that can subsequently be lookup()ed
   - `externalId`
   - specified property

### R2 - exchanges

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
