# lca-tools
A Python package for doing stuff with LCA data

This software is meant to provide a set of free tools for _building_, _analyzing_ and _sharing_ life cycle inventory (LCI) models.

# Quick Test

In the shell (within your virtualenv):

    $ pip install -r requirements.txt
    
To create persistent test catalog:

    $ export ANTELOPE_CATALOG_ROOT=/path/to/create/catalog
    $ python -m unittest antelope_catalog/data_sources/tests/test_aa_local.py

To test the code:

    $ python -m unittest antelope_catalog/data_sources/tests/test_uslci.py
    ...
    ...(many lines of output)
    ...
    registering local.uslci.olca/93a60a56-a3c8-17da-a746-0800200c9a66
    local.uslci.olca.index.20200829: <source removed>
    local.uslci.olca: /path/to/create/catalog/archives/local.uslci.olca.index.20200829_background.mat
    Completed in 2.39 sec
    ..completed 20 iterations
    .completed 20 iterations
    .
    ----------------------------------------------------------------------
    Ran 21 tests in 16.219s
    
    OK
    $

To learn more about the code, first look at these tests and others in  `antelope_catalog/data_sources/tests`


# Quick Start

Once you have USLCI installed (above), you can do basic LCA operations right away.

    >>> from antelope_catalog import LcCatalog
    >>> cat = LcCatalog('/path/to/create/catalog')
    >>> # if you run the tests first, this catalog will already have contents
    >>> cat.show_interfaces()
    lcia.ipcc.2007.traci21 [basic, index, quantity]
    local.lcia.traci.2.1 [basic, index, quantity]
    local.qdb [basic, index, quantity]
    local.uslci.ecospold [basic, inventory, quantity]
    local.uslci.ecospold.index.20200829 [background, basic, index]
    local.uslci.olca [basic, inventory, quantity]
    local.uslci.olca.index.20200829 [background, basic, index]

    >>> q = cat.query('local.uslci.olca')
    >>> q.count('process')
    767
    
    >>> r = next(q.processes(Name='refinery'))
    >>> r.show()  # your results may vary
    ProcessRef catalog reference (0aaf1e13-5d80-37f9-b7bb-81a6b8965c71)
    origin: local.uslci.olca
    UUID: 0aaf1e13-5d80-37f9-b7bb-81a6b8965c71
       Name: Petroleum refining, at refinery
    Comment: 
    ==Local Fields==
       SpatialScope: RNA
      TemporalScope: {'begin': '2003-01-01-05:00', 'end': '2003-01-01-05:00'}
    Classifications: ['Petroleum and Coal Products Manufacturing', 'Petroleum Refineries']

    >>> [str(x) for x in r.inventory()]
    ....

    >>> lci = list(r.lci())
    ...

    MultipleReferences: You must specify a reference flow

    >>> rxs = list(r.references())
    >>> str(rxs[0])

    >>> lci_0 = list(r.lci(rxs[0]))
    >>> gwp = next(cat.query('lcia.ipcc').quantities(name='global warming'))
    >>> res = gwp.do_lcia(lci_0)
    >>> res.total()
    399.983097567219

    >>> res.show_details()
    [lcia.ipcc.2007.traci21] Global Warming Air [kg CO2 eq] [LCIA] kg CO2 eq
    ------------------------------------------------------------
    
    [local.uslci.olca] Petroleum refining, at refinery [RNA]:
         269 =          1  x        269 [GLO] carbon dioxide, air
         119 =         25  x       4.76 [GLO] methane, air
        9.68 =         25  x      0.387 [GLO] methane, air
         1.3 =        298  x    0.00436 [GLO] nitrous oxide, air
       0.688 =          1  x      0.688 [GLO] carbon dioxide, air
     0.00159 =    1.4e+03  x   1.14e-06 [GLO] Methane, tetrachloro-, air
    0.000167 =        8.7  x   1.92e-05 [GLO] methylenechloride, air
    6.09e-08 =         13  x   4.69e-09 [GLO] methyl chloride, air
    2.58e-08 =        146  x   1.77e-10 [GLO] 1,1,1-trichloroethane, air
    1.62e-08 =         31  x   5.22e-10 [GLO] chloroform, air
    7.07e-09 =          5  x   1.41e-09 [GLO] Methane, bromo-, Halon 1001, air
        400 [lcia.ipcc.2007.traci21] Global Warming Air [kg CO2 eq] [LCIA]

Coming soon: a TRACI 2.1 test-driven initialization. Ecoinvent is also available but you must have the data files already.

## Core components

The `lca-tools` package includes the following components:

 * A set of `entities` that describe the parts of LCA models;
 * A standard `interface` to a collection of these entities;
 * A set of `providers` that implement the interface to provide access to LCA data;
 * A `catalog` of data providers that are available for a project;
 * A means for computing and storing `lcia_results`, and for displaying them in tables and charts;
 * A quantity database (currently inaptly named `flowdb`) for storing flow characterizations and mapping different flows to their characterizations;
 * A set of tools for managing a `foreground` LCA model.

That's kind of a lot of pieces. And none of them are unit tested (yet).


# Entities

LcEntity base class:

 - has fixed properties `entity_type`, `origin`, `uuid`, `external_ref` (note- only origin and external_ref may be set after initialization, and both of those can be set only once)
 - also has a reference_entity, which helps to define how the entity is interpreted.
 - Other than that, behaves like a dict, storing properties of an entity.
 - A set of mandatory properties makes up the entity's "signature".  For the base class these are `EntityType`, `Name`, and `Comment`.

## Quantities, Flows, Processes

LCI models are made up of a small number of entity classes: _flows_ are objects, materials, or services in the world; _quantities_ are the measurable dimensions of flows, and _processes_ are collections of flows that are all exchanged together in carrying out a particular activity. These three entity types define all data in conventional LCA.

As mentioned above, each entity is defined by a _reference entity_ of a subordinate type.  The reference entities by type are as follows:

 * Quantity (e.g. mass) -> has a reference unit of measure (given by string only) (e.g. "kg")
 * Flow -> has a reference quantity (e.g. "mass")
 * Process -> has a reference _exchange_ (which is a flow and a direction; see below)

## Exchanges and Characterizations

Exchanges and characterizations are quantitative relations between two different entity types.

 * An `exchange` is a relation between a process and a flow, with a _direction_ given (either Input or Output with respect to the process).  An exchange can also have a `termination` which specifies another process with respect to which the flow has the complementary direction.  Finally, exchanges can have _exchange values_ (about which more later).

 * A `characterization` is a relation between a flow and a quantity.  Characterizations have a _factor_ or a collection of geography-specific factors.  The factor reports the amount of the associated quantity is equivalent to a unit of the flow's _reference quantity_ (remember reference entities?)




----- -----
These should probably be entities, too, because of serialization + linked data more than anything.  I mean, they are entities in the LD schema.  But they don't have stand-alone identities (in LD terms they are 'blank nodes'); their origins are defined implicitly by the entities they belong to and so they don't have external references; and they contain numerical data, as opposed to other entities that strictly contain qualitative data.  So I think there is a sound conceptual reason for them to be their own object types rather than subclasses of LcEntity.









# Testing

The python `unittest` framework is used to implement unit tests.  The tests should be automatically discovered and run by running `python -m unittest` from the root directory of the repo.

In order to ensure that tests are discoverable, the test files must match the shell pattern `'test_*.py'` and the directory containing the files must have a `__init__.py`.

Currently tested modules:

 * `flowdb.compartments`



# Design Principles

You may ask why I am spending so much time writing open source LCA software in Python when there is already an excellent open source LCA library in python (Chris Mutel's brightway) and besides that there is another more mature and very powerful free, open source, cross-platform graphical tool for LCA (GreenDelta's OpenLCA).  This is, in all honesty, a very good question, and the main reason is that several activities that I believe to be central to the project of LCA are not addressed by these tools.

The most important is the __publication__ of LCA models and results.  I spent about 2 years developing an LCA publishing platform for CalRecycle after finishing a study of used oil management in California.  The end result can be seen here: http://uo-lca.github.io/, with the back-end API (written in .NET) available here: http://antelope-lca.net/uo-lca/

The objective of these projects was to provide _full transparency_ of the model and data (to the extent permitted by licensing) to any user in any software configuration.

Thus we come to the first design principle:

 * __LCA computation as web service__.  Finished models should be displayed and their LCIA results computed and validated over the web.  

Secondly, I learned mid-way through the CalRecycle LCA project that sharing models (particularly between software systems) is prohibitively difficult.  At the same time, however, the set of comprehensive LCA data sources is finite and very small.  Most LCA practitioners, when building models, must make reference to one of only a few self-consistent data providers, such as Ecoinvent or Thinkstep.  If I know the source of your data, and I have access to the same data, then it is easy in concept for me to rebuild your model.  In fact, it is so easy that it should be possible to do so mechanistically.  This crucial piece of information is not widely appreciated, but once grasped, it leads to the second design principle:

 * __knowledge by reference__.  Knowing the exact identity of a data set is tantamount to knowing everything in it, if the data set itself is published or publicly available.

If I know that a particular node in an LCI model is a unit process drawn from the ecoinvent database, then in principle I don't need to know anything else about it other than a reference to the precise process used.  Then I can obtain whatever information is desired about the process, including its metadata, elementary and intermediate exchanges, and aggregated LCIA scores, from an authoritative source.  If I have access to ecoinvent, then I should be able to locally store whatever of this information is relevant to me.

These two will have to do for now.

