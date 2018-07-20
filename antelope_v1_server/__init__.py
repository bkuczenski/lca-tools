"""
Antelope V1 Server

CalRecycle-originated RESTful publication of a single LCA foreground study

This package has two main components:
 * a study publisher, which takes live data from an LcCatalog and turns it into a set of static information

 * a Flask application to expose the LCA study to users and enable them to:
   i. compute results
   ii. perform scenario + contribution analysis

The objective here is to faithfully reimplement the functionality of the CalRecycle .NET Antelope server, so we can
stop paying for .NET hosting.
"""