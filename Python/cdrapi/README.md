# CDR API

The `cdrapi` package contains a replacement implementation of the functionality
originally provided by the C++ CDR Windows service. That service is being
retired as part of a project to reduce the footprint of the CDR. The following
modules make up the package.

## The `settings` module

The `settings` module implements the `Tier` class, which encapsulates
information (for example, host names, database ports, etc.) specific
to which of the tiers in the hosting environment the software is
running. The CDR has four tiers:

* ***DEV*** - where active development is done
* ***QA*** - where testing of new/modified software happens
* ***STAGE*** - where CBIIT practices release deployments (a.k.a. "TEST")
* ***PROD*** - the system used for actual production work

## The `db` module

The `db` module implements the global `connect()` function and a convenience
`Query` class for building database `SELECT` statements without having to
do direct SQL string manipulation.

## The `users` module

The `users` module implements the `Session` class, as well as a number of
nested classes for account and permissions management, as well as caching.
A `Session` object is present for all API requests, and manages database
connections, logging, and user accounts, authentication, and authorization.

## The `docs` module

The `docs` module is the core of the CDR API, supporting storing, retrieval,
validation, filtering, linking, locking, and glossifying of CDR documents.
The top-level classes are:

* `Doc` (represents a single CDR document, possibly a specific version, or
  possibly not yet saved)
* `Doctype` (specification of one of the document types used by the CDR)
* `Filter` (XSL/T filter document object capable of handling callbacks)
* `FilterSet` (named set of XSL/T filter documents stored in the CDR)
* `Resolver` (handles includes, imports, and other callbacks from XSL/T
  filtering scripts)
* `Local` (thread-specific storage for XSL/T filtering)
* `Schema` (used for validating the CDR document on the server)
* `DTD` (used for generating the DTD for document validation within XMetaL)
* `LinkType` (definition of allowable links)
* `Link` (link from one CDR document to another)
* `Term` (thesaurus term document, with heirarchical upcoding)
* `GlossaryTermName` (dictionary term with aliases, for glossification)

## The `searches` module

The `searches` module supports selection of CDR documents based on their
types and/or the information contained within them. The modle implements
two top-level classes.

* `Search` (search request with one or more assertion tests)
* `QueryTermDef` (specifies a portion of CDR documents to be indexed for
  searching)

## The `publishing` module

The `publishing` module implements the `Job` class, which represents a
single CDR publishing job, with a single public method, `create()` for
requesting that a new publishing job be queued for processing, as well
as various properties indicating the job's type, when it was created and
(if applicable) completed, parameters for the job, etc.

## The `reports` module

The `reports` provides support for common reports, via the
`Report` class.
