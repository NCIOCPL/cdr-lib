# CDR Python Modules

This repository contains the package for the core CDR API functionality.
For specific information on the modules in that package, consult its
separate [README](cdrapi/README.md) file.

In addition, there are individual modules providing a legacy wrapper
for that API functionality, as well as common code to support the
various components of the CDR.

## Original CDR Python Scripting Wrapper Module

The primary Python module in the system is implemented in the file
[cdr.py](cdr.py), which provides a Python wrapper for all
communication with the CDR tunneling server, using the CDR
client-server APIs, as well as other common functionality (e.g.,
logging or inter-process locking). When this module detects that it is
running directly on a CDR server, it bypasses the tunneling server and
invokes the functionality directly from the Python CDR API (cdrapi)
modules for efficiency. Creators of new scripts which are intended
only to be run on a CDR server (for example, the CGI scripts which
implement the CDR web admin interface) should consider the use the
Python CDR API classes directly, as in many cases this will result in
simpler and more efficient code.

## CGI Module

The [cdrcgi](cdrcgi.py) module implements classes and functions for
generating web forms and reports, as well as Excel workbooks.

## Publishing Modules

The following modules implement common functionality used by the
Publishing subsystem:

* [cdrpub](cdrpub.py)
* [RepublishDocs](RepublishDocs.py)

## Batch Processing Modules

The following modules support other long-running processing jobs
(e.g., complex reports or global change batches) which might take
longer than would complete during the window allowed by the web
server:

* [CdrLongReports](CdrLongReports.py)
* [cdrbatch](cdrbatch.py)
* [ModifyDocs](ModifyDocs.py)

## Common Mailer Code

The following modules factor out common classes and other code used by
the mailer subsystem:

* [cdrmailer](cdrmailer.py)
* [cdrmailcommon](cdrmailercommon.py)
* [RtfWriter](RtfWriter.py)

## Other Modules

The following modules provide additional common code support for the
CDR system:

* [cdr_dev_data](cdr_dev_data.py) (used by scripts to preserve data
on the DEV tier after a refresh from PROD)
* [cdrdocobject](cdrdocobject.py) (classes representing CDR documents
of specific types)
* [cdrpw](cdrpw.py) (interface to file containing system passwords)
* [nci_thesaurus](nci_thesaurus.py) (used by scripts dealing with
terminology documents)
* [WebService](WebService.py) (used by glossifier and ClientRefresh
services)
