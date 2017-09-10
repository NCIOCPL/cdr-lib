#!/usr/bin/python
#----------------------------------------------------------------------
#
# Simple Web service helper classes.
#
#----------------------------------------------------------------------
import lxml.etree as etree
import os
import re
import sys
import cdrdb2 as cdrdb

#----------------------------------------------------------------------
# Custom exception class to let handlers catch problems with HTTP
# method specification.
#----------------------------------------------------------------------
class WrongMethod(Exception):
    pass

#----------------------------------------------------------------------
# Windows needs stdio set for binary mode.
#----------------------------------------------------------------------
try:
    import msvcrt, cdr
    msvcrt.setmode (0, os.O_BINARY) # stdin  = 0
    msvcrt.setmode (1, os.O_BINARY) # stdout = 1
    WINDOWS = True
except ImportError:
    WINDOWS = False

#----------------------------------------------------------------------
# Object representing a client request, extracted from the XML
# document used to transmit the request.
#
#   message  - the raw XML for the request
#   doc      - the node for top-level element of the document
#   type     - string for the name of the command
#   logLevel - value controlling how much logging is performed
#              by the server; set by the client using the HTTP
#              headers.
#
# 2015-12-08: add option to use lxml.etree instead of minidom
#----------------------------------------------------------------------
class Request:

    def __init__(self, standalone=False, logger=None):
        self.message  = None
        self.doc      = None
        self.type     = None
        self.logLevel = 0
        self.logger   = logger
        if standalone:
            self.message = sys.stdin.read()
            self.client  = 'Standalone'
            debugLevel    = "0"
        else:
            defaultLevel  = self.defaultLevel()
            requestMethod = os.getenv("REQUEST_METHOD")
            self.client   = os.getenv("REMOTE_ADDR")
            remoteHost    = os.getenv("REMOTE_HOST")
            debugLevel    = os.getenv("HTTP_X_DEBUG_LEVEL") or defaultLevel
            if debugLevel > "1" and logger is not None:
                logger.setLevel("DEBUG")
                logger.debug("debugging level set to %s", debugLevel)
                if debugLevel > "2":
                    self.dumpenv()
            if remoteHost and remoteHost != self.client:
                self.client += " (%s)" % remoteHost
            if not requestMethod:
                raise WrongMethod("Request method not specified")
            if requestMethod == "OPTIONS":
                sys.stdout.write("""\
Content-Type: text/plain
Content-Length: 0
Access-Control-Allow-Headers: Content-Type,SOAPAction
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: POST, GET, OPTIONS

""")
                sys.exit(0)

            if requestMethod != "POST":
                raise WrongMethod("Request method should be POST; was %s" %
                                  requestMethod)
            lenString = os.getenv("CONTENT_LENGTH")
            if not lenString:
                raise Exception("Content length not specified")
            try:
                contentLength = int(lenString)
            except:
                raise Exception("Invalid content length: %s" % lenString)
            if contentLength < 1:
                raise Exception("Invalid content length: %s" % lenString)
            try:
                blocks = []
                nRead = 0
                while nRead < contentLength:
                    block = sys.stdin.read(contentLength - nRead)
                    nRead += len(block)
                    blocks.append(block)
            except Exception, e:
                raise Exception("Failure reading request: %s" % str(e))
            self.message = "".join(blocks)
            if logger:
                logger.debug("message: %r", self.message)
        try:
            self.doc = etree.XML(self.message)
            self.type = self.doc.tag
        except Exception, e:
            if logger:
                logger.exception("Failure parsing request")
            raise Exception("Failure parsing request: %s" % e)
        try:
            self.logLevel = int(debugLevel)
        except:
            self.logLevel = 1

    def defaultLevel(self):
        query = cdrdb.Query("ctl", "val")
        query.where("grp = 'WebService'")
        query.where("name = 'DebugLevel'")
        row = query.execute().fetchone()
        return row and row[0] or "1"

    def dumpenv(self):
        lines = ["WebService environment", "=" * 60]
        for e in sorted(os.environ):
            lines.append("%s=%s" % (e, os.environ[e]))
        self.logger.debug("\n".join(lines))

#----------------------------------------------------------------------
# Object for the server's response to the client's request.  Contains
# a send() method for returning the request.
#----------------------------------------------------------------------
class Response:

    def __init__(self, body, logger=None):
        self.logger = logger
        if not isinstance(body, basestring):
            body = etree.tostring(body, pretty_print=True)
        self.body = body

    def send(self, contentType = 'text/xml'):
        if type(self.body) == unicode:
            self.body = self.body.encode('utf-8')
        sys.stdout.write("Content-Type: %s; charset=utf-8\n" % contentType)
        sys.stdout.write("Content-Length: %d\n" % len(self.body))
        if not WINDOWS:
            sys.stdout.write("Access-Control-Allow-Headers: ")
            sys.stdout.write("Content-Type,SOAPAction\n")
            sys.stdout.write("Access-Control-Allow-Origin: *\n")
        sys.stdout.write("\n")
        sys.stdout.write(self.body)
        if self.logger:
            self.logger.debug("sending %r", self.body)
        sys.exit(0)

#----------------------------------------------------------------------
# Object for the message the server sends back to the client if
# a failure is detected.
#----------------------------------------------------------------------
class ErrorResponse(Response):

    def __init__(self, error, logger=None):
        self.logger = logger
        if type(error) == unicode:
            error = error.encode('utf-8')
        self.body = "<ERROR>%s</ERROR>" % error
