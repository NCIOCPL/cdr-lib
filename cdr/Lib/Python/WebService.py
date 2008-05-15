#!/usr/bin/python
#----------------------------------------------------------------------
# $Id: WebService.py,v 1.3 2008-05-15 13:19:48 bkline Exp $
#
# Simple Web service helper classes.
#
# $Log: not supported by cvs2svn $
# Revision 1.2  2008/05/14 14:40:46  bkline
# Added code to support Linux.
#
# Revision 1.1  2005/11/09 00:08:16  bkline
# Module used by the CDR client files refresh server to receive and
# respond to client requests contained in XML documents.
#
#----------------------------------------------------------------------
import os, sys, re, xml.dom.minidom

#----------------------------------------------------------------------
# Windows needs stdio set for binary mode.
#----------------------------------------------------------------------
try:
    import msvcrt
    msvcrt.setmode (0, os.O_BINARY) # stdin  = 0
    msvcrt.setmode (1, os.O_BINARY) # stdout = 1
except ImportError:
    pass

#----------------------------------------------------------------------
# Object representing a client request, extracted from the XML
# document used to transmit the request.
#
#   message  - the raw XML for the request
#   doc      - the DOM node for top-level element of the document
#   type     - string for the name of the command
#   logLevel - value controlling how much logging is performed
#              by the server; set by the client using the HTTP
#              headers.
#----------------------------------------------------------------------
class Request:

    def __init__(self, standalone = False, debugLog = None):
        self.message  = None
        self.doc      = None
        self.type     = None
        self.logLevel = 0
        if standalone:
            self.message = sys.stdin.read()
            self.client  = 'Standalone'
            debugLevel    = "0"
        else:
            requestMethod = os.getenv("REQUEST_METHOD")
            self.client   = os.getenv("REMOTE_ADDR")
            remoteHost    = os.getenv("REMOTE_HOST")
            debugLevel    = os.getenv("HTTP_X_DEBUG_LEVEL") or "1"
            if debugLevel > "1":
                self.dumpenv()
            if remoteHost and remoteHost != self.client:
                self.client += " (%s)" % remoteHost
            if not requestMethod:
                raise Exception("Request method not specified")
            if requestMethod != "POST":
                raise Exception("Request method should be POST; was %s" %
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
            if debugLog:
                debugLog("message: %s" % self.message)
        try:
            dom = xml.dom.minidom.parseString(self.message)
        except Exception, e:
            debugLog("Failure parsing request: %s" % repr(e))
            raise Exception("Failure parsing request '%s ...: %s" %
                            (self.message[:20], str(e)))
        self.doc  = dom.documentElement
        self.type = self.doc.nodeName
        try:
            #self.logLevel = int(self.doc.getAttribute('log-level'))
            self.logLevel = int(debugLevel)
        except:
            self.logLevel = 1

    def dumpenv(self):
        import os, time
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        divider = "=" * 60
        lines = [divider, now + ": WebService environment", divider]
        for e in os.environ:
            lines.append("%s=%s" % (e, os.environ[e]))
        lines.append("")
        try:
            f = open('d:/cdr/log/WebService.log', 'a')
        except:
            try:
                f = open('/tmp/WebService.log', 'a')
            except:
                return
        f.write("\n".join(lines) + "\n")
        f.close()

#----------------------------------------------------------------------
# Object for the server's response to the client's request.  Contains
# a send() method for returning the request.
#----------------------------------------------------------------------
class Response:

    def __init__(self, body):
        self.body = body
        

    def send(self, contentType = 'text/xml'):
        if type(self.body) == unicode:
            self.body = self.body.encode('utf-8')
        sys.stdout.write("Content-Type: %s; charset=utf-8\n" % contentType)
        sys.stdout.write("Content-Length: %d\n\n" % len(self.body))
        sys.stdout.write(self.body)
        sys.exit(0)

#----------------------------------------------------------------------
# Object for the message the server sends back to the client if
# a failure is detected.
#----------------------------------------------------------------------
class ErrorResponse(Response):

    def __init__(self, error):
        if type(error) == unicode:
            error = error.encode('utf-8')
        self.body = "<ERROR>%s</ERROR>" % error
