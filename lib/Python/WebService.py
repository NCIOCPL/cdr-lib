#!/usr/bin/python
#----------------------------------------------------------------------
# $Id$
#
# Simple Web service helper classes.
#
# $Log: WebService.py,v $
# Revision 1.3  2008/05/15 13:19:48  bkline
# Added Content-length header; added code to log parsing failure.
#
# Revision 1.2  2008/05/14 14:40:46  bkline
# Added code to support Linux.
#
# Revision 1.1  2005/11/09 00:08:16  bkline
# Module used by the CDR client files refresh server to receive and
# respond to client requests contained in XML documents.
#
#----------------------------------------------------------------------
import os, sys, re, xml.dom.minidom

class WrongMethod(Exception):
    pass

#----------------------------------------------------------------------
# Windows needs stdio set for binary mode.
#----------------------------------------------------------------------
try:
    import msvcrt
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
            if debugLog:
                debugLog("message: %s" % self.message, 2)
        try:
            dom = xml.dom.minidom.parseString(self.message)
        except Exception, e:
            debugLog("Failure parsing request: %s" % e)
            debugLog(repr(self.message))
            raise Exception("Failure parsing request: %s" % e)
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
            f = open('/weblogs/glossifier/WebService.log', 'a')
        except:
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
        sys.stdout.write("Content-Length: %d\n" % len(self.body))
        if not WINDOWS:
            sys.stdout.write("Access-Control-Allow-Headers: ")
            sys.stdout.write("Content-Type,SOAPAction\n")
            sys.stdout.write("Access-Control-Allow-Origin: *\n")
        sys.stdout.write("\n")
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
