#----------------------------------------------------------------------
#
# $Id: cdr2gk.py,v 1.2 2007-03-23 16:04:15 bkline Exp $
#
# Support routines for SOAP communication with Cancer.Gov's GateKeeper.
#
# $Log: not supported by cvs2svn $
# Revision 1.1  2007/03/19 18:00:46  bkline
# New program for GateKeeper2.0 client.
#
#----------------------------------------------------------------------
import httplib, re, sys, time, xml.dom.minidom, socket, string

#----------------------------------------------------------------------
# Module data.
#----------------------------------------------------------------------
debuglevel          = 0
localhost           = socket.gethostname()
host                = "gatekeeper.cancer.gov"
testhost            = "test4.cancer.gov"
testhost            = "gkdev.cancer.gov"
port                = 80
soapNamespace       = "http://schemas.xmlsoap.org/soap/envelope/"
gatekeeperNamespace = "http://www.cancer.gov/webservices/"
application         = "/GateKeeper/GateKeeper.asmx"
headers             = {
    'Content-type': 'text/xml; charset="utf-8"',
#    'SOAPAction'  : 'http://gatekeeper.cancer.gov/Request'
    'SOAPAction'  : 'http://www.cancer.gov/webservices/Request'
}
if string.upper(localhost) in ("MAHLER", "FRANCK"):
    host = testhost

#----------------------------------------------------------------------
# Module data used by publishing.py and cdrpub.py.
#----------------------------------------------------------------------
PUBTYPES = {
    'Full Load'       : 'Send all documents to Cancer.gov',
    'Export'          : 'Send specified documents to Cancer.gov',
    'Remove'          : 'Delete documents from Cancer.gov',
    'Hotfix (Remove)' : 'Delete individual documents from Cancer.gov',
    'Hotfix (Export)' : 'Send individual documents to Cancer.gov'
}
PDQDTD = "d:\\cdr\licensee\\PDQ.dtd"

#----------------------------------------------------------------------
# Module-level class definitions.
#----------------------------------------------------------------------

class Fault:
    """
    Holds detailed information about a SOAP failure.

    Public attributes:

        faultcode
            indicates the general nature of the problem, such as
            whether it originates on the client or the server

        faultstring
            description of the problem, possibly including a stack
            trace from the SOAP server
    """

    def __init__(self, node):
        faultCodeElem     = getChildElement(node, "faultcode", True)
        faultStringElem   = getChildElement(node, "faultstring", True)
        self.faultcode    = getTextContent(faultCodeElem)
        self.faultstring  = getTextContent(faultStringElem)
    def __repr__(self):
        return (u"Fault (faultcode: %s, faultstring: %s)" %
                (self.faultcode, self.faultstring))

class PubEventResponse:
    """
    Holds detailed information from a response to a request initiation
    or data prolog message.
    
    Public attributes:

        pubType
            One of Hotfix, Export, Remove, or Full Load; echoed
            from request

        lastJobId
            ID of last job of this type successfully processed by
            Cancer.Gov

        docCount
            echoed from request (data prolog message only)
    """

    def __init__(self, node):
        pubTypeElem        = getChildElement(node, "pubType", True)
        lastJobIdElem      = getChildElement(node, "lastJobID")
        nextJobIdElem      = getChildElement(node, "nextJobID")
        docCountElem       = getChildElement(node, "docCount")
        self.pubType       = getTextContent(pubTypeElem)
        self.highestDocNum = None
        self.totalPackets  = None
        if lastJobIdElem is not None:
            lastJobText = getTextContent(lastJobIdElem)
            try:
                self.lastJobId = int(lastJobText)
            except:
                self.lastJobId = lastJobText
        else:
            self.lastJobId = None
        if nextJobIdElem is not None:
            nextJobText = getTextContent(nextJobIdElem)
            try:
                self.nextJobId = int(nextJobText)
            except:
                self.nextJobId = nextJobText
        else:
            self.nextJobId = None
        if docCountElem is not None:
            docCountText = getTextContent(docCountElem)
            try:
                self.docCount = int(docCountText)
            except:
                self.docCount = docCountText
                try:
                    totalPackets, highestDocNum = docCountText.split(u'/')
                    self.totalPackets = int(totalPackets)
                    self.highestDocNum = int(highestDocNum)
                except:
                    pass
        else:
            self.docCount = None
    def __repr__(self):
        return (u"PubEventResponse "
                u"(pubType: %s, lastJobId: %s, nextJobId: %s, docCount: %s, "
                u"totalPackets: %s, highestDocNum: %s)"""
                % (self.pubType, self.lastJobId, self.nextJobId,
                   self.docCount, self.totalPackets, self.highestDocNum))

class PubDataResponse:
    """
    Holds detailed information from a response to a data transfer
    request.

    Public attributes:

        docNum
            echoed from request
    """

    def __init__(self, node):
        child = getChildElement(node, "docNum", True)
        self.docNum = int(getTextContent(child))
    def __repr__(self):
        return u"PubDataResponse (docNum: %d)" % self.docNum

class HttpError(StandardError):
    def __init__(self, xmlString):
        self.xmlString = xmlString
        bodyElem   = extractBodyElement(xmlString)
        faultElem  = bodyElem and getChildElement(bodyElem, "Fault") or None
        self.fault = faultElem and Fault(faultElem) or None
    def __repr__(self):
        if not self.fault:
            return self.xmlString
        return u"""\
HttpError (faultcode: %s, faultstring: %s)""" % (self.fault.faultcode,
                                                 self.fault.faultstring)

class Response:
    """
    Encapsulates the Cancer.Gov GateKeeper response to a SOAP request.

    Public attributes:

        type
            OK, Not Ready, or Error

        message
            e.g., Ready to Accept Data, Invalid Request, Bad lastJobID,
            etc.

        details
            PubEventResponse for request initiation and data prolog
            exchanges; PubDataResponse for document transfers

        fault
            SOAP fault object containing faultcode and faultstring
            members in the case of a SOAP failure
    """
    
    def __init__(self, xmlString, publishing = True):

        """Extract the values from the server's XML response string."""

        self.xmlString   = xmlString
        self.bodyElem    = extractBodyElement(xmlString)
        self.faultElem   = getChildElement(self.bodyElem, "Fault")
        self.fault       = self.faultElem and Fault(self.faultElem) or None
        self.publishing  = publishing
        if publishing:
            respElem         = extractResponseElement(self.bodyElem)
            respTypeElem     = getChildElement(respElem, "ResponseType", 1)
            respMsgElem      = getChildElement(respElem, "ResponseMessage", 1)
            peResponseElem   = getChildElement(respElem, "PubEventResponse")
            pdResponseElem   = getChildElement(respElem, "PubDataResponse")
            self.type        = getTextContent(respTypeElem)
            self.message     = getTextContent(respMsgElem)
            if peResponseElem:
                self.details = PubEventResponse(peResponseElem)
            elif pdResponseElem:
                self.details = PubDataResponse(pdResponseElem)
            else:
                self.details = None
        else:
            self.xmlResult   = extractXmlResult(self.bodyElem)
    def __repr__(self):
        pieces = [u"cdr2cg.Response "]
        if self.publishing:
            pieces.append(u"(type: %s, message: %s, details: %s" %
                          (self.type, self.message, self.details))
        else:
            pieces.append(u"(publish preview document")
        if self.fault:
            pieces.append(u", fault: %s" % self.fault)
        pieces.append(u")")
        return u"".join(pieces)

def getTextContent(node):
    text = ''
    for n in node.childNodes:
        if n.nodeType == xml.dom.minidom.Node.TEXT_NODE:
            text = text + n.nodeValue
    return text

def getChildElement(parent, name, required = False):
    child = None
    for node in parent.childNodes:
        if node.nodeType == node.ELEMENT_NODE and node.localName == name:
            child = node
            break
    if required and not child:
        raise StandardError("Response missing required %s element" % name)
    return child

def extractResponseElement(bodyNode):
    requestResponse = getChildElement(bodyNode,        "RequestResponse", 1)
    requestResult   = getChildElement(requestResponse, "RequestResult",   1)
    response        = getChildElement(requestResult,   "Response",        1)
    return response

def extractBodyElement(xmlString):
    dom     = xml.dom.minidom.parseString(xmlString)
    docElem = dom.documentElement
    body    = getChildElement(docElem, "Body", True)
    return body

# For publish preview.
def extractXmlResult(bodyNode):
    xmlResponse = getChildElement(bodyNode, "ReturnXMLResponse")
    if xmlResponse:
        xmlResult = getChildElement(xmlResponse, "ReturnXMLResult")
        if xmlResult:
            xmlString = getTextContent(xmlResult)
            return xmlString.replace("<![CDATA[", "").replace( "]]>", "")
    return None

def logString(type, str):
    if debuglevel:
        open("d:/cdr/log/cdr2gk.log", "ab").write("==== %s %s ====\n%s\n" % 
            (time.ctime(), type, re.sub("\r", "", str)))

def sendRequest(body, app = application, host = host, headers = headers):

    if type(body) == unicode:
        body = body.encode('utf-8')
    request = """\
<?xml version='1.0' encoding='utf-8'?>
<soap:Envelope xmlns:soap='%s'
               xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' 
               xmlns:xsd='http://www.w3.org/2001/XMLSchema'>
 <soap:Body>
%s
 </soap:Body>
</soap:Envelope>""" % (soapNamespace, body)

    # At one time it was tricky finding out where the requests were going.
    logString("REQUEST", "sending request to %s" % host)
    logString("REQUEST", request)
    
    # Defensive programming.
    tries = 3
    response = None
    while tries:
        try:

            # Set up the connection and the request.
            conn = httplib.HTTPConnection(host, port)

            # Submit the request and get the headers for the response.
            conn.request("POST", app, request, headers)
            response = conn.getresponse()
            #sys.stderr.write("got response from socket %s\n" %
            #                 repr(conn.sock))

            # Skip past any "Continue" responses.
            while response.status == 100:
                response.msg = None
                response.begin()

            # We can stop trying now, we got it.
            tries = 0
            
        except:
            if debuglevel:
                sys.stderr.write("caught http exception; trying again...\n")
            logString("RETRY", "%d retries left" % tries)
            if not tries:
                raise
            time.sleep(.5)
            tries -= 1

    # Check for failure.
    if not response:
        raise StandardError("tried to connect 3 times unsuccessfully")
    
    if response.status != 200:
        resp = response.read()
        logString("HTTP ERROR", resp)
        resp = "(occurred at %s) (%s)" % (time.ctime(), resp)
        raise StandardError("HTTP error: %d (%s) %s" % (response.status,
                                                     response.reason,
                                                     resp))

    # Get the response payload and return it.
    data = response.read()
    logString("RESPONSE", data)
    return data

def initiateRequest(pubType, pubTarget):
    """
        This is the replacement for initiateRequest, used for version
        2.0 of the Cancer.gov Gatekeeper.  Asks the GateKeeper if
        it's open for business.

        pubType   - one of 'Hotfix', 'Export', or 'Full Load'
        pubTarget - one of 'GateKeeper', 'Preview', or 'Live'
    """
    request = """\
  <Request xmlns='%s'>
   <source>CDR</source>  
   <requestID>Status Check</requestID>
   <message>
    <PubEvent>
     <pubType>%s</pubType>
     <pubTarget>%s</pubTarget>
    </PubEvent>
   </message>
  </Request>""" % (gatekeeperNamespace, pubType, pubTarget)
    xmlString = sendRequest(request)
    return Response(xmlString)

def sendDataProlog(jobDesc, jobId, pubType, pubTarget, lastJobId):
    request = u"""\
  <Request xmlns='%s'>
   <source>CDR</source>   
   <requestID>%s</requestID>
   <message>
    <PubEvent>
     <pubType>%s</pubType>
     <pubTarget>%s</pubTarget>
     <description>%s</description>
     <lastJobID>%s</lastJobID>
    </PubEvent>
   </message>
  </Request>""" % (gatekeeperNamespace, jobId, pubType, pubTarget, jobDesc,
                   lastJobId)
    xmlString = sendRequest(request, host = host)
    return Response(xmlString)

def sendDocument(jobId, docNum, transType, docType, docId, docVer,
                 groupNumber, doc = ""): 

    # Avoid the overhead of converting the doc to Unicode and back.
    request = (u"""\
  <Request xmlns='%s'>
   <source>CDR</source>   
   <requestID>%s</requestID>
   <message>
    <PubData>
     <docNum>%s</docNum>
     <transactionType>%s</transactionType>
     <CDRDoc Type    = '%s'
             ID      = 'CDR%010d'
             Version = '%d'
             Group   = '%d'>""" % (gatekeeperNamespace, jobId, docNum,
                                   transType, docType, docId, docVer,
                                   groupNumber)).encode('utf-8') + doc + """\
     </CDRDoc>
    </PubData>
   </message>
  </Request>"""
    xmlString = sendRequest(request, host = host)
    return Response(xmlString)

def sendJobComplete(jobId, pubType, count, status):
    request = u"""\
  <Request xmlns='%s'>
   <source>CDR</source>   
   <requestID>%s</requestID>
   <message>
    <PubEvent>
     <pubType>%s</pubType>
     <docCount>%d</docCount>
     <status>%s</status>
    </PubEvent>
   </message>
  </Request>""" % (gatekeeperNamespace, jobId, pubType, count, status)

    response = sendRequest(request, host = host)
    return Response(response)

def pubPreview(xml, typ):
    request = """
  <ReturnXML xmlns="http://gatekeeper.cancer.gov/CDRPreview/">
   <content>%s</content>
   <template_type>%s</template_type>
  </ReturnXML>
""" % (xml, typ)
    xmlString = sendRequest(
        request, 
        app     = '/CDRPreview/WSProtocol.asmx',
        host    = host,
        headers = { 'Content-type': 'text/xml; charset="utf-8"',
                    'SOAPAction'  :
                    'http://gatekeeper.cancer.gov/CDRPreview/ReturnXML' })
    return Response(xmlString, False)

#----------------------------------------------------------------------
# Take it out for a test spin.  Try with 43740 (a Country document).
#----------------------------------------------------------------------
if __name__ == "__main__":

    def getCursor():
        import cdrdb
        return cdrdb.connect('CdrGuest', dataSource = 'bach').cursor()
    def loadDocsOfType(t):
        cursor = getCursor()
        cursor.execute("""\
            SELECT c.id
              FROM pub_proc_cg c
              JOIN document d
                ON c.id = d.id
              JOIN doc_type t
                ON t.id = d.doc_type
             WHERE t.name = ?""", t, timeout = 300)
        return [str(row[0]) for row in cursor.fetchall()]
    class Doc:
        __cursor = getCursor()
        def __init__(self, docId):
            self.docId = int(re.sub("[^\\d]+", "", docId))
            self.group = 1
            Doc.__cursor.execute("""\
                SELECT c.xml, t.name, p.doc_version
                  FROM pub_proc_cg c
                  JOIN document d
                    ON c.id = d.id
                  JOIN doc_type t
                    ON t.id = d.doc_type
                  JOIN pub_proc_doc p
                    ON p.pub_proc = c.pub_proc
                   AND p.doc_id = d.id
                 WHERE d.id = ?""", self.docId)
            docXml, self.docType, self.docVer = Doc.__cursor.fetchall()[0]
            docXml = re.sub(u"<\\?xml[^>]+>\\s*", u"", docXml)
            self.xml = re.sub(u"<!DOCTYPE[^>]*>\\s*", u"", docXml)

    # If we're asked to abort a job, do it.
    debuglevel = 1
    if len(sys.argv) > 1 and sys.argv[1].startswith('abort='):
        jobId = sys.argv[1][len('abort='):]
        response = sendJobComplete(jobId, 'Export', 0, 'abort')
        print "response:\n%s" % response
        sys.exit(0)

    # Here's how to close the job by hand.
    if len(sys.argv) > 2 and sys.argv[1].startswith('complete='):
        jobId = sys.argv[1][len('complete='):]
        nDocs = int(sys.argv[2])
        response = sendJobComplete(jobId, 'Export', nDocs, 'complete')
        print "response:\n%s" % response
        sys.exit(0)
        
    # Get the document IDs from the command line.
    if len(sys.argv) > 1 and sys.argv[1].startswith('type='):
        docIds = loadDocsOfType(sys.argv[1][5:])
    else:
        docIds = sys.argv[1:]
    if not docIds:
        sys.stderr.write("you must specify at least one document ID\n")
        sys.exit(1)

    # See if the GateKeeper is awake and open for business.
    pubType   = 'Export'
    pubTarget = 'GateKeeper'
    jobDesc   = 'Command-line test from cdr2gk module.'
    response  = initiateRequest(pubType, pubTarget)
    if response.type != "OK":
        print "initiateRequest(): %s: %s" % (response.type, response.message)
        if response.fault:
            print "%s: %s" % (response.fault.faultcode,
                              response.fault.faultstring)
        elif response.details:
            print "Last job ID from server: %s" % response.details.lastJobId
        sys.exit(1)

    # Tell the GateKeeper we're about to send some documents.
    lastJobId = response.details.lastJobId
    print lastJobId, type(lastJobId)
    jobId = lastJobId + 1
    print "last job id: %d" % lastJobId
    print "new job id: %d" % jobId
    response = sendDataProlog(jobDesc, jobId, pubType, pubTarget, lastJobId)
    if response.type != "OK":
        print "sendDateProlog(): %s: %s" % (response.type, response.message)
        sys.exit(1)

    # Send the documents.
    print "sending %d docs" % len(docIds)
    docNum = 1
    for docId in docIds:
        doc = Doc(docId)
        print ("sendDocument(CDR%d) (%d of %d)..." % (doc.docId, docNum,
                                                      len(docIds))),
        response = sendDocument(jobId, docNum, 'Export', doc.docType,
                                doc.docId, doc.docVer, doc.group, doc.xml)
        if response.type != "OK":
            print "%s: %s" % (response.type, response.message)
        else:
            print "OK"
        docNum += 1

    # Wrap it up.
    response = sendJobComplete(jobId, pubType, len(docIds), 'complete')
    if response.type != 'OK':
        print "%s: %s" % (response.type, response.message)
