#----------------------------------------------------------------------
#
# $Id: cdr2gk.py,v 1.6 2007-05-02 23:08:04 venglisc Exp $
#
# Support routines for SOAP communication with Cancer.Gov's GateKeeper.
#
# $Log: not supported by cvs2svn $
# Revision 1.5  2007/04/20 03:46:25  bkline
# Finished status query code.
#
# Revision 1.4  2007/04/10 21:37:28  bkline
# Plugged in some additional unit-testing options, as well as the start
# of the status query method.
#
# Revision 1.3  2007/03/23 16:31:33  bkline
# Adjusted name of doc type for Protocol documents.
#
# Revision 1.2  2007/03/23 16:04:15  bkline
# First working version.
#
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
testhost            = "gkint.cancer.gov"
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
    'Reload'          : 'Re-send specified documents that failed loading',
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

class DocumentLocation:
    """
        Object used to hold information about which locations the
        document occupies.

        Public attributes:

            cdrId
                unique CDR identifier for the document

            gatekeeperJobId
                publishing job ID for the last push job to send this
                document to the gatekeeper; the job may have requested
                that the document stop at the gatekeeper, or that
                it be automatically sent on to the preview and/or
                live servers

            gatekeeperDateTime
                date/time of the last push of this document to
                the gatekeeper

            previewJobId
                publishing job ID for the last push job to send this
                document to the preview server; the document may or
                may not have also been sent on to the liver server

            previewDateTime
                date/time of the last push of this document to
                the preview server

            liveJobId
                CDR publishing job ID for the last push job to
                send this document to the Cancer.gov live server
        """

    def __init__(self, node):
        self.cdrId              = int(node.getAttribute('cdrid'))
        self.gatekeeperJobId    = node.getAttribute('gatekeeper')
        self.gatekeeperDateTime = node.getAttribute('gatekeeperDateTime')
        self.previewJobId       = node.getAttribute('preview')
        self.previewDateTime    = node.getAttribute('previewDateTime')
        self.liveJobId          = node.getAttribute('live')
        self.liveDateTime       = node.getAttribute('liveDateTime')
        if not self.liveJobId:
            self.liveJobId      = node.getAttribute('liveID')

class DocumentStatusList:
    """
        Object with a single public attribute ('docs'), which contains
        a list of zero or more DocumentLocation objects.
    """
    
    def __init__(self, node):
        self.docs = []
        for child in node.childNodes:
            if child.nodeName == "document":
                self.docs.append(DocumentLocation(child))

class StatusSummaryDocument:
    """
        Information about one of the documents in a CDR push job.

        Public attributes:
        
            packetNumber
                position of the document within the push job

            group
                ID of the subset of documents in the push job
                which must fail the load job if any document
                within the subset fails

            cdrId
                unique CDR identifier for the document

            pubType
                document publication type; possible values are
                Export or Remove

            docType
                type of the document; e.g., GlossaryTerm or
                Protocol

            docStatus
                OK, Error, or Warning

            dependentStatus
                the document status relative to other members of its
                data group; possible values are OK and Error

            location
                the system the document was most recently promoted to;
                possible values are GateKeeper, Staging, Preview, and
                Live

    """

    def __init__(self, node):
        self.packetNumber    = node.getAttribute('packet')
        self.group           = node.getAttribute('group')
        self.cdrId           = node.getAttribute('cdrid')
        self.pubType         = node.getAttribute('pubType')
        self.docType         = node.getAttribute('type')
        self.status          = node.getAttribute('status')
        self.dependentStatus = node.getAttribute('dependentStatus')
        self.location        = node.getAttribute('location')
                                
                                
class StatusSummary:

    """
        Represents status information for a single GateKeeper push job.

        Public attributes:

            jobId
                unique identifier in the CDR system for the push job

            requestType
                Hotfix, FullLoad, Export or Remove

            description
                description of the job provided by the CDR at the
                time the job was submitted

            status
                Receiving or DataReceived

            source
                only value used for this version of the GateKeeper is
                "CDR"

            initiated
                date/time the job was started

            completion
                date/time the job was finished or aborted (if appropriate)

            target
                GateKeeper, Preview, or Live

            expectedCount
                number of documents reported by the CDR in the "complete"
                message for the job

            actualCount
                number of documents the GateKeeper recorded as having
                been received for the job
    """

    def __init__(self, node):
        self.jobId       = node.getAttribute('job')
        self.requestType = node.getAttribute('type')
        self.description = node.getAttribute('description')
        self.status      = node.getAttribute('status')
        self.source      = node.getAttribute('source')
        self.initiated   = node.getAttribute('initiated')
        self.completion  = node.getAttribute('completion')
        self.target      = node.getAttribute('target')
        self.docs        = []
        expectedDocCount = node.getAttribute('expectedDocCount')
        actualDocCount   = node.getAttribute('actualDocCount')
        try:
            self.expectedCount = int(expectedDocCount)
        except:
            self.expectedCount = expectedDocCount
        try:
            self.actualCount   = int(actualDocCount)
        except:
            self.actualCount   = actualDocCount
        for child in node.childNodes:
            if child.nodeName == 'document':
                self.docs.append(StatusSummaryDocument(child))
        if not self.initiated:
            self.initiated = node.getAttribute('initated') # typo in spec

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
            exchanges; PubDataResponse for document transfers; JobSummary
            or DocumentList for RequestStatus responses

        fault
            SOAP fault object containing faultcode and faultstring
            members in the case of a SOAP failure
    """
    
    def __init__(self, xmlString, publishing = True, statusRequest = False):

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
        elif statusRequest:
            respElem         = extractStatusResponseElement(self.bodyElem)
            detailElem       = getChildElement(respElem, "detailedMessage", 1)
            requestElem      = getChildElement(detailElem, "request")
            docListElem      = getChildElement(detailElem, "documentList")
            if requestElem:
                self.details = StatusSummary(requestElem)
            else:
                self.details = DocumentStatusList(docListElem)
        else:
            self.xmlResult   = extractXmlResult(self.bodyElem)
    def __repr__(self):
        pieces = [u"cdr2gk.Response "]
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

def extractStatusResponseElement(bodyNode):
    outerWrapper    = "RequestStatusResponse"
    innerWrapper    = "RequestStatusResult"
    requestResponse = getChildElement(bodyNode,        outerWrapper,      1)
    requestResult   = getChildElement(requestResponse, innerWrapper,      1)
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
# statusType is 'Summary', 'RequestDetail', 'DocumentLocation' or
# 'SingleDocument'
# If statusType is 'SingleDocument' then requestId contains the CDR
# ID of the document for which the report is being generated.
# If statusType contains the value 'DocumentLocation' then requestId
# need not be present.
# Otherwise, requestId refers to the publishing job for which
# status is requested.
# RequestDetail is not yet implemented.
#----------------------------------------------------------------------
def requestStatus(statusType, requestId = ""):
    headers = {
        'Content-type': "text/xml; charset='utf-8'",
        'SOAPAction'  : 'http://www.cancer.gov/webservices/RequestStatus'
    }
    if statusType == 'DocumentLocation':
        body = u"""\
  <RequestStatus xmlns='%s'>
   <source>CDR</source>
   <!-- <requestID></requestID> -->
   <statusType>%s</statusType>
  </RequestStatus>""" % (gatekeeperNamespace, statusType)
    else:
        body = u"""\
  <RequestStatus xmlns='%s'>
   <source>CDR</source>
   <requestID>%s</requestID>
   <statusType>%s</statusType>
  </RequestStatus>""" % (gatekeeperNamespace, requestId, statusType)
    xmlString = sendRequest(body, host = host, headers = headers)
    # print xmlString
    return Response(xmlString, False, True)

#----------------------------------------------------------------------
# Take it out for a test spin.  Try with 43740 (a Country document).
#----------------------------------------------------------------------
if __name__ == "__main__":

    debuglevel = 1
    if len(sys.argv) > 2 and sys.argv[1] == 'status':
        requestStatus(sys.argv[2], len(sys.argv) > 3 and sys.argv[3] or "")
        sys.exit(0)
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
            if self.docType in ('InScopeProtocol', 'CTGovProtocol'):
                self.docType = 'Protocol'
            docXml = re.sub(u"<\\?xml[^>]+>\\s*", u"", docXml)
            self.xml = re.sub(u"<!DOCTYPE[^>]*>\\s*", u"", docXml)

    # If we're asked to abort a job, do it.
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
        if docId.startswith('remove='):
            docId = int(docId[len('remove='):])
            print ("removing CDR%d (%d of %d)..." % (docId,
                                                     docNum, len(docIds))),
            # XXX fix this (look up real doc type) after testing.
            response = sendDocument(jobId, docNum, "Remove",
                                    "GENETICSPROFESSIONAL", docId, 1, 1, "")
        else:
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
