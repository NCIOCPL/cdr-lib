#----------------------------------------------------------------------
#
# $Id: cdr2cg.py,v 1.1 2002-05-09 12:51:50 bkline Exp $
#
# Support routines for SOAP communication with Cancer.Gov's GateKeeper.
#
# $Log: not supported by cvs2svn $
#----------------------------------------------------------------------
import httplib, re, sys, xml.dom.minidom

#----------------------------------------------------------------------
# Namespaces we don't really need.
#----------------------------------------------------------------------
"""
    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
"""

#----------------------------------------------------------------------
# Module data.
#----------------------------------------------------------------------
debuglevel          = 0
host                = "gatekeeper.cancer.gov"
port                = 80
soapNamespace       = "http://schemas.xmlsoap.org/soap/envelope/"
application         = "/GateKeeper.asmx"
headers             = {
    'Content-type': 'text/xml; charset="utf-8"',
    'SOAPAction'  : 'http://gatekeeper.cancer.gov/Request'
}

#----------------------------------------------------------------------
# XML wrappers.
#----------------------------------------------------------------------
requestWrapper      = """\
<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="%s">
 <soap:Body>
%s
 </soap:Body>
</soap:Envelope>"""

initRequestTemplate = """\
  <Request xmlns="http://gatekeeper.cancer.gov">
   <source>CDR</source>
   <requestID>Initiate Request</requestID>
   <message>
    <PubEvent>
     <pubType>%s</pubType>
     <docType>%s</docType>
     <lastJobID>%d</lastJobID>
    </PubEvent>
   </message>
  </Request>"""

dataPrologTemplate = """\
  <Request xmlns="http://gatekeeper.cancer.gov">
   <source>CDR</source>
   <requestID>%d</requestID>
   <message>
    <PubEvent>
     <pubType>%s</pubType>
     <docType>%s</docType>
     <lastJobID>%d</lastJobID>
     <docCount>%d</docCount>
    </PubEvent>
   </message>
  </Request>"""

docTemplate = """\
  <Request xmlns="http://gatekeeper.cancer.gov">
   <source>CDR</source>
   <requestID>%d</requestID>
   <message>
    <PubData>
     <docNum>%d</docNum>
     <transactionType>%s</transactionType>
     <CDRDoc Type="%s" ID="CDR%010d">
%s
     </CDRDoc>
    </PubData>
   </message>
  </Request>"""

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
        faultCodeElem     = getChildElement(node, "faultcode", 1)
        faultStringElem   = getChildElement(node, "faultstring", 1)
        self.faultcode    = getTextContent(faultCodeElem)
        self.faultstring  = getTextContent(faultStringElem)
    
class PubEventResponse:
    """
    Holds detailed information from a response to a request initiation
    or data prolog message.
    
    Public attributes:

        pubType
            One of Hotfix, Export, Remove, or Full Load; echoed
            from request

        docType
            echoed from request

        lastJobId
            ID of last job of this type successfully processed by
            Cancer.Gov

        docCount
            echoed from request (data prolog message only)
    """

    def __init__(self, node):
        pubTypeElem       = getChildElement(node, "pubType", 1)
        docTypeElem       = getChildElement(node, "docType", 1)
        lastJobIdElem     = getChildElement(node, "lastJobID", 1)
        docCountElem      = getChildElement(node, "docCount")
        self.pubType      = getTextContent(pubTypeElem)
        self.docType      = getTextContent(docTypeElem)
        self.lastJobId    = int(getTextContent(lastJobIdElem))
        if docCountElem is not None:
            self.docCount = int(getTextContent(docCountElem))
        else:
            self.codCount = None

class PubDataResponse:
    """
    Holds detailed information from a response to a data transfer
    request.

    Public attributes:

        docNum
            echoed from request
    """

    def __init__(self, node):
        child = getChildElement(node, "docNum", 1)
        self.docNum = int(getTextContent(child))
        
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
    """
    
    def __init__(self, xmlString):

        """Extract the values from the server's XML response string."""

        bodyElem         = extractBodyElement(xmlString)
        respElem         = extractResponseElement(bodyElem)
        respTypeElem     = getChildElement(respElem, "ResponseType", 1)
        respMsgElem      = getChildElement(respElem, "ResponseMessage", 1)
        peResponseElem   = getChildElement(respElem, "PubEventResponse")
        pdResponseElem   = getChildElement(respElem, "PubDataResponse")
        faultElem        = getChildElement(bodyElem, "Fault")
        self.type        = getTextContent(respTypeElem)
        self.message     = getTextContent(respMsgElem)
        self.fault       = faultElem and Fault(faultElem) or None
        if peResponseElem:
            self.details = PubEventResponse(peResponseElem)
        elif pdResponseElem:
            self.details = PubDataResponse(pdResponseElem)
        else:
            self.details = None

def getTextContent(node):
    text = ''
    for n in node.childNodes:
        if n.nodeType == xml.dom.minidom.Node.TEXT_NODE:
            text = text + n.nodeValue
    return text

def getChildElement(parent, name, required = 0):
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
    body    = getChildElement(docElem, "Body", 1)
    return body

def sendRequest(body):

    # Set up the connection and the request.
    conn    = httplib.HTTPConnection(host, port)
    request = requestWrapper % (soapNamespace, body)
    if debuglevel:
        open("cdr2cggk.log", "a").write("==== REQUEST:\n%s" % re.sub("\r", 
                                                                     "",
                                                                     request))

    # Submit the request and get the headers for the response.
    conn.request("POST", "/GateKeeper.asmx", request, headers)
    response = conn.getresponse()

    # Skip past any "Continue" responses.
    while response.status == 100:
        response.msg = None
        response.begin()

    # Check for failure.
    if response.status != 200:
        raise StandardError("HTTP error: %d (%s)" % (response.status,
                                                     response.reason))

    # Get the response payload and return it.
    data = response.read()
    if debuglevel:
        open("cdr2cggk.log", "a").write("==== RESPONSE:\n%s" % re.sub("\r",
                                                                      "",
                                                                      data))
    return data

def initiateRequest(pubType, docType, lastJobId):
    xmlString = sendRequest(initRequestTemplate % (pubType, 
                                                   docType, 
                                                   lastJobId))
    return Response(xmlString)
    
def sendDataProlog(jobId, pubType, docType, lastJobId, docCount):
    xmlString = sendRequest(dataPrologTemplate % (jobId, 
                                                  pubType, 
                                                  docType, 
                                                  lastJobId,
                                                  docCount))
    return Response(xmlString)

def sendDocument(jobId, docNum, transactionType, docType, docId, doc):
    xmlString = sendRequest(docTemplate % (jobId,
                                           docNum,
                                           transactionType,
                                           docType,
                                           docId,
                                           doc))
    return Response(xmlString)

#----------------------------------------------------------------------
# Take it out for a test spin.
#----------------------------------------------------------------------
if __name__ == "__main__":

    # Set the command-line arguments.
    jobId     = 4
    lastJobId = 3
    if len(sys.argv) > 1: jobId = int(sys.argv[1])
    if len(sys.argv) > 2: lastJobId = int(sys.argv[2])
    sys.stderr.write("job ID %d\n" % jobId)
    sys.stderr.write("last job ID %d\n" % lastJobId)
    debuglevel = 1

    # See if the GateKeeper is awake.
    sys.stderr.write("initiating request ...\n")
    response = initiateRequest("Export", "GlossaryTerm", lastJobId)
    if response.type != "OK":
        print "%s: %s" % (response.type, response.message)
        if response.fault:
            print "%s: %s" % (response.fault.faultcode,
                              response.fault.faultstring)
        elif response.details:
            print "Last job ID from server: %d" % response.details.lastJobId
        sys.exit(1)

    # Prepare the server for a batch of documents.
    sys.stderr.write("sending data prolog ...\n")
    response = sendDataProlog(jobId, "Export", "GlossaryTerm",
                              response.details.lastJobId, 2)
    if response.type != "OK":
        print "%s: %s" % (response.type, response.message)
        sys.exit(1)

    # Send the first document.
    sys.stderr.write("sending first document ...\n")
    response = sendDocument(jobId, 1, "Export", "GlossaryTerm", 76608,
                            open("76608.xml").read())
    if response.type != "OK":
        print "%s: %s" % (response.type, response.message)
        sys.exit(1)

    # Send the second document.
    sys.stderr.write("sending second document ...\n")
    response = sendDocument(jobId, 2, "Export", "GlossaryTerm", 77330,
                            open("77330.xml").read())
    if response.type != "OK":
        print "%s: %s" % (response.type, response.message)
        sys.exit(1)
