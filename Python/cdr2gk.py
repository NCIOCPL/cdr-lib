"""
Support routines for SOAP communication with Cancer.Gov's GateKeeper

Public functions:
  pubPreview() - ask the service for a web page version of a CDR document
  initiateRequest() - see if the GateKeeper service is open for business
  sendDataProlog() - tell the service about the push job we're about to start
  sendDocument() - give the service a new document version or a remove request
  sendJobComplete() - tell the service the job is finished or aborted
  requestStatus() - ask the service for status info on documents or jobs

Older code would re-assign the module-level `HOST` variable. That's not
a good idea, because the scheduler loads this module once for many jobs,
and the `HOST` setting for one job could stomp on what another job assumes.
Instead, to override the default GateKeeper host, set the optional keyword
parameter available on the public functions listed above.

On the other hand, re-assigning the `DEBUGLEVEL` variable is supported
(though in the next incarnation of this module, that will be wrapped in
an instance of a new `Control` class.

For testing, look at the `main()` function at the bottom of this class.
You can invoke it by running this module as a script. For usage info, try

                          cdr2gk.py --help
"""

import time
import cdr
import requests
from lxml import etree
from cdrapi.settings import Tier


# ======================================================================
# Module data.
# ======================================================================

# Some defaults
TIER = Tier()
SOURCE_TIER = "CDR-%s" % TIER.name
LOGFILE = cdr.DEFAULT_LOGDIR + "/cdr2gk.log"
MAX_RETRIES = 10
RETRY_MULTIPLIER = 1.0
DEBUGLEVEL = 0
HOST = TIER.hosts["GK"]
SCHEME = "http"
SOAP_ACTION = "http://www.cancer.gov/webservices/Request"
APPLICATION = "/GateKeeper/GateKeeper.asmx"
HEADERS = {"Content-type": 'text/xml; charset="utf-8"'}

# Namespaces
SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
GATEKEEPER_NS = "http://www.cancer.gov/webservices/"
PREVIEW_NS = "http://gatekeeper.cancer.gov/CDRPreview/"
WRAP_IN_NS = lambda ns, local: "{{{}}}{}".format(ns, local)

# Qualified element names
ENVELOPE = WRAP_IN_NS(SOAP_NS, "Envelope")
BODY = WRAP_IN_NS(SOAP_NS, "Body")
FAULT = WRAP_IN_NS(SOAP_NS, "Fault")
REQUEST = WRAP_IN_NS(GATEKEEPER_NS, "Request")
REQUEST_RESPONSE = WRAP_IN_NS(GATEKEEPER_NS, "RequestResponse")
REQUEST_RESULT = WRAP_IN_NS(GATEKEEPER_NS, "RequestResult")
REQUEST_STATUS = WRAP_IN_NS(GATEKEEPER_NS, "RequestStatus")
REQUEST_STATUS_RESPONSE = WRAP_IN_NS(GATEKEEPER_NS, "RequestStatusResponse")
REQUEST_STATUS_RESULT = WRAP_IN_NS(GATEKEEPER_NS, "RequestStatusResult")
PREVIEW_REQUEST = WRAP_IN_NS(PREVIEW_NS, "ReturnXML")
PREVIEW_RESPONSE = WRAP_IN_NS(PREVIEW_NS, "ReturnXMLResponse")
PREVIEW_RESULT = WRAP_IN_NS(PREVIEW_NS, "ReturnXMLResult")


# ======================================================================
# Module-level class definitions.
# ======================================================================

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
        self.faultcode    = cdr.get_text(node.find("faultcode"))
        self.faultstring  = cdr.get_text(node.find("faultstring"))
        assert self.faultcode, "missing required faultcode"
        assert self.faultstring, "missing required faultstring"
    def __repr__(self):
        args = self.faultcode, self.faultstring
        return "Fault (faultcode: {}, faultstring: {})".format(*args)


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
        self.cdrId              = int(node.get("cdrid"))
        self.gatekeeperJobId    = node.get("gatekeeper")
        self.gatekeeperDateTime = node.get("gatekeeperDateTime")
        self.previewJobId       = node.get("preview")
        self.previewDateTime    = node.get("previewDateTime")
        self.liveJobId          = node.get("live") or node.get("liveID")
        self.liveDateTime       = node.get("liveDateTime")


class DocumentStatusList:
    """
        Object with a single public attribute ('docs'), which contains
        a list of zero or more DocumentLocation objects.
    """

    def __init__(self, node):
        doc = node.findall("document")
        self.docs = [DocumentLocation(doc) for doc in doc]


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
        self.packetNumber    = node.get("packet")
        self.group           = node.get("group")
        self.cdrId           = node.get("cdrid")
        self.pubType         = node.get("pubType")
        self.docType         = node.get("type")
        self.status          = node.get("status")
        self.dependentStatus = node.get("dependentStatus")
        self.location        = node.get("location")


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
                This does not seem to be used anywhere.  The source
                element sent to Gatekeeper is established separately
                from this.

            initiated
                date/time the job was started (misspelled 'initated'
                in original spec)

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
        self.jobId       = node.get("job")
        self.requestType = node.get("type")
        self.description = node.get("description")
        self.status      = node.get("status")
        self.source      = node.get("source")
        self.initiated   = node.get("initiated") or node.get("initated")
        self.completion  = node.get("completion")
        self.target      = node.get("target")
        expectedDocCount = node.get("expectedDocCount")
        actualDocCount   = node.get("actualDocCount")
        children = node.findall("document")
        self.docs = [StatusSummaryDocument(child) for child in children]
        try:
            self.expectedCount = int(expectedDocCount)
        except:
            self.expectedCount = expectedDocCount
        try:
            self.actualCount = int(actualDocCount)
        except:
            self.actualCount = actualDocCount


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
        self.highestDocNum = self.totalPackets = None
        self.pubType = cdr.get_text(node.find("pubType"))
        lastJobId = cdr.get_text(node.find("lastJobID"))
        nextJobId = cdr.get_text(node.find("nextJobID"))
        docCount = cdr.get_text(node.find("docCount"))
        if not self.pubType:
            raise Exception("missing required pubType element")
        self.totalPackets  = None
        try:
            self.lastJobId = int(lastJobId)
        except:
            self.lastJobId = lastJobId
        try:
            self.nextJobId = int(nextJobId)
        except:
            self.nextJobId = nextJobId
        try:
            self.docCount = int(docCount)
        except:
            self.docCount = docCount
            try:
                totalPackets, highestDocNum = docCount.split("/")
                self.totalPackets = int(totalPackets)
                self.highestDocNum = int(highestDocNum)
            except:
                pass
    def __repr__(self):
        return ("PubEventResponse "
                "(pubType: %s, lastJobId: %s, nextJobId: %s, docCount: %s, "
                "totalPackets: %s, highestDocNum: %s)"""
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
        self.docNum = int(cdr.get_text(node.find("docNum")))
    def __repr__(self):
        return "PubDataResponse (docNum: %d)" % self.docNum


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

        xmlResult
            utf-8 string for publish preview

        fault
            SOAP fault object containing faultcode and faultstring
            members in the case of a SOAP failure
    """

    def __init__(self, xml, publishing=True, statusRequest=False):

        """Extract the values from the server's XML response string."""

        # Parse the response and check for a Fault element.
        self.root = etree.fromstring(xml)
        self.body = self.root.find(BODY)
        self.fault = None
        node = self.body.find(FAULT)
        self.fault = None if node is None else Fault(node)
        self.publishing = publishing

        # Pull out response to a publishing request.
        if publishing:
            path = "{}/{}/Response".format(REQUEST_RESPONSE, REQUEST_RESULT)
            response = self.body.find(path)
            self.type = cdr.get_text(response.find("ResponseType"))
            self.message = cdr.get_text(response.find("ResponseMessage"))
            if not self.type:
                raise Exception("Missing required ResponseType element")
            if not self.message:
                raise Exception("Missing required ResponseMessage element")
            pub_event_response = response.find("PubEventResponse")
            pub_data_response = response.find("PubDataResponse")
            if pub_event_response is not None:
                self.details = PubEventResponse(pub_event_response)
            elif pub_data_response is not None:
                self.details = PubDataResponse(pub_data_response)
            else:
                self.details = None

        # Response to a status request.
        elif statusRequest:
            args = REQUEST_STATUS_RESPONSE, REQUEST_STATUS_RESULT
            path = "{}/{}/Response".format(*args)
            request = self.body.find(path)
            details = request.find("detailedMessage")
            if details is None:
                raise Exception("Missing detailedMessage element")
            request = details.find("request")
            doclist = details.find("documentList")
            if request is not None:
                self.details = StatusSummary(request)
            else:
                self.details = DocumentStatusList(doclist)

        # Response to a publish preview request.
        else:
            path = "{}/{}".format(PREVIEW_RESPONSE, PREVIEW_RESULT)
            self._xml = cdr.get_text(self.body.find(path))

    @property
    def xmlResult(self):
        """
        UTF-8 bytes for publish preview page
        """

        if not hasattr(self, "_xml") or self._xml is None:
            return None
        if isinstance(self._xml, str):
            return self._xml.encode("utf-8")
        else:
            return self._xml

    def __repr__(self):
        """
        Display for debugging/logging
        """

        pieces = ["cdr2gk.Response "]
        if self.publishing:
            pieces.append("(type: %s, message: %s, details: %s" %
                          (self.type, self.message, self.details))
        else:
            pieces.append("(publish preview document")
        if self.fault:
            pieces.append(", fault: %s" % self.fault)
        pieces.append(")")
        return "".join(pieces)


def _log(command_type, value, **opts):
    """
    Optionally write to the log file if `DEBUGLEVEL` is greater than zero

    This would have been replaced by the standard library's logging tools,
    except for the fact that GateKeeper will be going away very soon.
    Just as well, as the use of the module's DEBUGLEVEL (and the modifying
    of that value by outside code after the module was loaded) was not
    a good idea. It would have been better to have a function (or better,
    a class with a method) for modifying the logging level. Oh, well.

    Requred positional parameters:
        command_type - e.g., "SEND REQUEST"
        value - string to be written to the log file

    Optional keyword arguments:
        force - write even if `DEBUGLEVEL` is not greater than zero
        host - override `HOST`
    """

    if opts.get("force") or DEBUGLEVEL > 0:
        if not isinstance(value, str):
            value = value.decode("utf-8")
        host = (opts.get("host") or "").strip() or HOST
        args = time.ctime(), command_type, host, value.replace("\r", "")
        message = "==== {} {} (host={}) ====\n{!r}\n".format(*args)
        with open(LOGFILE, "ab") as fp:
            fp.write(message.encode("utf-8"))

def sendRequest(body, **opts):
    """
    Send a SOAP client request to the Gatekeeper host

    There's a bug in the `lxml` package and there's a bug in the original
    spec for the GateKeeper service, and they work together to cancel
    each other out. The bug in the spec is that it called for using a
    default namespace for the `body` node passed into this function,
    without overriding that namespace for the node's descendants, which
    causes all of the elements under that node to live in the same name-
    space (so, for example, every element in a CDR document sent through
    this SOAP interface is in a namespace when it gets to the GateKeeper,
    even though all of those elements are in the null namespace when they
    come out of the export filters. So either the GateKeeper code is
    looking for the elements it needs by specifying the namespaces it's
    getting, or it uses an API that lets it select nodes using only
    local tag names, or it's using raw string manipulation and/or regular
    expressions to pull apart the documents, or some combination of these
    techniques.

    The bug in the xml package is in its serializing logic. The
    package is aware that the top-level element of the CDR document,
    as well as all of its children, are in the null namespace when it
    parses the document, and it preserves that knowledge when the top
    node is attached to the SOAP document. But when the module's
    `tostring()` function is invoked, that information is lost, and
    the package fails to override the enclosing namespace. So the
    document being handed to GateKeeper matches the spec (though
    possibly not what the author of the spec really intended).

    I considered filing a bug report for the lxml bug, but I don't
    really want the bug fixed if GateKeeper really is relying on the
    extra namespaces it's getting.

    Required positional argument:
      body - single child node of the soap:Body wrapper

    Optional keyword arguments:
      app - resource portion of URL (default "/GateKeeper/GateKeeper.asmx")
      host - target server (default `HOST`)
      action - SOAP headers (default `SOAP_ACTION`)
      scheme - defaults to "http"
    """

    request = etree.Element(ENVELOPE, nsmap={"soap": SOAP_NS})
    wrapper = etree.SubElement(request, BODY)
    wrapper.append(body)
    if DEBUGLEVEL > 1:
        etree.dump(request)
    request_bytes = etree.tostring(request, encoding="utf-8")
    host = (opts.get("host") or "").strip() or HOST
    _log("REQUEST", request_bytes, host=host)
    tries = MAX_RETRIES
    response = None
    scheme = opts.get("scheme") or SCHEME
    application = opts.get("app") or APPLICATION
    url = "{}://{}{}".format(scheme, host, application)
    headers = dict(HEADERS)
    headers["SOAPAction"] = opts.get("action") or SOAP_ACTION
    while tries:
        try:
            response = requests.post(url, data=request_bytes, headers=headers)
            tries = 0
        except Exception as e:
            message = "sendRequest({!r}) caught exception {}".format(url, e)
            cdr.Logging.get_logger("cdr2gk").exception(message)
            wait = (MAX_RETRIES + 1 - tries) * RETRY_MULTIPLIER
            args = tries, wait
            message = "{} retries left; waiting {:f} seconds".format(*args)
            _log("SEND REQUEST", message, force=True, host=host)
            time.sleep(wait)
            tries -= 1
    if response is None:
        msg = "tried to connect {} times unsuccessfully".format(MAX_RETRIES)
        raise Exception(msg)
    if not response.ok:
        _log("HTTP ERROR", response.content, host=host)
        resp = "(occurred at {}) ({})".format(time.ctime(), response.text)
        args = response.status_code, response.reason, resp
        message = "HTTP error: {:d} ({}) {}".format(*args)
        raise Exception(message)
    _log("RESPONSE", response.content, host=host)
    return response.content


# ======================================================================
# PUBLIC REQUEST FUNCTIONS START HERE
# ======================================================================

def pubPreview(xml, template_type, **opts):
    """
    Ask the service to create a simulation of a CDR document's web page

    Pass:
      xml - serialized XML for the filtered CDR document
      template_type - e.g., "Summary", "GlossaryTerm, "DrugInfoSummary", etc.
      host - defaults to `HOST`

    Return:
      `Response` object
    """

    host = (opts.get("host") or "").strip() or HOST
    app = "/CDRPreviewWS/CDRPreview.asmx"
    action = "http://gatekeeper.cancer.gov/CDRPreview/ReturnXML"
    body = etree.Element(PREVIEW_REQUEST, nsmap={None: PREVIEW_NS})
    wrapper = etree.SubElement(body, "content")
    doc = etree.fromstring(xml)
    doc.nsmap[None] = ""
    wrapper.append(doc)
    etree.SubElement(body, "template_type").text = template_type
    xml = sendRequest(body, app=app, host=host, action=action)
    return Response(xml, publishing=False, statusRequest=False)

def initiateRequest(pub_type, pub_target, **opts):
    """
    Make sure the Gatekeeper is open for business

    Pass:
      pub_type - e.g., "Export", "Hotfix", "Full Load", etc.
      pub_target - "Preview", "Live", or "GateKeeper"
      host - optional keyword argument; defaults to `HOST`
      source - optional keyword argument; defaults to `SOURCE_TIER`

    Return:
      `Response` object
    """

    host = (opts.get("host") or "").strip() or HOST
    source = opts.get("source") or SOURCE_TIER
    body = etree.Element(REQUEST, nsmap={None: GATEKEEPER_NS})
    etree.SubElement(body, "source").text = source
    etree.SubElement(body, "requestID").text = "Status Check"
    message = etree.SubElement(body, "message")
    wrapper = etree.SubElement(message, "PubEvent")
    etree.SubElement(wrapper, "pubType").text = pub_type
    etree.SubElement(wrapper, "pubTarget").text = pub_target
    xml = sendRequest(body, host=host)
    return Response(xml, publishing=True, statusRequest=False)

def sendDataProlog(desc, job_id, pub_type, pub_target, last_id, **opts):
    """
    Tell the service about the upcoming job

    Pass:
      desc - string describing this job; supplied by user or scheduler
      job_id - integer primary key into the `pub_proc` table
      pub_type - e.g., "Export", "Hotfix", "Full Load", etc.
      pub_target - "Preview", "Live", or "GateKeeper"
      last_id - integer for what we think is the last push job
      host - optional keyword argument; defaults to `HOST`
      source - optional keyword argument; defaults to `SOURCE_TIER`

    Return:
      `Response` object
    """

    host = (opts.get("host") or "").strip() or HOST
    source = opts.get("source") or SOURCE_TIER
    body = etree.Element(REQUEST, nsmap={None: GATEKEEPER_NS})
    etree.SubElement(body, "source").text = source
    etree.SubElement(body, "requestID").text = str(job_id)
    message = etree.SubElement(body, "message")
    wrapper = etree.SubElement(message, "PubEvent")
    etree.SubElement(wrapper, "pubType").text = pub_type
    etree.SubElement(wrapper, "pubTarget").text = pub_target
    etree.SubElement(wrapper, "description").text = desc
    etree.SubElement(wrapper, "lastJobID").text = str(last_id)
    xml = sendRequest(body, host=host)
    return Response(xml, publishing=True, statusRequest=False)

def sendDocument(job, num, action, doctype, id, ver, group, xml=None, **opts):
    """
    Give the service a new document version or a remove request

    Pass:
      job - integer for the primary key of the job's row in `pub_proc`
      num - integer for the position of the document in this batch
      action - "Export" or "Remove"
      doctype - string identifying which type this document is (e.g., "Term")
      doc_id - integer for the document's unique identifier
      ver - integer for the version of the document being sent or removed
      group - integer for the subset which must fail if any in the group fails
      xml - utf-8 bytes for the document if action is "Export"; else ignored
      host - defaults to `HOST`
      source - defaults to `SOURCE_TIER`

    Return:
      `Response` object
    """

    host = (opts.get("host") or "").strip() or HOST
    source = opts.get("source") or SOURCE_TIER
    body = etree.Element(REQUEST, nsmap={None: GATEKEEPER_NS})
    etree.SubElement(body, "source").text = source
    etree.SubElement(body, "requestID").text = str(job)
    message = etree.SubElement(body, "message")
    wrapper = etree.SubElement(message, "PubData")
    etree.SubElement(wrapper, "docNum").text = str(num)
    etree.SubElement(wrapper, "transactionType").text = action
    cdr_doc = etree.SubElement(wrapper, "CDRDoc")
    cdr_doc.set("Type", doctype)
    cdr_doc.set("ID", cdr.normalize(id))
    cdr_doc.set("Version", str(ver))
    cdr_doc.set("Group", str(group))
    if xml is not None:
        doc = etree.fromstring(xml)
        doc.nsmap[None] = ""
        cdr_doc.append(doc)
    xml = sendRequest(body, host=host)
    return Response(xml, publishing=True, statusRequest=False)

def sendJobComplete(job_id, pub_type, count, status, **opts):
    """
    Tell the service a job has finished or is being aborted

    Pass:
      job_id - integer for the primary key of the job's row in `pub_proc`
      pub_type - e.g., "Export"
      count - number of documents sent
      status - "complete" or "abort"
      host - defaults to `HOST`
      source - defaults to `SOURCE_TIER`

    Return:
      `Response` object
    """

    host = (opts.get("host") or "").strip() or HOST
    source = opts.get("source") or SOURCE_TIER
    body = etree.Element(REQUEST, nsmap={None: GATEKEEPER_NS})
    etree.SubElement(body, "source").text = source
    etree.SubElement(body, "requestID").text = str(job_id)
    message = etree.SubElement(body, "message")
    wrapper = etree.SubElement(message, "PubEvent")
    etree.SubElement(wrapper, "pubType").text = pub_type
    etree.SubElement(wrapper, "docCount").text = str(count)
    etree.SubElement(wrapper, "status").text = status
    xml = sendRequest(body, host=host)
    return Response(xml, publishing=True, statusRequest=False)

def requestStatus(status_type, request_id="", **opts):
    """
    Ask the Gatekeeper server about the status of a job or documents

    Pass:
      status_type - which status we want; one of:
        * "SingleDocument" - (requestId contains the CDR document ID)
        * "DocumentLocation" - all documents in GK (requestId ignored)
        * "Summary" - documents for one job (requestId carries job ID)
        * "RequestDetail" - not yet implemented
      request_id - job or document ID as explained above
      host - defaults to `HOST`
      source - defaults to `SOURCE_TIER`

    Return:
      `Response` object
    """

    host = (opts.get("host") or "").strip() or HOST
    source = opts.get("source") or SOURCE_TIER
    action = "http://www.cancer.gov/webservices/RequestStatus"
    body = etree.Element(REQUEST_STATUS, nsmap={None: GATEKEEPER_NS})
    etree.SubElement(body, "source").text = source
    if status_type != "DocumentLocation":
        etree.SubElement(body, "requestID").text = str(request_id)
    etree.SubElement(body, "statusType").text = status_type
    xml = sendRequest(body, host=host, action=action)
    return Response(xml, publishing=False, statusRequest=True)


class Test:
    """
    Provide command-line access to the public functions

    For example:
      cdr2gk.py status --job-id 15115 --source CDR-PROD --status-type Summary
      cdr2gk.py preview --doc-id 44000 --doctype GlossaryTerm
    """

    from cdrapi.db import Query
    TYPES = "SingleDocument", "DocumentLocation", "Summary"
    SOURCES = "CDR-PROD", "CDR-STAGE", "CDR-QA", "CDR-DEV"
    PUB_TYPE = "Export"
    PUB_TARGET = "GateKeeper"
    PUB_TARGETS = "Preview", "Live", "GateKeeper"
    DESC = "Command-line test from cdr2gk module."
    COMMANDS = (
        "preview",
        "init",
        "prolog",
        "push",
        "remove",
        "complete",
        "abort",
        "status",
    )

    # Some examples
    DOC_ID = 44000 # sample GlossaryTerm document
    JOB_ID = 15115 # sample job ID from late 2017; use source CDR-PROD

    def __init__(self):
        """
        Capture command-line options
        """

        global DEBUGLEVEL
        import argparse
        status_opts = dict(choices=self.TYPES, default=self.TYPES[0])
        target_opts = dict(choices=self.PUB_TARGETS, default=self.PUB_TARGET)
        parser = argparse.ArgumentParser()
        parser.add_argument("command", choices=self.COMMANDS)
        parser.add_argument("--doc-id", type=int, default=self.DOC_ID)
        parser.add_argument("--job-id", type=int, default=self.JOB_ID)
        parser.add_argument("--status-type", **status_opts)
        parser.add_argument("--pub-type", default=self.PUB_TYPE)
        parser.add_argument("--pub-target", **target_opts)
        parser.add_argument("--debug-level", type=int, default=1)
        parser.add_argument("--host", default=HOST)
        parser.add_argument("--source", default=SOURCE_TIER)
        parser.add_argument("--doctype", default="GlossaryTerm")
        parser.add_argument("--count", help="doc count for 'complete' action")
        parser.add_argument("--last-id", help="for testing 'prolog' command")
        parser.add_argument("--desc", default=self.DESC, help="job desc")
        parser.add_argument("--group", type=int, default=1, help="fail group")
        parser.add_argument("--num", type=int, default=1, help="doc position")
        self.opts = parser.parse_args()
        DEBUGLEVEL = self.opts.debug_level

    def run(self):
        """
        Take the module out for a test spin.
        """

        getattr(self, "_{}".format(self.opts.command))()

    # ======================================================================
    # COMMAND IMPLEMENTATION METHODS START HERE
    # ======================================================================

    def _preview(self):
        """
        Ask GK for a web version of a CDR doc

        Required:
          --doc-id (which document to preview; default 44000, a GTN doc)
          --doctype (what name does GK know this type by?)

        Optional:
          --host
        """

        setname = doctype = self.opts.doctype
        assert doctype, "--doctype required for preview action"
        if doctype == "GlossaryTermName":
            setname = doctype = "GlossaryTerm"
        if setname == "Person":
            setname = "GeneticsProfessional"
        filters = ["set:Vendor {} Set".format(setname)]
        opts = dict(ver="lastp", parms=[["isPP", "Y"]])
        result = cdr.filterDoc("guest", filters, self.opts.doc_id, **opts)
        if isinstance(result, (str, bytes)):
            raise Exception(result)
        xml, messages = result
        response = None
        try:
            response = pubPreview(xml, doctype, host=self.opts.host)
            print(response.xmlResult.decode("utf-8").strip())
        except Exception as e:
            print(e)
            if response is not None:
                etree.dump(response.root)

    def _init(self):
        """
        See if the GateKeeper is alive and well

        This is not the constructor! :-)

        Required:
          --pub-type
          --pub-target

        Optional:
          --host
          --source
        """

        assert self.opts.pub_type, "--pub-type required for 'init' command"
        assert self.opts.pub_target, "--pub-target required for 'init' command"
        args = self.opts.pub_type, self.opts.pub_target
        opts = dict(host=self.opts.host, source=self.opts.source)
        response = initiateRequest(*args, **opts)
        etree.dump(response.root)

    def _prolog(self):
        """
        Tell GateKeeper about documents which are coming

        Required:
          --last-id (ID of the previous push job)
          --job-id (ID of the current job)
          --pub-type
          --pub-target

        Optional:
          --desc (description of the push job)
          --host
          --source
        """

        desc = self.opts.desc
        job_id, last_id = self.opts.job_id, self.opts.last_id
        assert job_id is not None, "--job-id required for 'prolog' command"
        assert last_id is not None, "--last-id required for 'prolog' command"
        assert self.opts.pub_type, "--pub-type required for 'init' command"
        assert self.opts.pub_target, "--pub-target required for 'init' command"
        args = desc, job_id, self.opts.pub_type, self.opts.pub_target, last_id
        opts = dict(host=self.opts.host, source=self.opts.source)
        response = sendDataProlog(*args, **opts)
        etree.dump(response.root)

    def _push(self):
        """
        Post a document to the service

        Required:
          --job-id
          --doc-id
          --num
          --group

        Optional:
          --host
          --source
        """

        from cdrapi.docs import Doc
        from cdrapi.users import Session
        job_id = self.opts.job_id
        num = self.opts.num
        group = self.opts.group
        assert job_id, "--job-id required for 'push' command"
        assert num, "--num required for 'push' command"
        assert group, "--group required for 'push' command"
        assert self.opts.doc_id, "--doc-id required for 'push' command"
        doc = Doc(Session("guest"), id=self.opts.doc_id, version="lastp")

        # Find the filter set with some mapping of document type names.
        ver = doc.version
        doctype = doc.doctype.name
        sets = dict(
            GlossaryTermName="GlossaryTerm",
            DrugInformationSummary="DrugInfoSummary",
            Person="GeneticsProfessional"
        )
        set_name = "set:Vendor {} Set".format(sets.get(doctype, doctype))

        # Filter the document and serialize it to utf-8 bytes.
        result = doc.filter(set_name)
        xml = str(result.result_tree)
        if isinstance(xml, str):
            xml = xml.encode("utf-8")

        # Map our doctype name to GateKeeper's.
        doctypes = dict(
            GlossaryTermName="GlossaryTerm",
            Person="GENETICSPROFESSIONAL",
            DrugInformationSummary="DrugInfoSummary"
        )
        doctype = doctypes.get(doctype, doctype)

        # Push the filtered document.
        args = job_id, num, "Export", doctype, doc.id, ver, group, xml
        opts = dict(host=self.opts.host, source=self.opts.source)
        response = sendDocument(*args, **opts)
        etree.dump(response.root)

    def _remove(self):
        """
        Tell GateKeeper to remove a cdr document

        Required:
          --job-id
          --doc-id
          --doc-type

        Optional:
          --host
          --source
        """

        job_id = self.opts.job_id
        doc_id = self.opts.doc_id
        doc_type = self.opts.doc_type
        assert job_id, "--job-id required for 'remove' command"
        assert doc_id, "--doc-id required for 'remove' command"
        assert doc_type, "--doc-type required for 'remove' command"
        args = job_id, 1, "Remove", doc_type.name, doc_id, 1, 1
        opts = dict(host=self.opts.host, source=self.opts.source)
        response = sendDocument(*args, **opts)
        etree.dump(response.root)

    def _complete(self):
        """
        Tell GK a job is done

        Required:
          --job-id (job we're talking about)
          --count (number of documents sent)

        Optional:
          --host
          --source
        """

        assert self.opts.job_id, "--job-id required for 'complete' command"
        opts = dict(host=self.opts.host, source=self.opts.source)
        args = self.opts.job_id, "Export", self.opts.count, "complete"
        response = sendJobComplete(*args, **opts)
        etree.dump(response.root)

    def _abort(self):
        """
        Tell GK a job is being killed

        Required:
          --job-id (job to abort)

        Optional:
          --host
          --source
        """

        assert self.opts.job_id, "--job-id required for 'abort' command"
        opts = dict(host=self.opts.host, source=self.opts.source)
        args = self.opts.job_id, "Export", 0, "abort"
        response = sendJobComplete(*args, **opts)
        etree.dump(response.root)

    def _status(self):
        """
        Ask GK for status of a job, or a document, or all documents

        Required:
          --status-type
          --doc-id if status-type is "SingleDocument"
          --job-id if status-type is "Summary"

        Optional:
          --host
          --source
        """

        if self.opts.status_type == "SingleDocument":
            request_id = self.opts.doc_id
        elif self.opts.status_type == "Summary":
            request_id = self.opts.job_id
        else:
            request_id = None
        opts = dict(host=self.opts.host, source=self.opts.source)
        response = requestStatus(self.opts.status_type, request_id, **opts)
        etree.dump(response.root)


if __name__ == "__main__":
    """
    Make it possible to run this as a script for testing the public functions
    """

    Test().run()
