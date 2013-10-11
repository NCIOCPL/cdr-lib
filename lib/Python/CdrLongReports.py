#----------------------------------------------------------------------
#
# $Id$
#
# CDR Reports too long to be run directly from CGI.
#
# BZIssue::1264 - Different filter set for OrgProtocolReview report
# BZIssue::1319 - Modifications to glossary term search report
# BZIssue::1337 - Modified displayed string; formatting changes
# BZIssue::1702
# BZIssue::3134 - Modifications to Protocol Processing Status report
# BZIssue::3627 - Modifications for OSP report
# BZIssue::4626 - Added class ProtocolOwnershipTransfer
# BZIssue::4711 - Adding GrantNo column to report output
# BZIssue::5086 - Changes to the Transferred Protocols Report (Issue 4626)
# BZIssue::5123 - Audio Pronunciation Tracking Report
# BZIssue::5237 - Report for publication document counts fails on
#                 non-production server
# BZIssue::5244 - URL Check report not working
#
#----------------------------------------------------------------------
import cdr, cdrdb, xml.dom.minidom, time, cdrcgi, cgi, sys, socket, cdrbatch
import string, re, urlparse, httplib, traceback, xml.sax.saxutils
import urllib2, HTMLParser, ExcelWriter, NCIThes, lxml.etree as etree

#----------------------------------------------------------------------
# Module values.
#----------------------------------------------------------------------
REPORTS_BASE = cdr.BASEDIR + "/reports"
LOGFILE      = cdr.DEFAULT_LOGDIR + "/reports.log"
### EMAILFROM    = 'cdr@%s' % cdr.getHostName()[1]
EMAILFROM    = 'cdr@%s' % cdr.CBIIT_NAMES[1]

#----------------------------------------------------------------------
# Send mail to the receipients specified for the job.
#----------------------------------------------------------------------
def sendMail(job, subject, message):
    cdr.logwrite("Sending email report to %s" % job.getEmail(), LOGFILE)
    recips = job.getEmail().replace(',', ' ').replace(';', ' ').split()
    cdr.sendMail(EMAILFROM, recips, subject, message)

#----------------------------------------------------------------------
# Create a string showing delta between two times.
#----------------------------------------------------------------------
def getElapsed(then, now):
    delta = now - then
    secs = delta % 60
    delta /= 60
    mins = delta % 60
    hours = delta / 60
    return "%02d:%02d:%02d" % (hours, mins, secs)

#----------------------------------------------------------------------
# Lookup document title given the document ID.
#----------------------------------------------------------------------
def getDocTitleFromId(id, conn):
    #cdr.logwrite("getDocTitleFromId (id = %d)" % id, LOGFILE)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT title FROM document WHERE id = ?", id)
        rows = cursor.fetchall()
        if not rows:
            return "ERROR: CDR%010d NOT FOUND" % id
        if len(rows) > 1:
            return "INTERNAL ERROR: MULTIPLE ROWS FOR CDR%010d" % id
        return rows[0][0]
    except:
        return "ERROR: DATABASE FAILURE RETRIEVING TITLE FOR CDR%010d" % id

# -------------------------------------------
# Getting the Protocol Grant information
# -------------------------------------------
def getGrantNo(id, docType, cursor):
    if docType == 'CTGovProtocol':
        protString = 'CTGovProtocol/PDQAdminInfo'
        nodeLoc    = 12

    else:
        protString = 'InScopeProtocol'
        nodeLoc    = 8

    query = """
        SELECT t.value, g.value
          FROM query_term g
          JOIN query_term t
            on t.doc_id = g.doc_id
           AND t.path = '/%s/FundingInfo' +
                         '/NIHGrantContract/NIHGrantContractType'
           AND left(g.node_loc, %d) = left(t.node_loc, %d)
         WHERE g.doc_id = %s
           AND g.path = '/%s/FundingInfo' +
                         '/NIHGrantContract/GrantContractNo'
               """ % (protString, nodeLoc, nodeLoc, id, protString)

    cursor.execute(query)

    rows = cursor.fetchall()
    grantNo = []
    for row in rows:
        grantNo.append(u'%s-%s' % (row[0], row[1]))

    grantNo.sort()

    return ", ".join(["%s" % g for g in grantNo])


# -------------------------------------------
# Getting the Protocol Phase information
# -------------------------------------------
def getPhase(id, cursor):
    query = """
        SELECT value
          FROM query_term
         WHERE doc_id = %s
           AND path = '/InScopeProtocol/ProtocolPhase'
               """ % (id)

    cursor.execute(query)

    rows = cursor.fetchall()
    protPhase = []

    return ", ".join(["%s" % g[0] for g in rows])


# -------------------------------------------
# Getting the FDA information
# -------------------------------------------
def getFdaInfo(id, cursor):
    # Getting the RegulatoryInformation
    # ---------------------------------
    query = """
         SELECT f.value, s.value, i.value
           FROM query_term f
LEFT OUTER JOIN query_term s
             ON s.doc_id = f.doc_id
            AND s.path = '/InScopeProtocol/RegulatoryInformation/Section801'
LEFT OUTER JOIN query_term i
             ON i.doc_id = f.doc_id
            AND i.path = '/InScopeProtocol/FDAINDInfo/INDNumber'
          WHERE f.doc_id = %s
            AND f.path = '/InScopeProtocol/RegulatoryInformation/FDARegulated'
               """ % id

    cursor.execute(query)
    row = cursor.fetchone()

    return row


#----------------------------------------------------------------------
# Examine a protocol site node to see whether it has a phone.
#----------------------------------------------------------------------
def hasSitePhone(node):
    for p in node.getElementsByTagName("ProtPerson"):
        for c in p.getElementsByTagName("Contact"):
            for d in c.getElementsByTagName("ContactDetail"):
                for ph in d.getElementsByTagName("Phone"):
                    if cdr.getTextContent(ph):
                        return 1
                for ph in d.getElementsByTagName("TollFreePhone"):
                    if cdr.getTextContent(ph):
                        return 1
        # Only look at the first ProtPerson, per Lakshmi 2003-02-25
        return 0

#----------------------------------------------------------------------
# Create a report on protocol sites without phones.
#----------------------------------------------------------------------
def protsWithoutPhones(job):
    try:
        conn = cdrdb.connect('CdrGuest')
        cursor = conn.cursor()
        cursor.execute("""\
      SELECT /* TOP 50 */ d.id, MAX(v.num)
        FROM doc_version v
        JOIN document d
          ON d.id = v.id
        JOIN query_term q
          ON q.doc_id = d.id
       WHERE q.path = '/InScopeProtocol/ProtocolAdminInfo'
                    + '/CurrentProtocolStatus'
         AND q.value IN ('Active', 'Approved-Not Yet Active')
         AND v.val_status = 'V'
         AND v.publishable = 'Y'
         AND d.active_status = 'A'
    GROUP BY d.id""")
        rows = cursor.fetchall()
    except:
        job.fail("Database failure extracting active protocols",
                 logfile = LOGFILE)

    class ReportItem:
        def __init__(self, protDocId, protVer, protId, #siteType,
                     siteName, siteId):
            self.protDocId   = protDocId
            self.protVer     = protVer
            self.protId      = protId
            # self.siteType    = siteType
            self.siteName    = siteName
            self.siteId      = siteId
    class Problem:
        def __init__(self, id, ver, errors):
            self.id     = id
            self.ver    = ver
            self.errors = errors
    reportItems = []
    errors      = []
    nRows = len(rows)
    done = 0
    start = time.time()
    for row in rows:
        cdr.logwrite("Checking CDR%010d/%d" % (row[0], row[1]), LOGFILE)
        resp = cdr.filterDoc('guest', ['set:Vendor InScopeProtocol Set'],
                             row[0], docVer = row[1])
        if type(resp) in (type(''), type(u'')):
            cdr.logwrite("failure filtering CDR%010d/%d" % (row[0], row[1]),
                         LOGFILE)
            errors.append(Problem(row[0], row[1], resp))
            continue
        try:
            prot = xml.dom.minidom.parseString(resp[0]).documentElement
        except:
            cdr.logwrite("failure parsing CDR%010d/%d" % (row[0], row[1]),
                         LOGFILE)
            errors.append(Problem(row[0], row[1], "XML parsing failure"))
            continue
        #cdr.logwrite("document parsed", LOGFILE)
        protId = "&nbsp;"
        for child in prot.childNodes:
            if child.nodeName == "ProtocolIDs":
                for c1 in child.childNodes:
                    if c1.nodeName == "PrimaryID":
                        for c2 in c1.childNodes:
                            if c2.nodeName == "IDString":
                                protId = cdr.getTextContent(c2)
        #cdr.logwrite("protId: %s" % protId, LOGFILE)
        for child in prot.childNodes:
            if child.nodeName == "ProtocolAdminInfo":
                for gc in child.childNodes:
                    if gc.nodeName == "ProtocolSites":
                        for ggc in gc.childNodes:
                            if ggc.nodeName == "ProtocolSite":
                                #siteType = ggc.getAttribute("sitetype")
                                siteId   = ggc.getAttribute("ref")
                                #cdr.logwrite("siteId=%s" % siteId, LOGFILE)
                                digits   = re.sub(r"[^\d]", "", siteId)
                                try:
                                    id = int(digits)
                                    siteName = getDocTitleFromId(id, conn)
                                except:
                                    siteName = ("FAILURE GETTING DOCTITLE "
                                                "FOR %" % siteId)
                                sitePhoneFound = hasSitePhone(ggc)
                                if not sitePhoneFound:
                                    item = ReportItem(row[0], row[1], protId,
                                                      siteName, siteId)
                                    reportItems.append(item)
                                    cdr.logwrite(
                                        "adding report item for %s" % siteId,
                                        LOGFILE)
        done += 1
        now = time.time()
        timer = getElapsed(start, now)
        msg = "Checked %d of %d protocols; elapsed: %s" % (done, nRows, timer)
        job.setProgressMsg(msg)
        cdr.logwrite(msg, LOGFILE)

    """
    h2 = "Checked %d Protocols; Found %d Sites Without Phone" % (nRows,
                                      len(reportItems))
    """
    cdr.logwrite("out of loop", LOGFILE)
    html = u"""\
<!DOCTYPE HTML PUBLIC '-//IETF//DTD HTML//EN'>
<html>
 <head>
  <title>Protocol Sites Without Phones</title>
  <style type='text/css'>
   h1         { font-family: serif; font-size: 14pt; color: black;
                font-weight: bold; text-align: center; }
   th         { font-family: Arial; font-size: 12pt; font-weight: bold; }
   td         { font-family: Arial; font-size: 11pt; }
  </style>
 </head>
 <body>
  <h1>Active Protocols with Sites without a Phone Number<br>%s</h1>
  <table border='1' cellpadding='2' cellspacing='0'>
   <tr>
    <th nowrap='1'>Doc ID</th>
    <th nowrap='1'>Version</th>
    <th nowrap='1'>Prot ID</th>
    <th nowrap='1'>Site Id</th>
    <th nowrap='1'>Site Name and Location</th>
   </tr>
""" % time.strftime("%B %d, %Y") # h2
    for item in reportItems:
        cdr.logwrite("writing row for %s" % item.protId, LOGFILE)
        cdr.logwrite("protDocId: %d" % item.protDocId, LOGFILE)
        siteName = cgi.escape(item.siteName)
        cdr.logwrite("protVer: %d" % item.protVer, LOGFILE)
        cdr.logwrite("siteId: %s" % item.siteId, LOGFILE)
        cdr.logwrite("siteName: %s" % siteName, LOGFILE)
        html += """\
   <tr>
    <td>CDR%010d</td>
    <td align='right'>%d</td>
    <td>%s</td>
    <td>%s</td>
    <td>%s</td>
   </tr>
""" % (item.protDocId, item.protVer, item.protId, item.siteId, siteName)
    html += """\
  </table>
"""
    cdr.logwrite("writing errors section", LOGFILE)
    if errors:
        html += """\
  <h1>Problems</h1>
  <table border='1' cellpadding='2' cellspacing='0'>
   <tr>
    <th nowrap='1'>Protocol ID</th>
    <th nowrap='1'>Protocol Version</th>
    <th nowrap='1'>Problem Description</th>
   </tr>
"""
        for error in errors:
            html += """\
   <tr>
    <td valign='top'>CDR%010d</td>
    <td valign='top' align='right'>%d</td>
    <td>%s</td>
   </tr>
""" % (error.id, error.ver, cgi.escape(error.errors))
        html += """\
  </table>
"""
    cdr.logwrite("done writing errors section", LOGFILE)
    html += """\
 </body>
</html>
"""
    reportName = "/ProtSitesWithoutPhones-Job%d.html" % job.getJobId()
    cdr.logwrite("reportName: %s" % reportName, LOGFILE)
    ### url = "%s/CdrReports%s" % (cdr.getHostName()[2], reportName)
    url = "%s/CdrReports%s" % (cdr.CBIIT_NAMES[2], reportName)
    cdr.logwrite("url: %s" % url, LOGFILE)
    msg += "<br>Report available at <a href='%s'><u>%s</u></a>." % (url,
                                                                    url)
    htmlFile = open(REPORTS_BASE + reportName, "w")
    cdr.logwrite("writing %s" % (REPORTS_BASE + reportName), LOGFILE)
    htmlFile.write(cdrcgi.unicodeToLatin1(html))
    htmlFile.close()
    body = """\
The report you requested on Protocol Sites Without Phones can be viewed at
%s.
""" % url

    if cdr.h.org == 'OCE':
        subject   = "%s: Report results" % cdr.PUB_NAME.capitalize()
    else:
        subject   = "%s-%s: Report results" % (cdr.h.org, cdr.h.tier)

    sendMail(job, subject, body)
    job.setProgressMsg(msg)
    job.setStatus(cdrbatch.ST_COMPLETED)
    cdr.logwrite("Completed report", LOGFILE)

class ProtocolStatus:
    "Protocol status for a given range of dates."
    def __init__(self, name, startDate, endDate = None):
        self.name      = name
        self.startDate = startDate
        self.endDate   = endDate

class LeadOrg:
    "Lead Organization for a protocol, with all its status history."
    def __init__(self, node):
        self.statuses = []
        for child in node.childNodes:
            if child.nodeName == "LeadOrgProtocolStatuses":
                for grandchild in child.childNodes:
                    if grandchild.nodeName in ("PreviousOrgStatus",
                                               "CurrentOrgStatus"):
                        name = ""
                        date = ""
                        for greatgrandchild in grandchild.childNodes:
                            if greatgrandchild.nodeName == "StatusDate":
                                date = cdr.getTextContent(greatgrandchild)
                            elif greatgrandchild.nodeName == "StatusName":
                                name = cdr.getTextContent(greatgrandchild)
                        if name and date:
                            self.statuses.append(ProtocolStatus(name, date))
        self.statuses.sort(lambda a, b: cmp(a.startDate, b.startDate))
        for i in range(len(self.statuses)):
            if i == len(self.statuses) - 1:
                self.statuses[i].endDate = time.strftime("%Y-%m-%d")
            else:
                self.statuses[i].endDate = self.statuses[i + 1].startDate

class Objectives:
    elementTypes = {}
    def __init__(self, node):
        self.node = node
    def toHtml(self):
        f = """\
<xsl:transform           xmlns:xsl = "http://www.w3.org/1999/XSL/Transform"
                           version = "1.0"
                         xmlns:cdr = "cips.nci.nih.gov/cdr">
 <xsl:output                method = "html"/>
 <xsl:include  href = "cdr:name:Module: Inline Markup Formatter"/>
</xsl:transform>
"""
        t = "<Objectives xmlns:cdr='cips.nci.nih.gov/cdr'"
        s = self.node.toxml('utf-8').replace("<Objectives",  t)
        r = cdr.filterDoc('guest', f, doc = s, inline = True)
        cdr.logwrite("Objectives:\n%s" % s, LOGFILE)
        if type(r) in (unicode, str):
            return u"<span style='color:red'>Filter failure: %s</span>" % r
        cdr.logwrite("HTML:\n%s" % r[0], LOGFILE)
        return unicode(r[0], 'utf-8')
        #html = []
        #for child in self.node.childNodes:
        #    self.__extractHtml(child, html)
##     def __extractHtml(self, node, html):
##         if node.nodeType == node.ELEMENT_NODE:
##             name = node.nodeName
##             if name in self.elementTypes:
##                 self.elementTypes[name] += 1
##             else:
##                 self.elementTypes[name] = 1
##             if name == 'Para':
##                 html.append(cgi.escape(cdr.getTextContent(node, True)))
##             elif name == 'ItemizedList':
##                 html.append(
##             for child in node.childNodes:
##                 self.__extractHtml(child, html)



class Protocol:
    "Protocol information used for OPS-like reports."

    def __init__(self, id, node, getObjectives = False):
        "Create a protocol object from the XML document."
        self.id         = id
        self.leadOrgs   = []
        self.statuses   = []
        self.status     = ""
        self.firstId    = ""
        self.otherIds   = []
        self.firstPub   = ""
        self.closed     = ""
        self.completed  = ""
        self.types      = []
        self.ageRange   = ""
        self.sponsors   = []
        self.title      = ""
        self.origTitle  = ""
        self.phases     = []
        self.objectives = None
        self.hasOutcome = False
        profTitle       = ""
        patientTitle    = ""
        originalTitle   = ""
        for child in node.childNodes:
            if child.nodeName == "ProtocolSponsors":
                for grandchild in child.childNodes:
                    if grandchild.nodeName == "SponsorName":
                        value = cdr.getTextContent(grandchild)
                        if value:
                            self.sponsors.append(value)
            elif child.nodeName == "ProtocolIDs":
                for grandchild in child.childNodes:
                    if grandchild.nodeName == "PrimaryID":
                        for greatgrandchild in grandchild.childNodes:
                            if greatgrandchild.nodeName == "IDString":
                                value = cdr.getTextContent(greatgrandchild)
                                self.firstId = value
                    if grandchild.nodeName == "OtherID":
                        for greatgrandchild in grandchild.childNodes:
                            if greatgrandchild.nodeName == "IDString":
                                value = cdr.getTextContent(greatgrandchild)
                                if value:
                                    self.otherIds.append(value)
            elif child.nodeName == "Eligibility":
                for grandchild in child.childNodes:
                    if grandchild.nodeName == "AgeText":
                        value = cdr.getTextContent(grandchild)
                        if value:
                            self.ageRange = value
            elif child.nodeName == "ProtocolTitle":
                titleType = child.getAttribute("Type")
                value     = cdr.getTextContent(child)
                if value:
                    if titleType == "Professional":
                        profTitle = value
                    elif titleType == "Patient":
                        patientTitle = value
                    elif titleType == "Original":
                        originalTitle = self.origTitle = value
            elif child.nodeName == "ProtocolAdminInfo":
                for grandchild in child.childNodes:
                    if grandchild.nodeName == "ProtocolLeadOrg":
                        self.leadOrgs.append(LeadOrg(grandchild))
                    elif grandchild.nodeName == "CurrentProtocolStatus":
                        value = cdr.getTextContent(grandchild)
                        if value:
                            self.status = value
            elif child.nodeName == "ProtocolDetail":
                for catName in child.getElementsByTagName(
                                                     "StudyCategoryName"):
                    value = cdr.getTextContent(catName)
                    if value:
                        self.types.append(value)
            elif child.nodeName == 'ProtocolPhase':
                self.phases.append(cdr.getTextContent(child))
            elif getObjectives and child.nodeName == "ProtocolAbstract":
                for grandchild in child.childNodes:
                    if grandchild.nodeName == "Professional":
                        for o in grandchild.childNodes:
                            if o.nodeName == "Objectives":
                                self.objectives = Objectives(o)
                            elif o.nodeName == "Outcome":
                                self.hasOutcome = True
        if profTitle:
            self.title = profTitle
        elif originalTitle:
            self.title = originalTitle
        elif patientTitle:
            self.title = patientTitle
        orgStatuses = []
        statuses    = {}
        i           = 0
        for leadOrg in self.leadOrgs:
            orgStatuses.append("")
            for orgStatus in leadOrg.statuses:
                startDate = orgStatus.startDate
                val = (i, orgStatus.name)
                statuses.setdefault(startDate, []).append(val)
            i += 1
        keys = statuses.keys()
        keys.sort()
        for startDate in keys:
            for i, orgStatus in statuses[startDate]:
                orgStatuses[i] = orgStatus
            protStatus = self.getProtStatus(orgStatuses)
            if protStatus == "Active" and not self.firstPub:
                self.firstPub = startDate
            if protStatus in ("Active", "Approved-not yet active",
                              "Temporarily closed"):
                self.closed = ""
            elif not self.closed:
                self.closed = startDate
            if protStatus == 'Completed':
                self.completed = startDate
            else:
                self.completed = ""
            if self.statuses:
                self.statuses[-1].endDate = startDate
            self.statuses.append(ProtocolStatus(protStatus, startDate))
        if self.statuses:
            self.statuses[-1].endDate = time.strftime("%Y-%m-%d")

    def getProtStatus(self, orgStatuses):
        "Look up the protocol status based on the status of the lead orgs."
        statusSet = {}
        for orgStatus in orgStatuses:
            key = orgStatus.upper()
            statusSet[key] = 1 + statusSet.get(key, 0)
        if len(statusSet) == 1:
            return orgStatuses[0]
        for status in ("Active",
                       "Temporarily closed",
                       "Completed",
                       "Closed",
                       "Approved-not yet active"):
            if status.upper() in statusSet:
                return status
        return ""

    def wasActive(self, start, end):
        "Was this protocol active at any time during the indicated range?"
        for status in self.statuses:
            if status.endDate > start:
                if status.startDate <= end:
                    if status.name.upper() in ("ACTIVE",
                                               #"APPROVED-NOT YET ACTIVE"
                                               ):
                        return 1
        return 0

#----------------------------------------------------------------------
# Create a report for compliance with ICMJE requirements.
#----------------------------------------------------------------------
def outcomeMeasuresCodingReport(job):

    #------------------------------------------------------------------
    # Object to hold just enough information to be able to sort and
    # count before assembling the report.
    #------------------------------------------------------------------
    class ProtocolRow:
        def __init__(self, p):
            sp        = u"&nbsp;"
            phases    = p.phases[:]
            phases.sort()
            phase     = u", ".join(phases)
            title     = p.title
            stat      = p.statuses and p.statuses[-1] or None
            status    = stat and stat.name or sp
            statDate  = stat and stat.startDate or sp
            obj       = p.objectives and p.objectives.toHtml() or sp
            self.key  = (phase, p.id)
            self.html = u"""\
  <!-- key: %s -->
  <tr>
   <td class='c'>%d</td>
   <td class='c'>%s</td>
   <td class='c'>%s</td>
   <td class='c'>%s</td>
   <td class='c'>%s</td>
  </tr>
  <tr>
   <td colspan='5' class='o'>%s</td>
  </tr>""" % (self.key, p.id, phase, title, status, statDate, obj)
        def __cmp__(self, other):
            return cmp(self.key, other.key)

    idsAndTitleName = job.getParm('ids-and-title')
    idsAndTitle     = idsAndTitleName and file(idsAndTitleName, 'wb') or None
    onlyMissing     = job.getParm('only-missing') == 'Y'
    start           = time.time()
    try:
        conn = cdrdb.connect('CdrGuest')
        cursor = conn.cursor()
        cursor.execute("""\
            SELECT v.id, MAX(v.num)
              FROM doc_version v
              JOIN doc_type t
                ON t.id = v.doc_type
             WHERE t.name = 'InScopeProtocol'
               AND v.val_status = 'V'
          GROUP BY v.id
          ORDER BY v.id""")
        rows = cursor.fetchall()
    except:
        raise
        job.fail("Database failure getting list of protocols.",
                 logfile = LOGFILE)

    #------------------------------------------------------------------
    # Process all candidate protocols.
    #------------------------------------------------------------------
    done      = 0
    startDate = "2005-07-01"
    endDate   = "3000-01-01"
    protocols = []
    for row in rows:
        cursor.execute("""\
            SELECT xml
              FROM doc_version
             WHERE id = ?
               AND num = ?""", (row[0], row[1]))
        docXml = cursor.fetchone()[0]
        dom = xml.dom.minidom.parseString(docXml.encode('utf-8'))
        prot = Protocol(row[0], dom.documentElement, True)
        if prot.wasActive(startDate, endDate):

            # Added for request #2513.
            if not (onlyMissing and prot.hasOutcome):
                protocols.append(ProtocolRow(prot))

        if idsAndTitle:
            line = u"%d\t%s\t%s" % (prot.id, prot.firstId, prot.origTitle)
            line = line.replace(u"\n", u" ").replace(u"\r", u"") + u"\r\n"
            idsAndTitle.write(line.encode('utf-8'))
        done += 1
        #if done > 1000:
        #    break
        now = time.time()
        timer = getElapsed(start, now)
        msg = "Processed %d of %d protocols; elapsed: %s" % (done,
                                                             len(rows),
                                                             timer)
        job.setProgressMsg(msg)
        cdr.logwrite(msg, LOGFILE)

    if idsAndTitle:
        idsAndTitle.close()

    #------------------------------------------------------------------
    # Put together the report, sorting by phase then by CDR ID.
    #------------------------------------------------------------------
    protocols.sort()
    report = [u"""\
<html>
 <head>
  <meta http-equiv='Content-Type' content='text/html; charset=utf-8' />
  <title>Outcome Measures Coding Report</title>
  <style type='text/css'>
   h1               { font-size: 16pt; }
   h2               { font-size: 14pt; }
   .c               { color: blue; }
   .o               { color: maroon; }
    body            { font-family: sans-serif; }
    b.native        { color: #007000 }
    p.native        { color: #007000; margin-top: 0 }
    p               { font-size: 12pt; }
    dt              { font-size: 12pt; }
    dd              { font-size: 12pt; }

    ul.lnone        { list-style: none; }
    ul.disc         { list-style: disc; }
    ul.square       { list-style: square; }
    ol.little-alpha { list-style: lower-alpha; }
    ol.A            { list-style: upper-alpha; }
    ol.little-roman { list-style: lower-roman; }
    ol.I            { list-style: upper-roman; }
    ol.d            { list-style: decimal; }
    ol.none         { list-style: none; }  /* Default if no attr specified */
    li.org          { vertical-align: top; }


    ol ol { marginx:0.em; }        /* No space before and after second level */
    ol ul { marginx:0.em; }        /* list                                   */
    ul ol { marginx:0.em; }        /* This white space must be suppressed in */
    ul ul { marginx:0.em; }        /* order to handle the Compact = No       */
    ul    { margin-top:0.em; }
    ol    { margin-top:0.em; }

    p.listtitletop { font-style:       italic;  /* Display the first level  */
                     font-weight:      bold;    /* list title               */
                     margin-top:       0.em;
                     margin-bottom:    0.em; }
    p.listtitle    { font-style:       italic;  /* Display para element     */
                     font-weight:      bold;    /* as a list title          */
                     margin-top:       0.em;
                     margin-bottom:    0.em; }
    p.itemtitle    { font-weight:      bold;    /* Display para element     */
                     margin-top:       0.em;    /* as a ListItemTitle       */
                     margin-bottom:    0.em; }
    p.nospace      { margin-top:       0.em;    /* Display a para element   */
                     margin-bottom:    0.em; }  /* without blank lines      */
    li.addspace    { margin-bottom:    1.3em; } /* Add space after listitem */
                                                /* if attribute compact = No*/
    caption        { font-weight:      bold;    /* Display caption left     */
                     text-align:       left; }  /* aligned and bold         */
    ul.term         {margin-left: 16 ; padding-left: 0;}
  </style>
 </head>
 <body>
 <h1>Outcome Measures Coding Report</h1>
 <h2>(Total: %d trials)</h2>
 <table border='1' cellpadding='2' cellspacing='0'>
  <tr>
   <th>CDRID</th>
   <th>Phase</th>
   <th>Health Professional Title</th>
   <th>Current Status</th>
   <th>Status Date</th>
  </tr>""" % len(protocols)]
    for p in protocols:
        report.append(p.html)

    # Save the report.
    name = "/OutcomeMeasuresCodingReport-Job%d.html" % job.getJobId()
    fullname = REPORTS_BASE + name
    #fullname = name
    fobj = file(fullname, "w")
    report.append(u"""\
  </table>
 </body>
</html>
""")
    fobj.write(u"\n".join(report).encode('utf-8'))
    fobj.close()
    cdr.logwrite("saving %s" % fullname, LOGFILE)
    ### url = "http://%s/CdrReports%s" % (cdrcgi.WEBSERVER, name)
    url = "%s/CdrReports%s" % (cdr.CBIIT_NAMES[2], name)
    cdr.logwrite("url: %s" % url, LOGFILE)
    msg += "<br>Report available at <a href='%s'><u>%s</u></a>." % (url, url)

    # Tell the user where to find it.
    body = """\
The Outcome Measures Coding report you requested can be viewed at
%s.
""" % url
    if cdr.h.org == 'OCE':
        subject   = "%s: Report results - Outcome Measures" % \
                                                cdr.PUB_NAME.capitalize()
    else:
        subject   = "%s-%s: Report results - Outcome Measures" % (
                                                   cdr.h.org, cdr.h.tier)

    sendMail(job, subject, body)
    job.setProgressMsg(msg)
    job.setStatus(cdrbatch.ST_COMPLETED)
    cdr.logwrite("Completed report", LOGFILE)

def NCITTermUpdate(job):
    doDBUpdate  = job.getParm('doDBUpdate') or '0'
    excelNoDefFile  = job.getParm('excelFile') or ''
    drug  = job.getParm('drug') or '1'
    session  = job.getParm('session') or ''

    doDBUpdate = int(doDBUpdate)
    drug = int(drug)

    name = time.strftime('/ImportResults-%Y%m%d%H%M%S.xls')
    fullname = REPORTS_BASE + name
    excelOutputFile = fullname

    NCIThes.updateAllTerms(job=job,session=session,excelNoDefFile=excelNoDefFile,
                           excelOutputFile=excelOutputFile,doUpdate=doDBUpdate,
                           drugTerms=drug)

    cdr.logwrite("saving %s" % excelOutputFile, LOGFILE)
    ### url = "http://%s%s/GetReportWorkbook.py?name=%s" % (cdrcgi.WEBSERVER,
    ###                                                     cdrcgi.BASE, name)
    url = "%s%s/GetReportWorkbook.py?name=%s" % (cdr.CBIIT_NAMES[2],
                                                        cdrcgi.BASE, name)
    cdr.logwrite("url: %s" % url, LOGFILE)
    msg = "<br>Report available at <a href='%s'><u>%s</u></a>." % (url, url)

    # Tell the user where to find it.
    body = """\
The NCIT Term Update report is available at
%s.
""" % url
    if cdr.h.org == 'OCE':
        subject   = "%s: Report results - NCIT Term Update" % \
                                                   cdr.PUB_NAME.capitalize()
    else:
        subject   = "%s-%s: Report results - NCIT Term Update" % (
                                                   cdr.h.org, cdr.h.tier)

    sendMail(job, subject, body)
    job.setProgressMsg(msg)
    job.setStatus(cdrbatch.ST_COMPLETED)
    cdr.logwrite("Completed report", LOGFILE)

#----------------------------------------------------------------------
# Generate a spreadsheet on selected protocols for the Office of
# Science Policy.
#----------------------------------------------------------------------
def ospReport(job):

    #----------------------------------------------------------------------
    # Start processing here for the OSP report.
    #----------------------------------------------------------------------
    startTime  = time.time()
    firstYear  = job.getParm('first-year') or '1999'
    lastYear   = job.getParm('last-year')  or '2004'
    yearType   = job.getParm('year-type')  or 'calendar'
    phases     = job.getParm('phases')     or ''
    termIds    = job.getParm('term-ids')   or ''
    termIds    = [int(termId) for termId in termIds.split(';')]
    phaseJoin  = ''
    phaseWhere = ''
    if phases:
        phases = phases.split(u';')
        titlePhases = u", ".join(phases)
        sqlPhases   = ", ".join(["'%s'" % p for p in phases])
        phaseJoin   = 'JOIN query_term p ON p.doc_id = t.doc_id'
        phaseWhere  = "AND p.path = '/InScopeProtocol/ProtocolPhase'"
        phaseWhere  = '%s AND p.value IN (%s)' % (phaseWhere, sqlPhases)
    try:
        conn = cdrdb.connect('CdrGuest')
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE #terms(id INTEGER)")
        conn.commit()
        termNames = []
        for termId in termIds:
            cursor.execute("""\
                SELECT value
                  FROM query_term
                 WHERE path = '/Term/PreferredName'
                   AND doc_id = ?""", termId)
            termNames.append(cursor.fetchall()[0][0])
            try:
                cursor.execute("INSERT INTO #terms VALUES(%d)" % termId)
                conn.commit()
            except Exception, e:
                cdr.logwrite("Failure inserting %d into #terms: %s" % (termId,
                                                                       str(e)),
                             LOGFILE)
        if yearType.lower() == 'calendar':
            yearRange = u"%s-%s" % (firstYear, lastYear)
        else:
            yearRange = u"FY%02d-FY%02d" % (int(firstYear) % 100,
                                            int(lastYear) % 100)
        title = (u"PDQ Clinical Trials for %s for %s" %
                 (u", ".join(termNames), yearRange))
        if phases:
            title = u"%s %s" % (titlePhases, title)
        #print title
        while 1:
            cursor.execute("""\
        INSERT INTO #terms
             SELECT q.doc_id
               FROM query_term q
               JOIN #terms t
                 ON t.id = q.int_val
              WHERE q.path = '/Term/TermRelationShip/ParentTerm' +
                             '/TermId/@cdr:ref'
                AND q.doc_id NOT IN (SELECT id FROM #terms)""", timeout = 360)
            if not cursor.rowcount:
                break
            conn.commit()

        cursor.execute("""\
                 SELECT DISTINCT t.doc_id, MAX(v.num)
                   FROM query_term t
                   JOIN query_term s
                     ON s.doc_id = t.doc_id
                   JOIN document d
                     ON d.id = s.doc_id
                   JOIN doc_version v
                     ON v.id = d.id
                   %s
                  WHERE t.path in ('/InScopeProtocol/ProtocolDetail' +
                                   '/Condition/@cdr:ref',
                                   '/InScopeProtocol/Eligibility' +
                                   '/Diagnosis/@cdr:ref')
                    AND t.int_val IN (SELECT id FROM #terms)
                    AND s.path = '/InScopeProtocol/ProtocolSponsors' +
                                 '/SponsorName'
                    AND s.value = 'NCI'
                    AND d.active_status = 'A'
                    AND v.publishable = 'Y'
                    %s
               GROUP BY t.doc_id
               ORDER BY t.doc_id""" % (phaseJoin, phaseWhere), timeout = 360)
        rows = cursor.fetchall()
    except Exception, e:
        job.fail("Database failure getting list of protocols: %s" % e,
                 logfile = LOGFILE)

    # Create the spreadsheet.
    wb = ExcelWriter.Workbook()
    b = ExcelWriter.Border()
    borders = ExcelWriter.Borders(b, b, b, b)
    font = ExcelWriter.Font(name = 'Times New Roman', size = 10)
    align = ExcelWriter.Alignment('Left', 'Top', wrap = True)
    style1 = wb.addStyle(alignment = align, font = font, borders = borders)
    urlFont = ExcelWriter.Font('blue', None, 'Times New Roman', size = 10)
    style4 = wb.addStyle(alignment = align, font = urlFont, borders = borders)
    ws = wb.addWorksheet("PDQ Clinical Trials", style1, 40, 1)
    style2 = wb.addStyle(alignment = align, font = font, borders = borders,
                         numFormat = 'YYYY-mm-dd')

    # Set the column widths to match the sample provided by OSP.
    ws.addCol( 1, 232.5)
    ws.addCol( 2, 100)
    ws.addCol( 3, 127.5)
    ws.addCol( 4, 104.25)
    ws.addCol( 5, 104.25)
    ws.addCol( 6, 104.25)
    ws.addCol( 7, 91.5)
    ws.addCol( 8, 84.75)
    ws.addCol( 9, 85.5)
    ws.addCol(10, 123)

    # Set up the title and header cells in the spreadsheet's top rows.
    font = ExcelWriter.Font(name = 'Times New Roman', bold = True, size = 10)
    align = ExcelWriter.Alignment('Center', 'Center', wrap = True)
    interior = ExcelWriter.Interior('#CCFFCC')
    style3 = wb.addStyle(alignment = align, font = font, borders = borders,
                         interior = interior)
    headings = (
        'PDQ Clinical Trials',
        'Primary ID',
        'Additional IDs',
        'Date First Activated',
        'Date Moved to Closed List',
        'Date Completed',
        'Current Status',
        'Type of Trial',
        'Age Range',
        'Sponsor of Trial'
        )
    row = ws.addRow(1, style3, 40)
    row.addCell(1, title, mergeAcross = len(headings) - 1)
    row = ws.addRow(2, style3, 40)
    for i in range(len(headings)):
        row.addCell(i + 1, headings[i])

    #------------------------------------------------------------------
    # Process all candidate protocols.
    #------------------------------------------------------------------
    done = 0
    protocols = []
    msg = ""
    if yearType == 'fiscal':
        firstYear = int(firstYear) - 1
        startDate = "%s-10-01" % firstYear
        endDate = "%s-09-30" % lastYear
    else:
        startDate = "%s-01-01" % firstYear
        endDate   = "%s-12-31" % lastYear
    for row in rows:
        cursor.execute("""\
            SELECT xml
              FROM doc_version
             WHERE id = ?
               AND num = ?""", row)
        docXml = cursor.fetchone()[0]
        dom = xml.dom.minidom.parseString(docXml.encode('utf-8'))
        prot = Protocol(row[0], dom.documentElement)
        if prot.wasActive(startDate, endDate):
            protocols.append(prot)
        done += 1
        now = time.time()
        timer = getElapsed(startTime, now)
        msg = "Processed %d of %d protocols; elapsed: %s" % (done,
                                                             len(rows),
                                                             timer)
        job.setProgressMsg(msg)
        cdr.logwrite(msg, LOGFILE)

    # Add one row for each protocol.
    rowNum = 2
    protocols.sort(lambda a,b: cmp(a.firstPub, b.firstPub))
    for prot in protocols:

        # Change requested by Lakshmi 2005-02-25 (request #1567).
        if prot.status in ('Closed', 'Completed'):
            closedDate = prot.closed
            if prot.status == 'Completed':
                completedDate = prot.completed
            else:
                completedDate = ''
        else:
            closedDate = ''
            completedDate = ''

        rowNum += 1
        row = ws.addRow(rowNum, style1, 40)
        tip = ("Left-click cell with mouse to view the protocol document.  "
               "Left-click and hold to select the cell.")
        url = ("http://www.cancer.gov/clinicaltrials/"
               "view_clinicaltrials.aspx?version=healthprofessional&"
               "cdrid=%d" % prot.id)
        row.addCell(1, prot.title)
        row.addCell(2, prot.firstId, href = url, tooltip = tip, style = style4)
        row.addCell(3, "; ".join(prot.otherIds))
        row.addCell(4, prot.firstPub, style = style2)
        row.addCell(5, closedDate, style = style2)
        row.addCell(6, completedDate, style = style2)
        row.addCell(7, prot.status)
        row.addCell(8, "; ". join(prot.types))
        row.addCell(9, prot.ageRange)
        row.addCell(10, "; ".join(prot.sponsors))
        row = cursor.fetchone()

    # Save the report.
    name = "/OSPReport-Job%d.xls" % job.getJobId()
    fullname = REPORTS_BASE + name
    fobj = file(fullname, "wb")
    wb.write(fobj, True)
    fobj.close()
    cdr.logwrite("saving %s" % fullname, LOGFILE)
    ### url = "http://%s%s/GetReportWorkbook.py?name=%s" % (cdrcgi.WEBSERVER,
    ###                                                     cdrcgi.BASE, name)
    url = "%s%s/GetReportWorkbook.py?name=%s" % (cdr.CBIIT_NAMES[2],
                                                        cdrcgi.BASE, name)
    cdr.logwrite("url: %s" % url, LOGFILE)
    msg += "<br>Report available at <a href='%s'><u>%s</u></a>." % (url, url)

    # Tell the user where to find it.
    body = """\
The OSP report you requested on Protocols can be viewed at
%s.
""" % url
    if cdr.h.org == 'OCE':
        subject   = "%s: Report results - OSP Report" % \
                                                    cdr.PUB_NAME.capitalize()
    else:
        subject   = "%s-%s: Report results - OSP Report" % (
                                                       cdr.h.org, cdr.h.tier)

    sendMail(job, subject, body)
    job.setProgressMsg(msg)
    job.setStatus(cdrbatch.ST_COMPLETED)
    cdr.logwrite("Completed report", LOGFILE)

class NonRespondentsReport:
    def __init__(self, age, docType, host = 'localhost'):
        self.age     = age
        self.docType = docType

        #--------------------------------------------------------------
        # Set up a database connection and cursor.
        #--------------------------------------------------------------
        # Setting up the propper database source
        # --------------------------------------
        import cdrutil
        h = cdrutil.AppHost(cdrutil.getEnvironment(), cdrutil.getTier(),
                            filename = cdr.WORK_DRIVE + ':/etc/cdrapphosts.rc')
        if h.org == 'OCE':
            host = 'localhost'
        else:
            host = h.host['DBWIN'][0]

        self.conn = cdrdb.connect('CdrGuest', dataSource = host)
        self.cursor = self.conn.cursor()

    def createSpreadsheet(self, job):

        now = time.localtime()
        startDate = list(now)
        endDate   = list(now)
        if self.age == "15":
            startDate[2] -= 29
            endDate[2]   -= 15
            ageString     = '15-29 days since last mailer'
        elif self.age == "30":
            startDate[2] -= 59
            endDate[2]   -= 30
            ageString     = '30-59 days since last mailer'
        else:
            startDate[2] -= 120
            endDate[2]   -= 60
            ageString     = '60-120 days since last mailer'
        # startDate[2] -= 200 # for testing
        regexp    = re.compile('since last mailer')
        ageText   = regexp.sub('prior', ageString)
        startDate = time.mktime(startDate)
        endDate   = time.mktime(endDate)
        startDate = time.strftime("%Y-%m-%d", time.localtime(startDate))
        endDate   = time.strftime("%Y-%m-%d 23:59:59.999",
                                  time.localtime(endDate))
        job.setProgressMsg("Job started")
        try:

            # Get the last mailer for each doc of this type.
            self.cursor.execute("""\
                CREATE TABLE #last_mailers
                     (doc_id INTEGER,
                   mailer_id INTEGER)""")
            self.conn.commit()
            job.setProgressMsg("#last_mailers table created")
            self.cursor.execute("""\
        INSERT INTO #last_mailers
             SELECT q.int_val, MAX(q.doc_id)
               FROM query_term q
               JOIN document d
                 ON d.id = q.int_val
               JOIN doc_type t
                 ON t.id = d.doc_type
               JOIN query_term mailer_type
                 ON mailer_type.doc_id = q.doc_id
              WHERE t.name = ?
                AND q.path = '/Mailer/Document/@cdr:ref'
                AND mailer_type.path = '/Mailer/Type'
                AND mailer_type.value NOT LIKE
                               'Protocol-%status/participant check'
           GROUP BY q.int_val""", self.docType, timeout = 300)

            self.conn.commit()
            job.setProgressMsg("#last_mailers table populated")

            # Find out which ones had their last mailer in the date range.
            self.cursor.execute("""\
               CREATE TABLE #in_scope (doc_id INTEGER, mailer_id INTEGER)""")
            self.conn.commit()
            job.setProgressMsg("#in_scope table created")
            self.cursor.execute("""\
        INSERT INTO #in_scope
             SELECT lm.doc_id, lm.mailer_id
               FROM #last_mailers lm
               JOIN query_term q
                 ON q.doc_id = lm.mailer_id
              WHERE q.path = '/Mailer/Sent'
                AND q.value BETWEEN ? AND ?""",
                                (startDate, endDate), timeout = 300)
            self.conn.commit()
            job.setProgressMsg("#in_scope table populated")

            # Which of these haven't had a reply yet?
            self.cursor.execute("""\
                CREATE TABLE #no_reply (doc_id INTEGER, mailer_id INTEGER)""")
            self.conn.commit()
            job.setProgressMsg("#no_reply table created")
            self.cursor.execute("""\
        INSERT INTO #no_reply
             SELECT i.doc_id, i.mailer_id
               FROM #in_scope i
              WHERE NOT EXISTS(SELECT *
                                 FROM query_term q
                                WHERE q.doc_id = i.mailer_id
                                  AND q.path = '/Mailer/Response/Received')""",
                           timeout = 300)
            self.conn.commit()
            job.setProgressMsg("#no_reply table populated")
            self.cursor.execute("""\
                 SELECT recip_name.title,
                        #no_reply.doc_id,
                        base_doc.doc_id,
                        mailer_type.value,
                        response_received.value,
                        changes_category.value
                   FROM document recip_name
                   JOIN query_term recipient
                     ON recipient.int_val = recip_name.id
                   JOIN #no_reply
                     ON #no_reply.mailer_id = recipient.doc_id
                   JOIN query_term base_doc
                     ON base_doc.int_val = #no_reply.doc_id
                   JOIN query_term mailer_type
                     ON mailer_type.doc_id = base_doc.doc_id
        LEFT OUTER JOIN query_term response_received
                     ON response_received.doc_id = base_doc.doc_id
                    AND response_received.path = '/Mailer/Response/Received'
        LEFT OUTER JOIN query_term changes_category
                     ON changes_category.doc_id = base_doc.doc_id
                    AND changes_category.path = '/Mailer/Response'
                                              + '/ChangesCategory'
                  WHERE recipient.path = '/Mailer/Recipient/@cdr:ref'
                    AND mailer_type.path = '/Mailer/Type'
                    AND base_doc.path = '/Mailer/Document/@cdr:ref'
                    AND mailer_type.value NOT LIKE
                                   'Protocol-%status/participant check'
               ORDER BY recip_name.title,
                        #no_reply.doc_id,
                        base_doc.doc_id DESC""",
            timeout = 300)
            job.setProgressMsg("report rows selected")
            rows = self.cursor.fetchall()
            job.setProgressMsg("%d report rows fetched" % len(rows))
        except Exception, info:
            job.fail("Database failure fetching report information: %s" %
                     str(info), logfile = LOGFILE)
        if self.docType == "InScopeProtocol":
            self.docType = "Protocol Summary"

        fn = 'Times New Roman'
        ff = 'Roman'
        wb = ExcelWriter.Workbook()
        b1 = ExcelWriter.Borders(top = ExcelWriter.Border())
        f1 = ExcelWriter.Font(name = fn, family = ff, size = 14, bold = True)
        f2 = ExcelWriter.Font(name = fn, family = ff, size = 12, bold = True)
        f3 = ExcelWriter.Font(name = fn, family = ff, size = 11)
        i1 = ExcelWriter.Interior('#CCFFCC')
        a1 = ExcelWriter.Alignment('Center', 'Center')
        a2 = ExcelWriter.Alignment('Left', 'Top', True)
        s1 = wb.addStyle(font = f1, alignment = a1, interior = i1)
        s2 = wb.addStyle(font = f2, alignment = a1, interior = i1)
        s3 = wb.addStyle(font = f2, interior = i1)
        s4 = wb.addStyle(font = f3, alignment = a2, borders = b1)
        s5 = wb.addStyle(font = f3, alignment = a2)
        ws = wb.addWorksheet("Mailer Non-Respondents", frozenRows = 5)
        r1 = ws.addRow(1, height = 40)
        r2 = ws.addRow(2, height = 16)
        r3 = ws.addRow(3, height = 16)
        r4 = ws.addRow(4, height = 16)
        r5 = ws.addRow(5, height = 20)
        t1 = "Mailer Non-Respondents Report"
        t2 = "For period of %s to %s" % (ageText, time.strftime("%B %d, %Y"))
        t3 = "Mailer Type: %s" % self.docType
        t4 = "Non-Response Time: %s" % ageString
        r1.addCell(1, t1, mergeAcross = 4, style = s1)
        r2.addCell(1, t2, mergeAcross = 4, style = s2)
        r3.addCell(1, t3, mergeAcross = 4, style = s2)
        r4.addCell(1, t4, mergeAcross = 4, style = s2)
        hdrs = ("Recipient Name", "DocId", "Mailer", "Mailer Type", "Response")
        widths = (200, 50, 50, 150, 75)
        headerRows = len(hdrs)
        for i in range(headerRows):
            ws.addCol(i + 1, widths[i])
            r5.addCell(i + 1, hdrs[i], style = s3)
        done = 0
        rowNum = headerRows + 1
        recipRows = 0
        lastRecipName = ""
        lastBaseDocId = None
        if not rows:
            job.fail("No data found for report", logfile = LOGFILE)
        for row in rows:
            if row[0] == lastRecipName:
                recipName = ""
                recipRows += 1
                if recipRows > 3:
                    done += 1
                    continue
                r = ws.addRow(rowNum, s5, 15)
            else:
                recipRows = 1
                recipName = lastRecipName = row[0]
                if recipName.startswith("Inactive;"):
                    recipName = recipName[len("Inactive;"):]
                semicolon = recipName.find(";")
                if semicolon != -1:
                    recipName = recipName[:semicolon]
                r = ws.addRow(rowNum, s4, 15)
            if row[1] == lastBaseDocId:
                baseDocId = ""
            else:
                baseDocId = "%d" % row[1]
                lastBaseDocId = row[1]
            responseDate  = row[4] and row[4][:10] or ""
            if row[5] == "Returned to sender":
                responseDate = "RTS"
            r.addCell(1, recipName)
            r.addCell(2, baseDocId)
            r.addCell(3, "%d" % row[2])
            r.addCell(4, row[3])
            r.addCell(5, responseDate)
            rowNum += 1
            done += 1
            msg = "%d rows of %d processed; %d rows added" % (
                done, len(rows), rowNum - headerRows)
            job.setProgressMsg(msg)

        # Make the top rows (the ones with column labels) always visible.
        #sheet.Rows.Range("%d:%d" % (headerRows + 1, headerRows + 1)).Select()
        #xl.ActiveWindow.FreezePanes = 1

        # Move to the first row of data.
        #xl.Range("A%d" % (headerRows + 1)).Select()

        # Save the report.
        name = "/MailerNonRespondentsReport-%d.xls" % job.getJobId()
        f = open(REPORTS_BASE + name, 'wb')
        wb.write(f, True)
        f.close()
        cdr.logwrite("saving %s" % (REPORTS_BASE + name), LOGFILE)
        ### url = "http://%s%s/GetReportWorkbook.py?name=%s" % (cdrcgi.WEBSERVER,
        ###                                                    cdrcgi.BASE,
        ###                                                    name)
        url = "%s%s/GetReportWorkbook.py?name=%s" % (cdr.CBIIT_NAMES[2],
                                                            cdrcgi.BASE,
                                                            name)
        cdr.logwrite("url: %s" % url, LOGFILE)
        msg += "<br>Report available at <a href='%s'><u>%s</u></a>." % (
            url, url)

        # Tell the user where to find it.
        body = """\
The %s Mailer Non-Respondents report you requested can be
viewed at %s.
""" % (self.docType, url)
        if cdr.h.org == 'OCE':
            subject   = "%s: Report results - Mailer (NR)" % \
                                                    cdr.PUB_NAME.capitalize()
        else:
            subject   = "%s-%s: Report results - Mailer (NR)" % (
                                                       cdr.h.org, cdr.h.tier)

        sendMail(job, subject, body)
        job.setProgressMsg(msg)
        job.setStatus(cdrbatch.ST_COMPLETED)
        cdr.logwrite("Completed report", LOGFILE)

#----------------------------------------------------------------------
# Run a report of mailers which haven't had responses.
#----------------------------------------------------------------------
def nonRespondentsReport(job):
    age         = job.getParm("Age")
    baseDocType = job.getParm("BaseDocType")
    host        = job.getParm("Host")
    report      = NonRespondentsReport(age, baseDocType, host)
    report.createSpreadsheet(job)

#----------------------------------------------------------------------
# Used for report on protocols associated with a given organization.
#----------------------------------------------------------------------
class OrgProtocolReview:
    def __init__(self, id, host = 'localhost'):
        self.id     = id
        # Setting up the propper database source
        # --------------------------------------
        import cdrutil
        h = cdrutil.AppHost(cdrutil.getEnvironment(), cdrutil.getTier(),
                            filename = cdr.WORK_DRIVE + ':/etc/cdrapphosts.rc')
        if h.org == 'OCE':
            host = 'localhost'
        else:
            host = h.host['DBWIN'][0]

        self.conn   = cdrdb.connect('CdrGuest', dataSource = host)
        self.cursor = self.conn.cursor()

    #------------------------------------------------------------------
    # Shorten role names (at Sheri's request 2003-07-01).
    #------------------------------------------------------------------
    def mapRole(self, role):
        if not role:
            return None
        ucRole = role.upper()
        if ucRole == "PRINCIPAL INVESTIGATOR":
            return "PI"
        elif ucRole == "STUDY COORDINATOR":
            return "SC"
        elif ucRole == "PROTOCOL CO-CHAIR":
            return "CC"
        elif ucRole == "PROTOCOL CHAIR":
            return "PC"
        elif ucRole == "UPDATE PERSON":
            return "PUP"
        elif ucRole == "RESEARCH COORDINATOR":
            return "RC"
        return role

    #------------------------------------------------------------------
    # This is the workhorse for the report.
    #------------------------------------------------------------------
    def report(self, job):

        #--------------------------------------------------------------
        # Object for a protocol person.
        #--------------------------------------------------------------
        class ProtPerson:
            def __init__(self, name):
                self.name  = name
                self.roles = []

        #--------------------------------------------------------------
        # Object type for representing a protocol link to our org document.
        #--------------------------------------------------------------
        class ProtLink:
            def __init__(self, docId, protId, loStat):
                self.docId              = docId
                self.protId             = protId
                self.orgStat            = None
                self.loStat             = loStat
                self.personnel          = {}
                self.isLeadOrg          = False
                self.isOrgSite          = False

        #--------------------------------------------------------------
        # Build the base html for the report.
        #--------------------------------------------------------------
        filters  = ['set:Denormalization Organization Set',
                    'name:Organization Protocol Review']
        job.setProgressMsg("Filtering organization document")
        cdr.logwrite("Filtering organization document", LOGFILE)
        response = cdr.filterDoc('guest', filters, self.id)
        if type(response) in (str, unicode):
            job.fail(response, logfile = LOGFILE)
        html = unicode(response[0], 'utf-8')

        #--------------------------------------------------------------
        # Get all the protocols which link to this organization.
        #--------------------------------------------------------------
        protLinks = {}
        try:

            #----------------------------------------------------------
            # Links to this org as the lead organization.
            #----------------------------------------------------------
            msg = "Gathering links to CDR%010d as lead org" % self.id
            job.setProgressMsg(msg)
            cdr.logwrite(msg, LOGFILE)
            self.cursor.execute("""\
SELECT DISTINCT prot_id.value, prot_id.doc_id, org_stat.value,
                person.title, person_role.value
           FROM query_term org_id
           JOIN query_term prot_id
             ON prot_id.doc_id = org_id.doc_id
            AND LEFT(prot_id.node_loc, 8) = LEFT(org_id.node_loc, 8)
           JOIN query_term org_stat
             ON org_stat.doc_id = org_id.doc_id
            AND LEFT(org_stat.node_loc, 8) = LEFT(org_id.node_loc, 8)
           JOIN query_term person_id
             ON person_id.doc_id = org_id.doc_id
            AND LEFT(person_id.node_loc, 8) = LEFT(org_id.node_loc, 8)
           JOIN query_term person_role
             ON person_role.doc_id = person_id.doc_id
            AND LEFT(person_role.node_loc, 12) = LEFT(person_id.node_loc, 12)
           JOIN document person
             ON person.id = person_id.int_val
          WHERE org_id.int_val   = ?
            AND org_id.path      = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/LeadOrganizationID'
                                 + '/@cdr:ref'
            AND prot_id.path     = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/LeadOrgProtocolID'
            AND org_stat.path    = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/LeadOrgProtocolStatuses'
                                 + '/CurrentOrgStatus/StatusName'
            AND person_id.path   = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/LeadOrgPersonnel'
                                 + '/Person/@cdr:ref'
            AND person_role.path = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/LeadOrgPersonnel'
                                 + '/PersonRole'""", self.id, timeout = 500)
            rows = self.cursor.fetchall()
            done = 0
            for (protId, docId, orgStat, personName, role) in rows:
                personName = personName.strip().split(u';')[0]
                key = (protId, docId, orgStat)
                if key not in protLinks:
                    protLink = ProtLink(docId, protId, orgStat)
                    protLinks[key] = protLink
                else:
                    protLink = protLinks[key]
                protLink.isLeadOrg = True
                if personName:
                    if personName not in protLink.personnel:
                        person = ProtPerson(personName)
                        protLink.personnel[personName] = person
                    else:
                        person = protLink.personnel[personName]
                    role = self.mapRole(role)
                    if role and role not in person.roles:
                        person.roles.append(role)
                done += 1
                #job.setProgressMsg(msg + (" (%d of %d rows processed)"
                #                       % (done, len(rows))))

            #----------------------------------------------------------
            # Links to this org as participating org with specific person.
            #----------------------------------------------------------
            msg = ("Gathering links to CDR%010d as "
                   "participating org with specific person"
                   % self.id)
            job.setProgressMsg(msg)
            cdr.logwrite(msg, LOGFILE)
            done = 0
            self.cursor.execute("""\
SELECT DISTINCT prot_id.value, prot_id.doc_id, org_stat.value,
                person.title, person_role.value, lo_stat.value
           FROM query_term org_id
           JOIN query_term prot_id
             ON prot_id.doc_id = org_id.doc_id
            AND LEFT(prot_id.node_loc, 8) = LEFT(org_id.node_loc, 8)
           JOIN query_term org_stat
             ON org_stat.doc_id = org_id.doc_id
            AND LEFT(org_stat.node_loc, 16) = LEFT(org_id.node_loc, 16)
           JOIN query_term person_id
             ON person_id.doc_id = org_id.doc_id
            AND LEFT(person_id.node_loc, 16) = LEFT(org_id.node_loc, 16)
           JOIN query_term person_role
             ON person_role.doc_id = person_id.doc_id
            AND LEFT(person_role.node_loc, 20) = LEFT(person_id.node_loc, 20)
           JOIN document person
             ON person.id = person_id.int_val
           JOIN query_term lo_stat
             ON lo_stat.doc_id = org_stat.doc_id
            AND LEFT(lo_stat.node_loc, 8) = LEFT(org_stat.node_loc, 8)
          WHERE org_id.int_val   = ?
            AND org_id.path      = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/ProtocolSites'
                                 + '/OrgSite/OrgSiteID/@cdr:ref'
            AND prot_id.path     = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/LeadOrgProtocolID'
            AND org_stat.path    = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/ProtocolSites'
                                 + '/OrgSite/OrgSiteStatus'
            AND person_id.path   = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/ProtocolSites'
                                 + '/OrgSite/OrgSiteContact'
                                 + '/SpecificPerson/Person/@cdr:ref'
            AND person_role.path = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/ProtocolSites'
                                 + '/OrgSite/OrgSiteContact'
                                 + '/SpecificPerson/Role'
            AND lo_stat.path     = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/LeadOrgProtocolStatuses'
                                 + '/CurrentOrgStatus/StatusName'""",
                                self.id, timeout = 500)
            rows = self.cursor.fetchall()
            for (protId, docId, orgStat, personName, role, loStat) in rows:
                personName = personName.strip().split(u';')[0]
                key = (protId, docId, loStat)
                if key not in protLinks:
                    protLinks[key] = protLink = ProtLink(docId, protId, loStat)
                else:
                    protLink = protLinks[key]
                protLink.isOrgSite = True
                protLink.orgStat = orgStat
                if personName:
                    if personName not in protLink.personnel:
                        person = ProtPerson(personName)
                        protLink.personnel[personName] = person
                    else:
                        person = protLink.personnel[personName]
                    role = self.mapRole(role)
                    if role and role not in person.roles:
                        person.roles.append(role)
                done += 1
                #job.setProgressMsg(msg + (" (%d of %d rows processed)"
                #                       % (done, len(rows))))

            #----------------------------------------------------------
            # Links to this org as participating org with generic person.
            #----------------------------------------------------------
            msg = ("Gathering links to CDR%010d as "
                   "participating org with generic person"
                   % self.id)
            job.setProgressMsg(msg)
            cdr.logwrite(msg, LOGFILE)
            done = 0
            self.cursor.execute("""\
SELECT DISTINCT prot_id.value, prot_id.doc_id, org_stat.value,
                person.value, lo_stat.value
           FROM query_term org_id
           JOIN query_term prot_id
             ON prot_id.doc_id = org_id.doc_id
            AND LEFT(prot_id.node_loc, 8) = LEFT(org_id.node_loc, 8)
           JOIN query_term org_stat
             ON org_stat.doc_id = org_id.doc_id
            AND LEFT(org_stat.node_loc, 16) = LEFT(org_id.node_loc, 16)
           JOIN query_term person
             ON person.doc_id = org_id.doc_id
            AND LEFT(person.node_loc, 16) = LEFT(org_id.node_loc, 16)
           JOIN query_term lo_stat
             ON lo_stat.doc_id = org_stat.doc_id
            AND LEFT(lo_stat.node_loc, 8) = LEFT(org_stat.node_loc, 8)
          WHERE org_id.int_val   = ?
            AND org_id.path      = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/ProtocolSites'
                                 + '/OrgSite/OrgSiteID/@cdr:ref'
            AND prot_id.path     = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/LeadOrgProtocolID'
            AND org_stat.path    = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/ProtocolSites'
                                 + '/OrgSite/OrgSiteStatus'
            AND person.path      = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/ProtocolSites'
                                 + '/OrgSite/OrgSiteContact'
                                 + '/GenericPerson/PersonTitle'
            AND lo_stat.path     = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/LeadOrgProtocolStatuses'
                                 + '/CurrentOrgStatus/StatusName'""",
                                self.id, timeout = 500)
            rows = self.cursor.fetchall()
            for (protId, docId, orgStat, personName, loStat) in rows:
                key = (protId, docId, loStat)
                if key not in protLinks:
                    protLinks[key] = protLink = ProtLink(docId, protId, loStat)
                else:
                    protLink = protLinks[key]
                protLink.isOrgSite = True
                protLink.orgStat = orgStat
                if personName and personName not in protLink.personnel:
                    protLink.personnel[personName] = ProtPerson(personName)
                done += 1
                #job.setProgressMsg(msg + (" (%d of %d rows processed)"
                #                       % (done, len(rows))))

            #---------------------------------------------------------------
            # Links to this org as participating org with Clinical Trial Off
            #---------------------------------------------------------------
            msg = ("Gathering links to CDR%010d as "
                   "participating org with Clinical Trial Office"
                   % self.id)
            job.setProgressMsg(msg)
            cdr.logwrite(msg, LOGFILE)
            done = 0
            self.cursor.execute("""\
SELECT DISTINCT prot_id.value, prot_id.doc_id, org_stat.value,
                'Clinical Trial Office', lo_stat.value
                -- , trial_office.value
           FROM query_term org_id
           JOIN query_term prot_id
             ON prot_id.doc_id = org_id.doc_id
            AND LEFT(prot_id.node_loc, 8) = LEFT(org_id.node_loc, 8)
           JOIN query_term org_stat
             ON org_stat.doc_id = org_id.doc_id
            AND LEFT(org_stat.node_loc, 16) = LEFT(org_id.node_loc, 16)
           JOIN query_term trial_office
             ON trial_office.doc_id = org_id.doc_id
            AND LEFT(trial_office.node_loc, 16) = LEFT(org_id.node_loc, 16)
           JOIN query_term lo_stat
             ON lo_stat.doc_id = org_stat.doc_id
            AND LEFT(lo_stat.node_loc, 8) = LEFT(org_stat.node_loc, 8)
          WHERE org_id.int_val   = ?
            AND org_id.path      = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/ProtocolSites'
                                 + '/OrgSite/OrgSiteID/@cdr:ref'
            AND prot_id.path     = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/LeadOrgProtocolID'
            AND org_stat.path    = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/ProtocolSites'
                                 + '/OrgSite/OrgSiteStatus'
            AND trial_office.path= '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/ProtocolSites'
                                 + '/OrgSite/OrgSiteContact'
                                 + '/ClinicalTrialOffice/@cdr:ref'
            AND lo_stat.path     = '/InScopeProtocol/ProtocolAdminInfo'
                                 + '/ProtocolLeadOrg/LeadOrgProtocolStatuses'
                                 + '/CurrentOrgStatus/StatusName'""",
                                self.id, timeout = 500)
            rows = self.cursor.fetchall()
            for (protId, docId, orgStat, personName, loStat) in rows:
                key = (protId, docId, loStat)
                if key not in protLinks:
                    protLinks[key] = protLink = ProtLink(docId, protId, loStat)
                else:
                    protLink = protLinks[key]
                protLink.isOrgSite = True
                protLink.orgStat = orgStat
                if personName and personName not in protLink.personnel:
                    protLink.personnel[personName] = ProtPerson(personName)
                done += 1
                #job.setProgressMsg(msg + (" (%d of %d rows processed)"
                #                       % (done, len(rows))))

        except cdrdb.Error, info:
            job.fail('Failure fetching protocols: %s' % info[1][0],
                     logfile = LOGFILE)

        #--------------------------------------------------------------
        # Build the table.
        #--------------------------------------------------------------
        job.setProgressMsg("Building report table")
        cdr.logwrite(msg, LOGFILE)
        table = u"""\
  <table border='1' cellpadding='2' cellspacing='0'>
   <tr>
    <th rowspan='2'>Protocol ID</th>
    <th rowspan='2'>Doc ID</th>
    <th rowspan='2'>Lead Org Status</th>
    <th rowspan='2'>Org Status</th>
    <th colspan='2'>Participation</th>
    <th rowspan='2'>Person</th>
   </tr>
   <tr>
    <th>Lead Org</th>
    <th>Org Site</th>
   </tr>
"""

        #--------------------------------------------------------------
        # Sort by status, then by protocol id.
        #--------------------------------------------------------------
        msg = ""
        keys = protLinks.keys()
        statusOrder = {
            'ACTIVE': 1,
            'APPROVED-NOT YET ACTIVE': 2,
            'TEMPORARILY CLOSED': 3,
            'CLOSED': 4,
            'COMPLETED': 5
            }
        def sorter(a, b):
            # key[0] is protId; key[1] is docId; key[2] is lead org status
            if a[2] == b[2]:
                if a[0] == b[0]:
                    return cmp(a[1], b[1])
                return cmp(a[0], b[0])
            return cmp(statusOrder.get(a[2].upper(), 999),
                       statusOrder.get(b[2].upper(), 999))
        keys.sort(sorter)
        done = 0
        for key in keys:
            protLink = protLinks[key]
            person = ""
            for protPerson in protLink.personnel:
                pp = protLink.personnel[protPerson]
                if person:
                    person += "<br>\n"
                person += pp.name
                if pp.roles:
                    sep = " ("
                    for role in pp.roles:
                        person += sep + role
                        sep = ", "
                    person += ")"
            if not person:
                person = "&nbsp;"
            leadOrg = protLink.isLeadOrg and "X" or "&nbsp;"
            orgSite = protLink.isOrgSite and "X" or "&nbsp;"
            if not protLink.orgStat:
                protLink.orgStat = protLink.loStat
            table += u"""\
   <tr>
    <td valign='top'>%s</td>
    <td valign='top' align='center'>%d</td>
    <td valign='top'>%s</td>
    <td valign='top'>%s</td>
    <td valign='top' align='center'>%s</td>
    <td valign='top' align='center'>%s</td>
    <td valign='top'>%s</td>
   </tr>
""" % (protLink.protId, protLink.docId, protLink.loStat,
       protLink.orgStat or u"&nbsp;",
       leadOrg, orgSite, person)
            done += 1
            msg = "Processed %d of %d rows" % (done, len(keys))
            job.setProgressMsg(msg)

        msg = "Extracting external site links"
        job.setProgressMsg(msg)
        table += u"\n  </table>\n" + self.makeExternalSiteLinksTable()
        html = html.replace("@@DOC-ID@@", "CDR%010d" % self.id) \
                   .replace("@@TABLE@@", table)
        # Save the report.
        name = "/OrgProtocolReview-%d.html" % job.getJobId()
        file = open(REPORTS_BASE + name, "wb")
        file.write(cdrcgi.unicodeToLatin1(html))
        file.close()
        cdr.logwrite("saving %s" % (REPORTS_BASE + name), LOGFILE)
        ### url = "%s/CdrReports%s" % (cdr.getHostName()[2], name)
        url = "%s/CdrReports%s" % (cdr.CBIIT_NAMES[2], name)
        cdr.logwrite("url: %s" % url, LOGFILE)
        msg += "<br>Report available at <a href='%s'><u>%s</u></a>." % (
            url, url)

        # Tell the user where to find it.
        body = """\
The Organization Protocol Review report you requested for CDR%d
can be viewed at
%s.
""" % (self.id, url)
        if cdr.h.org == 'OCE':
            subject   = "%s: Report results - Org protocol review" % \
                                                   cdr.PUB_NAME.capitalize()
        else:
            subject   = "%s-%s: Report results - Org protocol review" % (
                                                      cdr.h.org, cdr.h.tier)

        sendMail(job, subject, body)
        job.setProgressMsg(msg)
        job.setStatus(cdrbatch.ST_COMPLETED)
        cdr.logwrite("Completed report", LOGFILE)

    def makeExternalSiteLinksTable(self):
        self.cursor.execute("""\
            SELECT prot_id.value, prot_id.doc_id, person.title, status.value
              FROM query_term prot_id
              JOIN query_term org
                ON org.doc_id = prot_id.doc_id
              JOIN query_term status
                ON status.doc_id = prot_id.doc_id
   LEFT OUTER JOIN query_term pi
                ON pi.doc_id = prot_id.doc_id
               AND pi.path = '/InScopeProtocol/ProtocolAdminInfo'
                           + '/ExternalSites/ExternalSite/ExternalSitePI'
                           + '/ExternalSitePIID/@cdr:ref'
               AND LEFT(pi.node_loc, 12) = LEFT(org.node_loc, 12)
   LEFT OUTER JOIN document person
                ON person.id = pi.int_val
             WHERE org.int_val = ?
               AND org.path = '/InScopeProtocol/ProtocolAdminInfo'
                            + '/ExternalSites/ExternalSite/ExternalSiteOrg'
                            + '/ExternalSiteOrgID/@cdr:ref'
               AND prot_id.path = '/InScopeProtocol/ProtocolIDs/PrimaryID'
                                + '/IDString'
               AND status.path = '/InScopeProtocol/ProtocolAdminInfo'
                               + '/CurrentProtocolStatus'
          ORDER BY prot_id.value, prot_id.doc_id""", self.id, timeout = 300)
        rows = self.cursor.fetchall()
        if not rows:
            return u""
        table = u"""\
  <br><br>
  <b><font size='4'>External Sites</font></b>
  <br><br>
  <table border='1' cellpadding='2' cellspacing='0'>
   <tr>
    <th>Protocol ID</th>
    <th>Doc ID</th>
    <th>Current Protocol Status</th>
    <th>Person</th>
   </tr>
"""
        for row in rows:
            if row[2]:
                person = cgi.escape(row[2].strip().split(u';')[0])
            else:
                person = u"&nbsp;"
            table += u"""\
   <tr>
    <td>%s</td>
    <td>%d</td>
    <td>%s</td>
    <td>%s</td>
   </tr>
""" % (cgi.escape(row[0]), row[1], row[3] or u"&nsbp;", person)
        return table + u"""\
  </table>
"""

#----------------------------------------------------------------------
# Class for finding URLs which are not alive.
# Changing use of depricated class httplib.HTTP
#----------------------------------------------------------------------
class UrlCheck:
    def __init__(self):
        # Setting up the propper database source
        # --------------------------------------
        import cdrutil
        h = cdrutil.AppHost(cdrutil.getEnvironment(), cdrutil.getTier(),
                            filename = cdr.WORK_DRIVE + ':/etc/cdrapphosts.rc')
        if h.org == 'OCE':
            host = 'localhost'
        else:
            host = h.host['DBWIN'][0]

        self.conn    = cdrdb.connect('CdrGuest', dataSource = host)
        self.cursor  = self.conn.cursor()
        self.pattern = re.compile(u"([^/]+)/@cdr:xref$")
        self.docType  = job.getParm('docType')
        self.audience = job.getParm('audience')
        self.language = job.getParm('language')

    #------------------------------------------------------------------
    # Report on a dead URL
    #------------------------------------------------------------------
    def report(self, row, err):
        match = self.pattern.search(row[1])
        elem = match and match.group(1) or ""
        return u"""\
   <tr bgcolor='white'>
    <td>%d</td>
    <td>%s</td>
    <td nowrap='1'>%s</td>
    <td>%s</td>
   </tr>
""" % (row[0], row[2], err, elem)
#     <td>%s</td>
# """ % (row[3], row[0], row[2], err, elem)

    #------------------------------------------------------------------
    # Run the report.
    #------------------------------------------------------------------
    def run(self, job):
        job.setProgressMsg("Report started")
        cdr.logwrite("Starting UrlCheck report", LOGFILE)
        myLanguage = ''
        myAudience = ''

        # SQL Query for Summaries
        # -----------------------
        if self.docType == 'Summary':
            if self.language == 'EN':
                sqLanguage = 'English'
                myDefinition = 'TermDefinition'
            else:
                sqLanguage = 'Spanish'
                myDefinition = 'TranslatedTermDefinition'
            myLanguage = self.language

            if self.audience == 'HP':
                myAudience = 'Health professionals'
            else:
                myAudience = 'Patients'
            query = """\
  SELECT q.doc_id, q.path, q.value, a.value, l.value
    FROM query_term q
    JOIN document d
      ON d.id = q.doc_id
    JOIN query_term a
      ON d.id = a.doc_id
     AND a.path = '/Summary/SummaryMetaData/SummaryAudience'
     AND a.value = '%s'
    JOIN query_term l
      ON l.doc_id = d.id
     AND l.path = '/Summary/SummaryMetaData/SummaryLanguage'
     AND l.value = '%s'
   WHERE q.value LIKE 'http%%'
     AND q.path LIKE '%%/@cdr:xref'
ORDER BY q.doc_id
""" % (myAudience, sqLanguage)

        # SQL Query for Glossaries
        # ------------------------
        elif self.docType in ('Glossary', 'GlossaryTermConcept'):
            if self.language == 'EN':
                myLanguage = 'en'
                myDefinition = 'TermDefinition'
            else:
                myLanguage = 'es'
                myDefinition = 'TranslatedTermDefinition'
            myAudience = self.audience

            query = """\
  SELECT q.doc_id, q.path, q.value, t.name, l.value
    FROM query_term q
    JOIN document d
      ON d.id = q.doc_id
    JOIN doc_type t
      ON t.id = d.doc_type
     AND t.name = 'GlossaryTermConcept'
left outer JOIN query_term l
      ON l.doc_id = d.id
     AND l.path like '%%/@UseWith'
   WHERE q.value LIKE 'http%%'
     AND (q.path LIKE '%%/%s/%%/@cdr:xref'
          OR
          q.path LIKE '%%/RelatedExternalRef/@cdr:xref'
          AND
          l.value <> '%s'
         )
ORDER BY t.name, q.doc_id
""" % (myDefinition, myLanguage)

        # SQL Query for all others
        # ------------------------
        else:
            query  = """\
  SELECT q.doc_id, q.path, q.value, t.name
    FROM query_term q
    JOIN document d
      ON d.id = q.doc_id
    JOIN doc_type t
      ON t.id = d.doc_type
     AND t.name = '%s'
   WHERE value LIKE 'http%%'
     AND path LIKE '%%/@cdr:xref'
ORDER BY t.name, q.doc_id
""" % (self.docType)

        self.cursor.execute(query, timeout = 1200)
        rows = self.cursor.fetchall()

        #--------------------------------------------------------------
        # Keep track of hosts we know are not responding at all.
        #--------------------------------------------------------------
        deadHosts = {}

        #--------------------------------------------------------------
        # Another little optimization; don't ping same URL twice.
        #--------------------------------------------------------------
        deadUrls = {}
        goodUrls = {}

        #--------------------------------------------------------------
        # Create the HTML for the report.
        #--------------------------------------------------------------
        dtAdd = ""
        if myLanguage or myAudience:
            dtAdd = " - %s, %s" % (myLanguage.upper(), myAudience)
        html = [u"""\
<!DOCTYPE HTML PUBLIC '-//IETF//DTD HTML//EN'>
<html>
 <head>
  <title>CDR Report on Inactive Hyperlinks</title>
  <style type='text/css'>
   h1         { font-family: serif; font-size: 14pt; color: black;
                font-weight: bold; text-align: center; }
   th         { font-family: Arial; font-size: 12pt; font-weight: bold; }
   td         { font-family: Arial; font-size: 11pt; }
  </style>
 </head>
 <body>
  <h1>CDR Report on Inactive Hyperlinks<br>%s%s</h1>
  <table border='0' width='100%%' cellspacing='1' cellpadding='2'>
   <tr bgcolor='silver'>
    <td><b>Doc Type</b></td>
    <td><b>Source Doc</b></td>
    <td><b>URL</b></td>
    <td><b>Problem</b></td>
    <td><b>Element</b></td>
   </tr>
""" % (self.docType, dtAdd)]
        done = 0
        msg = "No URLs found!"
        for row in rows:
            done    += 1
            msg      = "checked %d of %d URLs" % (done, len(rows))
            url      = row[2]
            if goodUrls.has_key(url):
                continue
            if deadUrls.has_key(url):
                html.append(self.report(row, deadUrls[url]))
                continue
            pieces   = urlparse.urlparse(url)
            host     = pieces[1]
            selector = pieces[2]
            if pieces[3]: selector += ";" + pieces[3]
            if pieces[4]: selector += "?" + pieces[4]
            if pieces[5]: selector += "#" + pieces[5]
            if not host:
                html.append(self.report(row, "Malformed URL"))
                continue
            if deadHosts.has_key(host):
                html.append(self.report(row, "Host not responding"))
                continue
            if pieces[0] not in ('http','https'):
                html.append(self.report(row, "Unexpected protocol"))
                continue
            try:
                http = httplib.HTTPConnection(host)
                http.request('GET', selector)
                result = http.getresponse()

                if result.status / 100 != 2:
                    try:
                        message = unicode(result.reason, 'utf-8')
                    except:
                        message = unicode(result.reason)
                    message = u"%s: %s" % (result.status, message)
                    deadUrls[url] = message
                    html.append(self.report(row, message))
                else:
                    goodUrls[url] = 1
            except IOError, ioError:
                html.append(self.report(row, "IOError: %s" % ioError))
                cdr.logwrite("%s, IOError: %s" % (row, ioError), LOGFILE)
            except socket.error, socketError:
                deadHosts[host] = 1
                html.append(self.report(row, "Host not responding"))
                cdr.logwrite("%s Host not responding" % row, LOGFILE)
            except:
                html.append(self.report(row, "Unrecognized error"))
                cdr.logwrite("%s Unrecognized error" % row, LOGFILE)
            job.setProgressMsg(msg)
        html.append(u"""\
  </table>
 </body>
</html>
""")

        #--------------------------------------------------------------
        # Write out the report and tell the user where it is.
        #--------------------------------------------------------------
        name = "/UrlCheck-%d.html" % job.getJobId()
        file = open(REPORTS_BASE + name, "wb")
        file.write(cdrcgi.unicodeToLatin1(u"".join(html)))
        file.close()
        cdr.logwrite("saving %s" % (REPORTS_BASE + name), LOGFILE)
        ### url = "%s/CdrReports%s" % (cdr.getHostName()[2], name)
        url = "%s/CdrReports%s" % (cdr.CBIIT_NAMES[2], name)
        cdr.logwrite("url: %s" % url, LOGFILE)
        msg += "<br>Report available at <a href='%s'><u>%s</u></a>." % (
            url, url)

        body = """\
The URL report you requested can be viewed at
%s.
""" % (url)

        if cdr.h.org == 'OCE':
            subject   = "%s: Report results - URL Check" % \
                                                cdr.PUB_NAME.capitalize()
        else:
            subject   = "%s-%s: Report results - URL Check" % (
                                                   cdr.h.org, cdr.h.tier)

        sendMail(job, subject, body)
        job.setProgressMsg(msg)
        job.setStatus(cdrbatch.ST_COMPLETED)
        cdr.logwrite("Completed UrlCheck report", LOGFILE)

#----------------------------------------------------------------------
# Class for comparing stored ExternalRef/@ExRefPageTitle values with
# html head titles of the referenced web pages.
#
# This report selects all ExternalRef elements that match user entered
# report parameters (see CheckUrls.py) that contain an ExRefPageTitle
# attribute.  The referenced (via @cdr:xref) web page is fetched and
# parsed. and a normalized value of the /html/head/title element is
# compared to a similarly normalized @ExRefPageTitle.
#
# If the two do not match, an error is reported.
#----------------------------------------------------------------------
class ExRefPageTitleCheck:
    def __init__(self):
        # Database
        self.conn     = cdrdb.connect('CdrGuest')
        self.cursor   = self.conn.cursor()

        # Required, literal parameters
        self.docType  = job.getParm('docType')
        self.jobType  = job.getParm('jobType')

        # Optional Summary audience parameter
        audience = job.getParm('audience')
        if audience == 'Pat':
            self.audience = 'Patients'
        elif audience == 'HP':
            self.audience = 'Health Professionals'
        else:
            self.audience = None

        # Optional Language of Summary or Glossary term
        language = job.getParm('language')
        if language == 'EN':
            self.language = 'English'
        elif language == 'ES':
            self.language = 'Spanish'
        else:
            self.language = None

        # Gathers all the output
        self.html = []

    #------------------------------------------------------------------
    # Create an HTML SAX like parser for finding html titles
    #------------------------------------------------------------------
    class TitleParser(HTMLParser.HTMLParser):
        def __init__(self):
            # Super class constructor
            HTMLParser.HTMLParser.__init__(self)

            # Track where we are and accumulate title
            self.inHead       = False
            self.inTitle      = False
            self.titleContent = u""

        def handle_starttag(self, tag, attrs):
            """
            Callback when new tag recognized.
            """
            ltag = tag.lower()
            if ltag == "head":
                self.inHead = True
            elif ltag == "title":
                if self.inHead:
                    self.inTitle = True

        def handle_endtag(self, tag):
            """
            Callback when end tag recognized.
            """
            ltag = tag.lower()
            if ltag == "head":
                self.inHead = False
            elif ltag == "title":
                if self.inHead:
                    self.inTitle = False

        def handle_data(self, data):
            """
            Accumulate title text.  Using spaces to replace any interior tags.
            """
            if self.inTitle:
                self.titleContent += u" " + data

        def getTitleContent(self):
            """
            Return title with interior markup stripped, all spaces normalized
            to single space, and leading and trailing space stripped.
            """
            return ' '.join(self.titleContent.split())

    #------------------------------------------------------------------
    # Create a row to the report output table.
    #------------------------------------------------------------------
    def addReportRow(self, cdrId, docTitle, url, exRefTitle,
                     pageTitle, pageFlag):
        """
        Generate one row and append it to the report.

        Pass:
            cdrId       Integer doc ID of doc containing the ExternalRef
            docTitle    Title of the CDR document
            url         URL of the page linked-to
            exRefTitle  Page title stored in @ExRefPageTitle in the doc
            pageTitle   HTML title found in the linked-to page
                         If document not retrieved, or no title found,
                         contains error message
            pageFlag    Indication of what has been found:
                         "OK"       = title found and matched
                         "Mismatch" = title found, but doesn't match stored
                         "Error"    = Error message, not a title
        Return:
            Void.  Row is appended directly to html array
        """
        rowHtml = """\
 <tr>
  <td align='right'>%d</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td class='%s'>%s</td>
 </tr>
""" % (cdrId, docTitle, url, exRefTitle, pageFlag, pageTitle)

        # Append it to the output
        self.html.append(rowHtml)


    #------------------------------------------------------------------
    # Run the report.
    #------------------------------------------------------------------
    def run(self, job):
        """
        Conforms to the interface for all CdrLongReports.
        """
        job.setProgressMsg("Report started")
        cdr.logwrite("Starting ExRefPageTitle report", LOGFILE)

        # No restrictive query qualifiers yet
        qualifiers = ''

        # Define variable language and audience qualifiers of the
        # SQL query to find page titles
        if self.docType == "Summary":
            if self.language is not None:
                qualifiers = """
  JOIN query_term lnq
    ON lnq.doc_id = ptq.doc_id
   AND lnq.path = '/Summary/SummaryMetaData/SummaryLanguage'
   AND lnq.value = '%s'""" % self.language

            if self.audience is not None:
                qualifiers += """
  JOIN query_term audq
    ON audq.doc_id = ptq.doc_id
   AND audq.path = '/Summary/SummaryMetaData/SummaryAudience'
   AND audq.value = '%s'""" % self.audience

        # Glossary types have a language but no audience
        elif self.docType[0:8] == "Glossary":
            if self.language is not None:
                qualifiers = """
  JOIN query_term lnq
    ON lnq.doc_id = ptq.doc_id
   AND lnq.path LIKE '/GlossaryTerm%/Language'
   AND lnq.value = '%s'""" % self.language

        # Query to select all objects of interest
        #  ptq  = page title value query terms
        #  urlq = url value query terms
        query = """
SELECT DISTINCT ptq.doc_id, ptq.value, urlq.value, doc.title
  FROM query_term ptq
  JOIN query_term urlq
    ON ptq.doc_id = urlq.doc_id
   AND SUBSTRING(ptq.node_loc, 1, LEN(ptq.node_loc) - 4) =
       SUBSTRING(urlq.node_loc, 1, LEN(urlq.node_loc) - 4)
%s
  JOIN document doc
    ON ptq.doc_id = doc.id
 WHERE ptq.path LIKE '/%s/%%/@SourceTitle'
   AND urlq.path LIKE '/%s/%%/@cdr:xref'""" % (qualifiers,
                                               self.docType, self.docType)

        # DEBUG
        cdr.logwrite(query)

        # Select them
        rows = []
        try:
            self.cursor.execute(query, timeout = 1200)
            rows = self.cursor.fetchall()
        except cdrdb.Error, info:
            job.fail('Failure fetching ExRefPageTitles: %s' % info[1][0],
                     logfile = LOGFILE)

        # Might not be any
        if len(rows) == 0:
            job.fail("No data found for report with input parameters")

        # Create the output html
        self.html.append(u"""\
<!DOCTYPE HTML PUBLIC '-//IETF//DTD HTML//EN'>
<html>
 <head>
  <title>ExternalRefs Report</title>
  <style type='text/css'>
   h1           { font-family: serif; font-size: 14pt; color: black;
                  font-weight: bold; text-align: center; }
   th           { font-family: Arial; font-size: 12pt; font-weight: bold; }
   td           { font-family: Arial; font-size: 11pt; }
   td.OK        { font-family: Arial; font-size: 11pt; color: Cyan; }
   td.Mismatch  { font-family: Arial; font-size: 11pt; color: Red; }
   td.Error     { font-family: Arial; font-size: 11pt; color: Darkred; }
  </style>
 </head>
 <body>
  <h1>Comparison of Stored Page Titles with Actual Titles in Web Pages<h1>
  <h3>Parameters:</h3>
  <ul>
   <li> Language: %s</li>
   <li> Audience: %s</li>
   <li> Show All: %s</li>
  </ul>
  <table border='1' cellpadding='2' cellspacing='0'>
   <tr>
    <th nowrap='1'>CDR ID</th>
    <th nowrap='1'>CDR Doc Title</th>
    <th nowrap='1'>URL</th>
    <th nowrap='1'>CDR Stored ExRef Title</th>
    <th nowrap='1'>Web Page Current Title</th>
   </tr>
""" % (self.language, self.audience, self.jobType))

        # Short circuit getting same web page multiple times
        #  Key   = url
        #  Value = (pageFlag, pageTitle)
        resultCache = {}

        # Process each found external reference
        count = -1
        total = len(rows)
        msg = ''
        for row in rows:
            # For watchers
            count += 1
            job.setProgressMsg("Checked %d of %d URLs" % (count, total))

            # As found in the database
            docId, exRefTitle, url, docTitle = row

            # Have we already seen this web page?
            if resultCache.has_key(url):
                # Get the result of previous fetch
                pageFlag, pageTitle = resultCache(url)

                # If previous fetch failed, don't try again, re-report failure
                if pageFlag != "Error":
                    # Page title may not match different doc's exRefTitle
                    # Compare normalized strings, case insensitive
                    normalExRefTitle = ' '.join(exRefTitle.split())
                    if normalExRefTitle.lower() == pageTitle.lower():
                        pageFlag = "OK"
                    else:
                        pageFlag = "Mismatch"

                # Add the report row with the proper pageFlag
                self.addReportRow(docId, docTitle, url, exRefTitle,
                                  pageTitle, pageFlag)

            else:
                # Connect to remote host
                try:
                    response = urllib2.urlopen(url, timeout=30)
                except Exception, info:
                    pageFlag  = "Error"
                    errMsg    = str(info)
                    resultCache[url] = (pageFlag, errMsg)
                    self.addReportRow(docId, docTitle, url, exRefTitle,
                                      errMsg, pageFlag)
                    continue

                # Was it successful?
                rc = response.getcode()
                # sys.stderr.write("http response code=%d\n" % rc)
                code200 = rc - 200
                if code200 < 0 or code200 > 99:
                    pageFlag = "Error"
                    errMsg   = "%d error connecting to host"
                    resultCache[url] = (pageFlag, errMsg)
                    self.addReportRow(docId, docTitle, url, exRefTitle,
                                      errMsg, pageFlag)
                    continue

                # Get the full web page from the remote host
                pageHtml = response.read()
                if not pageHtml:
                    pageFlag = "Error"
                    errMsg   = "Unable to retrieve web page from host"
                    resultCache[url] = (pageFlag, errMsg)
                    self.addReportRow(docId, docTitle, url, exRefTitle,
                                      errMsg, pageFlag)
                    continue

                # Extract the title
                parser = self.TitleParser()
                try:
                    parser.feed(pageHtml)
                    pageTitle = parser.getTitleContent()
                except Error, info:
                    pageFlag = "Error"
                    errMsg   = "Error parsing web page to find the title: %s" \
                                % str(info)
                    resultCache[url] = (pageFlag, errMsg)
                    self.addReportRow(docId, docTitle, url, exRefTitle,
                                      errMsg, pageFlag)
                    continue

                # Compare the stored and found titles
                normalExRefTitle = ' '.join(exRefTitle.split())
                if normalExRefTitle.lower() == pageTitle.lower():
                    pageFlag = "OK"
                else:
                    pageFlag = "Mismatch"
                self.addReportRow(docId, docTitle, url, exRefTitle,
                                  pageTitle, pageFlag)

        # Terminate the output html
        self.html.append("""
  </table>
 </body>
</html>
""")

        #--------------------------------------------------------------
        # Write out the report and tell the user where it is.
        #--------------------------------------------------------------
        name = "/ExternalRefs-%d.html" % job.getJobId()
        file = open(REPORTS_BASE + name, "wb")
        file.write(cdrcgi.unicodeToLatin1(u"".join(self.html)))
        file.close()
        cdr.logwrite("saving %s" % (REPORTS_BASE + name), LOGFILE)
        url = "%s/CdrReports%s" % (cdr.CBIIT_NAMES[2], name)
        cdr.logwrite("url: %s" % url, LOGFILE)
        msg = "Checked %d of %d URLs<br>" \
              "Report available at <a href='%s'><u>%s</u></a>." % \
                (count, total, url, url)

        body = """\
The ExternalRefs check report you requested can be viewed at
%s.
""" % (url)

        subject   = "%s-%s: Report results - ExternalRefs Check" % (
                            cdr.h.org, cdr.h.tier)

        sendMail(job, subject, body)
        job.setProgressMsg(msg)
        job.setStatus(cdrbatch.ST_COMPLETED)
        cdr.logwrite("Completed ExternalRefs Check report", LOGFILE)


#----------------------------------------------------------------------
# Class for finding URLs which are not alive.
#----------------------------------------------------------------------
class countPublishedDocs:
    def __init__(self, host = 'localhost'):
        # Setting up the propper database source
        # --------------------------------------
        import cdrutil
        h = cdrutil.AppHost(cdrutil.getEnvironment(), cdrutil.getTier(),
                            filename = cdr.WORK_DRIVE + ':/etc/cdrapphosts.rc')
        if h.org == 'OCE':
            host = 'localhost'
        else:
            host = h.host['DBWIN'][0]

        self.conn    = cdrdb.connect('CdrGuest', dataSource = host)
        self.cursor  = self.conn.cursor()
        self.pattern = re.compile(u"([^/]+)/@cdr:xref$")

    #------------------------------------------------------------------
    # Report on a dead URL
    #------------------------------------------------------------------
    def report(self, row, err):
        match = self.pattern.search(row[1])
        elem = match and match.group(1) or ""
        return u"""\
   <tr bgcolor='white'>
    <td>%s</td>
    <td>%d</td>
    <td>%s</td>
    <td nowrap='1'>%s</td>
    <td>%s</td>
   </tr>
""" % (row[3], row[0], row[2], err, elem)

    #------------------------------------------------------------------
    # Run the report.
    #------------------------------------------------------------------
    def run(self, job):
        job.setProgressMsg("Report started")
        cdr.logwrite("Starting Published Documents Count report", LOGFILE)

        protQuery = """
            SELECT doc_id, doc_version
              FROM pub_proc_doc ppd
              JOIN active_doc d
                ON d.id = ppd.doc_id
              JOIN doc_type dt
                ON d.doc_type = dt.id
             WHERE pub_proc = %s
               AND dt.name = '%s'
        """

        # -------------------------------------------------------------------
        # Retrieve the XML from the database
        # Returns the protocol object
        # -------------------------------------------------------------------
        def getProtocol(docId, num, cursor):
            query = """
                SELECT xml
                  FROM doc_version
                 WHERE id = %s
                   AND num = %s
        """ % (docId, num)

            try:
                cursor.execute(query)
                row = cursor.fetchone()

            except cdrdb.Error, info:
                cdr.logwrite("Failure retrieving XML: %s" % info[1][0], LOGFILE)

            return etree.XML(row[0].encode('utf-8'))

        # -------------------------------------------------------------------
        # Count the InScopeProtocols by status
        # This version retrieves the documents from the database
        # -------------------------------------------------------------------
        def getInScopeCount(jobID = 4867):
            """
            Function to collect the InScopeProtocols from the last
            export job (given by the JobID) and counting the number
            of documents per CurrentStatus.
            """
            query = protQuery % (jobID, 'InScopeProtocol')

            try:
                # Selecting all protocol documents
                # --------------------------------
                job.setProgressMsg("Running InScopeProtocols")
                cdr.logwrite("Running InScopeProtocols", LOGFILE)
                self.cursor.execute(query)
                rows = self.cursor.fetchall()

                inScopeCount = {}

                # Inspecting and counting by status value
                # ---------------------------------------
                for (docID, version) in rows:
                    xml = getProtocol(docID, version, self.cursor)
                    curStatus = xml.find('./ProtocolAdminInfo/CurrentProtocolStatus')
                    status = curStatus.text

                    if inScopeCount.has_key(status):
                        inScopeCount[status] += 1
                    else:
                        inScopeCount[status]  = 1

            # except Exception as e:
            except cdrdb.Error, info:
                cdr.logwrite("Error collecting InScope filenames: %s" % \
                                                          info[1][0], LOGFILE)

            return(inScopeCount)

        # -------------------------------------------------------------------
        # Count the CTGovProtocols by status
        # This version retrieves the documents from the database
        # -------------------------------------------------------------------
        def getCTGovCount(jobID = 4867):
            """
            Function to collect the CTGovProtocols from the last
            export job (given by the JobID) and counting the number
            of documents per CurrentStatus.
            """
            query = protQuery % (jobID, 'CTGovProtocol')

            try:
                # Selecting all protocol documents
                # --------------------------------
                self.cursor.execute(query)
                rows = self.cursor.fetchall()

                ctgovCount = {}

                # Inspecting and counting by status value
                # ---------------------------------------
                for (docID, version) in rows:
                    xml = getProtocol(docID, version, self.cursor)
                    curStatus = xml.find('./OverallStatus')

                    if curStatus is not None:
                        status = curStatus.text

                    if ctgovCount.has_key(status):
                        ctgovCount[status] += 1
                    else:
                        ctgovCount[status]  = 1

            # except Exception as e:
            except cdrdb.Error, info:
                cdr.logwrite("Error collecting CTGov filenames: %s" % \
                                                          info[1][0], LOGFILE)

            return(ctgovCount)


        #----------------------------------------------------------------------
        # Build date string for header.
        #----------------------------------------------------------------------
        dateString = time.strftime("%B %d, %Y")

        #----------------------------------------------------------------------
        # Select the Job ID of the last Export job
        # ----------------------------------------
        query = """SELECT MAX(id)
                     FROM pub_proc
                    WHERE pub_system = 178
                      AND pub_subset = 'Export'
                      AND status = 'Success'"""
        try:
            #cursor = conn.cursor()
            self.cursor.execute(query)
            jobID = self.cursor.fetchone()

            # jobID = [9768]
            # print jobID

            self.cursor.close()
            #self.cursor = None
        except cdrdb.Error, info:
            cdr.logwrite("Failure selecting last Export JobId: %s" % \
                                                          info[1][0], LOGFILE)

        # Counting the documents by document type
        # ---------------------------------------
        query = """select dt.name, count(*)
                     from pub_proc_doc ppd
                     join document d
                       on d.id = ppd.doc_id
                     join doc_type dt
                       on dt.id = d.doc_type
                    where pub_proc = %s
                      and failure is null
                    group by dt.name
                    order by dt.name""" % (jobID[0])

        try:
            #cursor = conn.cursor()
            job.setProgressMsg("Counting all document types")
            cdr.logwrite("Counting all document types", LOGFILE)
            self.cursor.execute(query)
            docTypesCount = self.cursor.fetchall()
            self.cursor.close()
            #self.cursor = None
        except cdrdb.Error, info:
            cdr.logwrite("Failure selecting published document count: %s" % \
                                                          info[1][0], LOGFILE)

        #----------------------------------------------------------------------
        # Create the results page.
        #----------------------------------------------------------------------
        title     = "CDR Administration"
        header = u"""\
<!DOCTYPE HTML PUBLIC '-//W3C//DTD HTML 4.01 Transitional//EN'
                      'http://www.w3.org/TR/html4/loose.dtd'>
<html>
 <head>
  <title>Published Documents on Cancer.gov -- %s</title>
  <meta http-equiv='Content-Type' content='text/html;charset=utf-8'>
  <link rel='shortcut icon' href='/favicon.ico'>
  <LINK TYPE='text/css' REL='STYLESHEET' HREF='/stylesheets/dataform.css'>
  <style type="text/css">
        body         { background-color: #DFDFDF; }
        H5           { font-weight: bold;
                       font-family: Arial;
                       font-size: 13pt;
                       margin: 0pt; }
        TD.header    { font-weight: bold;
                       align: center; }
        TR.odd       { background-color: #F7F7F7; }
        TR.even      { background-color: #FFFFFF; }
        TR.head      { background-color: #B2B2B2; }
        *.center     { text-align: center; }
        *.footer     { font-size: 11pt;
                       background-color: #B2B2B2;
                       font-weight: bold;
                       border-top: 2px solid black;
                       border-bottom: 2px solid black; }
  </style>
 </head>
 <body>
        """ % dateString

        # -------------------------
        # Display the Report Title
        # -------------------------
        #report    = u"""\
        #   <INPUT TYPE='hidden' NAME='%s' VALUE='%s'>
        #  </FORM>
        #""" % (cdrcgi.SESSION, session)

        report = u"""
  <div class="center">
    <H3>Published Documents</H3>
     <b>Documents Listed from Last Weekly Export: Job%s</b>
     <br>
     <br>
  </div>
        """ % jobID[0]

        # Display the header row of the first table
        # ------------------------------------------
        report += u"""
   <table width="25%" align="center" border="0">
    <tr class="head">
     <td class="header" align="center" width="70%">Doc Type</td>
     <td class="header" align="center" width="30%">Count</td>
    </tr>"""

        # Displaying the records for the document type counts
        # ------------------------------------------------------------
        count = 0
        total = 0
        for docTypeCount in docTypesCount:
            total += docTypeCount[1]
            count += 1

            # Display the rows with different background color
            # ------------------------------------------------
            if count % 2 == 0:
                report += u"""
            <tr class="even">"""
            else:
                report += u"""
            <tr class="odd">"""
            report += u"""
             <td><b>%s</b></td>
             <td><b>%s</b></td>""" % (docTypeCount[0], docTypeCount[1])

            report += u"""
            </tr>"""

        report += u"""
            <tr class="footer">
             <td class="footer">Total</td>
             <td class="footer">%s</td>
            </tr>
           </table>""" % total

        # List an additional table for the breakdown of InScopeProtocols
        # by status
        # -----------------------------------------------------------------
        job.setProgressMsg("Counting InScopeProtocols by status")
        cdr.logwrite("Counting InScopeProtocols by status", LOGFILE)
        inScopeCount = getInScopeCount(jobID[0])

        inScopeKeys = inScopeCount.keys()
        inScopeKeys.sort()
        #print ""

        report += u"""
   <div class="center">
    <br>
     <b>InScopeProtocol by Status</b>
    <br>
   </div>
   <table width="35%" align="center" border="0">
    <tr class="head">
     <td class="header" align="center" width="50%">Status</td>
     <td class="header" align="center" width="20%">Count</td>
    </tr>"""

        # We want to display a summary line of total active/closed protocols
        # ------------------------------------------------------------------
        scount = 0
        acount = 0
        ccount = 0

        for status in inScopeKeys:
            # Counting total of active and closed protocols
            # ------------------------------------------------------
            if status in ('Active', 'Approved-not yet active'):
                acount += inScopeCount[status]
            else:
                ccount += inScopeCount[status]

            # Display the records for the status values
            # -----------------------------------------
            scount += 1
            if scount % 2 == 0:
                report += u"""
            <tr class="even">"""
            else:
                report += u"""
            <tr class="odd">"""
            report += u"""
             <td><b>%s</b></td>
             <td><b>%s</b></td>""" % (status, inScopeCount[status])

            report += u"""
            </tr>"""

        # Display the summary for active/closed protocols
        # -----------------------------------------------
        report += u"""
    <tr class="footer">
     <td class="footer">Total-active</td>
     <td class="footer">%s</td>
    </tr>
    <tr class="footer">
     <td class="footer">Total-closed</td>
     <td class="footer">%s</td>
    </tr>
   </table>""" % (acount, ccount)

        # Repeat the same for the CTGovProtocols
        # --------------------------------------
        job.setProgressMsg("Counting CTGovProtocols by status")
        cdr.logwrite("Counting CTGovProtocols by status", LOGFILE)
        ctgovCount = getCTGovCount(jobID[0])

        ctgovKeys = ctgovCount.keys()
        ctgovKeys.sort()

        report += u"""
   <div class="center">
    <br>
     <b>CTGovProtocol by Status</b>
    <br>
   </div>
   <table width="35%" align="center" border="0">
    <tr class="head">
     <td class="header" align="center" width="50%">Status</td>
     <td class="header" align="center" width="20%">Count</td>
    </tr>"""

        # We want to display a summary line of total active/closed protocols
        # ------------------------------------------------------------------
        scount = 0
        acount = 0
        ccount = 0

        for status in ctgovKeys:
            # Counting total of active and closed CTGovProtocols
            # --------------------------------------------------
            if status in ('Active', 'Approved-not yet active'):
                 acount += ctgovCount[status]
            else:
                 ccount += ctgovCount[status]

            # Display the records for the status values
            # -----------------------------------------
            scount += 1
            if scount % 2 == 0:
                report += u"""
            <tr class="even">"""
            else:
                report += u"""
            <tr class="odd">"""
            report += u"""
             <td><b>%s</b></td>
             <td><b>%s</b></td>""" % (status, ctgovCount[status])

            report += u"""
            </tr>"""


        # Display the summary for active/closed protocols
        # -----------------------------------------------
        report += """
    <tr class="footer">
     <td class="footer">Total-active</td>
     <td class="footer">%s</td>
    </tr>
    <tr class="footer">
     <td class="footer">Total-closed</td>
     <td class="footer">%s</td>
    </tr>
   </table>""" % (acount, ccount)

        footer = u"""
 </body>
</html>
        """

        #--------------------------------------------------------------
        # Write out the report and tell the user where it is.
        #--------------------------------------------------------------
        html = header + report + footer
        name = "/PubDocsCount-%d.html" % job.getJobId()
        file = open(REPORTS_BASE + name, "wb")
        file.write(cdrcgi.unicodeToLatin1(u"".join(html)))
        file.close()
        cdr.logwrite("saving %s" % (REPORTS_BASE + name), LOGFILE)
        ### url = "%s/CdrReports%s" % (cdr.getHostName()[2], name)
        url = "%s/CdrReports%s" % (cdr.CBIIT_NAMES[2], name)
        cdr.logwrite("url: %s" % url, LOGFILE)
        msg = "<br>Report available at <a href='%s'><u>%s</u></a>." % (
            url, url)

        body = """\
The Publishing Documents Count report you requested can be viewed at
%s.
""" % (url)
        if cdr.h.org == 'OCE':
            subject   = "%s: Report results - Pub doc count" % \
                                                   cdr.PUB_NAME.capitalize()
        else:
            subject   = "%s-%s: Report results - Pub doc count" % (
                                                      cdr.h.org, cdr.h.tier)

        sendMail(job, subject, body)
        job.setProgressMsg(msg)
        job.setStatus(cdrbatch.ST_COMPLETED)
        cdr.logwrite("Completed PubDocsCount report", LOGFILE)

#----------------------------------------------------------------------
# Find phrases which match a specified glossary term.
#----------------------------------------------------------------------
class GlossaryTermSearch:

    def __init__(self, id, types):
        self.docId = id
        self.types = types or "HPSummaries PatientSummaries PatientAbstracts"

    class Word:
        "Normalized token for one word in a phrase."
        def __init__(self, matchObj):
            self.match = matchObj
            lowerWord  = matchObj.group().lower()
            self.value = GlossaryTermSearch.squeeze.sub(u"", lowerWord)

    class Phrase:
        "Sequence of Word objects."
        def __init__(self, text, id):
            self.id    = id
            self.text  = text
            self.words = GlossaryTermSearch.getWords(text)

    class MatchingPhrase:
        "Remembers where a glossary term phrase was found."
        def __init__(self, phrase, title, id, section):
            self.phrase  = phrase
            self.title   = title
            self.docId   = id
            self.section = section

    class GlossaryNode:
        "Node in the tree of known glossary terms and their variant phrases."
        def __init__(self):
            self.docId   = None
            self.nodeMap = {}
            self.seen    = 0
        def clearFlags(self):
            self.seen = 0
            for node in self.nodeMap.values():
                node.clearFlags()

    class GlossaryTree(GlossaryNode):
        "Known glossary terms and their variant phrases."
        def __init__(self, phrases):
            GlossaryTermSearch.GlossaryNode.__init__(self)
            for phrase in phrases:
                currentMap  = self.nodeMap
                currentNode = None
                for word in phrase.words:
                    value = word.value
                    if value:
                        if currentMap.has_key(value):
                            currentNode = currentMap[value]
                        else:
                            currentNode = GlossaryTermSearch.GlossaryNode()
                            currentMap[value] = currentNode
                        currentMap = currentNode.nodeMap
                if currentNode:
                    currentNode.docId = phrase.id
        def findPhrases(self, text):
            "Returns sequence of strings for matching phrases."
            phrases     = []
            words       = GlossaryTermSearch.getWords(text)
            wordsLeft   = len(words)
            currentMap  = self.nodeMap
            currentWord = 0
            while wordsLeft > 0:
                nodes = []
                currentMap = self.nodeMap
                startPos = words[currentWord].match.start()
                endPos = startPos

                # Find the longest chain of matching words from this point.
                while len(nodes) < wordsLeft:
                    word = words[currentWord + len(nodes)]
                    node = currentMap.get(word.value)
                    if not node:
                        break
                    nodes.append(node)
                    currentMap = node.nodeMap

                # See if the chain (or part of it) matches a glossary term.
                while nodes:
                    lastNode = nodes[-1]

                    # A docId means this node is the end of a glossary term.
                    if lastNode.docId and not lastNode.seen:
                        start = words[currentWord].match.start()
                        end = words[currentWord + len(nodes) - 1].match.end()
                        phrase = text[start:end]
                        phrase = GlossaryTermSearch.strip(phrase)
                        phrases.append(phrase)
                        lastNode.seen = 1
                        break
                    nodes.pop()

                # Skip past the matched term (if any) or the current word.
                wordsToMove  = nodes and len(nodes) or 1
                wordsLeft   -= wordsToMove
                currentWord += wordsToMove
            return phrases

    class GlossaryTerm:
        "Glossary term and all phrases used for it."
        def __init__(self, cursor, id):
            cursor.execute("""\
                SELECT value
                  FROM query_term
                 WHERE path = '/GlossaryTermName/TermName/TermNameString'
                   AND doc_id = ?""", id)
            rows = cursor.fetchall()
            if not rows:
                raise Exception("GlossaryTermName %d not found" % id)
            self.id = id
            self.name = rows[0][0]
            cursor.execute("""\
                SELECT m.value
                  FROM external_map m
                  JOIN external_map_usage u
                    ON u.id = m.usage
                 WHERE m.doc_id = ?""", id)
            self.variants = []
            for row in cursor.fetchall():
                self.variants.append(row[0])

    def strip(s):
        "Remove unused punctuation from glossary term."
        return s.strip(GlossaryTermSearch.punct)

    def getWords(text):
        "Extract Word tokens from phrase or text block."
        words = []
        for w in GlossaryTermSearch.nonBlanks.finditer(text):
            words.append(GlossaryTermSearch.Word(w))
        return words

    def getTextContent(node):
        "Concatentate the values of all TEXT_NODE children of this node."
        text = ''
        for n in node.childNodes:
            if n.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                text = text + n.nodeValue
            else:
                text += GlossaryTermSearch.getTextContent(n)
        return text

    def protocolSorter(a, b):
        "Protocols are sorted by the glossary term phrase used."
        return cmp(a.phrase, b.phrase)

    def summarySorter(a, b):
        "Summaries are sorted by title, then by glossary term phrase."
        if a.title < b.title: return -1
        if a.title > b.title: return 1
        return cmp(a.phrase, b.phrase)

    def addMatchingPhrases(matches, sorter):
        "Generate the HTML for the list of matches found."
        html = u""
        matches.sort(sorter)
        for match in matches:
            html += u"""\
   <tr>
    <td>%s</td>
    <td>%s</td>
    <td>%d</td>
    <td>%s</td>
   <tr>
""" % (match.phrase, match.title, match.docId, match.section)
        return html

    #------------------------------------------------------------------
    # Build a table for glossary phrases in one set of CDR documents.
    #------------------------------------------------------------------
    def addTable(self, tableTitle, col4Header, query, sorter):
        if self.msg:
            self.msg += "<br>"
        html = u"""\
  <br />
  <br />
  <b>%s</b>
  <br />
  <br />
  <table border = '1' cellpadding = '2' cellspacing = '0'>
   <tr>
    <th>Matching phrase</th>
    <th>DocTitle</th>
    <th>DocId</th>
    <th>%s</th>
   </tr>
""" % (tableTitle, col4Header)
        matches = []
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        numRows = 0
        while row:
            self.tree.clearFlags()
            docId, docXml, docTitle = row
            title = cgi.escape(docTitle)
            dom = xml.dom.minidom.parseString(docXml.encode('utf-8'))
            for node in dom.documentElement.childNodes:
                if node.nodeName == "SummarySection":
                    text  = self.getTextContent(node).strip()
                    sectionTitle = u"[None]"
                    for child in node.childNodes:
                        if child.nodeName == "Title":
                            sectionTitle = self.getTextContent(child)
                            sectionTitle = cgi.escape(sectionTitle)
                            break
                    self.tree.clearFlags()
                    for phrase in self.tree.findPhrases(text):
                        phrase = cgi.escape(phrase)
                        mp = self.MatchingPhrase(phrase, title, docId,
                                                 sectionTitle)
                        matches.append(mp)
                elif node.nodeName == "ProtocolAbstract":
                    for child in node.childNodes:
                        if child.nodeName == "Patient":
                            for gc in child.childNodes:
                                if gc.nodeName in ('Rationale', 'Purpose',
                                                   'EligibilityText',
                                                   'TreatmentIntervention'):
                                    text  = self.getTextContent(gc).strip()
                                    for phrase in self.tree.findPhrases(text):
                                        phrase = cgi.escape(phrase)
                                        mp = self.MatchingPhrase(phrase,
                                                                 title, docId,
                                                                 gc.nodeName)
                                        matches.append(mp)
            row = self.cursor.fetchone()
            numRows += 1
            newMsg = u"Searched %d %s" % (numRows, tableTitle)
            self.job.setProgressMsg(self.msg + newMsg)
        self.msg += newMsg
        return html + GlossaryTermSearch.addMatchingPhrases(matches,
                                                            sorter) + u"""\
  </table>
"""

    def report(self, job):
        self.msg       = "Glossary tree built"
        self.job       = job
        self.conn      = cdrdb.connect('CdrGuest')
        self.cursor    = self.conn.cursor()
        self.term      = self.GlossaryTerm(self.cursor, self.docId)
        phrases        = [self.Phrase(self.term.name, self.docId)]
        for variant in self.term.variants:
            phrases.append(self.Phrase(variant, self.docId))
        self.tree      = self.GlossaryTree(phrases)
        job.setProgressMsg(self.msg)
        html           = u"""\
<html>
 <head>
  <style type = 'text/css'>
   body, tr, td { font-family: Arial; font-size: 12pt }
   h1           { font-family: "Arial"; font-size: 18pt; font-weight: bold }
  </style>
  <title>%s (CDR %d) %s</title>
 </head>
 <body>
  <center>
   <h1>Glossary Term Search Report</h1>
  </center>
  <br />
  <br />
  Term: <b>%s</b>
""" % (cgi.escape(self.term.name), self.docId, time.strftime("%B %d, %Y"),
       cgi.escape(self.term.name))

        tableTitles = (u"Cancer Information Health Professional Summaries",
                       u"Cancer Information Patient Summaries",
                       u"Patient Abstracts")

        summaryQuery = """\
    SELECT d.id, d.xml, d.title
      FROM document d
      JOIN query_term a
        ON a.doc_id = d.id
      JOIN query_term l
        ON l.doc_id = d.id
     WHERE l.path = '/Summary/SummaryMetaData/SummaryLanguage'
       AND l.value <> 'Spanish'
       AND a.path = '/Summary/SummaryMetaData/SummaryAudience'
       AND a.value = '%s'
       AND d.active_status = 'A'"""
        protocolQuery = """\
    SELECT d.id, d.xml, d.title
      FROM document d
      JOIN query_term s
        ON s.doc_id = d.id
     WHERE s.path = '/InScopeProtocol/ProtocolAdminInfo/CurrentProtocolStatus'
       AND s.value IN ('Active', 'Approved-not yet active',
                       'Temporarily closed')
       AND d.active_status = 'A'"""
        if "HPSummaries" in self.types:
            html += self.addTable(tableTitles[0], "SectionTitle",
                                  summaryQuery % 'Health professionals',
                                  self.summarySorter)
        if "PatientSummaries" in self.types:
            html += self.addTable(tableTitles[1], "SectionTitle",
                                  summaryQuery % 'Patients',
                                  self.summarySorter)
        if "PatientAbstracts" in self.types:
            html += self.addTable(tableTitles[2], "Element Name",
                                  protocolQuery,
                                  self.protocolSorter)
        html += u"""\
 </body>
</html>
"""

        #--------------------------------------------------------------
        # Write out the report and tell the user where it is.
        #--------------------------------------------------------------
        name = "/GlossaryTermSearch-%d.html" % job.getJobId()
        file = open(REPORTS_BASE + name, "wb")
        file.write(cdrcgi.unicodeToLatin1(html))
        file.close()
        cdr.logwrite("saving %s" % (REPORTS_BASE + name), LOGFILE)
        ### url = "%s/CdrReports%s" % (cdr.getHostName()[2], name)
        url = "%s/CdrReports%s" % (cdr.CBIIT_NAMES[2], name)
        cdr.logwrite("url: %s" % url, LOGFILE)
        self.msg += "<br>Report available at <a href='%s'><u>%s</u></a>." % (
            url, url)

        body = """\
The Glossary Term report you requested can be viewed at
%s.
""" % (url)
        if cdr.h.org == 'OCE':
            subject   = "%s: Report results - Glossary Term" % \
                                                      cdr.PUB_NAME.capitalize()
        else:
            subject   = "%s-%s: Report results - Glossary Term" % (
                                                      cdr.h.org, cdr.h.tier)

        sendMail(job, subject, body)
        job.setProgressMsg(self.msg)
        job.setStatus(cdrbatch.ST_COMPLETED)
        cdr.logwrite("Completed report", LOGFILE)

    # Class data.
    punct              = u"]['.,?!:;\u201c\u201d(){}<>"
    squeeze            = re.compile(u"[%s]" % punct)
    nonBlanks          = re.compile(u"[^\\s_-]+")

    # Class methods.
    protocolSorter     = staticmethod(protocolSorter)
    summarySorter      = staticmethod(summarySorter)
    getWords           = staticmethod(getWords)
    getTextContent     = staticmethod(getTextContent)
    strip              = staticmethod(strip)
    addMatchingPhrases = staticmethod(addMatchingPhrases)

#----------------------------------------------------------------------
# Processing Status Report for Protocols (Request #1897).
#----------------------------------------------------------------------
class ProtocolProcessingStatusReport:

    def __init__(self, job):
        self.job              = job
        self.conn             = cdrdb.connect('CdrGuest')
        self.cursor           = self.conn.cursor()
        self.msg              = ""
        self.includeProtTitle = job.getParm('IncludeTitle') and True or False

    def getCreationDate(self, cdrId):
        self.cursor.execute("""\
            SELECT MIN(dt)
              FROM audit_trail
             WHERE document = ?""", cdrId)
        rows = self.cursor.fetchall()
        return rows and rows[0][0] or None

    def getPrimaryId(self, protocolIdsNode):
        for child in protocolIdsNode.childNodes:
            if child.nodeName == 'PrimaryID':
                for gc in child.childNodes:
                    if gc.nodeName == 'IDString':
                        return cdr.getTextContent(gc)
        return None

    class ProtocolSource:
        def __init__(self, node):
            self.name                   = None
            self.receiptDate            = None
            self.mergeByDate            = None
            self.dateSubmissionComplete = None
            for child in node.childNodes:
                if child.nodeName == 'SourceName':
                    self.name = cdr.getTextContent(child)
                elif child.nodeName == 'DateReceived':
                    self.receiptDate = cdr.getTextContent(child)
                elif child.nodeName == 'MergeByDate':
                    self.mergeByDate = cdr.getTextContent(child)
                elif child.nodeName == 'DateSubmissionComplete':
                    self.dateSubmissionComplete = cdr.getTextContent(child)

    class OutOfScopeProtocol:

        def __init__(self, report, cdrId, node):
            self.cdrId         = cdrId
            self.primaryId     = None
            self.sources       = []
            self.recordEntered = report.getCreationDate(cdrId)
            self.originalTitle = None
            for child in node.childNodes:
                if child.nodeName == 'ProtocolIDs':
                    self.primaryId = report.getPrimaryId(child)
                elif child.nodeName == 'ProtocolTitle':
                    titleType = None
                    titleText = None
                    for gc in child.childNodes:
                        if gc.nodeName == 'TitleType':
                            titleType = cdr.getTextContent(gc)
                        elif gc.nodeName == 'TitleText':
                            titleText = cdr.getTextContent(gc)
                    if titleType == 'Original':
                        self.originalTitle = titleText
                elif child.nodeName == 'ProtocolSources':
                    for gc in child.childNodes:
                        if gc.nodeName == 'ProtocolSource':
                            source = report.ProtocolSource(gc)
                            self.sources.append(source)

    class CTGovProtocol:
        def __init__(self, report, cdrId, node):
            self.cdrId       = cdrId
            self.title       = None
            self.dateCreated = report.getCreationDate(cdrId)
            self.orgStudyId  = None
            self.nctId       = None
            self.statuses    = []
            self.statusKeys  = {}
            self.phases      = []
            self.status      = None
            for child in node.childNodes:
                if child.nodeName == 'IDInfo':
                    for gc in child.childNodes:
                        if gc.nodeName == 'OrgStudyID':
                            self.orgStudyId = cdr.getTextContent(gc)
                        elif gc.nodeName == 'NCTID':
                            self.nctId = cdr.getTextContent(gc)
                elif child.nodeName == 'ProtocolProcessingDetails':
                    for gc in child.childNodes:
                        if gc.nodeName == 'ProcessingStatus':
                            s = cdr.getTextContent(gc).strip()
                            if s:
                                self.statuses.append(s)
                                self.statusKeys[s.upper()] = s
                elif child.nodeName == 'OfficialTitle':
                    t = cdr.getTextContent(child).strip()
                    self.title = t.replace("\n", " ").replace("\r", "")
                elif child.nodeName == 'Phase':
                    self.phases.append(cdr.getTextContent(child).strip())
                elif child.nodeName == 'OverallStatus':
                    self.status = cdr.getTextContent(child).strip()

    class InScopeProtocol:
        def __init__(self, report, cdrId, node, publishable):
            self.title              = None
            self.inScopeId          = cdrId
            self.publishable        = publishable
            self.scientificId       = None
            self.protocolId         = None
            self.protocolSources    = []
            self.adminStatuses      = []
            self.adminUsers         = []
            self.scientificStatuses = []
            self.scientificUsers    = []
            self.statusKeys         = {}
            self.protocolStatus     = None
            for child in node.childNodes:
                if child.nodeName == 'ProtocolIDs':
                    self.protocolId = report.getPrimaryId(child)
                elif child.nodeName == 'ProtocolTitle':
                    if child.getAttribute('Type') == 'Original':
                        t = cdr.getTextContent(child).strip()
                        if t:
                            self.title = t.replace("\n", ' ').replace("\r", "")
                elif child.nodeName == 'ProtocolSources':
                    for gc in child.childNodes:
                        if gc.nodeName == 'ProtocolSource':
                            source = report.ProtocolSource(gc)
                            self.protocolSources.append(source)
                elif child.nodeName == 'ProtocolProcessingDetails':
                    for ps in child.getElementsByTagName('ProcessingStatus'):
                        status = cdr.getTextContent(ps).strip()
                        self.adminStatuses.append(status)
                        self.statusKeys[status.upper()] = status
                    for tagName in ('EnteredBy', 'User'):
                        for elem in child.getElementsByTagName(tagName):
                            user = cdr.getTextContent(elem)
                            self.adminUsers.append(user)
                elif child.nodeName == 'ProtocolAdminInfo':
                    for gc in child.childNodes:
                        if gc.nodeName == 'CurrentProtocolStatus':
                            self.protocolStatus = cdr.getTextContent(gc)
            report.cursor.execute("""\
                SELECT doc_id
                  FROM query_term
                 WHERE path = '/ScientificProtocolInfo/InScopeDocID/@cdr:ref'
                   AND int_val = ?""", cdrId)
            rows = report.cursor.fetchall()
            if rows:
                self.scientificId = rows[0][0]
                report.cursor.execute("SELECT xml FROM document WHERE id = ?",
                                      self.scientificId)
                rows = report.cursor.fetchall()
                if rows:
                    xmlDoc = rows[0][0]
                    dom = xml.dom.minidom.parseString(xmlDoc.encode('utf-8'))
                    for node in dom.documentElement.childNodes:
                        if node.nodeName == 'ProtocolProcessingDetails':
                            tagName = 'ProcessingStatus'
                            for ps in node.getElementsByTagName(tagName):
                                status = cdr.getTextContent(ps).strip()
                                self.scientificStatuses.append(status)
                            for tagName in ('EnteredBy', 'User'):
                                for elem in node.getElementsByTagName(tagName):
                                    user = cdr.getTextContent(elem)
                                    self.scientificUsers.append(user)

        def isResearch(self):
            return not self.publishable and "RESEARCH STUDY" in self.statusKeys
        def isDisapproved(self):
            if "DISAPPROVED BY PDQ EDITORIAL BOARD" in self.statusKeys:
                return True
            return "WITHDRAWN" in self.statusKeys
        def isDuplicate(self):
            return not self.publishable and "DUPLICATE" in self.statusKeys
        def isLegacy(self):
            return "LEGACY - DO NOT PUBLISH" in self.statusKeys
        def isHeld(self):
            return "HOLD" in self.statusKeys

    def run(self):

        #--------------------------------------------------------------
        # Local variables.
        #--------------------------------------------------------------
        totalInScope     = 0
        totalCtGov       = 0
        totalCtGovWd     = 0
        totalResearch    = 0
        totalDisapproved = 0
        totalOutOfScope  = 0
        totalDuplicate   = 0
        totalLegacy      = 0
        totalHeld        = 0
        rowInScope       = 2
        rowCtGov         = 2
        rowCtGovWd       = 2
        rowResearch      = 2
        rowDisapproved   = 2
        rowOutOfScope    = 2
        rowDuplicate     = 2
        rowLegacy        = 2
        rowHold          = 2

        #--------------------------------------------------------------
        # Create the Excel workbook and the styles we need.
        #--------------------------------------------------------------
        wb = ExcelWriter.Workbook()
        b = ExcelWriter.Border()
        borders = ExcelWriter.Borders(b, b, b, b)
        font = ExcelWriter.Font(name = 'Arial', size = 10)
        align = ExcelWriter.Alignment('Left', 'Top', wrap = True)
        self.normalStyle = wb.addStyle(alignment = align, font = font,
                                       borders = borders)
        self.dateStyle = wb.addStyle(alignment = align, font = font,
                                     borders = borders,
                                     numFormat = 'YYYY-mm-dd')
        font = ExcelWriter.Font(name = 'Arial', size = 10, bold = True)
        labelStyle = wb.addStyle(alignment = align, font = font,
                                 borders = borders)

        #--------------------------------------------------------------
        # Create the book's sheets.
        #--------------------------------------------------------------
        wsInScope     = wb.addWorksheet("InScope", self.normalStyle)
        wsCtGov       = wb.addWorksheet("CTGov", self.normalStyle)
        wsCtGovWd     = wb.addWorksheet("CTGov_Withdrawn", self.normalStyle)
        wsResearch    = wb.addWorksheet("Research", self.normalStyle)
        wsDisapproved = wb.addWorksheet("Disapproved_Withdrawn",
                                        self.normalStyle)
        wsOutOfScope  = wb.addWorksheet("Out of Scope", self.normalStyle)
        wsDuplicate   = wb.addWorksheet("Duplicate", self.normalStyle)
        wsLegacy      = wb.addWorksheet("Legacy", self.normalStyle)
        wsHold        = wb.addWorksheet("Hold", self.normalStyle)

        #--------------------------------------------------------------
        # Set the column widths.
        #--------------------------------------------------------------
        colNum = 1
        titleWidth = 360
        protocolIdColNum = 3
        ctGovDocIdColNum = 1
        for w in (60, 60, 120, 100, 100, 100, 100, 80, 100, 80, 100):
            wsInScope.addCol(colNum, w)
            wsResearch.addCol(colNum, w)
            wsDisapproved.addCol(colNum, w)
            wsDuplicate.addCol(colNum, w)
            wsLegacy.addCol(colNum, w)
            wsHold.addCol(colNum, w)
            if colNum == protocolIdColNum and self.includeProtTitle:
                colNum += 1
                wsInScope.addCol(colNum, titleWidth)
                wsResearch.addCol(colNum, titleWidth)
                wsDisapproved.addCol(colNum, titleWidth)
                wsDuplicate.addCol(colNum, titleWidth)
                wsLegacy.addCol(colNum, titleWidth)
                wsHold.addCol(colNum, titleWidth)
            colNum += 1
        colNum = 1
        for w in (120, 60, 100, 100, 100, 400):
            wsOutOfScope.addCol(colNum, w)
            colNum += 1
        colNum = 1
        for w in (60, 150, 80, 80, 160, 80):
            wsCtGov.addCol(colNum, w)
            wsCtGovWd.addCol(colNum, w)
            if colNum == ctGovDocIdColNum and self.includeProtTitle:
                colNum += 1
                wsCtGov.addCol(colNum, titleWidth)
                wsCtGovWd.addCol(colNum, titleWidth)
            colNum += 1

        #--------------------------------------------------------------
        # Create the label rows.
        #--------------------------------------------------------------
        headerRows   = (wsInScope.addRow(1, labelStyle),
                        wsResearch.addRow(1, labelStyle),
                        wsDisapproved.addRow(1, labelStyle),
                        wsDuplicate.addRow(1, labelStyle),
                        wsLegacy.addRow(1, labelStyle),
                        wsHold.addRow(1, labelStyle))
        colNum        = 1
        for label in ("CDR InScope ID", "CDR Scientific ID", "Protocol ID",
                      "Date Received", "Submission Complete", "Merge Date",
                      "Admin Processing Status", "Admin User",
                      "Scientific Processing Status", "Scientific User",
                      "Current Protocol Status"):
            for row in headerRows:
                row.addCell(colNum, label)
            if colNum == protocolIdColNum and self.includeProtTitle:
                colNum += 1
                for row in headerRows:
                    row.addCell(colNum, "Protocol Title")
            colNum += 1
        headerRows = (wsCtGov.addRow(1, labelStyle),
                      wsCtGovWd.addRow(1, labelStyle))
        colNum = 1
        for label in ("Doc ID", "OrgStudyID", "NCTID", "Date Created",
                      "AdminProcessingStatus", "Phase"):
            for row in headerRows:
                row.addCell(colNum, label)
            if colNum == ctGovDocIdColNum and self.includeProtTitle:
                colNum += 1
                for row in headerRows:
                    row.addCell(colNum, "Protocol Title")
            colNum += 1
        row = wsOutOfScope.addRow(1, labelStyle)
        colNum = 1
        for label in ("Primary ID", "Doc ID", "Date Received", "Source",
                      "Record Entered", "Original Title"):
            row.addCell(colNum, label)
            colNum += 1

        #--------------------------------------------------------------
        # Remember which documents are publishable so we can skip them.
        #--------------------------------------------------------------
        self.cursor.execute("CREATE TABLE #publishable (doc_id INTEGER)")
        self.conn.commit()
        self.cursor.execute("""\
            INSERT INTO #publishable
                 SELECT DISTINCT v.id
                   FROM doc_version v
                   JOIN doc_type t
                     ON v.doc_type = t.id
                  WHERE t.name IN ('InScopeProtocol', 'CTGovProtocol')
                    AND v.publishable = 'Y'""", timeout = 300)
        self.conn.commit()
        self.msg = "publishable protocols identified"
        self.cursor.execute("SELECT doc_id FROM #publishable")
        publishableDocs = {}
        for row in self.cursor.fetchall():
            publishableDocs[row[0]] = True
        self.job.setProgressMsg(self.msg)

        #--------------------------------------------------------------
        # Process the In-Scope Protocol documents.
        #--------------------------------------------------------------
        self.cursor.execute("""\
            SELECT d.id, d.active_status
              FROM document d
              JOIN doc_type t
                ON t.id = d.doc_type
             WHERE t.name = 'InScopeProtocol'
          ORDER BY d.id""", timeout = 300)
        rows = self.cursor.fetchall()
        n = len(rows)
        self.msg += "<br>%d InScopeProtocol documents selected" % n
        self.job.setProgressMsg(self.msg)
        i = 0
        msg = ''
        for cdrId, activeStatus in rows:
            docXml   = self.getDocXml(cdrId).encode('utf-8')
            try:
                dom  = xml.dom.minidom.parseString(docXml)
            except Exception, e:
                cdr.logwrite("failure parsing CDR%d: %s" % (cdrId, e), LOGFILE)
                continue
            docElem  = dom.documentElement
            pub      = cdrId in publishableDocs
            protocol = self.InScopeProtocol(self, cdrId, docElem, pub)
            used     = False

            if protocol.isResearch():
                used = True
                rowResearch += self.addInScopeProtocol(wsResearch,
                                                       rowResearch,
                                                       protocol)
                totalResearch += 1
            if protocol.isDisapproved():
                used = True
                rowDisapproved += self.addInScopeProtocol(wsDisapproved,
                                                          rowDisapproved,
                                                          protocol)
                totalDisapproved += 1
            if protocol.isDuplicate():
                used = True
                rowDuplicate += self.addInScopeProtocol(wsDuplicate,
                                                        rowDuplicate,
                                                        protocol)
                totalDuplicate += 1
            if protocol.isLegacy():
                used = True
                rowLegacy += self.addInScopeProtocol(wsLegacy,
                                                     rowLegacy,
                                                     protocol)
                totalLegacy += 1
            if protocol.isHeld():
                used = True
                rowHold += self.addInScopeProtocol(wsHold,
                                                   rowHold,
                                                   protocol)
                totalHeld += 1
            if not used and not protocol.publishable and activeStatus == 'A':
                rowInScope += self.addInScopeProtocol(wsInScope,
                                                      rowInScope,
                                                      protocol)
                totalInScope += 1
            i += 1
            msg = "<br>loaded %d of %d InScopeProtocol documents" % (i, n)
            self.job.setProgressMsg(self.msg + msg)
        self.msg += msg
        self.addTotal(wsResearch, totalResearch, rowResearch)
        self.addTotal(wsDisapproved, totalDisapproved, rowDisapproved)
        self.addTotal(wsDuplicate, totalDuplicate, rowDuplicate)
        self.addTotal(wsInScope, totalInScope, rowInScope)
        self.addTotal(wsLegacy, totalLegacy, rowLegacy)
        self.addTotal(wsHold, totalHeld, rowHold)

        #--------------------------------------------------------------
        # Process the CTGov Protocol documents.
        #--------------------------------------------------------------
        self.cursor.execute("""\
            SELECT d.id
              FROM document d
              JOIN doc_type t
                ON t.id = d.doc_type
             WHERE t.name = 'CTGovProtocol'
               AND d.id NOT IN (SELECT doc_id FROM #publishable)
               AND d.active_status = 'A'
          ORDER BY d.id""", timeout = 300)
        ctGovProtocolIds = [row[0] for row in self.cursor.fetchall()]
        n = len(ctGovProtocolIds)
        i = 0
        self.msg += "<br>%d CTGovProtocol documents selected" % n
        self.job.setProgressMsg(self.msg)
        msg = ''
        for cdrId in ctGovProtocolIds:
            docXml  = self.getDocXml(cdrId).encode('utf-8')
            try:
                dom = xml.dom.minidom.parseString(docXml)
            except Exception, e:
                cdr.logwrite("failure parsing CDR%d: %s" % (cdrId, e), LOGFILE)
                continue

            docElem = dom.documentElement
            protocol = self.CTGovProtocol(self, cdrId, docElem)
            if 'WITHDRAWN' not in protocol.statusKeys:
                rowCtGov += self.addCtGovProtocol(wsCtGov, rowCtGov, protocol)
                totalCtGov += 1
            else:
                rowCtGovWd += self.addCtGovProtocol(wsCtGovWd, rowCtGovWd,
                                                    protocol)
                totalCtGovWd += 1
            i += 1
            msg = "<br>loaded %d of %d CTGovProtocol documents" % (i, n)
            self.job.setProgressMsg(self.msg + msg)
        self.msg += msg
        self.addTotal(wsCtGov, totalCtGov, rowCtGov)
        self.addTotal(wsCtGovWd, totalCtGovWd, rowCtGovWd)

        #--------------------------------------------------------------
        # Process the Out-Of-Scope Protocol documents.
        #--------------------------------------------------------------
        self.cursor.execute("""\
            SELECT d.id
              FROM document d
              JOIN doc_type t
                ON t.id = d.doc_type
             WHERE t.name = 'OutOfScopeProtocol'
          ORDER BY d.id""", timeout = 300)
        outOfScopeProtocolIds = [row[0] for row in self.cursor.fetchall()]
        n = len(outOfScopeProtocolIds)
        i = 0
        self.msg += "<br>%d OutOfScopeProtocol documents selected" % n
        self.job.setProgressMsg(self.msg)
        msg = ''
        for cdrId in outOfScopeProtocolIds:
            docXml  = self.getDocXml(cdrId).encode('utf-8')
            try:
                dom = xml.dom.minidom.parseString(docXml)
            except Exception, e:
                cdr.logwrite("failure parsing CDR%d: %s" % (cdrId, e), LOGFILE)
                continue
            docElem = dom.documentElement
            protocol = self.OutOfScopeProtocol(self, cdrId, docElem)
            rowOutOfScope += self.addOutOfScopeProtocol(wsOutOfScope,
                                                        rowOutOfScope,
                                                        protocol)
            i += 1
            msg = "<br>loaded %d of %d OutOfScopeProtocol documents" % (i, n)
            self.job.setProgressMsg(self.msg + msg)
            totalOutOfScope += 1
        self.msg += msg
        self.addTotal(wsOutOfScope, totalOutOfScope, rowOutOfScope)

        #--------------------------------------------------------------
        # Write out the report and tell the user where it is.
        #--------------------------------------------------------------
        name = "ProtocolProcessing-%d.xls" % job.getJobId()
        f = open(REPORTS_BASE + "/" + name, "wb")
        wb.write(f, True)
        f.close()
        cdr.logwrite("saving %s" % (REPORTS_BASE + "/" + name), LOGFILE)
        ### url = "http://%s%s/GetReportWorkbook.py?name=%s" % (cdrcgi.WEBSERVER,
        ###                                                     cdrcgi.BASE,
        ###                                                     name)
        url = "%s%s/GetReportWorkbook.py?name=%s" % (cdr.CBIIT_NAMES[2],
                                                            cdrcgi.BASE,
                                                            name)
        cdr.logwrite("url: %s" % url, LOGFILE)
        self.msg += ("<br>Report available at <a href='%s'><u>%s</u></a>." %
                     (url, url))

        body = """\
The Protocol Processing report you requested can be viewed at
%s.
""" % (url)
        if cdr.h.org == 'OCE':
            subject   = "%s: Report results - Prot processing" % \
                                                    cdr.PUB_NAME.capitalize()
        else:
            subject   = "%s-%s: Report results - Prot processing" % (
                                                       cdr.h.org, cdr.h.tier)

        sendMail(job, subject, body)
        job.setProgressMsg(self.msg)
        job.setStatus(cdrbatch.ST_COMPLETED)
        cdr.logwrite("Completed report", LOGFILE)

    def addInScopeProtocol(self, sheet, rowNum, prot):
        nRows = len(prot.protocolSources) or 1
        mergeRows = nRows - 1
        row = sheet.addRow(rowNum, self.normalStyle)
        if prot.inScopeId:
            row.addCell(1, prot.inScopeId, 'Number', mergeDown = mergeRows)
        else:
            row.addCell(1, '', mergeDown = mergeRows)
        if prot.scientificId:
            row.addCell(2, prot.scientificId, 'Number', mergeDown = mergeRows)
        else:
            row.addCell(2, '', mergeDown = mergeRows)
        if prot.protocolId:
            row.addCell(3, prot.protocolId, mergeDown = mergeRows)
        else:
            row.addCell(3, '', mergeDown = mergeRows)
        if self.includeProtTitle:
            row.addCell(4, prot.title or '', mergeDown = mergeRows)
            extra = 1
        else:
            extra = 0
        if prot.protocolSources:
            i = 0
            for s in prot.protocolSources:
                r = i and sheet.addRow(rowNum + i, self.normalStyle) or row
                if s.receiptDate:
                    d = self.fixDate(s.receiptDate)
                    r.addCell(4 + extra, d) #, 'DateTime', self.dateStyle)
                else:
                    r.addCell(4 + extra, '')
                if s.dateSubmissionComplete:
                    d = self.fixDate(s.dateSubmissionComplete)
                    r.addCell(5 + extra, d) #, 'DateTime', self.dateStyle)
                else:
                    r.addCell(5 + extra, '')
                if s.mergeByDate:
                    d = self.fixDate(s.mergeByDate)
                    r.addCell(6 + extra, d) #, 'DateTime', self.dateStyle)
                else:
                    r.addCell(6 + extra, '')
                i += 1
        else:
            row.addCell(4 + extra, '')
            row.addCell(5 + extra, '')
            row.addCell(6 + extra, '')
        row.addCell(7 + extra, self.mergeValues(prot.adminStatuses),
                    mergeDown = mergeRows)
        row.addCell(8 + extra, self.mergeValues(prot.adminUsers),
                    mergeDown = mergeRows)
        row.addCell(9 + extra, self.mergeValues(prot.scientificStatuses),
                    mergeDown = mergeRows)
        row.addCell(10 + extra, self.mergeValues(prot.scientificUsers),
                    mergeDown = mergeRows)
        status = prot.protocolStatus
        if status == 'No valid lead organization status found.':
            status = ''
        row.addCell(11 + extra, status or '', mergeDown = mergeRows)
        return nRows

    def addCtGovProtocol(self, sheet, rowNum, prot):
        row = sheet.addRow(rowNum, self.normalStyle)
        if prot.cdrId:
            row.addCell(1, prot.cdrId, 'Number')
        else:
            row.addCell(1, '')
        if self.includeProtTitle:
            extra = 1
            row.addCell(2, prot.title or '')
        else:
            extra = 0
        row.addCell(2 + extra, prot.orgStudyId or '')
        row.addCell(3 + extra, prot.nctId or '')
        if prot.dateCreated:
            d = self.fixDate(prot.dateCreated)
            row.addCell(4 + extra, d) #, 'DateTime', self.dateStyle)
        else:
            row.addCell(4 + extra, '')
        row.addCell(5 + extra, self.mergeValues(prot.statuses))
        row.addCell(6 + extra, '; '.join(prot.phases))
        return 1

    def addOutOfScopeProtocol(self, sheet, rowNum, prot):
        nRows = len(prot.sources) or 1
        mergeRows = nRows - 1
        row = sheet.addRow(rowNum, self.normalStyle)
        row.addCell(1, prot.primaryId or '')
        if prot.cdrId:
            row.addCell(2, prot.cdrId, 'Number', mergeDown = mergeRows)
        else:
            row.addCell(2, '', mergeDown = mergeRows)
        if prot.sources:
            i = 0
            for s in prot.sources:
                r = i and sheet.addRow(rowNum + i, self.normalStyle) or row
                if s.receiptDate:
                    d = self.fixDate(s.receiptDate)
                    r.addCell(3, d) #, 'DateTime', self.dateStyle)
                else:
                    r.addCell(3, '')
                r.addCell(4, s.name or '')
                i += 1
        else:
            row.addCell(3, '')
            row.addCell(4, '')
        if prot.recordEntered:
            d = self.fixDate(prot.recordEntered)
            row.addCell(5, d, #'DateTime', self.dateStyle,
                        mergeDown = mergeRows)
        else:
            row.addCell(5, '', mergeDown = mergeRows)
        row.addCell(6, prot.originalTitle or '', mergeDown = mergeRows)
        return nRows

    def mergeValues(self, statuses):
        return u'\n'.join(statuses)

    def addTotal(self, sheet, total, rowNum):
        row = sheet.addRow(rowNum + 2, self.normalStyle)
        row.addCell(1, "COUNT of UNIQUE Trials")
        row.addCell(2, total, "Number")

    def testReport():
        import sys
        class Job:
            def setProgressMsg(self, msg):
                sys.stderr.write("%s\n" % msg)
        report = ProtocolProcessingStatusReport(Job())
        report.run()
    testReport = staticmethod(testReport)

    def getDocXml(self, id):
        self.cursor.execute("SELECT xml FROM document WHERE id = ?", id)
        return self.cursor.fetchall()[0][0]

    def fixDate(self, d):
        if not d:
            return ""
        if len(d) > 10:
            return d[:10] #+ 'T' + d[11:]
        return d

#----------------------------------------------------------------------
# The Spanish version of this report now takes too long to run as CGI.
# This is because the requirements were modified to have the software
# find values even when they were wrapped in editing markup (Insertion
# and Deletion elements) which cased the values to be stripped out
# at document save time when query terms are extracted from the document.
# This means that we must parse each document and look for the values
# by hand, with no fixed expectation of what the exact paths will be
# for the elements we're looking for.  See request #1861.
#----------------------------------------------------------------------
class SpanishGlossaryTermsByStatus:

    #------------------------------------------------------------------
    # Capture the parameters used to create the report's job.
    #------------------------------------------------------------------
    def __init__(self, job):
        self.job    = job
        self.start  = job.getParm('from')
        self.end    = job.getParm('to')
        self.status = job.getParm('status')

    #------------------------------------------------------------------
    # Generate the report.
    #------------------------------------------------------------------
    def run(self):

        # Get a list of all glossary term document IDs.
        start = time.time()
        conn = cdrdb.connect('CdrGuest')
        cursor = conn.cursor()
        cursor.execute("""\
            SELECT d.id
              FROM document d
              JOIN doc_type t
                ON d.doc_type = t.id
             WHERE t.name = 'GlossaryTerm'""")
        docIds = [row[0] for row in cursor.fetchall()]

        # Walk through the list, parsing each document.
        terms = []
        counter = 0
        for docId in docIds:
            doc = cdr.getDoc('guest', docId, getObject = True)
            dom = xml.dom.minidom.parseString(doc.xml)
            term = self.GlossaryTerm(docId, dom.documentElement)
            counter += 1
            now = time.time()
            timer = getElapsed(start, now)
            msg = ("Processed %d of %d glossary terms; elapsed: %s" %
                   (counter, len(docIds), timer))
            self.job.setProgressMsg(msg)

            # Determine whether the term should be included on the report.
            if term.matches(self.status, self.start, self.end):
                terms.append(term)

        # Create HTML for the report.
        self.job.setProgressMsg("processed %d of %d" % (counter, len(docIds)))
        terms.sort()
        html = [u"""\
<!DOCTYPE HTML PUBLIC '-//IETF//DTD HTML//EN'>
<html>
 <head>
  <title>Spanish Glossary Terms by Status</title>
  <style type 'text/css'>
   body    { font-family: Arial, Helvetica, sans-serif }
   span.t1 { font-size: 14pt; font-weight: bold }
   span.t2 { font-size: 12pt; font-weight: bold }
   th      { font-size: 10pt; font-weight: bold }
   td      { font-size: 10pt; font-weight: normal }
   @page   { margin-left: 0cm; margin-right: 0cm; }
   body, table   { margin-left: 0cm; margin-right: 0cm; }
  </style>
 </head>
 <body>
  <center>
   <span class='t1'>Spanish Glossary Terms by Status</span>
   <br />
   <br />
   <span class='t2'>%s Terms<br />From %s to %s<br />Total: %d</span>
  </center>
  <table border='1' cellspacing='0' cellpadding='2' width='100%%'>
""" % (self.status, self.start, self.end, len(terms))]

        if self.status.upper() == 'TRANSLATION REVISION PENDING':
            html.append(u"""\
   <tr>
    <th>CDR ID</th>
    <th>English Term</th>
    <th>Spanish Term</th>
    <th>Approved English Definition Revision</th>
    <th>Pending Spanish Translation Revision</th>
    <th>Translation Resource</th>
    <th>Status Date</th>
    <th>Comment</th>
   </tr>
""")
        else:
            html.append(u"""\
   <tr>
    <th>CDR ID</th>
    <th>English Term</th>
    <th>Spanish Term</th>
    <th>English Definition</th>
    <th>Spanish Translation</th>
    <th>Translation Resource</th>
    <th>Status Date</th>
    <th>Comment</th>
   </tr>
""")

        # Add one row for each glossary term included on the report.
        for term in terms:
            englishDefs = [d.text for d in term.definitions]
            spanishDefs = []
            resources   = []
            statusDates = []
            comments    = []
            for definition in term.spanishDefinitions:
                if definition.comment:
                    comments.append(definition.comment)
                if definition.text:
                    spanishDefs.append(definition.text)
                if definition.translationResource:
                    resources.append(definition.translationResource)
                if definition.statusDate:
                    statusDates.append(definition.statusDate)
            html.append(u"""\
   <tr>
    <td>%d</td>
    <td>%s</td>
    <td>%s</td>
    <td>%s</td>
    <td>%s</td>
    <td>%s</td>
    <td>%s</td>
    <td>%s</td>
   </tr>
""" % (term.id,
       term.name or u"&nbsp;",
       term.spanishName or u"&nbsp;",
       englishDefs and "; ".join(englishDefs) or u"&nbsp;",
       spanishDefs and "; ".join(spanishDefs) or u"&nbsp;",
       resources   and "; ".join(resources)   or u"&nbsp;",
       statusDates and "; ".join(statusDates) or u"&nbsp;",
       comments    and "; ".join(comments)    or u"&nbsp;"))
        html.append(u"""\
  </table>
 </body>
</html>
""")

        # Save the report and tell the user(s) where to find it.
        html = u"".join(html)
        reportName = ("/SpanishGlossaryTermsByStatus-Job%d.html" %
                      job.getJobId())
        cdr.logwrite("reportName: %s" % reportName, LOGFILE)
        ### url = "http://%s/CdrReports%s" % (cdrcgi.WEBSERVER, reportName)
        url = "%s/CdrReports%s" % (cdr.CBIIT_NAMES[2], reportName)
        cdr.logwrite("url: %s" % url, LOGFILE)
        msg += "<br>Report available at <a href='%s'><u>%s</u></a>." % (url,
                                                                        url)
        htmlFile = open(REPORTS_BASE + reportName, "w")
        cdr.logwrite("writing %s" % (REPORTS_BASE + reportName), LOGFILE)
        htmlFile.write(cdrcgi.unicodeToLatin1(html))
        htmlFile.close()
        body = """\
The report you requested on Spanish Glossary Terms by Status can be viewed at:

%s.
    """ % url
        if cdr.h.org == 'OCE':
            subject   = "%s: Report results - Glossary term (ES)" % \
                                                   cdr.PUB_NAME.capitalize()
        else:
            subject   = "%s-%s: Report results - Glossary term (ES)" % (
                                                      cdr.h.org, cdr.h.tier)

        sendMail(job, subject, body)
        job.setProgressMsg(msg)
        job.setStatus(cdrbatch.ST_COMPLETED)
        cdr.logwrite("Completed report", LOGFILE)

    #----------------------------------------------------------------------
    # Recursively extract the complete content of an element, tags and all.
    #----------------------------------------------------------------------
    def getNodeContent(node, pieces = None):
        if pieces is None:
            pieces = []
        strikeout = u"<span style='text-decoration: line-through'>"
        red       = u"<span style='color: red'>"
        for child in node.childNodes:
            if child.nodeType in (child.TEXT_NODE, child.CDATA_SECTION_NODE):
                if child.nodeValue:
                    pieces.append(xml.sax.saxutils.escape(child.nodeValue))
            elif child.nodeType == child.ELEMENT_NODE:
                if child.nodeName == 'Insertion':
                    pieces.append(red)
                    SpanishGlossaryTermsByStatus.getNodeContent(child, pieces)
                    pieces.append(u"</span>")
                elif child.nodeName == 'Deletion':
                    pieces.append(strikeout)
                    SpanishGlossaryTermsByStatus.getNodeContent(child, pieces)
                    pieces.append(u"</span>")
                elif child.nodeName == 'Strong':
                    pieces.append(u"<b>")
                    SpanishGlossaryTermsByStatus.getNodeContent(child, pieces)
                    pieces.append(u"</b>")
                elif child.nodeName in ('Emphasis', 'ScientificName'):
                    pieces.append(u"<i>")
                    SpanishGlossaryTermsByStatus.getNodeContent(child, pieces)
                    pieces.append(u"</i>")
                else:
                    SpanishGlossaryTermsByStatus.getNodeContent(child, pieces)
        return u"".join(pieces)
    getNodeContent = staticmethod(getNodeContent)

    #----------------------------------------------------------------------
    # Object to represent the Spanish and English for a glossary term.
    #----------------------------------------------------------------------
    class GlossaryTerm:

        def __init__(self, id, node):
            self.id = id
            self.name = None
            self.spanishName = None
            self.definitions = []
            self.spanishDefinitions = []
            gnc = SpanishGlossaryTermsByStatus.getNodeContent
            for child in node.getElementsByTagName('TermName'):
                self.name = gnc(child)
            for child in node.getElementsByTagName('SpanishTermName'):
                self.spanishName = gnc(child)
            for child in node.getElementsByTagName('TermDefinition'):
                self.definitions.append(self.Definition(child))
            for child in node.getElementsByTagName('SpanishTermDefinition'):
                self.spanishDefinitions.append(self.Definition(child, True))

        # Determine whether the glossary term should be included in the report.
        def matches(self, status, startDate, endDate):
            for d in self.spanishDefinitions:
                if d.status == status:
                    if d.statusDate >= startDate:
                        if d.statusDate <= endDate:
                            return True
            return False

        def __cmp__(self, other):
            return cmp(self.name, other.name)

        #------------------------------------------------------------------
        # Object for a term's Spanish or English definition.
        #------------------------------------------------------------------
        class Definition:
            def __init__(self, node, spanish = False):
                gnc = SpanishGlossaryTermsByStatus.getNodeContent
                self.spanish = spanish
                self.text = None
                self.status = None
                self.statusDate = None
                self.comment = None
                self.translationResource = None
                for child in node.getElementsByTagName('DefinitionText'):
                    self.text = gnc(child)
                if spanish:
                    for e in node.getElementsByTagName('TranslationResource'):
                        self.translationResource = gnc(e)
                    for e in node.getElementsByTagName('DefinitionStatus'):
                        self.status = gnc(e)
                    for e in node.getElementsByTagName('StatusDate'):
                        self.statusDate = gnc(e)
                    for e in node.getElementsByTagName('Comment'):
                        comment = cdr.getTextContent(e).strip()
                        if comment:
                            self.comment = xml.sax.saxutils.escape(comment)

# ---------------------------------------------------------------------
# This Protocol Owner Transfer report takes a long time because
# several elements have to be displayed for which we need to identify
# the node location.  When a function has to be used within the query
# the indeces are not being used.
# ---------------------------------------------------------------------
class ProtocolOwnershipTransfer:
    def __init__(self, host = 'localhost'):
        #--------------------------------------------------------------
        # Set up a database connection and cursor.
        #--------------------------------------------------------------
        import cdrutil
        h = cdrutil.AppHost(cdrutil.getEnvironment(), cdrutil.getTier(),
                            filename = cdr.WORK_DRIVE + ':/etc/cdrapphosts.rc')
        if h.org == 'OCE':
            host = 'localhost'
        else:
            host = h.host['DBWIN'][0]

        self.conn = cdrdb.connect('CdrGuest', dataSource = host)
        self.cursor = self.conn.cursor()

    # -------------------------------------------------------------------
    # Transferred Protocols class to identify the elements requested
    # The information for blocked and CTGov blocked protocols is being
    # populated by calling a function for each protocol found.
    # -------------------------------------------------------------------
    class TransferredProtocols:
        def __init__(self, cursor):
            self.protocols = {}

            #cursor.execute("""\
            #   SELECT t.doc_id, pid.value AS "Primary ID", n.value AS "NCT-ID",
            #          t.value AS "Owner Org", p.value AS "PRS User",
            #          d.value AS "Tr Date", s.value AS "Status"
            #     FROM query_term t
            #     JOIN query_term pid
            #       ON t.doc_id  = pid.doc_id
            #      AND pid.path  = '/InScopeProtocol/ProtocolIDs/PrimaryID' +
            #                      '/IDString'
            #     JOIN query_term n
            #       ON t.doc_id  = n.doc_id
            #      AND n.path = '/InScopeProtocol/ProtocolIDs/OtherID/IDString'
            #     JOIN query_term nct
            #       ON t.doc_id  = nct.doc_id
            #      AND nct.path  = '/InScopeProtocol/ProtocolIDs/OtherID/IDType'
            #      AND nct.value = 'ClinicalTrials.gov ID'
            #      AND left(n.node_loc, 8) = left(nct.node_loc, 8)
            #     JOIN query_term p
            #       ON t.doc_id  = p.doc_id
            #      AND p.path = '/InScopeProtocol/CTGovOwnershipTransferInfo' +
            #                   '/PRSUserName'
            #     JOIN query_term d
            #       ON t.doc_id  = d.doc_id
            #      AND d.path = '/InScopeProtocol/CTGovOwnershipTransferInfo' +
            #                   '/CTGovOwnershipTransferDate'
            #     JOIN query_term s
            #       ON t.doc_id  = s.doc_id
            #      AND s.path = '/InScopeProtocol/ProtocolAdminInfo'          +
            #                   '/CurrentProtocolStatus'
            #    where t.path = '/InScopeProtocol/CTGovOwnershipTransferInfo' +
            #                   '/CTGovOwnerOrganization' """)

            cursor.execute("""\
               SELECT t.doc_id, pid.value AS "Primary ID", n.value AS "NCT-ID",
                      t.value AS "Owner Org", p.value AS "PRS User",
                      d.value AS "Tr Date", s.value AS "Status",
                      oo.value AS "OrgName",
                      o.int_val AS "Org-ID", dt.name AS "DocType"
                 INTO #tprotocols
                 FROM query_term t
                 JOIN document doc
                   ON doc.id = t.doc_id
                 JOIN doc_type dt
                   ON doc.doc_type = dt.id
                 JOIN query_term pid
                   ON t.doc_id  = pid.doc_id
                  AND pid.path  = '/CTGovProtocol/PDQAdminInfo' +
                                  '/PDQProtocolIDs/PrimaryID/IDString'
                 JOIN query_term n
                   ON t.doc_id  = n.doc_id
                  AND n.path = '/CTGovProtocol/IDInfo/NCTID'
                 JOIN query_term p
                   ON t.doc_id  = p.doc_id
                  AND p.path    = '/CTGovProtocol/PDQAdminInfo' +
                                  '/CTGovOwnershipTransferInfo' +
                                  '/PRSUserName'
                 JOIN query_term d
                   ON t.doc_id  = d.doc_id
                  AND d.path    = '/CTGovProtocol/PDQAdminInfo' +
                                  '/CTGovOwnershipTransferInfo' +
                                  '/CTGovOwnershipTransferDate'
                 JOIN query_term s
                   ON t.doc_id  = s.doc_id
                  AND s.path    = '/CTGovProtocol/OverallStatus'
                 JOIN query_term o
                   ON t.doc_id  = o.doc_id
                  AND o.path    = '/CTGovProtocol/Location/Facility' +
                                  '/Name/@cdr:ref'
                  -- AND o.int_val = %s
                 JOIN query_term oo
                   ON oo.doc_id = o.int_val
                  AND oo.path   = '/Organization/OrganizationnameInformation' +
                                  '/OfficialName/Name'
                WHERE t.path    = '/CTGovProtocol/PDQAdminInfo' +
                                  '/CTGovOwnershipTransferInfo' +
                                  '/CTGovOwnerOrganization'
                  AND dt.name in ('CTGovProtocol', 'InScopeProtocol')
                --AND n.value like 'NCT%%'
                ORDER BY s.value, n.value""", timeout = 300)

            # This query is not necessary if we're only looking at
            # CTGovProtocols but we need it if InScopeProtocols need to be
            # considered.
            # I'm still waiting if we'll need to look at both doc types.
            # -------------------------------------------------------------
            cursor.execute("""\
                SELECT *
                  FROM #tprotocols
                 WHERE "NCT-ID" like 'NCT%'
                 ORDER BY doc_id""")

            rows = cursor.fetchall()

            # Assign all the elements to the protocols dictionary
            # ---------------------------------------------------
            for row in rows:
                cdrId = row[0]
                self.protocols[row[0]] = {u'pId' : row[1],
                                          u'nctId'  : row[2],
                                          u'trOrg'  : row[3],
                                          u'trUser'  : row[4],
                                          u'trDate'  : row[5],
                                          u'status'  : row[6],
                                          u'orgName' : row[7],
                                          u'orgId'   : row[8],
                                          u'docType' : row[9]}

                self.protocols[row[0]][u'blocked']  = \
                                          self.checkBlocked(cdrId, cursor)
                self.protocols[row[0]][u'ctgovBlk'] = \
                                          self.checkCtgovBlocked(cdrId,
                                                                 row[9],
                                                                 cursor)
                self.protocols[row[0]][u'grantNo'] = \
                                          getGrantNo(cdrId, row[9], cursor)

        # --------------------------------------
        # Check if a protocol is blocked or not
        # --------------------------------------
        def checkBlocked(self, id, cursor):
            cursor.execute("""\
               SELECT *
                 FROM document
                WHERE id = %s
                  AND active_status = 'A' """ % id)
            row = cursor.fetchone()

            if row:  return 'No'
            return 'Yes'

        # -------------------------------------------------
        # Check if a protocol is blocked from CTGov or not
        # -------------------------------------------------
        def checkCtgovBlocked(self, id, docType, cursor):
            if docType == 'CTGovProtocol':
                return 'N/A'

            cursor.execute("""\
               SELECT *
                 FROM query_term
                WHERE doc_id = %s
                  AND path = '/InScopeProtocol/BlockedFromCTGov' """ % id)
            row = cursor.fetchone()

            if row: return 'Yes'
            return 'No'


    # -------------------------------------------------------------------
    # Not Transferred Protocols class to identify the elements requested
    # Only PUP information needs to get populated.  We're doing this by
    # sending the vendor filter output through a new filter extracting
    # this information.
    # -------------------------------------------------------------------
    class NotTransferredProtocols:

        # ---------------------------------------------------------------
        # PUP class to identify the protocol update person information
        # If the personRole is specified as 'Protocol chair' the chair's
        # information is selected instead.
        # ---------------------------------------------------------------
        class PUP:
            def __init__(self, id, cursor, persRole = 'Update person'):
                self.cdrId       = id
                self.persId      = None
                self.persFname   = None
                self.persLname   = None
                self.persPhone   = None
                self.persEmail   = None
                self.persContact = None
                self.persRole    = persRole

                # Get the person name
                # -------------------
                cursor.execute("""\
                  SELECT q.doc_id, u.int_val,
                         g.value as "FName", l.value as "LName",
                         c.value as Contact
                    FROM query_term_pub q
                    JOIN query_term_pub u
                      ON q.doc_id = u.doc_id
                     AND u.path   = '/InScopeProtocol/ProtocolAdminInfo' +
                                    '/ProtocolLeadOrg/LeadOrgPersonnel'  +
                                    '/Person/@cdr:ref'
                     AND left(q.node_loc, 12) = left(u.node_loc, 12)
                    JOIN query_term g
                      ON u.int_val = g.doc_id
                     AND g.path   = '/Person/PersonNameInformation/GivenName'
                    JOIN query_term l
                      ON g.doc_id = l.doc_id
                     AND l.path   = '/Person/PersonNameInformation/SurName'
                    JOIN query_term c
                      ON g.doc_id = c.doc_id
                     AND c.path   = '/Person/PersonLocations/CIPSContact'
                   WHERE q.doc_id = %s
                     AND q.value  = '%s'
                """ % (self.cdrId, self.persRole))

                rows = cursor.fetchall()

                for row in rows:
                    self.cdrId       = row[0]
                    self.persId      = row[1]
                    self.persFname   = row[2]
                    self.persLname   = row[3]
                    self.persContact = row[4]

                # Get the person's email and phone if a PUP was found
                # ---------------------------------------------------
                if self.persId:
                    cursor.execute("""\
                  SELECT q.doc_id, c.value, p.value, e.value
                    FROM query_term q
                    JOIN query_term c
                      ON c.doc_id = q.doc_id
                     AND c.path = '/Person/PersonLocations' +
                                  '/OtherPracticeLocation/@cdr:id'
         LEFT OUTER JOIN query_term p
                      ON c.doc_id = p.doc_id
                     AND p.path = '/Person/PersonLocations' +
                                  '/OtherPracticeLocation/SpecificPhone'
                     AND LEFT(c.node_loc, 8) = LEFT(p.node_loc, 8)
         LEFT OUTER JOIN query_term e
                      ON c.doc_id = e.doc_id
                     AND e.path = '/Person/PersonLocations' +
                                  '/OtherPracticeLocation/SpecificEmail'
                     AND LEFT(c.node_loc, 8) = LEFT(e.node_loc, 8)
                   WHERE q.path = '/Person/PersonLocations/CIPSContact'
                     AND q.value = c.value
                     AND q.doc_id = %s
                    """ % self.persId)

                    rows = cursor.fetchall()

                    for row in rows:
                        self.persPhone   = row[2]
                        self.persEmail   = row[3]


        # -----------------------------------------------------
        # Create the dictionary holding all protocols with the
        # information to display on the spreadsheet.
        # -----------------------------------------------------
        def __init__(self, cursor):
            self.protocols = {}

            cursor.execute("""\
                SELECT s.doc_id, pid.value AS "Primary ID", n.value AS "NCT-ID",
                       r.value as "TResponse", d.value as "TR date",
                       t.value as "Log date",
                       s.value AS "Status",  sd.value AS "Status Date" --,
                       -- ln.value AS "OrgName"
                  FROM query_term_pub s
                  JOIN query_term pid
                    ON s.doc_id  = pid.doc_id
                   AND pid.path  = '/InScopeProtocol/ProtocolIDs/PrimaryID' +
                                   '/IDString'
                  JOIN query_term n
                    ON s.doc_id  = n.doc_id
                   AND n.path    = '/InScopeProtocol/ProtocolIDs/OtherID'   +
                                   '/IDString'
                  JOIN query_term nct
                    ON s.doc_id  = nct.doc_id
                   AND nct.path  = '/InScopeProtocol/ProtocolIDs/OtherID'   +
                                   '/IDType'
                   AND nct.value = 'ClinicalTrials.gov ID'
                   AND left(n.node_loc, 8) = left(nct.node_loc, 8)
                  JOIN query_term sd
                    ON s.doc_id  = sd.doc_id
                   AND sd.path   = '/InScopeProtocol/ProtocolAdminInfo'     +
                                   '/ProtocolLeadOrg/LeadOrgProtocolStatuses' +
                                   '/CurrentOrgStatus/StatusDate'
                  JOIN query_term sp
                    ON s.doc_id  = sp.doc_id
                   AND sp.path   = '/InScopeProtocol/ProtocolAdminInfo'      +
                                   '/ProtocolLeadOrg/LeadOrgRole'
                   AND sp.value  = 'Primary'
                   AND left(sp.node_loc, 8) = left(sd.node_loc, 8)
       LEFT OUTER JOIN query_term d
                    ON s.doc_id  = d.doc_id
                   AND d.path    = '/InScopeProtocol'                        +
                                   '/CTGovOwnershipTransferInfo'             +
                                   '/CTGovOwnershipTransferDate'
       LEFT OUTER JOIN query_term r
                    ON s.doc_id  = r.doc_id
                   AND r.path    = '/InScopeProtocol'                        +
                                   '/CTGovOwnershipTransferContactLog'       +
                                   '/CTGovOwnershipTransferContactResponse'
       LEFT OUTER JOIN query_term t
                    ON s.doc_id  = t.doc_id
                   AND t.path    = '/InScopeProtocol'                        +
                                   '/CTGovOwnershipTransferContactLog'       +
                                   '/Date'
                 WHERE s.path    = '/InScopeProtocol/ProtocolAdminInfo'      +
                                   '/CurrentProtocolStatus'
                   AND d.value is null
            """, timeout = 300)

            rows = cursor.fetchall()

            job.setProgressMsg("Selecting PUP, Org Name, and Source")
            for row in rows:
                cdrId = row[0]
                self.protocols[cdrId] = {u'pId'      : row[1],
                                         u'nctId'    : row[2],
                                         u'tResponse': row[3],
                                         u'trDate'   : row[4],
                                         u'logDate'  : row[5],
                                         u'status'   : row[6],
                                         u'statDate' : row[7]}

                # Populate the Orgname
                # --------------------
                self.protocols[cdrId][u'orgName'] = \
                                      self.getOrgName(cdrId, cursor)

                # Populate the PUP information
                pup = self.PUP(cdrId, cursor)
                if pup.persId:
                    self.protocols[cdrId][u'pup'] = pup
                else:
                    pup = self.PUP(cdrId, cursor, 'Protocol chair')
                    self.protocols[cdrId][u'pup'] = pup

                # Populate the Source information
                # -------------------------------
                self.protocols[cdrId][u'protSource'] = \
                                      self.getSource(cdrId, cursor)

                # Populate the GrantNo information
                # --------------------------------
                self.protocols[cdrId][u'grantNo'] = \
                                          getGrantNo(cdrId,
                                                     'InScopeProtocol',
                                                     cursor)

                # Populate the Phase information
                # --------------------------------
                self.protocols[cdrId][u'phase'] = \
                                          getPhase(cdrId, cursor)

                # Populate the FDA information
                # --------------------------------
                fdaInfo = getFdaInfo(cdrId, cursor)
                if fdaInfo:
                    self.protocols[cdrId][u'fdaReg'] = fdaInfo[0]
                    self.protocols[cdrId][u'fda801'] = fdaInfo[1]
                    self.protocols[cdrId][u'fdaInd'] = fdaInfo[2]

        # ---------------------------------------
        # Getting the official organization name
        # ---------------------------------------
        def getOrgName(self, id, cursor):
            cursor.execute("""\
                SELECT ln.value
                  FROM query_term lo
                  JOIN query_term ln
                    ON lo.int_val = ln.doc_id
                   AND ln.path = '/Organization/OrganizationNameInformation' +
                                 '/OfficialName/Name'
                  JOIN query_term sp
                    ON lo.doc_id = sp.doc_id
                   AND sp.path = '/InScopeProtocol/ProtocolAdminInfo' +
                                 '/ProtocolLeadOrg/LeadOrgRole'
                   AND sp.value = 'Primary'
                   AND left(sp.node_loc, 8) = left(lo.node_loc, 8)
                 WHERE lo.doc_id = %s
                   AND lo.path = '/InScopeProtocol/ProtocolAdminInfo' +
                                 '/ProtocolLeadOrg/LeadOrganizationId/@cdr:ref'
            """ % id)
            row = cursor.fetchone()

            return row[0]


        # -------------------------------------------
        # Getting the Protocol SourceName information
        # -------------------------------------------
        def getSource(self, id, cursor):
            cursor.execute("""\
                SELECT value
                  FROM query_term
                 WHERE doc_id = %s
                   AND path = '/InScopeProtocol/ProtocolSources' +
                              '/ProtocolSource/SourceName'
            """ % id)
            rows = cursor.fetchall()
            sourceNames = []
            for row in rows:
                sourceNames.append(row[0])

            sourceNames.sort()

            return ", ".join(["%s" % s for s in sourceNames])


    # ----------------------------------------------------------------
    # Creating a spreadsheet for displaying the protocols transferred
    # and not yet transferred to CT.gov
    # ----------------------------------------------------------------
    def createTransferSpreadsheet(self, job):

        now = time.localtime()
        startDate = list(now)
        endDate   = list(now)

        job.setProgressMsg("Job started")

        # Transferred Protocols
        job.setProgressMsg("Selecting transferred protocols")
        tps  = self.TransferredProtocols(self.cursor)

        # Not Transferred Protocols
        job.setProgressMsg("Selecting non-transferred protocols")
        ntps = self.NotTransferredProtocols(self.cursor)

        job.setProgressMsg("Creating spreadsheet")

        # Create the spreadsheet and define default style, etc.
        # -----------------------------------------------------
        wsTitle = {u'tr':u'Transferred',
                   u'ntr':u'Not Transferred'}
        wb      = ExcelWriter.Workbook()
        b       = ExcelWriter.Border()
        borders = ExcelWriter.Borders(b, b, b, b)
        font    = ExcelWriter.Font(name = 'Times New Roman', size = 11)
        align   = ExcelWriter.Alignment('Left', 'Top', wrap = True)
        style1  = wb.addStyle(alignment = align, font = font)
        urlFont = ExcelWriter.Font('blue', None, 'Times New Roman', size = 11)
        style4  = wb.addStyle(alignment = align, font = urlFont)
        style2  = wb.addStyle(alignment = align, font = font,
                                 numFormat = 'YYYY-mm-dd')
        alignH  = ExcelWriter.Alignment('Left', 'Bottom', wrap = True)
        headFont= ExcelWriter.Font(bold=True, name = 'Times New Roman',
                                                                    size = 12)
        boldFont= ExcelWriter.Font(bold=True, name = 'Times New Roman',
                                                                    size = 11)
        styleH  = wb.addStyle(alignment = alignH, font = headFont)
        style1b = wb.addStyle(alignment = align,  font = boldFont)

        for key in wsTitle.keys():
            ws      = wb.addWorksheet(wsTitle[key], style1, 45, 1)

            # Set the column width
            # --------------------
            if key == 'ntr':
                ws.addCol( 1,   60)
                ws.addCol( 2,   90)
                ws.addCol( 3,   80)
                ws.addCol( 4,  100)
                ws.addCol( 5,   60)
                ws.addCol( 6,   60)
                ws.addCol( 7,   60)
                ws.addCol( 8,   60)
                ws.addCol( 9,  120)
                ws.addCol( 10,  90)
                ws.addCol( 11,  90)
                ws.addCol( 12, 140)
                ws.addCol( 13,  90)
            else:
                ws.addCol( 1,  60)
                ws.addCol( 2,  90)
                ws.addCol( 3,  80)
                ws.addCol( 4,  90)
                ws.addCol( 5,  70)
                ws.addCol( 6,  70)
                ws.addCol( 7,  60)
                ws.addCol( 8,  60)
                ws.addCol( 9,  90)
                ws.addCol(10,  90)
                ws.addCol(11,  60)
                ws.addCol(12,  60)

            # Create the Header row
            # ---------------------
            if key == 'ntr':
                exRow = ws.addRow(1, styleH)
                exRow.addCell(1,  'CDR-ID')
                exRow.addCell(2,  'Primary ID')
                exRow.addCell(3,  'NCT-ID')
                exRow.addCell(4,  'Source')
                exRow.addCell(5,  'Transfer Response')
                exRow.addCell(6,  'Date')
                exRow.addCell(7,  'Protocol Status')
                exRow.addCell(8,  'Status Date')
                exRow.addCell(9,  'Primary Lead Org Name')
                exRow.addCell(10, 'PUP')
                exRow.addCell(11, 'Phone')
                exRow.addCell(12, 'Email')
                exRow.addCell(13, 'FDA Regulated')
                exRow.addCell(14, 'Section 801')
                exRow.addCell(15, 'FDA IND')
                exRow.addCell(16, 'Phase')
                exRow.addCell(17, 'GrantNo')
            else:
                exRow = ws.addRow(1, styleH)
                exRow.addCell(1, 'CDR-ID')
                exRow.addCell(2, 'Primary ID')
                exRow.addCell(3, 'NCT-ID')
                exRow.addCell(4, 'CTGov Owner Organization')
                exRow.addCell(5, 'PRS User Name')
                exRow.addCell(6, 'CTGov Ownership Transfer Date')
                exRow.addCell(7, 'Blocked From CTGov')
                exRow.addCell(8, 'Blocked From Publication')
                exRow.addCell(9, 'Current Protocol Status')
                exRow.addCell(10, 'Organization Name')
                exRow.addCell(11, 'Org CDR-ID')
                #exRow.addCell(12, 'FDA Regulated')
                #exRow.addCell(13, 'Section 801')
                #exRow.addCell(14, 'FDA IND')
                #exRow.addCell(15, 'Phase')
                exRow.addCell(12, 'GrantNo')

            # Add the protocol data one record at a time beginning after
            # the header row
            # ----------------------------------------------------------
            rowNum = 1
            if key == 'ntr':
                Prot = ntps
            else:
                Prot = tps

            if key == 'ntr':
                for row in Prot.protocols:
                    # print rowNum
                    rowNum += 1
                    exRow = ws.addRow(rowNum, style1, 40)
                    exRow.addCell(1, row)
                    cgUrl =  'http://www.cancer.gov/clinicaltrials/search/'
                    cgUrl += 'view?cdrid=%s&version=HealthProfessional' % row
                    exRow.addCell(2, Prot.protocols[row][u'pId'],
                                                href = cgUrl, style = style4)
                    ctUrl = 'http://clinicaltrials.gov/ct/show'
                    ctUrl += '/%s' % Prot.protocols[row][u'nctId']
                    exRow.addCell(3, Prot.protocols[row][u'nctId'],
                                                href = ctUrl, style = style4)

                    if Prot.protocols[row].has_key('protSource'):
                        exRow.addCell(4, Prot.protocols[row][u'protSource'])

                    if Prot.protocols[row].has_key('tResponse'):
                        exRow.addCell(5, Prot.protocols[row][u'tResponse'])

                    if Prot.protocols[row].has_key('logDate'):
                        exRow.addCell(6, Prot.protocols[row][u'logDate'])

                    exRow.addCell(7, Prot.protocols[row][u'status'])

                    if Prot.protocols[row].has_key('statDate'):
                        exRow.addCell(8, Prot.protocols[row][u'statDate'])

                        exRow.addCell(9, Prot.protocols[row][u'orgName'])
                    if Prot.protocols[row].has_key('pup'):
                        if Prot.protocols[row][u'pup'].persFname and \
                           Prot.protocols[row][u'pup'].persLname:
                            exRow.addCell(10,
                                 Prot.protocols[row][u'pup'].persFname + \
                                  " " + Prot.protocols[row][u'pup'].persLname)

                        exRow.addCell(11, Prot.protocols[row][u'pup'].persPhone)
                        exRow.addCell(12, Prot.protocols[row][u'pup'].persEmail)

                    if Prot.protocols[row].has_key(u'fdaReg'):
                        exRow.addCell(13, Prot.protocols[row][u'fdaReg'])
                    if Prot.protocols[row].has_key(u'fda801'):
                        exRow.addCell(14, Prot.protocols[row][u'fda801'])
                    if Prot.protocols[row].has_key(u'fdaInd'):
                        exRow.addCell(15, Prot.protocols[row][u'fdaInd'])

                    if Prot.protocols[row].has_key(u'phase'):
                        exRow.addCell(16, Prot.protocols[row][u'phase'])

                    if Prot.protocols[row].has_key('grantNo'):
                        exRow.addCell(17, Prot.protocols[row][u'grantNo'])
            else:
                for row in Prot.protocols:
                    rowNum += 1
                    exRow = ws.addRow(rowNum, style1, 40)
                    exRow.addCell(1, row)
                    cgUrl =  'http://www.cancer.gov/clinicaltrials/search/'
                    cgUrl += 'view?cdrid=%s&version=HealthProfessional' % row
                    exRow.addCell(2, Prot.protocols[row][u'pId'],
                                                href = cgUrl, style = style4)
                    ctUrl = 'http://clinicaltrials.gov/ct/show'
                    ctUrl += '/%s' % Prot.protocols[row][u'nctId']
                    exRow.addCell(3, Prot.protocols[row][u'nctId'],
                                                href = ctUrl, style = style4)

                    if Prot.protocols[row].has_key('trOrg'):
                        exRow.addCell(4, Prot.protocols[row][u'trOrg'])

                    if Prot.protocols[row].has_key('trUser'):
                        exRow.addCell(5, Prot.protocols[row][u'trUser'])

                    if Prot.protocols[row].has_key('trDate'):
                        exRow.addCell(6, Prot.protocols[row][u'trDate'])

                    if Prot.protocols[row].has_key('blocked'):
                        exRow.addCell(7, Prot.protocols[row][u'blocked'])

                    if Prot.protocols[row].has_key('ctgovBlk'):
                        exRow.addCell(8, Prot.protocols[row][u'ctgovBlk'])

                    exRow.addCell(9, Prot.protocols[row][u'status'])

                    if exRow.addCell(10, Prot.protocols[row][u'orgName']):
                        exRow.addCell(10, Prot.protocols[row][u'orgName'])

                    if exRow.addCell(11, Prot.protocols[row][u'orgId']):
                        exRow.addCell(11, Prot.protocols[row][u'orgId'])

                    if exRow.addCell(12, Prot.protocols[row][u'grantNo']):
                        exRow.addCell(12, Prot.protocols[row][u'grantNo'])

        t = time.strftime("%Y%m%d%H%M%S")
        job.setProgressMsg("Saving spreadsheet")

        # Save the report.
        # ----------------
        name = "/ProtocolOwnershipTransferReport-%d.xls" % job.getJobId()
        f = open(REPORTS_BASE + name, 'wb')
        wb.write(f, True)
        f.close()

        cdr.logwrite("saving %s" % (REPORTS_BASE + name), LOGFILE)
        ### url = "http://%s%s/GetReportWorkbook.py?name=%s" % (cdrcgi.WEBSERVER,
        ###                                                     cdrcgi.BASE,
        ###                                                     name)
        url = "%s%s/GetReportWorkbook.py?name=%s" % (cdr.CBIIT_NAMES[2],
                                                            cdrcgi.BASE,
                                                            name)
        cdr.logwrite("url: %s" % url, LOGFILE)
        msg = "Report available at <br><a href='%s'><u>%s</u></a>." % (
            url, url)

        # Tell the user where to find it.
        # -------------------------------
        body = """\
The Protocol Ownership Transfer Report you requested can be viewed at

  %s
""" % (url)
        if cdr.h.org == 'OCE':
            subject   = "%s: Report results - Protocol Transfer" % \
                                                   cdr.PUB_NAME.capitalize()
        else:
            subject   = "%s-%s: Report results - Protocol Transfer" % \
                                                   (cdr.h.org, cdr.h.tier)

        sendMail(job, subject, body)
        job.setProgressMsg(msg)
        job.setStatus(cdrbatch.ST_COMPLETED)
        cdr.logwrite("Completed report", LOGFILE)


#----------------------------------------------------------------------
# Report for tracking audio pronunciation recordings.
#----------------------------------------------------------------------
class PronunciationRecordingsReport:
    def __init__(self, job):
        self.job      = job
        self.start    = job.getParm('start')
        self.end      = job.getParm('end')
        self.language = job.getParm('language')
        self.cursor   = cdrdb.connect('CdrGuest').cursor()
    def run(self):
        lang = ''
        if self.language == 'en':
            lang = "AND t.value NOT LIKE '%-Spanish'"
        elif self.language == 'es':
            lang = "AND t.value LIKE '%-Spanish'"
        self.cursor.execute("""\
SELECT DISTINCT e.doc_id, c.created, t.value
           FROM query_term e
           JOIN doc_created c
             ON c.doc_id = e.doc_id
           JOIN query_term g
             ON g.doc_id = c.doc_id
           JOIN query_term t
             ON t.doc_id = g.doc_id
          WHERE e.path = '/Media/PhysicalMedia/SoundData/SoundEncoding'
            AND e.value = 'MP3'
            AND c.created BETWEEN ? AND ?
            AND g.path = '/Media/MediaContent/Categories/Category'
            AND g.value = 'pronunciation'
            AND t.path = '/Media/MediaTitle'
            %s""" % lang, (self.start, self.end + ' 23:59:59'), timeout=600)
        docs = []
        rows = self.cursor.fetchall()
        for docId, created, title in rows:
            doc = self.MediaDoc(docId, created, title, self.cursor)
            docs.append(doc)
            self.job.setProgressMsg("processed %d of %d" %
                                    (len(docs), len(rows)))
        docs.sort()
        book = ExcelWriter.Workbook()
        border = ExcelWriter.Border()
        borders = ExcelWriter.Borders(border, border, border, border)
        font = ExcelWriter.Font(name='Arial', size=12)
        align = ExcelWriter.Alignment('Left', 'Top', wrap=True)
        style = book.addStyle(alignment=align, font=font, borders=borders)
        title = "Pronunciations"
        sheet = book.addWorksheet(title, style)
        col = 1
        for width in (50, 200, 200, 150, 75, 150, 100, 75, 75, 75):
            sheet.addCol(col, width)
            col += 1
        col = 1
        align = ExcelWriter.Alignment('Center')
        font = ExcelWriter.Font(bold=True, size=14)
        style = book.addStyle(alignment=align, font=font)
        row = sheet.addRow(1, style)
        lang = "ALL"
        if self.language == 'en':
            lang = "English"
        elif self.language == 'es':
            lang = "Spanish"
        title = "Audio Pronunciation Recordings Tracking Report - %s" % lang
        row.addCell(1, title, mergeAcross=9)
        font = ExcelWriter.Font(bold=True, size=12, name='Arial')
        align = ExcelWriter.Alignment('Center', 'Center', wrap=True)
        style = book.addStyle(alignment=align, font=font)
        row = sheet.addRow(2, style)
        dates = "From %s - %s" % (self.start, self.end)
        row.addCell(1, dates, mergeAcross=9)
        row = sheet.addRow(3, style)
        for label in ('CDRID', 'Title', 'Proposed Glossary Terms',
                      'Processing Status', 'Processing Status Date',
                      'Comments', 'Last Version Publishable?',
                      'Date First Published', 'Date Last Modified',
                      'Published Date'):
            row.addCell(col, label)
            col += 1
        rowNumber = 4
        for doc in docs:
            row = sheet.addRow(rowNumber)
            row.addCell(1, doc.docId)
            row.addCell(2, doc.title)
            row.addCell(3, u"; ".join(doc.glossaryTerms))
            row.addCell(4, doc.status)
            row.addCell(5, doc.statusDate)
            row.addCell(6, u"; ".join(doc.comments))
            row.addCell(7, doc.lastVersionPublishable and u"Y" or u"N")
            row.addCell(8, doc.firstPub and doc.firstPub[:10] or u"")
            row.addCell(9, doc.lastMod and doc.lastMod[:10] or u"")
            row.addCell(10, doc.pubDate and doc.pubDate[:10] or u"")
            rowNumber += 1

        job.setProgressMsg("Saving spreadsheet")

        # Save the report.
        name = "/PronunciationRecordings-%s.xls" % job.getJobId()
        fp = open(REPORTS_BASE + name, 'wb')
        book.write(fp, True)
        fp.close()

        cdr.logwrite("saving %s" % (REPORTS_BASE + name), LOGFILE)
        #url = "http://%s%s/GetReportWorkbook.py?name=%s" % (cdrcgi.WEBSERVER,
        #                                                    cdrcgi.BASE,
        #                                                    name)
        url = "%s%s/GetReportWorkbook.py?name=%s" % (cdr.CBIIT_NAMES[2],
                                                         cdrcgi.BASE, name)

        cdr.logwrite("url: %s" % url, LOGFILE)
        msg = "Report available at <br><a href='%s'><u>%s</u></a>." % (
            url, url)

        # Tell the user where to find it.
        # -------------------------------
        body = """\
The Audio Recordings Tracking Report you requested can be viewed at

  %s
""" % (url)
        if cdr.h.org == 'OCE':
            subject   = "%s: Audio Recordings Tracking Report results" % \
                                                   cdr.PUB_NAME.capitalize()
        else:
            subject   = "%s-%s: Audio Recordings Tracking Report results" % \
                                                   (cdr.h.org, cdr.h.tier)

        sendMail(job, subject, body)
        job.setProgressMsg(msg)
        job.setStatus(cdrbatch.ST_COMPLETED)
        cdr.logwrite("Completed report", LOGFILE)

    class MediaDoc:
        def __init__(self, docId, created, title, cursor):
            self.docId = docId
            self.created = created
            self.title = title
            self.status = self.statusDate = self.firstPub = self.lastMod = None
            self.pubDate = None
            self.glossaryTerms = []
            self.comments = []
            self.lastVersionPublishable = False
            versions = cdr.lastVersions('guest', 'CDR%010d' % docId)
            if versions[0] == versions[1] and versions[0] > 0:
                self.lastVersionPublishable = True
            cursor.execute("""\
SELECT dt
  FROM last_doc_publication
 WHERE doc_id = ?""", docId, timeout=300)
            rows = cursor.fetchall()
            if rows:
                self.pubDate = rows[0][0]
            cursor.execute("""\
  SELECT n.value
    FROM query_term n
    JOIN query_term u
      ON u.int_val = n.doc_id
   WHERE u.doc_id = ?
     AND n.path = '/GlossaryTermName/TermName/TermNameString'
     AND u.path = '/Media/ProposedUse/Glossary/@cdr:ref'
ORDER BY n.value""", docId, timeout=300)
            for row in cursor.fetchall():
                self.glossaryTerms.append(row[0])
            cursor.execute("SELECT first_pub, xml FROM document WHERE id = ?",
                           docId, timeout=300)
            self.firstPub, docXml = cursor.fetchall()[0]
            tree = etree.XML(docXml.encode('utf-8'))
            #for node in tree.findall('ProposedUse/Glossary'):
            #    self.glossaryTerms.append(node.get("{%s}ref" % NAMESPACE))
            for node in tree.findall('DateLastModified'):
                self.lastMod = node.text
            for node in tree.findall('ProcessingStatuses/ProcessingStatus'):
                for child in node:
                    if child.tag == 'ProcessingStatusValue':
                        self.status = child.text
                    elif child.tag == 'ProcessingStatusDate':
                        self.statusDate = child.text
                    elif child.tag == 'Comment':
                        self.comments.append(child.text)
                break
        def __cmp__(self, other):
            diff = cmp(self.status, other.status)
            if diff:
                return diff
            if self.lastVersionPublishable == other.lastVersionPublishable:
                return cmp(self.lastMod, other.lastMod)
            if self.lastVersionPublishable:
                return -1
            return 1

#----------------------------------------------------------------------
# Run a report of protocols that have been transferred to the NLM and
# that still have to get transferred.
#----------------------------------------------------------------------
def protOwnershipTransfer(job):
    host        = job.getParm("Host")
    report      = ProtocolOwnershipTransfer(host)
    report.createTransferSpreadsheet(job)

#----------------------------------------------------------------------
# Run a report of dead URLs.
#----------------------------------------------------------------------
def checkUrls(job):
    if job.getParm('jobType') == "UrlErrs":
        report = UrlCheck()
    else:
        report = ExRefPageTitleCheck()
    report.run(job)

#----------------------------------------------------------------------
# Run a report of dead URLs.
#----------------------------------------------------------------------
def pubCount(job):
    report = countPublishedDocs()
    report.run(job)

#----------------------------------------------------------------------
# Run a report of protocols connected with a specific organization.
#----------------------------------------------------------------------
def orgProtocolReview(job):
    docId  = job.getParm("id")
    digits = re.sub(r"[^\d]", "", docId)
    docId  = int(digits)
    report = OrgProtocolReview(docId)
    report.report(job)

#----------------------------------------------------------------------
# Report on phrases matching a specified glossary term.
#----------------------------------------------------------------------
def glossaryTermSearch(job):
    docId  = job.getParm("id")
    types  = job.getParm("types")
    digits = re.sub(r"[^\d]", "", docId)
    docId  = int(digits)
    report = GlossaryTermSearch(docId, types)
    report.report(job)

#----------------------------------------------------------------------
# Dummy job for command-line testing.
#----------------------------------------------------------------------
class TestJob:
    def __init__(self, jobId, email, argv):
        self.jobId = jobId
        self.email = email
        self.parms = {}
        for arg in argv:
            name, value = arg.split('=')
            self.parms[name] = value
    def getParm(self, name):
        return self.parms.get(name)
    def setStatus(self, s): sys.stderr.write("STATUS: %s\n" % s)
    def getJobId(self): return self.jobId
    def fail(self, m, logfile = None):
        sys.stderr.write("FAIL: %s\n" % m);
        sys.exit(1)
    def getEmail(self): return self.email
    def setProgressMsg(self, m): print m

#----------------------------------------------------------------------
# Top level entry point.
#----------------------------------------------------------------------
if __name__ == "__main__":

    # What's our job ID?
    if len(sys.argv) < 2:
        cdr.logwrite("No batch job id passed to CdrLongReports.py", LOGFILE)
        sys.exit(1)
    try:
        jobIdArg = sys.argv.pop()
        jobId    = int(jobIdArg)
    except ValueError:
        cdr.logwrite("Invalid job id passed to CdrLongReports.py: %s" %
                     jobIdArg, LOGFILE)
        sys.exit(1)
    cdr.logwrite("CdrLongReports: job id %d" % jobId, LOGFILE)

    # Get args for test invocations, for which sys.argv[1] is the report name.
    # Job id parm has already been popped from the list of command-line args.
    # Remaining args (after the report name) are an optional email address
    # and optional name=value parameters for the job.  If the value (or the
    # name, for that matter) has a space embedded in it, enclose the entire
    # "name=value with spaces" argument in quotes.  If there are any name=value
    # parameters on the command line, the email argument is not optional.
    # So, if you want to perform a test invocation, append a dummy job ID
    # to the end of these arguments so it can be popped above.
    if len(sys.argv) > 1:
        reportName = sys.argv[1]
        email      = len(sys.argv) > 2 and sys.argv[2] or None
        parms      = sys.argv[3:]

        # Test version of Spanish Glossary Term report; needs status, to, from.
        if reportName == 'SpanishGlossaryTermsByStatus':
            SpanishGlossaryTermsByStatus(TestJob(jobId, email, parms)).run()
            sys.exit(0)

        # Special handling for running outcome measures coding report by hand.
        # This version produces separate file used for web-based Trial Outcome
        # update service.
        if reportName == 'OutcomeMeasuresCodingReport':
            testJob  = TestJob(jobId, email, parms)
            outcomeMeasuresCodingReport(testJob)
            sys.exit(0)

        # Test invocation of Protocol Processing Status Report.
        # Takes optional parm IncludeTitle=true
        if reportName == 'ProtocolProcessingStatusReport':
            job = TestJob(jobId, email, parms)
            ProtocolProcessingStatusReport(job).run()
            sys.exit(0)

        # Test invocation of Audio Pronunciation Recordings Tracking Report.
        # Args: start=yyyy-mm-dd end=yyyy-mm-dd language=ALL|en|es
        if reportName == 'pron':
            job = TestJob(jobId, email, parms)
            PronunciationRecordingsReport(job).run()
            sys.exit(0)

        # Test invocation of report for Office of Science Policy.
        # Needs the following arguments:
        #     term-ids=cdr-id[;cdr-id ...]
        #     phases=phase[;phase] (e.g.Phase I;Phase II)
        #     first-year=year (e.g., 2000)
        #     last-year=year (e.g., 2006)
        #     year-type=year-type (fiscal or calendar)
        if reportName.upper() == 'OSP':
            job = TestJob(jobId, email, parms)
            ospReport(job)
            sys.exit(0)

    # Create the job object.
    try:
        job = cdrbatch.CdrBatch(jobId = jobId)
    except cdrbatch.BatchException, be:
        cdr.logwrite("Unable to create batch job object: %s" % str(be),
                     LOGFILE)
        sys.exit(1)

    # Find out what we're supposed to do.
    jobName = job.getJobName()
    #print "job name is %s" % jobName

    try:
        if jobName == "Protocol Sites Without Phones":
            protsWithoutPhones(job)
        elif jobName == "Report for Office of Science Policy":
            ospReport(job)
        elif jobName == "Mailer Non-Respondents":
            nonRespondentsReport(job)
        elif jobName == "Organization Protocol Review":
            orgProtocolReview(job)
        elif jobName == "URL Check":
            checkUrls(job)
        elif jobName == "Glossary Term Search":
            glossaryTermSearch(job)
        elif jobName == "Protocol Processing Status Report":
            ProtocolProcessingStatusReport(job).run()
        elif jobName == "Outcome Measures Coding Report":
            outcomeMeasuresCodingReport(job)
        elif jobName == "Spanish Glossary Terms by Status":
            SpanishGlossaryTermsByStatus(job).run()
        elif jobName == "Update all Drug/Agent Terms from NCI Thesaurus":
            NCITTermUpdate(job)
        elif jobName == "Update all Disease Terms from NCI Thesaurus":
            NCITTermUpdate(job)
        elif jobName == "Protocol Ownership Transfer":
            protOwnershipTransfer(job)
        elif jobName == "Audio Pronunciation Recordings Tracking Report":
            PronunciationRecordingsReport(job).run()
        elif jobName == "Published Documents Count":
            pubCount(job)
        # That's all we know how to do right now.
        else:
            job.fail("CdrLongReports: unknown job name '%s'" % jobName,
                     logfile = LOGFILE)
    except Exception, info:
        cdr.logwrite("Failure executing job %d: %s" % (jobId, str(info)),
                     LOGFILE, 1)
        job.fail("Caught exception: %s" % str(info), logfile = LOGFILE)
