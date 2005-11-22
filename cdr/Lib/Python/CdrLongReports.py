#----------------------------------------------------------------------
#
# $Id: CdrLongReports.py,v 1.23 2005-11-22 13:32:26 bkline Exp $
#
# CDR Reports too long to be run directly from CGI.
#
# $Log: not supported by cvs2svn $
# Revision 1.22  2005/11/10 14:56:21  bkline
# Rewrote OSP report to use new ExcelWriter module.  Added column
# to report for date trials completed.
#
# Revision 1.21  2005/03/10 14:18:20  bkline
# Fixed bugs, changed algorithm for determining protocol statuses
# in OSP report.
#
# Revision 1.20  2005/03/01 21:10:22  bkline
# Made active date range controlled by parameters.
#
# Revision 1.19  2005/03/01 15:35:03  bkline
# Modified the date range for the OSP report.
#
# Revision 1.18  2005/01/19 23:27:19  venglisc
# Added section to search for and display protocols that are listing the
# organization in question as a Clinical Trial Office.
#
# Revision 1.17  2004/09/23 14:07:46  venglisc
# Modified string passed by UI to display on report. (Bug 1337)
#
# Revision 1.16  2004/09/21 14:57:48  venglisc
# Added third header line to Excel report output.  Minor formatting of header
# (increased row size). (Bug 1337)
#
# Revision 1.15  2004/08/27 14:27:31  bkline
# Modified the glossary term search report to restrict its search to
# active and temporarily closed protocols for the protocols portion
# (enhancement request #1319).
#
# Revision 1.14  2004/08/27 13:50:45  bkline
# Added support for restricting glossary term search report by document
# type; plugged in new (Python 2.3) approach to stripping specified
# characters from the ends of a string.
#
# Revision 1.13  2004/08/06 22:31:02  bkline
# Made table titles bold for Glossary Term Search report at Margaret's
# request.
#
# Revision 1.12  2004/07/28 20:56:37  venglisc
# Modified to use different filter set for OrgProtocolReview report.
# Requested under Bug 1264.
#
# Revision 1.11  2004/04/26 20:55:12  bkline
# Added report on glossary term phrases.
#
# Revision 1.10  2004/02/10 14:19:30  bkline
# Modified date range for OSP report.
#
# Revision 1.9  2003/12/16 16:17:38  bkline
# Added URL check report.
#
# Revision 1.8  2003/09/11 12:40:30  bkline
# Fixed email message for two of the report types.
#
# Revision 1.7  2003/09/10 12:51:16  bkline
# Broke out logic to restrict mailer-non-respondent report to the
# specified date range into a separate SQL query.
#
# Revision 1.6  2003/09/09 22:18:23  bkline
# Fixed SQL queries and name bug for inactive persons in mailer
# non-respondent report.
#
# Revision 1.5  2003/09/04 15:31:23  bkline
# Replaced calls to reportFailure() with job.fail().
#
# Revision 1.4  2003/08/21 19:25:45  bkline
# Added Org Protocol Review report.
#
# Revision 1.3  2003/07/29 13:08:45  bkline
# Added NonRespondents report.
#
# Revision 1.2  2003/05/08 20:36:52  bkline
# Added report for the Office of Science Policy.
#
# Revision 1.1  2003/02/26 01:37:37  bkline
# Script for executing reports which are too long to be handled directly
# by CGI.
#
#----------------------------------------------------------------------
import cdr, cdrdb, xml.dom.minidom, time, cdrcgi, cgi, sys, socket, cdrbatch
import string, re, win32com.client, urlparse, httplib, traceback
import ExcelWriter

#----------------------------------------------------------------------
# Module values.
#----------------------------------------------------------------------
REPORTS_BASE = 'd:/cdr/reports'
LOGFILE      = cdr.DEFAULT_LOGDIR + "/reports.log"
EMAILFROM    = 'cdr@%s.nci.nih.gov' % socket.gethostname()

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
    url = "http://%s.nci.nih.gov/CdrReports%s" % (socket.gethostname(),
                                                  reportName)
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
    sendMail(job, "Report results", body)
    job.setProgressMsg(msg)
    job.setStatus(cdrbatch.ST_COMPLETED)
    cdr.logwrite("Completed report", LOGFILE)

#----------------------------------------------------------------------
# Generate a spreadsheet on selected protocols for the Office of
# Science Policy.
#----------------------------------------------------------------------
def ospReport(job):

    class Status:
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
                                self.statuses.append(Status(name, date))
            self.statuses.sort(lambda a, b: cmp(a.startDate, b.startDate))
            for i in range(len(self.statuses)):
                if i == len(self.statuses) - 1:
                    self.statuses[i].endDate = time.strftime("%Y-%m-%d")
                else:
                    self.statuses[i].endDate = self.statuses[i + 1].startDate

    class Protocol:
        "Protocol information used for an OPS report spreadsheet."
        
        def __init__(self, id, node):
            "Create a protocol object from the XML document."
            self.id        = id
            self.leadOrgs  = []
            self.statuses  = []
            self.status    = ""
            self.firstId   = ""
            self.otherIds  = []
            self.firstPub  = ""
            self.closed    = ""
            self.completed = ""
            self.types     = []
            self.ageRange  = ""
            self.sponsors  = []
            self.title     = ""
            profTitle      = ""
            patientTitle   = ""
            originalTitle  = ""
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
                            originalTitle = value
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
                self.statuses.append(Status(protStatus, startDate))
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
    # Start processing here for the OSP report.
    #----------------------------------------------------------------------
    start = time.time()
    #print "milestone 1"
    termIds = []
    phaseJoin = ''
    phaseWhere = ''
    yearType = 'calendar'
    args = job.getArgs()
    for key in args:
        if key.upper().startswith("TERMID"):
            try:
                termIds.append(int(args[key]))
            except Exception, e:
                cdr.logwrite("Invalid term ID %s: %s" % (args[key], str(e)),
                             LOGFILE)
        elif key == 'Phases':
            phaseJoin = 'JOIN query_term p ON p.doc_id = t.doc_id'
            phaseWhere = 'AND p.value %s' % args[key]
        elif key == 'year':
            yearType = args[key]
    try:
        conn = cdrdb.connect('CdrGuest')
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE #terms(id INTEGER)")
        conn.commit()
        for termId in termIds:
            try:
                cursor.execute("INSERT INTO #terms VALUES(%d)" % termId)
                conn.commit()
            except Exception, e:
                cdr.logwrite("Failure inserting %d into #terms: %s" % (termId,
                                                                       str(e)),
                             LOGFILE)
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
    except:
        job.fail("Database failure getting list of protocols.",
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

    # Set up the header cells in the spreadsheet's top row.
    font = ExcelWriter.Font(name = 'Times New Roman', bold = True, size = 10)
    align = ExcelWriter.Alignment('Center', 'Center', wrap = True)
    interior = ExcelWriter.Interior('#CCFFCC')
    style3 = wb.addStyle(alignment = align, font = font, borders = borders,
                         interior = interior)
    row = ws.addRow(1, style3, 40)
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
    for i in range(len(headings)):
        row.addCell(i + 1, headings[i])

    #------------------------------------------------------------------
    # Process all candidate protocols.
    #------------------------------------------------------------------
    done = 0
    protocols = []
    for row in rows:
        cursor.execute("""\
            SELECT xml
              FROM doc_version
             WHERE id = ?
               AND num = ?""", row)
        docXml = cursor.fetchone()[0]
        dom = xml.dom.minidom.parseString(docXml.encode('utf-8'))
        prot = Protocol(row[0], dom.documentElement)
        startYear = job.getParm('begin') or '1999'
        endYear   = job.getParm('end')   or '2004'
        if yearType == 'fiscal':
            firstYear = int(startYear) - 1
            startDate = "%s-10-01" % firstYear
            endDate = "%s-09-30" % endYear
        else:
            startDate = "%s-01-01" % startYear
            endDate   = "%s-12-31" % endYear
        if prot.wasActive(startDate, endDate):
            protocols.append(prot)
        done += 1
        now = time.time()
        timer = getElapsed(start, now)
        msg = "Processed %d of %d protocols; elapsed: %s" % (done,
                                                             len(rows),
                                                             timer)
        job.setProgressMsg(msg)
        cdr.logwrite(msg, LOGFILE)

    # Add one row for each protocol.
    rowNum = 1
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
    name = "/OSPReport-Job%d.xml" % job.getJobId()
    fullname = REPORTS_BASE + name
    fobj = file(fullname, "w")
    wb.write(fobj)
    fobj.close()
    cdr.logwrite("saving %s" % fullname, LOGFILE)
    url = "http://%s%s/GetReportWorkbook.py?name=%s" % (cdrcgi.WEBSERVER,
                                                        cdrcgi.BASE, name)
    cdr.logwrite("url: %s" % url, LOGFILE)
    msg += "<br>Report available at <a href='%s'><u>%s</u></a>." % (url, url)
    
    # Tell the user where to find it.
    body = """\
The OSP report you requested on Protocols can be viewed at
%s.
""" % url
    sendMail(job, "Report results", body)
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
                (
                    doc_id    INTEGER,
                    mailer_id INTEGER
                )""")
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

        xl = win32com.client.Dispatch("Excel.Application")
        xl.Visible = 0
        wb = xl.Workbooks.Add()

        # Get rid of unwanted sheets, and rename the one we're keeping.
        xl.DisplayAlerts = 0
        sheet = xl.Sheets("Sheet1")
        sheet.Name = "Mailer Non-Respondents"
        xl.Sheets("Sheet2").Select()
        xl.ActiveWindow.SelectedSheets.Delete()
        xl.Sheets("Sheet3").Select()
        xl.ActiveWindow.SelectedSheets.Delete()
        sheet.Activate()
        
        # Doesn't work: bug in Excel.
        #sheet.PageSetup.Orientation = win32com.client.constants.xlPortrait
        headings = (
            "Recipient Name",
            "DocId",
            "Mailer",
            "Mailer Type",
         #  "Generated",
            "Response"
            )
        sheet.Cells(1, 1).Value = "Mailer Non-Respondents Report"
        cells = sheet.Cells.Range("A1:E1")
        cells.Font.Bold = 1
        cells.Font.Name = 'Times New Roman'
        cells.Font.Size = 14
        cells.MergeCells = 1
        cells.RowHeight = 20
        cells.Interior.ColorIndex = 35
        cells.VerticalAlignment = win32com.client.constants.xlCenter
        cells.HorizontalAlignment = win32com.client.constants.xlCenter
        cells = sheet.Cells.Range("A2:E2")
        cells.MergeCells = 1
        cells.VerticalAlignment = win32com.client.constants.xlCenter
        cells.HorizontalAlignment = win32com.client.constants.xlCenter
        cells = sheet.Cells.Range("A2:E3")
        cells.Font.Bold = 1
        cells.Font.Name = 'Times New Roman'
        cells.Font.Size = 12
        cells.Interior.ColorIndex = 35

        sheet.Cells(2, 1).Value = "For period of %s to %s" % (
            ageText, time.strftime("%B %d, %Y"))
        cells = sheet.Cells.Range("A2:E2")
        cells.Font.Bold = 1
        cells.Font.Name = 'Times New Roman'
        cells.Font.Size = 12
        cells.MergeCells = 1
        cells.RowHeight = 16
        cells.Interior.ColorIndex = 35
        cells.VerticalAlignment = win32com.client.constants.xlCenter
        cells.HorizontalAlignment = win32com.client.constants.xlCenter
        cells = sheet.Cells.Range("A2:E2")
        cells.MergeCells = 1
        cells.VerticalAlignment = win32com.client.constants.xlCenter
        cells.HorizontalAlignment = win32com.client.constants.xlCenter
        cells = sheet.Cells.Range("A2:E3")
        cells.Font.Bold = 1
        cells.Font.Name = 'Times New Roman'
        cells.Font.Size = 12
        cells.Interior.ColorIndex = 35

        sheet.Cells(3, 1).Value = "Mailer Type: %s" % self.docType
        cells = sheet.Cells.Range("A3:E3")
        cells.Font.Bold = 1
        cells.Font.Name = 'Times New Roman'
        cells.Font.Size = 12
        cells.MergeCells = 1
        cells.RowHeight = 16
        cells.Interior.ColorIndex = 35
        cells.VerticalAlignment = win32com.client.constants.xlCenter
        cells.HorizontalAlignment = win32com.client.constants.xlCenter
        cells = sheet.Cells.Range("A3:E3")
        cells.MergeCells = 1
        cells.VerticalAlignment = win32com.client.constants.xlCenter
        cells.HorizontalAlignment = win32com.client.constants.xlCenter
        cells = sheet.Cells.Range("A3:E3")
        cells.Font.Bold = 1
        cells.Font.Name = 'Times New Roman'
        cells.Font.Size = 12
        cells.Interior.ColorIndex = 35

        sheet.Cells(4, 1).Value = "Non-Response Time: %s" % ageString
        cells = sheet.Cells.Range("A4:E4")
        cells.Font.Bold = 1
        cells.Font.Name = 'Times New Roman'
        cells.Font.Size = 12
        cells.Interior.ColorIndex = 35
        cells.RowHeight = 20
        headerRows = 4
        for i in range(len(headings)):
            sheet.Cells(headerRows, i + 1).Value = headings[i]

        done = 0
        rowNum = headerRows + 1
        recipRows = 0
        lastRecipName = ""
        lastBaseDocId = None
        if not rows:
            job.fail("No data found for report", logfile = LOGFILE)
        for row in rows:
            cells = sheet.Cells.Range("%d:%d" % (rowNum, rowNum))
            cells.Font.Name = 'Times New Roman'
            cells.Font.Size = 11
            cells.WrapText = 1
            cells.VerticalAlignment = win32com.client.constants.xlTop
            cells.HorizontalAlignment = win32com.client.constants.xlLeft
            #cells = sheet.Cells.Range("A%d:E%d" % (rowNum, rowNum))
            if row[0] == lastRecipName:
                cells.RowHeight = 15.0
                recipName = ""
                recipRows += 1
                if recipRows > 3:
                    done += 1
                    continue
            else:
                #cells.Rows.AutoFit()
                cells.RowHeight = 15.0 
                #cells.RowHeight = 12.75
                border = cells.Borders(win32com.client.constants.xlEdgeTop)
                border.LineStyle  = win32com.client.constants.xlContinuous
                border.Weight     = win32com.client.constants.xlThin
                border.ColorIndex = win32com.client.constants.xlAutomatic
                recipRows = 1
                recipName = lastRecipName = row[0]
                if recipName.startswith("Inactive;"):
                    recipName = recipName[len("Inactive;"):]
                semicolon = recipName.find(";")
                if semicolon != -1:
                    recipName = recipName[:semicolon]
                recipName = cdrcgi.unicodeToLatin1(recipName)
            if row[1] == lastBaseDocId:
                baseDocId = ""
            else:
                baseDocId = "%d" % row[1]
                lastBaseDocId = row[1]
            # generatedDate = row[4] and row[4][:10] or ""
            responseDate  = row[4] and row[4][:10] or ""
            if row[5] == "Returned to sender":
                responseDate = "RTS"
            sheet.Cells(rowNum, 1).Value = recipName
            sheet.Cells(rowNum, 2).Value = baseDocId
            sheet.Cells(rowNum, 3).Value = "%d" % row[2]
            sheet.Cells(rowNum, 4).Value = row[3]
            #sheet.Cells(rowNum, 5).Value = generatedDate
            sheet.Cells(rowNum, 5).Value = responseDate
            #cells.RowHeight = 14
            rowNum += 1
            done += 1
            msg = "%d rows of %d processed; %d rows added" % (
                done, len(rows), rowNum - headerRows)
            job.setProgressMsg(msg)

        sheet.Columns.Range("A:A").ColumnWidth = 36.00
        sheet.Columns.Range("B:B").ColumnWidth =  9.00
        sheet.Columns.Range("C:C").ColumnWidth =  7.00
        sheet.Columns.Range("D:D").ColumnWidth = 24.00
        #sheet.Columns.Range("E:E").ColumnWidth = 10.00
        sheet.Columns.Range("E:E").ColumnWidth =  9.57

        # Make the top rows (the ones with column labels) always visible.
        sheet.Rows.Range("%d:%d" % (headerRows + 1, headerRows + 1)).Select()
        xl.ActiveWindow.FreezePanes = 1

        # Move to the first row of data.
        xl.Range("A%d" % (headerRows + 1)).Select()

        # Save the report.
        name = "/MailerNonRespondentsReport-%d.xls" % job.getJobId()
        wb.SaveAs(Filename = REPORTS_BASE + name)
        cdr.logwrite("saving %s" % (REPORTS_BASE + name), LOGFILE)
        url = "http://mahler.nci.nih.gov/CdrReports%s" % name
        cdr.logwrite("url: %s" % url, LOGFILE)
        msg += "<br>Report available at <a href='%s'><u>%s</u></a>." % (
            url, url)
        wb.Close(SaveChanges = 0)

        # Tell the user where to find it.
        body = """\
The %s Mailer Non-Respondents report you requested can be
viewed at %s.
""" % (self.docType, url)
        sendMail(job, "Report results", body)
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
        self.conn   = cdrdb.connect('CdrGuest', dataSource = host)
        self.cursor = self.conn.cursor()

    #------------------------------------------------------------------
    # Shorten role names (at Sheri's request 2003-07-01).
    #------------------------------------------------------------------
    def mapRole(self, role):
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
                self.isLeadOrg          = 0
                self.isOrgSite          = 0
        
        #--------------------------------------------------------------
        # Build the base html for the report.
        #--------------------------------------------------------------
        filters  = ['set:Denormalization Organization Set',
                    'name:Organization Protocol Review']
#        filters  = ['name:Organization Protocol Review Report Filter 1',
#                    'name:Organization Protocol Review Report Filter 2']
        job.setProgressMsg("Filtering organization document")
        cdr.logwrite("Filtering organization document", LOGFILE)
        response = cdr.filterDoc('guest', filters, self.id) #, host = 'mahler')
        if type(response) in (type(''), type(u'')):
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
                                 + '/PersonRole'
            /*
            AND org_stat.value IN ('Active',
                                   'Approved-not yet active',
                                   'Temporarily closed') */""", self.id,
                       timeout = 500)
            rows = self.cursor.fetchall()
            done = 0
            for (protId, docId, orgStat, personName,
                 role) in rows:
                semicolon = personName.find(';')
                if semicolon != -1:
                    personName = personName[:semicolon]
                    key = (protId, docId, orgStat)
                    if not protLinks.has_key(key):
                        protLinks[key] = protLink = ProtLink(docId, protId,
                                                             orgStat)
                else:
                    protLink = protLinks[key]
                protLink.isLeadOrg = 1
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
                                 + '/CurrentOrgStatus/StatusName'
/*
            AND org_stat.value IN ('Active',
                                   'Approved-not yet active',
                                   'Temporarily closed')
                                   */""", self.id, timeout = 500)
            for (protId, docId, orgStat, personName, role,
                 loStat) in self.cursor.fetchall():
                semicolon = personName.find(';')
                if semicolon != -1:
                    personName = personName[:semicolon]
                key = (protId, docId, loStat)
                if not protLinks.has_key(key):
                    protLinks[key] = protLink = ProtLink(docId, protId, loStat)
                else:
                    protLink = protLinks[key]
                protLink.isOrgSite = 1
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
                                 + '/CurrentOrgStatus/StatusName'
/*
            AND org_stat.value IN ('Active',
                                   'Approved-not yet active',
                                   'Temporarily closed')
                                   */ """, self.id, timeout = 500)
            for (protId, docId, orgStat, personName,
                 loStat) in self.cursor.fetchall():
                key = (protId, docId, loStat)
                if not protLinks.has_key(key):
                    protLinks[key] = protLink = ProtLink(docId, protId, loStat)
                else:
                    protLink = protLinks[key]
                protLink.isOrgSite = 1
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
                                 + '/CurrentOrgStatus/StatusName'
                                   """, self.id, timeout = 500)
            for (protId, docId, orgStat, personName,
                 loStat) in self.cursor.fetchall():
                key = (protId, docId, loStat)
                if not protLinks.has_key(key):
                    protLinks[key] = protLink = ProtLink(docId, protId, loStat)
                else:
                    protLink = protLinks[key]
                protLink.isOrgSite = 1
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
        table = """\
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
            table += """\
   <tr>
    <td valign='top'>%s</td>
    <td valign='top' align='center'>%d</td>
    <td valign='top'>%s</td>
    <td valign='top'>%s</td>
    <td valign='top' align='center'>%s</td>
    <td valign='top' align='center'>%s</td>
    <td valign='top'>%s</td>
   </tr>
""" % (protLink.protId, protLink.docId, protLink.loStat, protLink.orgStat,
       leadOrg, orgSite, person)
            done += 1
            msg = "Processed %d of %d rows" % (done, len(keys))
            job.setProgressMsg(msg)

        html = html.replace("@@DOC-ID@@", "CDR%010d" % self.id) \
                   .replace("@@TABLE@@", table)
        # Save the report.
        name = "/OrgProtocolReview-%d.html" % job.getJobId()
        file = open(REPORTS_BASE + name, "wb")
        file.write(cdrcgi.unicodeToLatin1(html))
        file.close()
        cdr.logwrite("saving %s" % (REPORTS_BASE + name), LOGFILE)
        url = "http://%s.nci.nih.gov/CdrReports%s" % (socket.gethostname(),
                                                      name)
        cdr.logwrite("url: %s" % url, LOGFILE)
        msg += "<br>Report available at <a href='%s'><u>%s</u></a>." % (
            url, url)

        # Tell the user where to find it.
        body = """\
The Organization Protocol Review report you requested for CDR%d
can be viewed at
%s.
""" % (self.id, url)
        sendMail(job, "Report results", body)
        job.setProgressMsg(msg)
        job.setStatus(cdrbatch.ST_COMPLETED)
        cdr.logwrite("Completed report", LOGFILE)

#----------------------------------------------------------------------
# Class for finding URLs which are not alive.
#----------------------------------------------------------------------
class UrlCheck:
    def __init__(self, host = 'localhost'):
        self.conn    = cdrdb.connect('CdrGuest', dataSource = host)
        self.cursor  = self.conn.cursor()
        self.pattern = re.compile("([^/]+)/@cdr:xref$")

    #------------------------------------------------------------------
    # Report on a dead URL
    #------------------------------------------------------------------
    def report(self, row, err):
        match = self.pattern.search(row[1])
        elem = match and match.group(1) or ""
        return """\
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
        query  = """\
  SELECT q.doc_id, q.path, q.value, t.name
    FROM query_term q
    JOIN document d
      ON d.id = q.doc_id
    JOIN doc_type t
      ON t.id = d.doc_type
   WHERE value LIKE 'http%'
     AND path LIKE '%/@cdr:xref'
ORDER BY t.name, q.doc_id
"""
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
        html = """\
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
  <h1>CDR Report on Inactive Hyperlinks</h1>
  <table border='0' width='100%' cellspacing='1' cellpadding='2'>
   <tr bgcolor='silver'>
    <td><b>Doc Type</b></td>
    <td><b>Source Doc</b></td>
    <td><b>URL</b></td>
    <td><b>Problem</b></td>
    <td><b>Element</b></td>
   </tr>
"""
        done = 0
        for row in rows:
            done    += 1
            msg      = "checked %d of %d URLs" % (done, len(rows))
            url      = row[2]
            if goodUrls.has_key(url):
                continue
            if deadUrls.has_key(url):
                html += self.report(row, deadUrls[url])
                continue
            pieces   = urlparse.urlparse(url)
            host     = pieces[1]
            selector = pieces[2]
            if pieces[3]: selector += ";" + pieces[3]
            if pieces[4]: selector += "?" + pieces[4]
            if pieces[5]: selector += "#" + pieces[5]
            if not host:
                html += self.report(row, "Malformed URL")
                continue
            if deadHosts.has_key(host):
                html += self.report(row, "Host not responding")
                continue
            if pieces[0] not in ('http','https'):
                html += self.report(row, "Unexpected protocol")
                continue
            try:
                http = httplib.HTTP(host)
                http.putrequest('GET', selector)
                http.endheaders()
                reply = http.getreply()
                if reply[0] / 100 != 2:
                    message = "%s: %s" % (reply[0], reply[1])
                    deadUrls[url] = message
                    html += self.report(row, message)
                else:
                    goodUrls[url] = 1
            except IOError, ioError:
                html += self.report(row, "IOError: %s" % str(ioError))
            except socket.error, socketError:
                deadHosts[host] = 1
                html += self.report(row, "Host not responding")
            except:
                html += self.report(row, "Unrecognized error")
            job.setProgressMsg(msg)
        html += """\
  </table>
 </body>
</html>
"""

        #--------------------------------------------------------------
        # Write out the report and tell the user where it is.
        #--------------------------------------------------------------
        name = "/UrlCheck-%d.html" % job.getJobId()
        file = open(REPORTS_BASE + name, "wb")
        file.write(cdrcgi.unicodeToLatin1(html))
        file.close()
        cdr.logwrite("saving %s" % (REPORTS_BASE + name), LOGFILE)
        url = "http://%s.nci.nih.gov/CdrReports%s" % (socket.gethostname(),
                                                      name)
        cdr.logwrite("url: %s" % url, LOGFILE)
        msg += "<br>Report available at <a href='%s'><u>%s</u></a>." % (
            url, url)

        body = """\
The URL report you requested can be viewed at
%s.
""" % (url)
        sendMail(job, "Report results", body)
        job.setProgressMsg(msg)
        job.setStatus(cdrbatch.ST_COMPLETED)
        cdr.logwrite("Completed report", LOGFILE)

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
                 WHERE path = '/GlossaryTerm/TermName'
                   AND doc_id = ?""", id)
            rows = cursor.fetchall()
            if not rows:
                raise Exception("GlossaryTerm %d not found" % id)
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
        url = "http://%s.nci.nih.gov/CdrReports%s" % (socket.gethostname(),
                                                      name)
        cdr.logwrite("url: %s" % url, LOGFILE)
        self.msg += "<br>Report available at <a href='%s'><u>%s</u></a>." % (
            url, url)

        body = """\
The Glossary Term report you requested can be viewed at
%s.
""" % (url)
        sendMail(job, "Report results", body)
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
# Run a report of dead URLs.
#----------------------------------------------------------------------
def checkUrls(job):
    report = UrlCheck()
    report.run(job)

#----------------------------------------------------------------------
# Run a report of protocols connected with a specific organization.
#----------------------------------------------------------------------
def nonRespondentsReport(job):
    age         = job.getParm("Age")
    baseDocType = job.getParm("BaseDocType")
    host        = job.getParm("Host")
    report      = NonRespondentsReport(age, baseDocType, host)
    report.createSpreadsheet(job)
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

## class Job:
##     def setProgressMsg(self, m): print m
##     def getEmail(self): return "***REMOVED***"
##     def getJobId(self): return 1111
##     def setStatus(self, status): print "setting status %s" % str(status)
##     def getArgs(self): return { "id": 29500 } # 36354
##     def fail(self, msg): print "FAIL: %s" % msg; sys.exit(1)
## job = Job()
## opr = OrgProtocolReview(29500, 'mahler')
## #opr = OrgProtocolReview(36354)
## opr.report(job)

#----------------------------------------------------------------------
# Top level entry point.
#----------------------------------------------------------------------
if __name__ == "__main__":

    # What's our job ID?
    if len(sys.argv) < 2:
        cdr.logwrite("No batch job id passed to CdrLongReports.py", LOGFILE)
        sys.exit(1)
    try:
        jobId = int(sys.argv[-1])
    except ValueError:
        cdr.logwrite("Invalid job id passed to CdrLongReports.py: %s" %
                     sys.argv[-1], LOGFILE)
        sys.exit(1)
    cdr.logwrite("CdrLongReports: job id %d" % jobId, LOGFILE)

    # Create the job object.
    try:
        job = cdrbatch.CdrBatch(jobId = jobId)
    except cdrbatch.BatchException, be:
        #print "milestone 0x"
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
        # That's all we know how to do right now.
        else:
            job.fail("CdrLogReports: unknown job name '%s'" % jobName,
                     logfile = LOGFILE)
    except Exception, info:
        cdr.logwrite("Failure executing job %d: %s" % (jobId, str(info)),
                     LOGFILE, 1)
        job.fail("Caught exception: %s" % str(info), logfile = LOGFILE)
