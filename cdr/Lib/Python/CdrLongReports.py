#----------------------------------------------------------------------
#
# $Id: CdrLongReports.py,v 1.6 2003-09-09 22:18:23 bkline Exp $
#
# CDR Reports too long to be run directly from CGI.
#
# $Log: not supported by cvs2svn $
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
import string, re, win32com.client

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

    def setBorders(range):
        "Add borders to all the cells in a row."
        for edge in (win32com.client.constants.xlEdgeLeft,
                     win32com.client.constants.xlEdgeTop,
                     win32com.client.constants.xlEdgeBottom,
                     win32com.client.constants.xlEdgeRight,
                     win32com.client.constants.xlInsideVertical):
            border            = range.Borders(edge)
            border.LineStyle  = win32com.client.constants.xlContinuous
            border.Weight     = win32com.client.constants.xlThin
            border.ColorIndex = win32com.client.constants.xlAutomatic

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
            for i in range(len(self.statuses)):
                if i == len(self.statuses) - 1:
                    self.statuses[i].endDate = time.strftime("%Y-%m-%d")
                else:
                    self.statuses[i].endDate = self.statuses[i + 1].startDate

    class Protocol:
        "Protocol information used for an OPS report spreadsheet."
        
        def __init__(self, id, node):
            "Create a protocol object from the XML document."
            self.id       = id
            self.leadOrgs = []
            self.statuses = []
            self.status   = ""
            self.firstId  = ""
            self.otherIds = []
            self.firstPub = ""
            self.closed   = ""
            self.types    = []
            self.ageRange = ""
            self.sponsors = []
            self.title    = ""
            profTitle     = ""
            patientTitle  = ""
            originalTitle = ""
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
                if status in statusSet:
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
    args = job.getArgs()
    for key in args:
        if key.upper().startswith("TERMID"):
            try:
                termIds.append(int(args[key]))
            except Exception, e:
                cdr.logwrite("Invalid term ID %s: %s" % (args[key], str(e)),
                             LOGFILE)
    try:
        conn = cdrdb.connect('CdrGuest', dataSource = 'bach.nci.nih.gov')
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
                 SELECT t.doc_id, MAX(v.num)
                   FROM query_term t
                   JOIN query_term s
                     ON s.doc_id = t.doc_id
                   JOIN document d
                     ON d.id = s.doc_id
                   JOIN doc_version v
                     ON v.id = d.id
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
               GROUP BY t.doc_id
               ORDER BY t.doc_id""", timeout = 360)
        rows = cursor.fetchall()
    except:
        job.fail("Database failure getting list of protocols.",
                 logfile = LOGFILE)
    xl = win32com.client.Dispatch("Excel.Application")
    xl.Visible = 0
    wb = xl.Workbooks.Add()
    sheet = xl.ActiveSheet

    # Set up the header cells in the spreadsheet's top row.
    headings = (
        'PDQ Clinical Trials',
        'Primary ID',
        'Additional IDs',
        'Date First Activated',
        'Date Moved to Closed List',
        'Type of Trial',
        'Status',
        'Age Range',
        'Sponsor of Trial'
        )
    for i in range(len(headings)):
        sheet.Cells(1, i + 1).Value = headings[i]
    cells = sheet.Cells.Range("1:1")
    cells.Font.Bold = 1
    cells.Font.Name = 'Times New Roman'
    cells.Font.Size = 10
    cells.WrapText = 1
    cells.RowHeight = 40
    cells.Interior.ColorIndex = 35
    cells.VerticalAlignment = win32com.client.constants.xlCenter
    cells.HorizontalAlignment = win32com.client.constants.xlCenter
    setBorders(cells)

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
        if prot.wasActive("1998-01-31", "2002-12-31"):
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
        rowNum += 1
        cell = sheet.Cells(rowNum, 1)
        url = ("http://www.cancer.gov/clinicaltrials/"
               "view_clinicaltrials.aspx?version=healthprofessional&"
               "cdrid=%d" % prot.id)
        hLink = sheet.Hyperlinks.Add(cell, url, "")
        wb.Styles("Hyperlink").Font.ColorIndex = 1
        hLink.TextToDisplay = prot.title
        hLink.ScreenTip = ("Left-click cell with mouse to view the "
                           "protocol document.  Left-click and hold to "
                           "select the cell.")
        sheet.Cells(rowNum, 2).Value = prot.firstId
        sheet.Cells(rowNum, 3).Value = "; ".join(prot.otherIds)
        sheet.Cells(rowNum, 4).Value = prot.firstPub
        sheet.Cells(rowNum, 5).Value = prot.closed
        sheet.Cells(rowNum, 6).Value = "; ".join(prot.types)
        sheet.Cells(rowNum, 7).Value = prot.status
        sheet.Cells(rowNum, 8).Value = prot.ageRange
        sheet.Cells(rowNum, 9).Value = "; ".join(prot.sponsors)
        row = cursor.fetchone()
        cells = sheet.Cells.Range("%d:%d" % (rowNum, rowNum))
        cells.Font.Name = 'Times New Roman'
        cells.Font.Size = 10
        cells.WrapText = 1
        cells.VerticalAlignment = win32com.client.constants.xlTop
        cells.HorizontalAlignment = win32com.client.constants.xlLeft
        cells.RowHeight = 40
        setBorders(cells)

    # Set the column widths to match the sample provided by OSP.
    sheet.Columns.Range("A:A").ColumnWidth = 43.57
    sheet.Columns.Range("B:B").ColumnWidth = 17.14
    sheet.Columns.Range("C:C").ColumnWidth = 23.57
    sheet.Columns.Range("D:D").ColumnWidth = 19.14
    sheet.Columns.Range("E:E").ColumnWidth = 19.14
    sheet.Columns.Range("F:F").ColumnWidth = 15.43
    sheet.Columns.Range("G:G").ColumnWidth = 16.71
    sheet.Columns.Range("H:H").ColumnWidth = 15.57
    sheet.Columns.Range("I:I").ColumnWidth = 22.71

    # Make the top row (the one with column labels) always visible.
    sheet.Rows.Range("2:2").Select()
    xl.ActiveWindow.FreezePanes = 1

    # Make the row heights match the sample provided by OSP.
    sheet.Cells.Select()
    xl.Selection.RowHeight = 40

    # Get rid of unwanted sheets, and rename the one we're keeping.
    xl.DisplayAlerts = 0
    xl.Sheets("Sheet1").Name = "PDQ Clinical Trials"
    xl.Sheets("Sheet2").Select()
    xl.ActiveWindow.SelectedSheets.Delete()
    xl.Sheets("Sheet3").Select()
    xl.ActiveWindow.SelectedSheets.Delete()

    # Move to the first protocol's title.
    xl.Range("A2").Select()

    # Save the report.
    name = "/OSPReport-Job%d.xls" % job.getJobId()
    wb.SaveAs(Filename = REPORTS_BASE + name)
    cdr.logwrite("saving %s" % (REPORTS_BASE + name), LOGFILE)
    url = "http://mahler.nci.nih.gov/CdrReports%s" % name
    cdr.logwrite("url: %s" % url, LOGFILE)
    msg += "<br>Report available at <a href='%s'><u>%s</u></a>." % (url, url)
    wb.Close(SaveChanges = 0)
    
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
        startDate = time.mktime(startDate)
        endDate   = time.mktime(endDate)
        startDate = time.strftime("%Y-%m-%d", time.localtime(startDate))
        endDate   = time.strftime("%Y-%m-%d 23:59:59.999",
                                  time.localtime(endDate))
        job.setProgressMsg("Job started")
        try:
            self.cursor.execute("""\
                CREATE TABLE #last_mailers
                (
                    doc_id    INTEGER,
                    mailer_id INTEGER,
                    sent_date VARCHAR(32)
                )""")
            self.conn.commit()
            job.setProgressMsg("#last_mailers table created")
            self.cursor.execute("""\
        INSERT INTO #last_mailers
             SELECT q1.int_val, MAX(q1.doc_id), q2.value
               FROM query_term q1
               JOIN query_term q2
                 ON q1.doc_id = q2.doc_id
               JOIN document d
                 ON d.id = q1.int_val
               JOIN doc_type t
                 ON t.id = d.doc_type
               JOIN query_term mailer_type
                 ON mailer_type.doc_id = q1.doc_id
              WHERE t.name = ?
                AND q1.path = '/Mailer/Document/@cdr:ref'
                AND q2.path = '/Mailer/Sent'
                AND mailer_type.path = '/Mailer/Type'
                AND mailer_type.value NOT LIKE
                               'Protocol-%%status/participant check'
           GROUP BY q1.int_val, q2.value
             HAVING q2.value BETWEEN '%s' AND '%s'""" % (startDate, endDate),
                                self.docType,
                                timeout = 300)
            self.conn.commit()
            #job.fail("Got to milestone 2", logfile = LOGFILE)
            job.setProgressMsg("#last_mailers table populated")
            self.cursor.execute("""\
                CREATE TABLE #no_reply (doc_id INTEGER, mailer_id INTEGER)""")
            self.conn.commit()
            job.setProgressMsg("#no_reply table created")
            self.cursor.execute("""\
        INSERT INTO #no_reply
             SELECT lm.doc_id, lm.mailer_id
               FROM #last_mailers lm
              WHERE NOT EXISTS(SELECT *
                                 FROM query_term q
                                WHERE q.doc_id = lm.mailer_id
                                  AND q.path = '/Mailer/Response/Received')""",
                           timeout = 300)
            self.conn.commit()
            job.setProgressMsg("#no_reply table populated")
            self.cursor.execute("""\
                 SELECT recip_name.title, 
                        #no_reply.doc_id, 
                        base_doc.doc_id, 
                        mailer_type.value, 
                     -- mailer_sent.value, 
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
                -- JOIN query_term mailer_sent
                --   ON mailer_sent.doc_id = base_doc.doc_id
        LEFT OUTER JOIN query_term response_received
                     ON response_received.doc_id = base_doc.doc_id
                    AND response_received.path = '/Mailer/Response/Received'
        LEFT OUTER JOIN query_term changes_category
                     ON changes_category.doc_id = base_doc.doc_id
                    AND changes_category.path = '/Mailer/Response'
                                              + '/ChangesCategory'
                  WHERE recipient.path = '/Mailer/Recipient/@cdr:ref'
                    AND mailer_type.path = '/Mailer/Type'
                --  AND mailer_sent.path = '/Mailer/Sent'
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
        sheet.Cells(1, 1).Value = "Mailer Non-Respondents Report %s" % (
            time.strftime("%B %d, %Y"))
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
        sheet.Cells(2, 1).Value = "Mailer Type: %s" % self.docType
        sheet.Cells(3, 1).Value = "Non-Response Time: %s" % ageString
        headerRows = 3
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
                cells.RowHeight = 12.75
                recipName = ""
                recipRows += 1
                if recipRows > 3:
                    done += 1
                    continue
            else:
                #cells.Rows.AutoFit()
                cells.RowHeight = 12.75
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
The OSP report you requested on Protocols can be viewed at
%s.
""" % url
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
        filters  = ['name:Organization Protocol Review Report Filter 1',
                    'name:Organization Protocol Review Report Filter 2']
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
The report you requested on Protocols associated with CDR%d
can be viewed at
%s.
""" % (self.id, url)
        sendMail(job, "Report results", body)
        job.setProgressMsg(msg)
        job.setStatus(cdrbatch.ST_COMPLETED)
        cdr.logwrite("Completed report", LOGFILE)

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
        # That's all we know how to do right now.
        else:
            job.fail("CdrLogReports: unknown job name '%s'" % jobName,
                     logfile = LOGFILE)
    except Exception, info:
        cdr.logwrite("Failure executing job %d: %s" % (jobId, str(info)),
                     LOGFILE, 1)
        job.fail("Caught exception: %s" % str(info), logfile = LOGFILE)
