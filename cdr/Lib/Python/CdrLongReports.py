#----------------------------------------------------------------------
#
# $Id: CdrLongReports.py,v 1.1 2003-02-26 01:37:37 bkline Exp $
#
# CDR Reports too long to be run directly from CGI.
#
# $Log: not supported by cvs2svn $
#----------------------------------------------------------------------
import cdr, cdrdb, xml.dom.minidom, time, cdrcgi, cgi, sys, socket, cdrbatch
import string

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
        reportFailure(emailList,
                      "Database failure extracting active protocols")
    class ReportItem:
        def __init__(self, protId, protVer, siteType, siteName, siteId):
            self.protId      = protId
            self.protVer     = protVer
            self.siteType    = siteType
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
        for ai in prot.getElementsByTagName("ProtocolAdminInfo"):
            for ps in ai.getElementsByTagName("ProtocolSites"):
                for s in ps.getElementsByTagName("ProtocolSite"):
                    siteType = s.getAttribute("sitetype")
                    siteId   = s.getAttribute("ref")
                    #cdr.logwrite("checking site %s" % siteId, LOGFILE)
                    siteName = ""
                    for n in s.getElementsByTagName("SiteName"):
                        siteName = cdr.getTextContent(n)
                    noPhone = 1
                    for p in s.getElementsByTagName("ProtPerson"):
                        for c in p.getElementsByTagName("Contact"):
                            for d in c.getElementsByTagName("ContactDetail"):
                                for ph in d.getElementsByTagName("Phone"):
                                    if cdr.getTextContent(ph):
                                        noPhone = 0
                                    break
                                for ph in d.getElementsByTagName(
                                                         "TollFreePhone"):
                                    if cdr.getTextContent(ph):
                                        noPhone = 0
                                    break
                        # Only look at the first, per Lakshmi 2003-02-25
                        break
                    if noPhone:
                        item = ReportItem(row[0], row[1], siteType, siteName,
                                          siteId)
                        reportItems.append(item)
                        cdr.logwrite("adding report item for %s" % siteId,
                                     LOGFILE)
        done += 1
        now = time.time()
        timer = getElapsed(start, now)
        msg = "Checked %d of %d protocols; elapsed: %s" % (done, nRows, timer)
        job.setProgressMsg(msg)
        cdr.logwrite(msg, LOGFILE)

    h2 = "Checked %d Protocols; Found %d Sites Without Phone" % (nRows,
                                      len(reportItems))
    html = """\
<!DOCTYPE HTML PUBLIC '-//IETF//DTD HTML//EN'>
<html>
 <head>
  <title>Protocol Sites Without Phones</title>
  <style type='text/css'>
   h1         { font-family: serif; font-size: 16pt; color: black; }
   h2         { font-family: serif; font-size: 14pt; color: black; }
   th         { font-family: Arial; font-size: 12pt; }
   td         { font-family: Arial; font-size: 11pt; }
  </style>
 </head>
 <body>
  <h1>Protocol Sites Without Phones</h1>
  <h2>%s</h2>
  <table border='1' cellpadding='2' cellspacing='0'>
   <tr>
    <th nowrap='1'>Protocol ID</th>
    <th nowrap='1'>Protocol Version</th>
    <th nowrap='1'>Site Type</th>
    <th nowrap='1'>Site Id</th>
    <th nowrap='1'>Site Name</th>
   </tr>
""" % h2
    for item in reportItems:
        html += """\
   <tr>
    <td>CDR%010d</td>
    <td align='right'>%d</td>
    <td>%s</td>
    <td>%s</td>
    <td>%s</td>
   </tr>
""" % (item.protId, item.protVer, item.siteType, item.siteId,
       cgi.escape(item.siteName))
    html += """\
  </table>
"""
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
    html += """\
 </body>
</html>
"""
    reportName = "/ProtSitesWithoutPhones-Job%d.html" % job.getJobId()
    url = "http://%s.nci.nih.gov/CdrReports%s" % (socket.gethostname(),
                                                  reportName)
    msg += "<br>Report available at <a href='%s'><u>%s</u></a>." % (url,
                                                                    url)
    htmlFile = open(REPORTS_BASE + reportName, "w")
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
        cdr.logwrite("Unable to create batch job object: %s" % str(be),
                     LOGFILE)
        sys.exit(1)

    # Find out what we're supposed to do.
    jobName = job.getJobName()
    if jobName == "Protocol Sites Without Phones":
        protsWithoutPhones(job)

    # That's all we know how to do right now.
    else:
        job.fail("CdrLogReports: unknown job name '%s'" % jobName,
                 logfile = LOGFILE)

