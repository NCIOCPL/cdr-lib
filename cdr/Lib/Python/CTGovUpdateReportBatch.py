#----------------------------------------------------------------------
# Compare CTgovProtocol documents from an import job with the current
# publishable version of the the same documents.
#
# This is the batch portion of a report launched by CTGovUpdateReport.py.
#
# Done for Bugzilla issue #1881
#
# $Id: CTGovUpdateReportBatch.py,v 1.1 2006-07-03 20:10:03 ameyer Exp $
#
# $Log: not supported by cvs2svn $
#
#----------------------------------------------------------------------

import sys, time, cdr, cdrdb, cdrxdiff, cdrbatch, CTGovUpdateCommon

hostName = cdr.getHostName()[0]

# These are also used in interactive portion, which imports this file
JOB_NAME    = CTGovUpdateCommon.JOB_NAME
REPORT_FILE = CTGovUpdateCommon.REPORT_FILE
REPORT_URL  = CTGovUpdateCommon.REPORT_URL
SCRIPT      = CTGovUpdateCommon.SCRIPT
LF          = CTGovUpdateCommon.LF

# Fully qualified file name
REPORT_BASE = "d:/cdr/reports/"
REPORT_PATH = REPORT_BASE + REPORT_FILE

# Report buffer
reportBuf = []

#----------------------------------------------------------------------
# Handle fatal errors
#----------------------------------------------------------------------
def fatal(msg, batchJob=None):
    """
    Report messages to the log file and to the output file so
    users will be sure to see them.
    Then exit.

    Pass:
        Single message string or tuple of strings.
        Optional batch job object to fail.
    """
    global LF

    # Write to logfile
    cdr.logwrite(msg, LF)

    # Output to report file
    appendReport("<h2>FATAL ERROR</h2>")
    appendReport(msg)
    reportWrite()

    if batchJob:
        batchJob.fail("Job failed see debug log", logfile=LF)

    sys.exit(1)

#----------------------------------------------------------------------
# Output to report buffer
#----------------------------------------------------------------------
def appendReport(msg):
    """
    Accumulate data for the report in memory

    Pass:
        Single message string or tuple of strings.
    """
    global reportBuf

    # Convert to sequence if necessary
    if type(msg) != type(()) and type(msg) != type([]):
        msgs = (msg,)
    else:
        msgs = msg

    # Copy all messages in sequence to output buffer
    for ms in msgs:
        if type(ms) == type(u""):
            ms = ms.encode('utf-8')
        reportBuf.append(ms)
        reportBuf.append("\n")

#----------------------------------------------------------------------
# Output report buffer to file as HTML
#----------------------------------------------------------------------
def reportWrite():
    """
    Write a proper CDR style HTML header, all lines in the report
    buffer, and a proper close.
    """
    global reportBuf, REPORT_PATH

    try:
        outf = open(REPORT_PATH, "w")
    except IOError, e:
        fatal(("%s unable to open output file %s" % (SCRIPT, REPORT_PATH),
               str(e)))

    # header
    outf.write("""\
<!DOCTYPE HTML PUBLIC '-//IETF//DTD HTML//EN'>
<html>
 <head>
  <title>Imported CTGovProtocol vs. Current Working Documents Report</title>
  %s
 </head>
 <body>
 <h1>Imported CTGovProtocol vs. Current Working Documents</h1>

<h2>Date: %s</h2>

<p>
""" % (diffObj.getStyleHtml(), time.ctime()))

    # Body
    for line in (reportBuf):
        outf.write(line)

    # End
    outf.write("""\
</body>
</html>
""")
    outf.close()

#----------------------------------------------------------------------
# Find imported documents
#----------------------------------------------------------------------
def findImportedDocs(firstJob, lastJob):
    """
    Find the document ID, version number, and date for each document
    in a range of one or more import jobs.

    Document ids are identified in the ctgov_import table, but
    version numbers are not identified anywhere.  The numbers
    have to be deduced by looking for versions created:

        After the earliest job start date in the range.
        Before the next job after the latest in the range.
        Having a comment like 'ImportCTGovProtocols: %'.

    Pass:
        firstJob - Earliest ctgov_import_job job id in the range.
        lastJob  - Last in range, may be the same as firstJob.

    Return:
        List of triples of:
            Document ID
            Version number
            Version creation date
    """
    conn   = None
    cursor = None
    # Connect
    try:
        conn   = cdrdb.connect('CdrGuest')
        cursor = conn.cursor()
    except cdrdb.Error, info:
        fatal("Unable to connect to retrieve doc IDs: %s" % str(info))

    # Find the date_time of the firstJob
    try:
        cursor.execute("SELECT dt FROM ctgov_import_job WHERE id = %d" \
                        % firstJob)
        firstDate = cursor.fetchone()[0]
    except cdrdb.Error, info:
        fatal("Unable to find date of first job: %s" % str(info))

    # Find date_time of the next job after the last in range
    # If none, use today's date_time
    try:
        cursor.execute("SELECT id, dt FROM ctgov_import_job WHERE id = %d" \
                        % (lastJob + 1))
        limitJob = cursor.fetchone()
        if limitJob:
            limitDate  = limitJob[1]
        else:
            limitDate  = time.strftime("%Y-%m-%d %H:%M:%S")
    except cdrdb.Error, info:
        fatal("Unable to find date of job after last: %s" % str(info))

    # Find doc IDs in the requested range
    try:
        cursor.execute("""
         SELECT d.cdr_id AS id, max(j.dt) AS dt
           INTO #ctgov_diff_temp
           FROM ctgov_import d,
                ctgov_import_event e,
                ctgov_import_job j
          WHERE d.nlm_id = e.nlm_id
            AND e.job = j.id
            AND j.id <= %d
            AND j.id >= %d
       GROUP BY d.cdr_id
            """ % (lastJob, firstJob))
    except cdrdb.Error, info:
        fatal("Unable to select doc IDs: %s" % str(info))

    # Find the latest version number in range for each doc
    try:
        cursor.execute("""
         SELECT v.id, MAX(v.num), t.dt
           FROM doc_version v, #ctgov_diff_temp t
          WHERE v.id = t.id
            AND v.dt > '%s'
            AND v.dt < '%s'
            AND v.comment LIKE 'ImportCTGovProtocols: %%'
       GROUP BY v.id, t.dt""" % (firstDate, limitDate))
        docIdVer = cursor.fetchall()
    except cdrdb.Error, info:
        fatal("Unable to select version numbers: %s" % str(info))

    cursor.close()

    return docIdVer

#----------------------------------------------------------------------
# Main
#----------------------------------------------------------------------
# Get job id
if len (sys.argv) < 2:
    cdr.logwrite ("No batch job id passed to CTGovUpdateReportBatch.py", LF)
    sys.exit (1)

jobIdArg = sys.argv[len(sys.argv)-1]
try:
    jobId = int (jobIdArg)
except ValueError:
    cdr.logwrite (\
        "Last parm '%s' passed to CTGovUpdateReportBatch.py is not a job id"
        % jobIdArg, LF)
    sys.exit (1)

# Create the job object
# This loads a row from the batch_job table and sets the status
#   to 'In process'
try:
    batchObj = cdrbatch.CdrBatch (jobId=jobId)
except cdrbatch.BatchException, be:
    cdr.logwrite ("Unable to create batch job object: %s" % str(be), LF)
    sys.exit (1)

# Parameters in job object
importJobs = batchObj.getParm('importJobs')
diffFmt    = batchObj.getParm('diffFmt')

# Convert job ids from strings to numbers for min/max check
jobNums = []
for job in importJobs:
    jobNums.append(int(job))

# Generate list of docId, verNum pairs from user selected jobs
idVerDt = findImportedDocs(min(jobNums), max(jobNums))

# Create an object for differencing the docs
diffObj = None
if   diffFmt == "XDiff": diffObj = cdrxdiff.XDiff()
elif diffFmt == "UDiff": diffObj = cdrxdiff.UDiff()
if not diffObj:
    fatal("Internal error: Unrecognized diffFmt '%s'" % diffFmt)

# Put color info in the diff buffer, then fetch it out again
if diffFmt == "XDiff":
    diffObj.showColors("newer version", "older version")
    colors = diffObj.getDiffText()
else:
    # XXX Future
    colors = ""

appendReport("test")

if len(importJobs) == 1:
    appendReport(\
        "This report compares all documents imported by job number %s" % \
         importJobs[0])
else:
    appendReport("""\
This report compares the last version of each document imported
between job number %s and job number %s
""" % (importJobs[-1], importJobs[0]))

appendReport("""
against the current working document for each of the documents.</p>

<p>For each imported document, the report lists the:</p>
<ul>
 <li>Document ID.</li>
 <li>Version number of the version created by the import program.</li>
 <li>Date/time imported.</li>
 <li>Date/time of last update of the current working document.</li>
 <li>A difference report or a note that no differences were found.</li>
</ul>

<p>The documents are pre-filtered before comparing them so that
only significant fields are compared.</p>

<hr />
<center>
 %s
</center>
<hr />
""" % colors)

# Counters
docCount  = 0   # Total docs we compare
diffCount = 0   # Total that were different from CWDs

# Get a connection for efficiency
try:
    conn   = cdrdb.connect('CdrGuest')
    cursor = conn.cursor()
except cdrdb.Error, info:
    fatal("Unable to connect to DB to start run: %s" % str(info))

# Run the difference report
for (docId, docVer, docDt) in idVerDt:
    # Header for one document
    appendReport("""
<br /><font size="+1">%s version: %d dated: %s vs CWD dated: %s</font><br />
""" % \
                    (cdr.exNormalize(docId)[0], docVer, docDt,
                    cdr.getCWDDate(docId, conn)))

    # Do the diff
    diffText = diffObj.diff(doc1Id=docId, doc1Ver=docVer, doc2Ver=0,
           filter=['name:Extract Significant CTGovProtocol Elements'])
    if diffText:
        appendReport(diffText)
        appendReport("<br />")
        diffCount += 1
    else:
        appendReport("[No significant differences]")
    docCount += 1

# Summary and termination
appendReport("""
<center>
<hr />
<h2>Summary</h2>
<table border='2' cellpadding='10'>
 <tr>
  <th align='right'>Total documents processed: </th>
  <th>%d</th>
 </tr>
 <tr>
  <th align='right'>Documents with differences: </th>
  <th>%d</th>
 </tr>
</table>
</center>
""" % (docCount, diffCount))

# Output the report
reportWrite()

# Notify user by email
emailList = batchObj.getEmailList()
if len(emailList):
    resp = cdr.sendMail("cdr@%s.nci.nih.gov" % hostName, emailList,
                     subject="CTGov Update Report has completed", body="""
The CTGov Update report has completed.
The report can be viewed at: <a href="%s">%s</a>
""" % (REPORT_URL, REPORT_URL), html=1)

    if resp:
        # Returns None if no error
        cdr.logwrite("Email of CTGovUpdateReportBatch notification failed: %s"\
                      % resp, LF)

# Signify completion in the database
batchObj.setStatus(cdrbatch.ST_COMPLETED)

sys.exit(0)
