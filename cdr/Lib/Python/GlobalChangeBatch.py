#----------------------------------------------------------------------
# $Id: GlobalChangeBatch.py,v 1.15 2003-09-09 15:41:51 ameyer Exp $
#
# Perform a global change
#
# This is the background half of a process begun interactively with
# GlobalChange.py.
#
# The interactive portion has gathered user parameters, verified
# them, and gotten user permission to start a global change to
# protocols in the database.  It then queues the job and returns
# to the user.  A background daemon finds the queued job and
# spawns this program to perform the changes.
#
# The program is run in batch because it can take a long time to run,
# depending on the number of protocols changed and the size of each
# one.
#
# Results are emailed to the user.
#
# Command line:
#   Last argument = job id of the Global Change job to run.
#                   Identifies row in batch_job table.
#
# $Log: not supported by cvs2svn $
# Revision 1.14  2003/06/05 18:38:29  ameyer
# Added proper character encodings for report emailed to users.
#
# Revision 1.13  2003/04/26 16:33:12  bkline
# Added encoding argument to cdr.Doc() constructor invocation.
#
# Revision 1.12  2003/04/08 18:29:39  ameyer
# Modified to always create a new version, whether the CWD is identical to the
# last publishable version or not.  Pre and post-change versions of the
# protocols are thus always created.
#
# Revision 1.11  2003/03/27 18:36:04  ameyer
# Added some logic for new insert org global change.
# Simplified and centralized handling of filter parameter and comment
# data passed to cdr.filterDoc.
#
# Revision 1.10  2002/11/27 01:38:32  ameyer
# Slight change to double check that personId can only be sent if
# leadOrgId also exists.
#
# Revision 1.9  2002/11/27 01:36:52  ameyer
# Added parameters to status change filter for the lead org id and
# person id, if user has entered them.
#
# Revision 1.8  2002/11/13 02:38:32  ameyer
# Added support for multiple emails.
#
# Revision 1.7  2002/09/24 19:31:16  ameyer
# Fixed bugs in handling of data returned by cdr.lastVersions().
#
# Revision 1.6  2002/09/19 18:01:09  ameyer
# Reorganized to place try block inside for loop instead of outside so
# that an exception would not halt all processing.
#
# Revision 1.5  2002/08/16 03:15:11  ameyer
# Switched from logging "reason" to "comment".
# Fixed serious bug in version handling.
# Added column to report to indicate whether publishable version existed.
# Made cosmetic changes.
#
# Revision 1.4  2002/08/13 21:15:36  ameyer
# Finished the third type of global change.
# Ready for production unless further testing reveals some problem.
#
# Revision 1.3  2002/08/09 03:48:05  ameyer
# Changes for organization status protocol global change.
#
# Revision 1.2  2002/08/08 18:05:18  ameyer
# Revised handling of filtering of publishable versions.
# Increased reporting to users in emailed report.
#
# Revision 1.1  2002/08/02 03:35:43  ameyer
# Batch/background portion of global protocol change.
# More to come.  This is the first working version.
#
#
#----------------------------------------------------------------------

import sys, socket, time, string, cdr, cdrbatch, cdrglblchg, cdrcgi, traceback


# Define log file
LF = cdr.DEFAULT_LOGDIR + "/GlobalChange.log"

# Quit processing if this many surprise exceptions are raised
MAX_EXCEPTIONS = 5

#----------------------------------------------------------------------
# Utility function to log an error for a specific document
#----------------------------------------------------------------------
def logDocErr (docId, where, msg):
    """
    Write a message to the log file and return it.

    Pass:
        Doc id  - Document id (numeric form).
        where   - Where it happened
        msg     - Error message.

    Return:
        Error message suitable for display.
    """
    # If the error message is in XML, extract error portion from it
    msg = cdr.getErrors (msg)

    # Put it together for log file
    cdr.logwrite (("Error on doc %d %s:" % (docId, where), msg), LF)

    # Different format for HTML report to user
    return ("Error %s:<br>%s" % (where, msg))


#----------------------------------------------------------------------
# Setup job
#----------------------------------------------------------------------
cdr.logwrite ("GCBatch: Started batch job", LF) # DEBUG

# Get job id
if len (sys.argv) < 2:
    cdr.logwrite ("No batch job id passed to GlobalChangeBatch.py", LF)
    sys.exit (1)

jobIdArg = sys.argv[len(sys.argv)-1]
try:
    jobId = int (jobIdArg)
except ValueError:
    cdr.logwrite (\
        "Last parameter '%s' passed to GlobalChangeBatch.py is not a job id"
        % jobIdArg, LF)
    sys.exit (1)
cdr.logwrite ("GCBatch: Got jobId=%d" % jobId, LF) # DEBUG

# Create the job object
# This loads a row from the batch_job table and sets the status to 'In process'
try:
    jobObj = cdrbatch.CdrBatch (jobId=jobId)
except cdrbatch.BatchException, be:
    cdr.logwrite ("Unable to create batch job object: %s" % str(be), LF)
    sys.exit (1)

# Create a global change object of the proper type
try:
    chg = cdrglblchg.createChg (jobObj.getArgs())
    chgType = jobObj.getParm ('chgType')
except cdrbatch.BatchException, be:
    # Log it and exit
    jobObj.fail ("GCBatch: create chg object failed: %s" %\
                 str(be), logfile=LF)

cdr.logwrite ("GCBatch: Created chg object of type=%s" % chgType,LF) # DEBUG


#----------------------------------------------------------------------
# Setup session
#----------------------------------------------------------------------
# Find original session
session = jobObj.getParm (cdrcgi.SESSION)
cdr.logwrite ("GCBatch: Using session: %s" % session, LF)

# Find the original user associated with this session
resp = cdr.idSessionUser (None, session)
if type(resp)==type("") or type(resp)==type(u""):
    # Failed - log and exit
    jobObj.fail ("Can't identify original user: " + resp, logfile=LF)
cdr.logwrite ("GCBatch: Got userid=%s" % resp[0], LF) # DEBUG

# Re-login the original user
session = cdr.login (resp[0], resp[1])
if session.startswith ("<Error"):
    jobObj.fail ("Can't login original user: %s:%s" % (resp[0], resp[1]),
                 logfile=LF)
cdr.logwrite ("GCBatch: Got new session: %s" % session, LF) # DEBUG

# Object needs to know new session, not old one
chg.ssVars[cdrcgi.SESSION] = session


#----------------------------------------------------------------------
# Run the actual global change
#----------------------------------------------------------------------
# Set filter parameters and comments for different types of change
parms = []
description = ""
if chgType == cdrglblchg.PERSON_CHG or chgType == cdrglblchg.ORG_CHG:
    parms.append (['changeFrom', jobObj.getParm ('fromId')])
    parms.append (['changeTo', jobObj.getParm ('toId')])
    description = "global change of %s to %s on %s" % \
                   (jobObj.getParm ('fromId'),
                    jobObj.getParm ('toId'),
                    time.ctime (time.time()))

elif chgType == cdrglblchg.INS_ORG_CHG:
    parms.append (['leadOrgID', jobObj.getParm ('restrId')])
    parms.append (['newOrgSiteID', jobObj.getParm ('insertOrgId')])
    parms.append (['newPersonID', jobObj.getParm ('insertPersId')])
    if jobObj.getParm ('coopType'):
        parms.append (['coop', jobObj.getParm ('coopType')])
    if jobObj.getParm ('specificRole'):
        parms.append (['setRole', jobObj.getParm ('specificRole')])
    if jobObj.getParm ('specificPhone'):
        parms.append (['setSpecificPhone', jobObj.getParm ('specificPhone')])
    description = "global insert of orgsite %s on %s" % \
                  (jobObj.getParm ('insertOrgId'),
                    time.ctime (time.time()))

elif chgType == cdrglblchg.STATUS_CHG:
    parms.append (['orgId', jobObj.getParm ('fromId')])
    parms.append (['oldStatus', jobObj.getParm ('fromStatusName')])
    parms.append (['newStatus', jobObj.getParm ('toStatusName')])
    if jobObj.getParm ('restrId'):
        parms.append (['leadOrgId', jobObj.getParm ('restrId')])
        if jobObj.getParm ('restrPiId'):
            parms.append (['personId', jobObj.getParm ('restrPiId')])
    description = "global status change from %s to %s on %s" % \
                  (jobObj.getParm ('fromStatusName'),
                   jobObj.getParm ('toStatusName'),
                    time.ctime (time.time()))

# We'll build two lists of docs to report
# Documents successfully changed, id + title tuples
changedDocs = [("<b>CDR ID</b>", "<b>P</b>", "<b>Title</b>")]

# Couldn't be changed, id +title + error text
failedDocs  = [("<b>CDR ID</b>", "<b>P</b>", "<b>Title</b>", "<b>Reason</b>")]

# Get the list of documents - different for each type of change
# Gets list of tuples of id + title
cdr.logwrite ("Selecting docs for final processing", LF)
try:
    originalDocs = chg.selDocs()
except cdrbatch.BatchException, be:
    jobObj.fail ("GCBatch: Unable to select docs: %s" % str(be), logfile=LF)

# Initialize counts
totalCount = len (originalDocs)
goodCount  = 0
failCount  = 0
excpCount  = 0

# Log
cdr.logwrite ("Done selecting docs for final processing", LF)
cdr.logwrite ("Processing %d docs" % totalCount, LF)

# Process each one
progressMsg = "No docs processed yet"

for idTitle in originalDocs:
    try:

        # Identify doc
        docId    = idTitle[0]
        title    = idTitle[1]
        docIdStr = cdr.exNormalize(docId)[0]

        # We'll store up to 3 versions of doc
        oldCwdXml    = None     # Original current working document
        chgCwdXml    = None     # Transformed CWD
        chgPubVerXml = None     # Transformed version of last pub version

        # No problems yet
        failed     = None
        checkedOut = 0

        # Assumption is that we will not save the filtered doc as a
        #   publishable version.  That changes if there is already
        #   a publishable version exactly matching the CWD.
        saveCWDPubVer = 'N'

        # Attempt to check it out, getting a Doc object (in cdr.py)
        cdr.logwrite ("Fetching doc %d for final processing" % docId, LF)
        oldCwdDocObj = cdr.getDoc (session, docId=docId, checkout='Y',
                                   version='Current', getObject=1)
        cdr.logwrite ("Finished fetch of doc %d for final processing" % docId,
                      LF)

        # Got a Doc object, or a string of errors
        if type (oldCwdDocObj) == type (""):
            failed = logDocErr (docId, "checking out document", oldCwdDocObj)
        else:
            oldCwdXml = oldCwdDocObj.xml

            # Remember that we need to check this back in at end
            checkedOut = 1

        if not failed:

            # Get version info
            cdr.logwrite ("Checking lastVersions", LF)
            result = cdr.lastVersions (session, docIdStr)
            cdr.logwrite ("Finished checking lastVersions", LF)
            if type(result) == type("") or type(result) == type(u""):
                failed = logDocErr (docId, "fetching last version information",
                                    result)
            else:
                (lastVerNum, lastPubVerNum, isChanged) = result

                # Filter doc to get new, changed CWD
                cdr.logwrite ("Filtering doc", LF)
                filtResp = cdr.filterDoc (session, filter=chg.chgFilter,
                                          parm=parms, docId=docId, docVer=None)

                if type(filtResp) != type(()):
                    failed = logDocErr (docId, "filtering CWD", filtResp)
                else:
                    # Get document, ignore messages (filtResp[1])
                    chgCwdXml = filtResp[0]
                cdr.logwrite ("Finished filtering doc", LF)

        if not failed:

            # If there was a publishable version, we need to alter it
            # This has to be done because the document may be published
            #   again before a human ever reviews it and makes a publishable
            #   version
            if lastPubVerNum >= 0:

                # If the publishable version is the same as the last
                #   saved version, and the last saved version is the same
                #   as the CWD, then we can just save the transformed
                #   CWD with instructions to make a publishable version.
                # This is better than retrieving the last publishable
                #   version and filtering it not only because it's faster,
                #   but also because it makes the last publishable version
                #   and the CWD identical in the eyes of the version
                #   control software.
                if lastPubVerNum == lastVerNum and isChanged == 'N':
                    saveCWDPubVer = 'Y'
                    cdr.logwrite ("Publishable version matches CWD, " \
                                  "will save it as publishable version", LF)

                else:
                    # Last published version is different from the CWD
                    # Fetch and filter it
                    cdr.logwrite ("Filtering last version", LF)
                    filtResp = cdr.filterDoc (session, filter=chg.chgFilter,
                                              parm=parms, docId=docId,
                                              docVer=lastPubVerNum)
                    if type(filtResp) != type(()):
                        failed = logDocErr(docId,
                                 "filtering last publishable version", filtResp)
                    else:
                        chgPubVerXml = filtResp[0]
                    cdr.logwrite ("Finished filtering last version", LF)

        if not failed:
            # For debug
            # willDo = ""
            # if isChanged:
            #     willDo = "<h1>Saved old working copy:</h1>" + oldCwdXml
            # if chgPubVerXml:
            #     willDo += "<h1>Changed published version:</h1>" + chgPubVerXml
            # willDo += "<h1>New CWD version:</h1>" + chgCwdXml

            # Store documents in the following order:
            #    CWD before filtering - if it's not the same as last version
            #    Filtered publishable version, if there is one
            #    Filtered CWD
            if isChanged == 'Y':
                cdr.logwrite ("Saving copy of working doc before change", LF)
                repDocResp = cdr.repDoc (session, doc=str(oldCwdDocObj),
                    ver='Y', checkIn='N', verPublishable='N',
                    comment="Copy of working document before %s" % description)
                if repDocResp.startswith ("<Errors"):
                    failed = logDocErr (docId,
                             "attempting to create version of pre-change doc",
                             repDocResp)
                    cdr.logwrite (("Creating pre-change doc", "Original CWD:",
                                   oldCwdXml, "================"), LF)
                cdr.logwrite (\
                       "Finished saving copy of working doc before change", LF)

        if not failed:
            # If new publishable version was created, store it
            if chgPubVerXml:
                cdr.logwrite ("About to create Doc object for version", LF)
                chgPubVerDocObj = cdr.Doc(id=docIdStr, type='InScopeProtocol',
                                          x=chgPubVerXml, encoding='utf-8')
                cdr.logwrite ("About to replace published version in CDR", LF)
                repDocResp = cdr.repDoc (session, doc=str(chgPubVerDocObj),
                    ver='Y', val='Y', checkIn='N', verPublishable='Y',
                    comment="Last publishable version, revised by %s" % \
                             description)
                cdr.logwrite ("Replaced published version in CDR", LF)
                if repDocResp.startswith ("<Errors"):
                    failed = logDocErr (docId,
                    "attempting to store last publishable version after change",
                    repDocResp)

        if not failed:
            # Finally, the working document
            chgCwdDocObj = cdr.Doc(id=docIdStr, type='InScopeProtocol',
                                   x=chgCwdXml, encoding='utf-8')
            cdr.logwrite ("Saving CWD after change", LF)
            repDocResp = cdr.repDoc (session, doc=str(chgCwdDocObj),
                ver="Y", verPublishable=saveCWDPubVer,
                val=saveCWDPubVer, checkIn='Y',
                comment="Revised by %s" % description)
            if repDocResp.startswith ("<Errors"):
                failed = logDocErr (docId, "attempting to store changed CWD",
                                    repDocResp)

            else:
                # Replace was successful.  Document checked in
                checkedOut = 0
            cdr.logwrite ("Finished saving CWD after change", LF)

        # If we did not complete all the way to check-in, have to unlock doc
        if checkedOut:
            cdr.unlock (session, docId)
            cdr.logwrite ("Unlocking doc %d after failure" % docId, LF)
            checkedOut = 0

        # If successful, add this document to the list of sucesses
        if not failed:
            changedDocs.append ((docId, saveCWDPubVer, title))
            goodCount += 1
        else:
            failedDocs.append ((docId, saveCWDPubVer, title, failed))
            failCount += 1

        # Record progress for user
        progressMsg = "Completed %d of %d changes, %d ok, %d failed" % \
                      (goodCount + failCount, totalCount, goodCount, failCount)
        jobObj.setProgressMsg (progressMsg)

        # Has user cancelled job?
        status = jobObj.getStatus()[0]
        if status != cdrbatch.ST_IN_PROCESS:
            progressMsg += "<br>Stopped job after seeing status = %s" % status
            cdr.logwrite (progressMsg, LF)
            jobObj.setStatus (cdrbatch.ST_STOPPED)
            jobObj.setProgressMsg (progressMsg)
            break

    except Exception, ex:
        # Write message and traceback to log file
        cdr.logwrite ("Exception halted processing of doc %d:" % docId,
                      LF, tback=1)

        # Tell user (also goes to log)
        failed = logDocErr (docId, "PLEASE INFORM SUPPORT STAFF",
                            "Exception halted processing: %s" % str(ex))
        failedDocs.append ((docId, saveCWDPubVer, title, failed))
        failCount += 1

        # Append info to message header to grab user's attention
        progressMsg += \
            "<br><h3>Exception halted processing on doc %d<br>" \
            "Please inform support staff</h3>\n" % docId

        # If we reached our limit, quit
        excpCount += 1
        if excpCount > MAX_EXCEPTIONS:
            progressMsg += "<br><h3>Stopped after %d exceptions</h3>" % \
                           excpCount
            break

# Final report
cdr.logwrite ("Finished processing", LF)
cdr.logwrite ("Final status: %s" % progressMsg, LF)

html = """
<html><head><title>Global change report</title></head>
<body>
<h2>Final report on global change</h2>
"""
html += chg.showSoFarHtml()

html += "<h2>Final status:</h2>\n<p>" + progressMsg + "</p><hr>\n"

if failCount:
    html += \
    "<h2>Documents that could <font color='red'>NOT</font> be changed</h2>\n"+\
    cdr.tabularize (failedDocs, "border='1' align='center'")

if goodCount:
    html += "<h2>Documents successfully changed</h2>\n" +\
           cdr.tabularize (changedDocs, "border='1' align='center'")

html += "\n</body></html>\n"

# Convert string of email addresses to a list
trTbl = string.maketrans (",;", "  ")
email = string.translate (jobObj.getEmail().encode("ascii"), trTbl)
emailList = string.split (email)

# Make html safe for email
if type(html)==type(u""):
    safeHtml = cdrcgi.unicodeToLatin1 (html)
else:
    safeHtml = html

# Send it by email
cdr.logwrite ("About to email final report to %s" % jobObj.getEmail(), LF)
resp = cdr.sendMail ("cdr@%s.nci.nih.gov" % socket.gethostname(),
                     emailList,
                     subject="Final report on global change",
                     body=safeHtml,
                     html=1)
if resp:
    # Returns None if no error
    cdr.logwrite ("Email of final report failed: %s" % resp, LF)
else:
    cdr.logwrite ("Completed Global Change - %s" % progressMsg, LF)

# Signal completion
jobObj.setStatus (cdrbatch.ST_COMPLETED)

sys.exit (0)
