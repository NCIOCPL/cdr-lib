#----------------------------------------------------------------------
# $Id: GlobalChangeBatch.py,v 1.21 2004-02-04 00:51:51 ameyer Exp $
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
# Revision 1.20  2004/01/30 02:25:27  ameyer
# Now processing documents with ID's passed from the CGI portion, rather
# than re-executing the selection statement.  This is needed because a
# user can now individually select or deselect documents from the list
# for processing.
#
# Revision 1.19  2003/12/24 18:17:07  ameyer
# Added retrieval of warnings from the cdr.repDoc calls.  Any warnings are
# included in the final output report.
#
# Revision 1.18  2003/11/14 02:16:34  ameyer
# Completion of changes for global terminology change.
# More may be done as testing continues.
#
# Revision 1.17  2003/11/05 01:44:54  ameyer
# Changes to allow a document to be filtered multiple times for one global
# change, each with different filters and/or parameters.
#
# Revision 1.16  2003/09/16 19:43:28  ameyer
# Moved assignment of filter parameters from here to cdrglblchg.py.
# Now performing filterDoc operations in a loop, allowing multiple
# filters with dynamically assigned parameters to be applied to each
# document.
#
# Revision 1.15  2003/09/09 15:41:51  ameyer
# Added proper job failure after database error selecting docs.
#
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

import sys, socket, time, string, re, cdr, cdrbatch, cdrglblchg, cdrcgi, traceback


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
# Utility function to filter a document
#----------------------------------------------------------------------
def runFilters (docId, docVerType, docVer):
    """
    Execute one or more filters to produce changed doc.

    If more than one filter is required then:
        Each filter has different parameters.
        First filter operates on doc in database.
        Subsequent filters operate on output of last filtering.

    Pass:
        docId      - Numeric document ID.
        docVerType - Tells whether we're processing the current
                     working document or a publishable version, one of:
                        cdrglblchg.FLTR_CWD
                        cdrglblchg.FLTR_PUB
        docVer     - Version number, or None for CWD.

    Return:
        Sequence of:
            Modified document (None if filtering failed).
            Messages (None if no errors or warnings).

    Notes:
        runFilters() may return a document plus warning messages.
        But if there is a serious error, a document is never returned.
    """

    # Setup name of what we're doing based on document version type
    if docVerType == cdrglblchg.FLTR_CWD:
        docVerName = "CWD"
    elif docVerType == cdrglblchg.FLTR_PUB:
        docVerName = "last publishable version"
    else:
        errs = logDocErr (docId, "runFilters",
                          "Bad docVerType %d passed" % docVerType)
        return (None, errs)

    # Filtering can be done in multiple passes
    passNumber  = 0

    # No results yet
    filteredDoc = None
    errs        = None

    # Same as docId to start, then becomes None
    docNum = docId

    # What we are about to do
    cdr.logwrite ("Filtering doc=%d docVerName=%s docVer=%s" % \
                   (docId, docVerName, str(docVer)), LF)

    # Make each pass until no more filter info to process
    while 1:
        # Find filter name and parameters
        fltrInfo = chg.getFilterInfo (docVerType)

        # If there aren't any more, we're done
        if not fltrInfo:
            break;

        # Log all filter info for debugging
        cdr.logwrite ("Type docId=%s" % str(type(docId)), LF)
        cdr.logwrite ("filter=%s\n  parms=%s\n  docId=%d pass=%d" % \
           (fltrInfo[0], fltrInfo[1], docId, passNumber), LF)

        # First pass uses docNum and optional docVer
        # Next pass(es) use filteredDoc
        filtResp = cdr.filterDoc (session,
                   filter=fltrInfo[0], parm=fltrInfo[1],
                   docId=docNum, doc=filteredDoc, docVer=docVer)

        # Finished a filter pass
        passNumber += 1

        # Did it fail?
        if type(filtResp) != type(()):
            logDocErr (docId, "filter failure, pass=%d" % passNumber, filtResp)

            # Setup error return to caller
            filteredDoc = None
            errs        = filtResp
            break

        # Else success
        # Get document and messages
        filteredDoc = filtResp[0]
        if filtResp[1]:
            # Transform XSLT message output format to simple html
            filtMsgs = re.sub (r"<message>", "", filtResp[1])
            filtMsgs = re.sub (r"</message>", "<br>", filtMsgs)

            if errs:
                errs += "<br>" + filtMsgs
            else:
                errs = filtMsgs

        # In next iteration (if any) we filter the results of
        #   the last filtering rather than the repository doc
        docNum = None
        docVer = None

    # Did we fail to find any filters at all?
    if passNumber == 0:
        errs = logDocErr (docId, "Filtering",
                          "No filters found via getFilterInfo()");

    cdr.logwrite ("Finished filtering %s" % docVerName, LF)

    return (filteredDoc, errs)

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

# Debug logging
cdr.logwrite ("GCBatch: Created chg object of type=%s" % chgType,LF)
chg.dumpSsVars()

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

# We'll build two lists of docs to report
# Documents successfully changed, id + title tuples
changedDocs = [("<b>CDR ID</b>", "<b>P</b>", "<b>Title</b>", "<b>Msgs</b>")]

# Couldn't be changed, id +title + error text
failedDocs  = [("<b>CDR ID</b>", "<b>P</b>", "<b>Title</b>", "<b>Reason</b>")]

# Get the list of documents - different for each type of change
docIdStr = jobObj.getParm ("glblDocIds")
if not docIdStr:
    jobObj.fail ("Could not retrieve document IDs from CGI form", logfile=LF)
docIdList = docIdStr.split(',')

cdr.logwrite ("Retrieving doc titles for final processing", LF)

# Gets list of tuples of id + title
try:
    originalDocs = cdrglblchg.getIdTitles (docIdList)
except cdrbatch.BatchException, be:
    cdr.logwrite ("Exception in getIdTitles", LF)
    jobObj.fail ("GCBatch: Unable to select docs: %s" % str(be), logfile=LF)

# Initialize counts
totalCount = len (originalDocs)
goodCount  = 0
failCount  = 0
excpCount  = 0

# Log
cdr.logwrite ("Done selecting doc titles for final processing", LF)
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

        # No filtering done yet
        chg.filtered = [0,0]
        chg.doneChgs = []
        chg.doneChgs.append ({})
        chg.doneChgs.append ({})

        # No problems yet
        failed     = None
        cwdMsgs    = None
        pubMsgs    = None
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
            if type(result) == type("") or type(result) == type(u""):
                failed = logDocErr (docId, "fetching last version information",
                                    result)
            else:
                (lastVerNum, lastPubVerNum, isChanged) = result
                cdr.logwrite ("lastVerNum=%d lastPubVerNum=%d isChanged=%s" %\
                              (lastVerNum, lastPubVerNum, isChanged), LF)

                # Execute one or more filters to produce changed CWD
                (chgCwdXml, errs) = runFilters (docId, cdrglblchg.FLTR_CWD,
                                                None)

                # No doc means errors only
                if not chgCwdXml:
                    failed = errs
                else:
                    # Else any messages are warnings to be shown
                    cwdMsgs = errs

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
                    (chgPubVerXml, errs) = runFilters (docId,
                                                       cdrglblchg.FLTR_PUB,
                                                       lastPubVerNum)

                # No doc means errors only
                if not chgCwdXml:
                    failed = errs
                else:
                    # Else any messages are warnings to be shown
                    pubMsgs = errs

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
                    comment="Copy of working document before %s" % \
                             chg.description)
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
                (repId, repErrs) = cdr.repDoc(session,doc=str(chgPubVerDocObj),
                    ver='Y', val='Y', checkIn='N', verPublishable='Y',
                    showWarnings = 1,
                    comment="Last publishable version, revised by %s" % \
                             chg.description)
                cdr.logwrite ("Replaced published version in CDR", LF)
                if repErrs:
                    msg = \
                 "attempting to store last publishable version after change" +\
                 "<br>Store may have failed, or version may not be publishable"
                    failed = logDocErr (docId, msg, repErrs)

        if not failed:
            # Finally, the working document
            chgCwdDocObj = cdr.Doc(id=docIdStr, type='InScopeProtocol',
                                   x=chgCwdXml, encoding='utf-8')
            cdr.logwrite ("Saving CWD after change", LF)
            (repId, repErrs) = cdr.repDoc (session, doc=str(chgCwdDocObj),
                ver="Y", verPublishable=saveCWDPubVer,
                val=saveCWDPubVer, checkIn='Y', showWarnings = 1,
                comment="Revised by %s" % chg.description)
            if repErrs:
                msg = "attempting to store changed CWD"
                if saveCWDPubVer == 'Y':
                    msg += \
                 "<br>Store may have failed, or version may not be publishable"
                failed = logDocErr (docId, msg, repErrs)

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
            # Gather up any messages produced, or keep column blankd
            msgs = ""
            if cwdMsgs or pubMsgs:
                if cwdMsgs:
                    msgs = "CWD:<br>" + cwdMsgs
                if pubMsgs:
                    if len(msgs) > 0:
                        msgs += "<br>Pub Version:<br>"
                    msgs += pubMsgs
            else:
                msgs = "&nbsp;"

            changedDocs.append ((docId, saveCWDPubVer, title, msgs))
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
