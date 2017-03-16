#----------------------------------------------------------------------
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
#----------------------------------------------------------------------
import sys, socket, time, string, re, cdr, cdrbatch, cdrglblchg, cdrcgi, traceback


# Define log file
LF = cdr.DEFAULT_LOGDIR + "/GlobalChange.log"

# Quit processing if this many surprise exceptions are raised
MAX_EXCEPTIONS = 5

# Global message
G_progressMsg = "No docs processed yet"

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
    xmlPat = re.compile("<Err.*</Err", re.DOTALL)
    if xmlPat.findall(msg):
        msg = cdr.getErrors (msg, True)

    # Put it together for log file
    cdr.logwrite (("Error on doc %d %s:" % (docId, where), msg), LF)

    # Different format for HTML report to user
    return ("Error %s:<br>%s" % (where, msg))

#----------------------------------------------------------------------
# Utility function to log progress to the batch reporter.
#----------------------------------------------------------------------
def logDocProgress (jobObj, goodCount, failCount, totalCount):
    global G_progressMsg
    G_progressMsg = "Completed %d of %d changes, %d ok, %d failed" % \
                  (goodCount + failCount, totalCount, goodCount, failCount)
    jobObj.setProgressMsg (G_progressMsg)

#----------------------------------------------------------------------
# Utility function to append error messages together
#----------------------------------------------------------------------
def appendErr (initialErrs, addErr):
    """
    Append an error message to an existing message, separated by
    HTML <br> tag.

    Pass:
        initialErrs - What to append to, may be None.
        addErr      - Append this.

    Return:
        New error string.
    """

    # Append to errs or create new error msg
    if initialErrs:
        errs = initialErrs + "<br>" + addErr
    else:
        errs = addErr

    return errs

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
        cdr.logwrite ("filter=%s\n  parms=%s\n  save=%s\n  docId=%d pass=%d" %\
           (fltrInfo[0], fltrInfo[1], fltrInfo[2], docId, passNumber), LF)

        # First pass uses docNum and optional docVer
        # Next pass(es) use filteredDoc
        filtResp = cdr.filterDoc (session,
                   filter=fltrInfo[0], parm=fltrInfo[1],
                   docId=docNum, doc=filteredDoc, docVer=docVer)

        # Finished a filter pass
        passNumber += 1

        # Did it fail?
        if type(filtResp) != type(()):
            # Setup error return to caller
            filteredDoc = None
            errs        = appendErr (errs, logDocErr (docId,
                             "filter failure, pass=%d" % passNumber,
                             filtResp))
            break

        # Success. Save output to use as input to next round, or final result
        # But some filters just do checks, only save results if
        #   third return item indicates that this should be saved, i.e.,
        #   filter was not used for validation only
        # If not saving, then filteredDoc=result of previous filter, or None
        #   if this was the first pass
        if fltrInfo[2]:
            # Save output
            filteredDoc = filtResp[0]

            # In next iteration (if any) we filter the results of
            #   the last filtering rather than the repository doc
            docNum = None
            docVer = None

        # If messages, save them too (or only if this was a check)
        if filtResp[1]:
            # Transform XSLT message output format to simple html
            filtMsgs = re.sub (r"<message>", "", filtResp[1])
            filtMsgs = re.sub (r"</message>", "<br>", filtMsgs)
            errs = appendErr (errs, filtMsgs)

    # Did we fail to find any filters at all?
    if passNumber == 0:
        errs = appendErr (errs, logDocErr (docId, "Filtering",
                         "No filters found via getFilterInfo()"));

    # Did we fail to produce any output?
    if not filteredDoc:
        errs = appendErr (errs, logDocErr (docId, "Filtering",
                         "No output produced by filtering"));

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
# cdr.logwrite ("GCBatch: Created chg object of type=%s" % chgType,LF)
# chg.dumpSsVars()

#----------------------------------------------------------------------
# Setup session
#----------------------------------------------------------------------
# Find original session
session = jobObj.getParm (cdrcgi.SESSION)

#----------------------------------------------------------------------
# Run the actual global change
#----------------------------------------------------------------------

# We'll build two lists of docs to report
# Documents successfully changed, id + title tuples
changedDocs = [("<b>CDR ID</b>", "<b>P</b>", "<b>Title</b>", "<b>Msgs</b>")]

# Couldn't be changed, id +title + error text
failedDocs  = [("<b>CDR ID</b>", "<b>P</b>", "<b>Title</b>", "<b>Reason</b>")]

# Get the list of documents - different for each type of change
docIds = jobObj.getParm ("glblDocIds")
if not docIds:
    jobObj.fail ("Could not retrieve document IDs from CGI form", logfile=LF)

# We might have retrieved a single doc id, or a list
# Make sure the format is a list
docIdList = []
if type(docIds) == type([]):
    docIdList = docIds
else:
    docIdList.append(docIds)

cdr.logwrite ("Retrieving doc titles for final processing", LF)

# Gets list of tuples of id + title
try:
    originalDocs = cdrglblchg.getIdTitles (docIdList)
except cdrbatch.BatchException, be:
    cdr.logwrite ("Exception in getIdTitles", LF)
    jobObj.fail ("GCBatch: Unable to select docs: %s" % str(be), logfile=LF)

# Are we running in test mode only, i.e., output to files not database?
testOnly = jobObj.getParm ('testOnly')
if testOnly:
    # Create an output directory
    outputDir = cdrglblchg.createOutputDir()
    cdr.logwrite ("Running in testOnly mode\n  Output to %s" % outputDir, LF)

# Initialize counts
totalCount = len (originalDocs)
goodCount  = 0
failCount  = 0
excpCount  = 0

# Log
cdr.logwrite ("Done selecting doc titles for final processing", LF)
cdr.logwrite ("Processing %d docs" % totalCount, LF)

# Process each one
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
            docType   = oldCwdDocObj.type

            # Remember that we need to check this back in at end
            checkedOut = 1

        if not failed:

            cdr.logwrite ("Doctype='%s'" % docType, LF)

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

                    # Make sure that filtering did not invalidate a
                    #   valid document
                    valErrs = cdr.valPair(session, docType,
                                          oldCwdXml, chgCwdXml)
                    if valErrs:
                        failed = "<font color='red'>Change introduced " +\
                         "validation errors in CWD!</font>"
                        for msg in result:
                            failed += "<br>%s" % msg

                    # If test mode, write out the old, new and diff files
                    # We do this even if valPair shows that filtering
                    #   created new errors
                    if testOnly:
                        cdrglblchg.writeDocs (outputDir, docId, oldCwdXml,
                                              chgCwdXml, "cwd", valErrs)

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
                    # If we're running in test mode, fetch it for output
                    if testOnly:
                        oldPubVerDocObj = cdr.getDoc (session, docId=docId,
                                        checkout='N', version=lastPubVerNum,
                                        getObject=1)
                        oldPubVerXml = oldPubVerDocObj.xml

                    # Fetch changed/filtered form
                    (chgPubVerXml, errs) = runFilters (docId,
                                                       cdrglblchg.FLTR_PUB,
                                                       lastPubVerNum)

                    # No doc means errors only
                    if not chgPubVerXml:
                        failed = errs
                    else:
                        # Else any messages are warnings to be shown
                        pubMsgs = errs

                        # Validate the publishable version, don't try to
                        #   store it if there are validation errors
                        result = cdr.valDoc(session,
                                  docType,
                                  doc=cdr.makeCdrDoc(chgPubVerXml, docType))
                        valErrs = cdr.deDupErrs(result)

                        # Don't try to store it if there are validation errors
                        if valErrs:
                            failed = "<font color='red'>Change introduced " +\
                             "validation errors in publishable version!</font>"
                            for msg in valErrs:
                                failed += "<br>%s" % msg

                        # If in test mode, write out old, new and diff files
                        if testOnly:
                            cdrglblchg.writeDocs (outputDir, docId,
                                                  oldPubVerXml, chgPubVerXml,
                                                  "pub", valErrs)

        if not failed and not testOnly:
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
                         "Attempting to create version of pre-change doc - "+\
                         " we are aborting save of all other versions: ",
                             repDocResp)
                    cdr.logwrite (("Creating pre-change doc", "Original CWD:",
                                   oldCwdXml, "================"), LF)
                cdr.logwrite (\
                       "Finished saving copy of working doc before change", LF)

        # If new publishable version was created, store it.
        if not failed and not testOnly:
            if chgPubVerXml:
                cdr.logwrite ("About to create Doc object for version", LF)
                chgPubVerDocObj = cdr.Doc(id=docIdStr, type=docType,
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

            # Finally, the working document
            # At this point, we used to quit on failure, but it can cause
            #   problems if we do.  A publishable version can fail validation
            #   but still be stored as a non-publishable version.  If we
            #   don't go on to save the modified CWD, the last publishable
            #   version will replace it, without our wanting it to.
            chgCwdDocObj = cdr.Doc(id=docIdStr, type=docType,
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
        # This always happens in testOnly mode
        if checkedOut:
            if testOnly:
                unlockReason = "test change"
            else:
                unlockReason = "failure"
            unlockMsg = "Unlocking doc %d after %s" % (docId, unlockReason)

            # Try to unlock
            unlockResult = cdr.unlock (session, docId)
            if unlockResult:
                unlockMsg += " - FAILED: %s" % unlockResult
            else:
                checkedOut = 0

            cdr.logwrite (unlockMsg, LF)

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
        logDocProgress (jobObj, goodCount, failCount, totalCount)

        # Has user cancelled job?
        status = jobObj.getStatus()[0]
        if status != cdrbatch.ST_IN_PROCESS:
            G_progressMsg += "<br>Stopped job after seeing status = %s" % status
            cdr.logwrite (G_progressMsg, LF)
            jobObj.setStatus (cdrbatch.ST_STOPPED)
            jobObj.setProgressMsg (G_progressMsg)
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
        G_progressMsg += \
            "<br><h3>Exception halted processing on doc %d<br>" \
            "Please inform support staff</h3>\n" % docId

        # If we reached our limit, quit
        excpCount += 1
        if excpCount > MAX_EXCEPTIONS:
            G_progressMsg += "<br><h3>Stopped after %d exceptions</h3>" % \
                           excpCount
            break

# Final report
cdr.logwrite ("Finished processing", LF)
cdr.logwrite ("Final status: %s" % G_progressMsg, LF)
logDocProgress (jobObj, goodCount, failCount, totalCount)

html = """
<html><head><title>Global change report</title></head>
<body>
<h2>Final report on global change</h2>
"""
html += chg.showSoFarHtml()

html += "<h2>Final status:</h2>\n<p>" + G_progressMsg + "</p>\n"

if testOnly:
    html += "<p>Program ran in test mode, output data is in directory:<br>" +\
            " &nbsp; %s</p>\n" % outputDir
html += "<hr>\n"

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
resp = cdr.sendMail ("cdr@%s" % cdr.getHostName()[1],
                     emailList,
                     subject="Final report on global change",
                     body=safeHtml,
                     html=1)
if resp:
    # Returns None if no error
    cdr.logwrite ("Email of final report failed: %s" % resp, LF)
else:
    cdr.logwrite ("Completed Global Change - %s" % G_progressMsg, LF)

# Signal completion
jobObj.setStatus (cdrbatch.ST_COMPLETED)

# Don't leave session hanging around
cdr.logout(session)
sys.exit (0)
