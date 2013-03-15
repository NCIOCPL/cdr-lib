#----------------------------------------------------------------------
#
# $Id$
#
# Harness for one-off jobs to apply a custom modification to a group
# of CDR documents.
#
# $Log: not supported by cvs2svn $
# Revision 1.30  2009/07/08 03:05:50  ameyer
# Added some functions to provide more reporting capability.
#
# Revision 1.29  2009/07/07 21:00:14  ameyer
# Fixed bug in setMaxErrors(), was modifying the wrong global variable.
#
# Revision 1.28  2008/06/21 13:35:44  bkline
# Passed in errorsExpected = False for call to getErrors().
#
# Revision 1.27  2008/06/10 15:29:26  bkline
# Replaced regular-expression parsing of errors with call to
# cdr.getErrors().
#
# Revision 1.26  2007/10/26 04:45:34  ameyer
# Made a number of tweaks to the statistical info that can be used
# in a report.
#
# Revision 1.25  2007/10/16 21:21:39  ameyer
# Beefed up getProcessed() to be able to do a lot more.
#
# Revision 1.24  2007/10/12 05:20:09  ameyer
# Only unlocking doc if we locked it.
#
# Revision 1.23  2007/10/10 04:04:51  ameyer
# Added logic and interfaces to enable a calling program to get more
# information about what happened with each document selected for change.
#
# Revision 1.22  2007/09/12 00:19:46  ameyer
# Added graceful return for empty doc ID list, plus getters for stats.
#
# Revision 1.21  2007/02/01 16:00:52  bkline
# Cleaned up Doc.saveChanges() so that it takes parameters specific to
# what is needed rather than an entire Job object.
#
# Revision 1.20  2007/01/23 19:02:15  ameyer
# Added error count to final report.
#
# Revision 1.19  2007/01/16 19:06:20  ameyer
# Added separate exception catcher for DocumentLocked exception - no abort.
# Replaced yes/no DEBUG flag with error counter and error max.
#
# Revision 1.18  2007/01/10 05:50:37  ameyer
# Significant further revisions to the way versions are handled in order
# to get right versioning in right order.
# Also added additional logging, tracking, and debugging controls.
#
# Revision 1.17  2006/12/08 03:31:56  ameyer
# And once more.  I removed the _transformCWD and setter for it.  The CWD
# will always be modified if any version is modified, so there's no point in
# creating a control to turn off modification of the CWD.
#
# Revision 1.16  2006/12/08 03:07:12  ameyer
# Once more into the breach with version controlled transformations.
# This is a surprisingly complicated program.
#
# Revision 1.15  2006/12/08 02:44:20  ameyer
# Last modifications to handle control over version saving were too simple.
# This one tries to get it right.
#
# Revision 1.14  2006/12/06 04:51:29  ameyer
# Added controls to allow a programmer to turn on or off processing of
# specific version types, i.e., CWD, last version, or last published version.
#
# Revision 1.13  2006/01/26 21:47:21  ameyer
# Added function to suppress logging to stderr - useful when running
# a ModifyDocs object in a cgi program.
#
# Revision 1.12  2005/08/15 21:05:07  ameyer
# Added validation after transform and before store.  If a document was
# valid but became invalid, it will not be stored.  Messages are logged
# and written to the output test directory if in test mode.
#
# Revision 1.11  2005/05/26 23:45:07  bkline
# Fix to create output directory in test mode for SiteImporter subclass.
#
# Revision 1.10  2005/03/04 19:19:45  bkline
# Suppressed logging of everValidated setting.
#
# Revision 1.9  2005/01/26 23:43:21  bkline
# Added custom exception DocumentLocked.
#
# Revision 1.8  2005/01/26 00:14:51  bkline
# Changed logic to avoid validating documents for which we have no
# record of validation having been performed in the past.  This
# avoids stripping XMetaL processing instruction prompts from
# protocols for which the ScientificInfo document has not yet been
# folded in.
#
# Revision 1.7  2004/11/22 20:58:46  bkline
# Fixed a typo in the comment for the transformation/versioning logic.
# Added another substantial comment to explain why the test to determine
# whether it's necessary to create a new unpublishable version works
# correctly.
#
# Revision 1.6  2004/09/23 21:29:23  ameyer
# Added ability to write output to file system or to database, depending
# on whether we are in test mode or regular run mode.
# Changed save routine to save CWD before modifications.
#
# Revision 1.5  2004/07/27 15:46:38  bkline
# Added log entry to say how many documents were selected.
#
# Revision 1.4  2004/03/31 13:46:04  bkline
# Fixed typo (lastp for lastv); added DEBUG flag, which if set will
# re-throw any exceptions caught.
#
# Revision 1.3  2003/09/11 22:12:24  bkline
# Suppressed debugging `raise' statement.
#
# Revision 1.2  2003/09/02 14:00:14  bkline
# Suppressed logging of warnings when saving unmodified CWD.
#
# Revision 1.1  2003/08/21 19:29:02  bkline
# Harness for one-off global changes.
#
#----------------------------------------------------------------------
import cdr, cdrdb, cdrglblchg, sys, time, copy

LOGFILE = 'd:/cdr/log/ModifyDocs.log'

#----------------------------------------------------------------------
# Custom exception indicating that we can't check out a document.
#----------------------------------------------------------------------
class DocumentLocked(Exception): pass


#----------------------------------------------------------------------
# Error controls
# If exception caught at top level and Job._self.countErrors > _maxErrors,
#   halt processing
# NOTES: Global change program that instantiates a ModifyDocs.Job may
#        have its own exception handlers in its own run() callback
#        routine.  Hence some exceptions may never reach the Job level
#        error counter, never increment the count, and never stop the Job.
#----------------------------------------------------------------------
_maxErrors = 0

# Caller can alter this
def setMaxErrors(maxErrs):
    global _maxErrors
    _maxErrors = maxErrs

#----------------------------------------------------------------------
# Module level variables (statics)
#----------------------------------------------------------------------
_testMode    = True   # True=Output to files only, not database
_outputDir   = None   # Files go in this directory
_validate    = True   # True=Check that change didn't invalidate valid doc
_noSaveOnErr = False  # True=Don't save any doc if change invalidate a version

#----------------------------------------------------------------------
# Class to track the disposition of a single document
#
# One of these created for each document to indicate what happened
# to it.
#----------------------------------------------------------------------
class Disposition:
    def __init__(self, docId, errMsg=None):
        """
        Describes disposition of a document.
        Normally created with an empty array of error messages, but
        the caller can create one of these just to record a message.
        """
        self.docId        = docId
        self.cwdChanged   = False
        self.lastvChanged = False
        self.lastpChanged = False
        self.errMsg       = errMsg
        self.warnMsgs     = None

    def __str__(self):
        return "%s: cwd=%s lv=%s lpv=%s msgs=%s warns=%s" % (self.docId,
               self.cwdChanged, self.lastvChanged, self.lastpChanged,
               self.errMsg, self.warnMsgs)

#----------------------------------------------------------------------
# Class for one modification job.
#----------------------------------------------------------------------
class Job:
    """
    The main class for global changes.
    """
    def __init__(self, uid, pwd, filter, transform, comment, testMode=True,
                 logFile=LOGFILE, validate=True, haltOnValErr=False):
        """
        Create a new one-off job to apply a custom modification to
        a group of CDR documents.

        Pass:
            uid        - CDR user ID of operator, or active session ID.
            pwd        - Password for CDR account.
                         If !pw, then uid = session ID, else uid = user ID.
            filter     - Object with method to get document IDs to be
                         processed; must have method getDocIds() that returns
                         a sequence of CDR IDs as integers.
            transform  - Object which knows how to take the old XML
                         for a CDR document and transform it using
                         the algorithm appropriate to this job; the
                         name of this method must be run().
                         transform and filter can be methods in the same
                         object.
            comment    - String to be stored with new versions.
            testMode   - True = write output to files, do not update database.
                         False = Modify the database.
            logFile    - Optional path for logfile.
            validate   - True=validate that the transform did not invalidate
                         a previously valid document.  NB: if the original
                         document was invalid, we assume that the transformed
                         one will be invalid and don't validate it.  We
                         will ignore haltOnValErr.
          haltOnValErr - True  = If the transform invalidates a valid doc,
                                 don't save any versions of that doc.
                         False = Save anyway.
                         sets _noSaveOnErr

        Notes:
            It can be useful to set the Job object as a field in the
            transform/filter objects.  Then logging with object.job.log()
            will intersperse logged comments with logging from ModifyDocs in
            one log file.
        """
        global _testMode
        global _validate
        global _noSaveOnErr

        self.logFile   = open(logFile, 'a')
        self.logOpen   = True
        self.filter    = filter
        self.transform = transform
        self.comment   = comment
        self.conn      = cdrdb.connect('CdrGuest')
        self.cursor    = self.conn.cursor()

        # Set session based on passed uid/session id
        if pwd:
            # Caller passed a user id + password
            self.session = cdr.login(uid, pwd)
        else:
            # Caller passed a session id
            self.session = session
        error = cdr.checkErr(self.session)
        if error:
            raise Exception("Failure logging into CDR: %s" % error)

        self.noStdErr  = False
        _testMode      = testMode
        _validate      = validate
        _noSaveOnErr   = haltOnValErr

        # Controls for which versions are transformed, with defaults
        # To override, call Job.setTransformVER() before calling run().
        self.__transformVER = True   # Transform CWD plus lastp and lastv
                                     # False = transform CWD only

        # Control to change the active_status of all documents in this job
        # Used for blocking docs, though could conceivably used for other
        #   purposes.
        # None = no change.
        self.__docActiveStatus = None

        # Max docs to process, call setMaxDocs to use a lower number for
        #   debugging or to prevent runaways
        # To override, call Job.setMaxDocs() before calling run().
        self.__maxDocs = 9999999

        # Statistics
        self.__countDocsSelected  = 0
        self.__countDocsProcessed = 0
        self.__countDocsSaved     = 0
        self.__countVersionsSaved = 0
        self.__countLocked        = 0
        self.__countErrors        = 0

        # Sequence of Dispositions, one for each doc examined
        self.__dispositions = []

        # Counters for different types of saves
        # Key = message passed to Doc.__saveDoc, e.g., " new pub"
        # Value = count of versions saved with that message
        self.__countMsgs = {}

    def createOutputDir(self):
        global _outputDir

        # Raises exception to exit program if fails
        _outputDir = cdrglblchg.createOutputDir()
        self.log("Running in test mode.  Output to: %s" % _outputDir)

    #------------------------------------------------------------------
    # Setter for transform version control
    #
    # If set False, no versions will be transformed or saved.
    #
    # There is no _transformCWD or setTransformCWD().
    # It isn't possible to save a version without overwriting the current
    # working document.  Therefore the program ALWAYS modifies the current
    # working document.  Only the last version and last publishable
    # version can be blocked from change.
    #
    # Note however that a change to the CWD will always create a new last,
    # non-publishable version if it is different from the current last
    # version (whether publishable or not.)  That is by design to avoid
    # complete, unrecoverable obliteration of the previous CWD.
    #
    # This MUST be called before calling Job.run() or it is too late to
    # set it False.
    #------------------------------------------------------------------
    def setTransformVER(self, setting):
        self.__transformVER = setting

    #------------------------------------------------------------------
    # Setter to force all documents saved to have a particular active_status.
    # Typically used for blocking documents.
    #
    # Call before calling job.run().
    #
    # See Doc.setActiveStatus() and Doc.__saveDoc().
    #------------------------------------------------------------------
    def setActiveStatus(self, setting):
        self.__docActiveStatus = setting

    #------------------------------------------------------------------
    # Limit processing to no more than this number of docs
    #
    # This MUST be called before calling Job.run() or it is too late to
    # set it False.
    #------------------------------------------------------------------
    def setMaxDocs(self, maxDocs):
        self.__maxDocs = maxDocs

    #------------------------------------------------------------------
    # Get statistics for the run
    #------------------------------------------------------------------
    def getCountDocsSelected(self):
        return self.__countDocsSelected

    def getCountDocsProcessed(self):
        return self.__countDocsProcessed

    def getCountDocsSaved(self):
        return self.__countDocsSaved

    def getCountVersionsSaved(self):
        return self.__countVersionsSaved

    def getCountDocsLocked(self):
        return self.__countLocked

    def getCountErrors(self):
        return self.__countErrors

    def getNotCheckedOut(self, markup=False):
        """
        Get a list or an HTML table of docs not checked out, with
        associated messages.

        Pass:
            markup - True = return data as an HTML table.

        Return:
            If markup:
                A string containing HTML.
            Else:
                A sequence of tuples of docId, error message.

            If no docs failed checkout:
                None
        """
        docCount = 0
        results  = []
        if markup:
            # Column headers for HTML table
            results.append(("<b>Doc ID</b>", "<b>Reason</b>"))

        # Read the dispositions
        for disp in self.__dispositions:

            # Only report those with errors
            if disp.errMsg:
                results.append((disp.docId, disp.errMsg))
                docCount += 1

        # Return info in requested format
        if docCount:
            if markup:
                return cdr.tabularize(results, "border='1' align='center'")
            else:
                return results

        # Or no results
        return None

    def getSummary(self, markup=False):
        """
        Produce a summary of the run.

        Pass:
            markup - True = return data as an HTML table, else text.

        Return:
            String of text or HTML.
        """
        if not markup:
            report = """
           Selected docs: %d
          Processed docs: %d
              Docs saved: %d
        Versions changed: %d
   Docs locked by others: %d
""" % (self.getCountDocsSelected(), self.getCountDocsProcessed(),
       self.getCountDocsSaved(), self.getCountVersionsSaved(),
       self.getCountDocsLocked())

        else:
            report = """
<table border="1">
 <tr><td align='right'>Selected docs: </td><td>%d</td></tr>
 <tr><td align='right'>Processed docs: </td><td>%d</td></tr>
 <tr><td align='right'>Docs saved: </td><td>%d</td></tr>
 <tr><td align='right'>Versions changed: </td><td>%d</td></tr>
 <tr><td align='right'>Locked by others: </td><td>%d</td></tr>
</table>
""" % (self.getCountDocsSelected(), self.getCountDocsProcessed(),
       self.getCountDocsSaved(), self.getCountVersionsSaved(),
       self.getCountDocsLocked())

        return report


    def getProcessed(self, changed=True, unchanged=True, countOnly=False,
                     docIdOnly=False, markup=False):
        """
        Get a list or an HTML table of docs that were processed, with
        information about what was done for each.

        Pass:
            changed   - True = Include docs that show a change by
                        filtering.
            unchanged - True = Include docs that were not changed by
                        filtering.
            countOnly - True = Don't return any specific document data,
                        just return a count of qualifying docs.
            docIdOnly - True = Don't return any document version data,
                        just return a list of doc IDs.
            markup    - True = return data as an HTML table.
                        Only makes sense if countOnly=False.

        Return:
            If markup:
                A string containing HTML.
            Else:
                if countOnly:
                    A number.
                else if docIdOnly:
                    A sequence of doc IDs.
                else:
                    A sequence of tuples of docId + Y/N flags for each of
                    current working document, last version, and last pub
                    version.

            If no docs processed:
                if countOnly:
                    0
                else:
                    None
        """
        # Validate parms
        if not changed and not unchanged:
            raise cdr.Exception(
            "ModifyDocs.getProcessed called with changed/unchanged both False")

        docCount = 0
        results  = []
        if markup:
            # Column headers for HTML table
            results.append(("<b>Doc ID</b>", "<b>CWD</b>", "<b>LV</b>",
                            "<b>LPV</b>", "<b>Warnings</b>"))

        # Read the dispositions
        for disp in self.__dispositions:

            # Doc not counted as modified unless we find that it was
            docModified = False

            # Only report those with no errors
            if not disp.errMsg:
                cwd = 'N'
                if disp.cwdChanged:
                    cwd = 'Y'
                    docModified = True
                lv = 'N'
                if disp.lastvChanged:
                    lv = 'Y'
                    docModified = True
                lpv = 'N'
                if disp.lastpChanged:
                    lpv = 'Y'
                    if disp.warnMsgs:
                        lpv = 'I'
                    docModified = True

                # Convert warnings as needed for response
                warnStr = ""
                if disp.warnMsgs:
                    warnStr = "<font color='red'>"
                    if type(disp.warnMsgs) in (type([]), type(())):
                        count   = 0
                        for warn in disp.warnMsgs:
                            warnStr += warn
                            count += 1
                            if count < len(disp.warnMsgs):
                                if markup:
                                    warnStr += "<br />"
                                else:
                                    warnStr += "; "
                    else:
                        warnStr = disp.warnMsgs
                    warnStr += "</font>"

                # Did we find what the caller wanted?
                if ( (docModified and changed) or
                     (not docModified and unchanged) ):
                    docCount += 1
                    if not countOnly:
                        if docIdOnly:
                            results.append(disp.docId)
                        else:
                            results.append((disp.docId, cwd, lv, lpv, warnStr))

        # Return info in requested format
        if docCount:
            if countOnly:
                return docCount
            if markup:
                return cdr.tabularize(results, "border='1' align='center'")
            else:
                return results

        # Or no results
        if countOnly:
            return 0
        return None

    #------------------------------------------------------------------
    # Check for locked documents
    #------------------------------------------------------------------
    def checkLocks(self):
        """
        Check each document selected for the global.
        Report any that are locked to the logfile and standard out.

        Typical usage:
            if job.checkLocks() != 0:
                job.log("Documents were locked.  See log file.  Exiting.")
                sys.exit(1)

        Return:
            Count of currently locked docs.  0 if none.
        """
        # Begin logging
        self.log("\nChecking document locks for global change:\n%s" %
                  self.comment)

        idList = self.filter.getDocIds()
        totalCount  = len(idList)
        lockedCount = 0

        for docId in idList:
            lockObj = cdr.isCheckedOut(docId)
            if lockObj:
                # Report and count each one
                msg = "Locked %7s: %s: %s\n" % \
                      (lockObj.docId, lockObj.docType, lockObj.docTitle)
                msg += "            by: %s Since: %s" % \
                      (lockObj.userFullName, lockObj.dateOut)
                self.log(msg)
                lockedCount += 1

        # Report summary

        msg = """Checkout summary:
            Total documents processed: %5d
      Total currently locked by users: %5d
      Total unlocked, okay for global: %5d
""" % (totalCount, lockedCount, totalCount - lockedCount)
        self.log(msg)

        return lockedCount

    #------------------------------------------------------------------
    # Run a transformation.
    #
    # Create the job, set any desired post constructor settings,
    # then call self.run().
    #------------------------------------------------------------------
    def run(self):
        global _maxErrors

        # In test mode, create output directory for files
        if _testMode:
            self.createOutputDir()
        else:
            self.log("Running in real mode.  Updating the database")

        # For reporting time
        startTime = time.time()

        # Get docs
        ids = self.filter.getDocIds()

        # If no docs, log info and abort
        if not ids or len(ids) == 0:
            self.log("No documents selected.\n  Purpose: %s\n  Ending run!\n"
                     % self.comment)
            return

        # Get docs, log count and purpose/comment for job
        self.log("%d documents selected\n  Purpose: %s" % (len(ids),
                                                           self.comment))
        self.__countDocsSelected = len(ids)


        # Change all docs
        logger = self
        for docId in ids:
            doc = None
            lockedDoc = True
            try:
                # Need this info for logging
                # Getting it again in Doc constructor, but it's cheap,
                #   cached in the DB, and offers less disturbance to
                #   existing division of labor to fetch it twice
                # XXX might just pass it in
                # XXX might pass in reference to job and put it in self
                (lastAny, lastPub, changedYN) = \
                    cdr.lastVersions('guest', "CDR%010d" % docId)

                # Log doc ID, active status, version info
                # Format of version info is:
                #   [lastPubVerNum/lastVerNum/changedYN]
                #   changedYN: 'y' = CWD changed from last version.
                msg = "Processing CDR%010d" % docId
                if cdr.getDocStatus("guest", docId) == 'I':
                    msg += " (BLOCKED)"
                msg += " ["
                if lastPub >=0:
                    msg += "pub:%d" % lastPub
                msg += '/'
                if lastAny >=0:
                    msg += "last:%d" % lastAny
                msg += "/cwd:%s]" % ('New' if changedYN=='y' else str(lastAny))
                self.log(msg)

                # Process doc
                # Instantiation of the ModifyDocs.Doc object runs all of
                # the retrievals and performs all of the transforms for
                # all versions needing transformation.
                doc = Doc(docId, self.session, self.transform, self.comment,
                          self.__transformVER)

                # If caller wants to change document status (e.g., block
                #   all docs), signify that here.  Doc.__saveDoc() will
                #   use this.
                if self.__docActiveStatus is not None:
                    doc.setActiveStatus(self.__docActiveStatus)

                # Transforms are now complete and cached in memory
                # saveChanges() writes whatever must be written to the
                # database (run mode) or file system (test mode)
                doc.saveChanges(self.cursor, logger)

                # One distinct doc saved
                if doc.versionMessages:
                    self.__countDocsSaved += 1

                # Number of distinct versions of this doc saved
                if doc.disp.cwdChanged:
                    self.__countVersionsSaved += 1
                if doc.disp.lastvChanged:
                    self.__countVersionsSaved += 1
                if doc.disp.lastpChanged:
                    self.__countVersionsSaved += 1

                for msg in doc.versionMessages:
                    self.__countMsgs[msg] = self.__countMsgs.get(msg, 0) + 1
            except DocumentLocked, info:
                # Lock failed
                lockedDoc = False
                self.__countLocked += 1
                # Log it, but always continue
                self.log("Document %d: %s" % (docId, str(info)))
            except Exception, info:
                self.log("Document %d: %s" % (docId, str(info)))
                self.__countErrors += 1
                if self.__countErrors > _maxErrors:
                    raise

            # Save disposition
            if doc:
                self.__dispositions.append(doc.disp)
            else:
                # If exception prevented creation of a Doc object
                self.__dispositions.append(Disposition(docId, str(info)))

            # Unlock, but only if we locked it
            if lockedDoc:
                cdr.unlock(self.session, "CDR%010d" % docId)

            # Progress count
            self.__countDocsProcessed += 1
            if self.__countDocsProcessed >= self.__maxDocs:
                self.log(
                  "Halting processing after reaching MAXDOCS limit of %d docs"
                  % self.__maxDocs)
                break

        # Cumulate results
        msgKeys = self.__countMsgs.keys()
        msgKeys.sort()
        msgReport = []
        if msgKeys:
            msgReport = ["Specific versions saved:\n"]
            for msg in msgKeys:
                msgReport.append("  %s = %d\n" % (msg, self.__countMsgs[msg]))

        # Report results
        self.log(
"""Run completed.
   Docs examined    = %d
   Docs changed     = %d
   Versions changed = %d
   Could not lock   = %d
   Errors           = %d
   Time (hh:mm:ss)  = %s
 %s""" % (self.getCountDocsProcessed(), self.getCountDocsSaved(),
          self.getCountVersionsSaved(), self.getCountDocsLocked(),
          self.getCountErrors(), cdr.getElapsed(startTime),
          "".join(msgReport)))


    #------------------------------------------------------------------
    # Call this to start/stop use of stderr in logging.
    # If this is not called, logging will occur.
    #------------------------------------------------------------------
    def suppressStdErrLogging(self, suppress=True):
        self.noStdErr = suppress

    #------------------------------------------------------------------
    # Log processing/error information with a timestamp.
    #------------------------------------------------------------------
    def log(self, what):
        what = "%s: %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), what)
        self.logFile.write(what)
        if not self.noStdErr:
            sys.stderr.write(what)

    # Flush and close logfile
    def __del__(self):
        try:
            if self.logOpen:
                self.logFile.close()
                self.logOpen = False
        except:
            pass

#----------------------------------------------------------------------
# Class for a CDR document.
#----------------------------------------------------------------------
class Doc(object):

    def __init__(self, id, session, transform, comment, transformVER=True):

        self.id           = id
        self.session      = session
        self.transform    = transform
        self.comment      = comment
        self.versions     = cdr.lastVersions('guest', "CDR%010d" % id)
        self.__messages   = []
        self.disp         = Disposition(id)
        self.activeStatus = None
        self.transformVER = transformVER
        self.loadAndTransform()

    #------------------------------------------------------------------
    # Class to expose read-only access to messages used to describe
    # versions of the document being saved, without the overhead of
    # cloning the list each time it's accessed.
    #------------------------------------------------------------------
    class VersionMessages:
        def __init__(self, messages):
            self.__i = 0
            self.__m = messages
        def __iter__(self):
            return self
        def next(self):
            if self.__i >= len(self.__m):
                raise StopIteration
            self.__i += 1
            return self.__m[self.__i - 1]
        def __nonzero__(self):
            return len(self.__m) > 0
        def __len__(self):
            return len(self.__m)
    def __getVersionMessages(self):
        return Doc.VersionMessages(self.__messages)
    versionMessages = property(__getVersionMessages)

    #------------------------------------------------------------------
    # Uses xml comparator from cdr module.
    #------------------------------------------------------------------
    def compare(self, a, b):
        return cdr.compareXmlDocs(a, b)

    #------------------------------------------------------------------
    # Force a change in active_status, e.g., to block this document
    # during a __saveDoc operation.
    #
    # This should be called in the derived class run() method, before
    # returning XML to the ModifyDocs module for saving.
    #
    # Unless a Doc object is reused for multiple documents (not currently
    # done in this module), the change in active_status will only affect
    # the one document represented by this Doc object.
    #------------------------------------------------------------------
    def setActiveStatus(self, status):
        # Only allow valid values
        if status not in ('A', 'I', 'D', None):
            raise cdr.Exception(
                "Invalid status '%s' in ModifyDocs.Doc.setActiveStatus" %
                 status)
        self.activeStatus = status

    #------------------------------------------------------------------
    # Check out the CWD, last version, and last publishable version,
    # and run them through the transformation for this job.
    #------------------------------------------------------------------
    def loadAndTransform(self):

        # Stored versions
        self.cwd          = None
        self.lastv        = None
        self.lastp        = None
        self.newCwdXml    = None
        self.newLastvXml  = None
        self.newLastpXml  = None
        self.cwdVals      = None
        self.lastvVals    = None
        self.lastpVals    = None
        (lastAny, lastPub, changedYN) = self.versions

        # We only need to checkout the docs in live runs
        # Test mode won't save anything, so we don't need checkouts
        if _testMode:
            checkout = 'N'
        else:
            checkout = 'Y'

        # Checkout current working document to get doc and lock
        self.cwd = cdr.getDoc(self.session, self.id, checkout, getObject = True)
        if type(self.cwd) in (str, unicode):
            err = cdr.checkErr(self.cwd) or self.cwd
            raise DocumentLocked("Unable to check out CWD for CDR%010d: %s" %
                                 (self.id, err))

        # If the cwd is not the same as the last version, or there is no
        #   saved version, we'll save the cwd and store it as a version
        #   before storing anything else
        # We don't store it yet because we don't know yet that anything
        #   else will actually need to be stored
        if (changedYN == 'Y' or lastAny < 1):
            self.saveThisDocFirst = copy.deepcopy(self.cwd)
        else:
            self.saveThisDocFirst = None

        # Run the transformation filter on current working doc
        self.newCwdXml = self.transform.run(self.cwd)

        # If (and only if) old version was valid, validate new version
        # cwdVal is array of error messages or empty array
        if _validate:
            self.cwdVals = cdr.valPair(self.session, self.cwd.type,
                                       self.cwd.xml, self.newCwdXml)

        # If we're processing versions
        if self.transformVER:

            # If there is a last version
            if lastAny > 0:

                # And it's not the same as the CWD
                if changedYN == 'Y':
                    self.lastv = cdr.getDoc(self.session, self.id, checkout,
                                            str(lastAny),
                                            getObject = 1)
                    if type(self.lastv) in (type(""), type(u"")):
                        err = cdr.checkErr(self.lastv) or self.lastv
                        raise Exception("Failure retrieving lastv (%d) "
                                        "for CDR%010d: %s" % (lastAny,
                                                           self.id, err))

                    # Transform
                    self.newLastvXml = self.transform.run(self.lastv)
                    if _validate:
                        self.lastvVals = cdr.valPair(self.session,
                            self.lastv.type, self.lastv.xml, self.newLastvXml)
                else:
                    # Lastv was same as cwd, don't need to load it, just
                    #   reference the existing self.cwd
                    # See warning ("BEWARE") below
                    # After this, any change to lastv.xml changes cwd.xml
                    self.lastv       = self.cwd
                    self.newLastvXml = self.newCwdXml
                    self.lastvVals   = self.cwdVals

            # If there is a last publishable version
            if lastPub > 0:

                # If it's not the same as the last version
                if lastPub != lastAny:
                    self.lastp = cdr.getDoc(self.session, self.id, checkout,
                                            str(lastPub),
                                            getObject = 1)
                    if type(self.lastp) in (type(""), type(u"")):
                        err = cdr.checkErr(self.lastp) or self.lastp
                        raise Exception("Failure retrieving lastp (%d) "
                                        "for CDR%010d: %s" % (lastPub,
                                                           self.id, err))

                    # Transform
                    self.newLastpXml = self.transform.run(self.lastp)
                    if _validate:
                        # Original plan was to validate pub version even if
                        #   last pub was invalid, but this is safe since
                        #   attempt to save an invalid pub version will cause
                        #   it to be marked non-publishable
                        self.lastpVals = cdr.valPair(self.session,
                            self.lastp.type, self.lastp.xml, self.newLastpXml)

                else:
                    # Lastp was same as lastv, don't need to load it, just
                    #   reference the existing self.lastv
                    # See warning ("BEWARE") below
                    # After this, any change to lastp.xml changes lastv.xml
                    #   and maybe also cwd.xml
                    self.lastp       = self.lastv
                    self.newLastpXml = self.newLastvXml
                    self.lastpVals   = self.lastvVals


    class DummyLogger:
        """
        Used as default logger object by the saveChanges() method of the
        Doc class.  Does nothing.
        """
        def log(): pass

    def saveChanges(self, cursor, logger = DummyLogger()):
        """
        In run mode, saves all versions of a document needing to be saved.
        In test mode, writes to output files, leaving the database alone.

        Parameters:

            cursor     - cursor for connection to CDR database
            logger     - object with a single log() method, taking a
                         string to be logged (without trailing newline)

        Uses the following logic:

        Let PV     = publishable version
        Let LPV    = latest publishable version
        Let LPV(t) = transformed copy of latest publishable version
        Let NPV    = non-publishable version
        Let CWD    = copy of current working document when job begins
        Let CWD(t) = transformed copy of CWD
        Let LV     = latest version (regardless of whether publishable)
        Let LV(t)  = transformed copy of LV
        Let LS     = last saved copy of document (versioned or not)

        If CWD <> LV:
            Create new NPV from unmodified CWD
            Preserves the original CWD which otherwise would be lost.
        If LPV(t) <> LPV:
            Create new PV using LPV(t)
        If LV(t) <> LV:
            Create new NPV from LV(t)
        If CWD(t) <> LS:
            Create new CWD using CWD(t)

        Often, one or more of the following is true:
            CWD==LV, CWD==LPV, LV==LPV

        BEWARE! If versions are equivalent, references to objects are
        manipulated in tricky ways to ensure that the Right Thing is done.
        """

        global _testMode, _outputDir

        lastSavedXml  = None
        docId         = self.cwd.id
        everValidated = self.lastp and True or self.__everValidated(cursor)

        # If change made a valid doc invalid, report it to console & log
        # Note this only happens if _validate caused one of the Vals
        #   msg arrays to be created and validation populated it with err msgs
        vals = self.lastpVals or self.cwdVals or self.lastvVals
        if vals:
            msg = "Doc %s made invalid by change - will NOT store it" % docId
            if self.cwdVals:
                msg += "\nCurrent working document would become invalid"
            if self.lastpVals:
                msg += "\nNew pub version would become invalid"
            if self.lastvVals:
                msg += "\nNew last version would become invalid"

            # Just appending one set of messages, others should be similar
            for val in vals:
                msg += "\n%s" % val
            logger.log(msg)

        if _testMode:
            # Write new/original CWDs
            cdrglblchg.writeDocs(_outputDir, docId,
                                 self.cwd.xml, self.newCwdXml, 'cwd',
                                 self.cwdVals)

        # If publishable version changed ...
        # When self.transformVER is False, self.lastp is always None
        if self.lastp and self.compare(self.lastp.xml, self.newLastpXml):
            if _testMode:
                # Write new/original last pub version
                cdrglblchg.writeDocs(_outputDir, docId,
                                     self.lastp.xml, self.newLastpXml, 'pub',
                                     self.lastpVals)
            else:
                # See "BEWARE" above.
                # self.lastp may be a reference to
                self.lastp.xml = self.newLastpXml
                # If not validating or passed validation,
                #   save new last pub version
                if not (vals and _noSaveOnErr):
                    self.__saveDoc(str(self.lastp), ver='Y', pub='Y',
                                   logger=logger, val='Y', msg=' new pub')
                    lastSavedXml = self.newLastpXml

            # Record versions that were changed
            self.disp.cwdChanged = self.disp.lastpChanged = True

        #--------------------------------------------------------------
        # Note that in the very common case in which the last created
        # version and the most recent publishable version are the same
        # version, the test below will report no differences between
        # self.lastv.xml and self.newLastvXml.  This is because in this
        # case self.lastv and self.lastp are references to the same
        # cdr.Doc object, and the assignment above of self.newLastpXml
        # to self.lastp.xml also changes self.lastv.xml.  This is
        # almost too clever and tricky (it has confused the author
        # of this code on at least one occasion -- hence this comment),
        # but it's more efficient than doing deep copies of the cdr.Doc
        # objects, and (equally important) it does the Right Thing.
        #--------------------------------------------------------------
        # When self.transformVER is False, self.lastv is always None
        if self.lastv and self.compare(self.lastv.xml, self.newLastvXml):
            if _testMode:
                # Write new/original last non-pub version results
                cdrglblchg.writeDocs(_outputDir, docId,
                                     self.lastv.xml, self.newLastvXml, 'lastv',
                                     self.lastvVals)
            # Reflect the changes
            self.lastv.xml = self.newLastvXml

            if not _testMode:
                # Save new last non-pub version
                # If doc was never validated, we don't validate this
                #   time because of validation side effects, e.g., stripping
                #   XMetal PIs.
                if not (vals and _noSaveOnErr):
                    self.__saveDoc(str(self.lastv), ver='Y', pub='N',
                                   logger=logger,
                                   val=(everValidated and 'Y' or 'N'),
                                   msg=' new ver')
                    lastSavedXml = self.newLastvXml

            # Record versions that were changed
            self.disp.cwdChanged = self.disp.lastvChanged = True

        # If no XML has been saved, and the new and old cwd are different
        #   or
        # If last XML saved is not the same as the new cwd
        #   then
        # Save the new current working document
        if ((not lastSavedXml and self.compare(self.newCwdXml, self.cwd.xml))
             or (lastSavedXml and self.compare(self.newCwdXml, lastSavedXml))):
            if not _testMode:
                # Save new CWD
                self.cwd.xml = self.newCwdXml
                if not (vals and _noSaveOnErr):
                    self.__saveDoc(str(self.cwd), ver='N', pub='N',
                                   logger=logger,
                                   val=(everValidated and 'Y' or 'N'),
                                   msg=' new cwd')

            # Record versions that were changed
            self.disp.cwdChanged = True

    #------------------------------------------------------------------
    # Find out whether the document has ever been validated.
    #------------------------------------------------------------------
    def __everValidated(self, cursor):
        cursor.execute("""\
                SELECT val_status
                  FROM document
                 WHERE id = ?""", self.id)
        rows = cursor.fetchall()
        if not rows:
            raise Exception("Failure retrieving val status for CDR%d" %
                            self.id)
        if rows[0][0] != 'U':
            return True
        cursor.execute("""\
                SELECT COUNT(*)
                  FROM doc_version
                 WHERE id = ?
                   AND val_status <> 'U'""", self.id)
        rows = cursor.fetchall()
        if not rows:
            raise Exception("Failure retrieving val status for CDR%d" %
                            self.id)
        return rows[0][0] > 0

    #------------------------------------------------------------------
    # Invoke the CdrRepDoc command.
    #------------------------------------------------------------------
    def __saveDoc(self, docStr, ver, pub, logger, val='Y', logWarnings=True,
                  msg=''):
        # Debug
        # return 1

        # It may be necessary to save the current working doc if not saved yet
        if self.saveThisDocFirst:
            # Move the doc then save via recursive call, avoiding
            #   infinite recursion
            tempRefToDoc          = self.saveThisDocFirst
            self.saveThisDocFirst = None
            self.__saveDoc(docStr=str(tempRefToDoc), ver="Y", pub="N",
                           logger=logger, val='N', msg=" old cwd")

        # Record what we're about to do
        logger.log("saveDoc(%d, ver='%s' pub='%s' val='%s'%s)" %
                (self.id, ver, pub, val, msg))

        response = cdr.repDoc(self.session, doc = docStr, ver = ver,
                              val = val,
                              verPublishable = pub,
                              reason = self.comment, comment = self.comment,
                              showWarnings = 'Y',
                              activeStatus = self.activeStatus)

        # Response missing first element means save failed
        # Almost certainly caused by locked doc
        if not response[0]:
            raise Exception("Failure saving changes for CDR%010d: %s" %
                            (self.id, response[1]))

        # Second element contains XSLT filter warning(s), if any
        if response[1]:
            warnings = cdr.getErrors(response[1], asSequence = True,
                                     errorsExpected = False)
            if logWarnings:
                for warning in warnings:
                    logger.log("Warning for CDR%010d: %s" % (self.id,
                                                             warning))
            self.disp.warnMsgs = warnings

        # Remember the message used to describe this version of the document.
        self.__messages.append(msg)
