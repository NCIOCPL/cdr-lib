#----------------------------------------------------------------------
#
# Module used by CDR Publishing daemon to process queued publishing jobs.
#
# BZIssue::2207 - Added pubProcDate parameter with initialization
# BZIssue::3488 - Added code to retry failed write of document to disk
# BZIssue::3491 - Rename GlossaryTermName -> GlossaryTerm for Cancer.gov
# BZIssue::3923 - Replaced PyXML validator with Lxml
# BZIssue::3951 - Fixed problem with docs deleted after pub job starts
# BZIssue::4629 - Vendor Filter Changes for GenProf publishing
# BZIssue::4869 - [Internal] Remove Parameter IncludeLinkedDocs
# BZIssue::5176 - Modify Publishing Program
# OCECDR-3570   - Rerun Previous Publishing Run
# OCECDR-3951   - Add per document type error thresholds
#
#----------------------------------------------------------------------

# Standard library imports
import base64
import glob
import os
import re
import sys
import threading
import time
import xml.dom.minidom

# Third-party imports
import lxml.etree

# Local application/library specific imports
import cdr
import cdrdb
import cdr2gk
import AssignGroupNums
from cdrapi.settings import Tier

#-----------------------------------------------------------------------
# Value for controlling debugging output.  None means no debugging
# output is generated.  An empty string means debugging output is
# written to the standard error file.  Any other string is used as
# the pathname of the logfile to which to write debugging output.
#-----------------------------------------------------------------------
LOG = cdr.PUBLOG

# Where are we running?
TIER = Tier()

# Default number of publishing threads to use
PUB_THREADS = 4
threadMsg   = "Threads=%d" % PUB_THREADS

# Have we set a different number in the database control table?
try:
    conn = cdrdb.connect("cdr")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT val
          FROM ctl
         WHERE grp = 'Publishing'
           AND name = 'ThreadCount'
           AND INACTIVATED IS NULL""")
    row = cursor.fetchone()
    if row:
        PUB_THREADS = int(row[0])
        threadMsg = "Using %d threads, defined in ctl table" % PUB_THREADS
    else:
        threadMsg = "Using default %d threads" % PUB_THREADS

except cdrdb.Error, info:
    threadMsg = "Database failure selecting Publishing/ThreadCount: %s" \
                " using default %d threads" % (str(info), PUB_THREADS)
cdr.logwrite(threadMsg, LOG)

# Publish this many docs of one doctype between reports
LOG_MODULUS = 1000 # 10

# XXX For testing, wiring target to "GateKeeper" rather than "Live" or
# XXX "Preview".
# XXX Need a way to set this from outside?
# CG_PUB_TARGET = "Preview"

#-----------------------------------------------------------------------
# class: Doc
#   Information about a document to be published, or that is published.
#   A list of these objects is created either by selecting documents
#   that a user has already placed in the pub_proc_doc table, or by
#   selecting documents via a SQL query for a publishing subset.
#-----------------------------------------------------------------------
class Doc:
    def __init__(self, docId, version, docTypeStr, recorded=False):

        self.__docId      = docId       # CDR id
        self.__version    = version     # Last publishable version num
        self.__docTypeStr = docTypeStr  # e.g., 'InScopeProtocol'
        self.__msgs       = None        # Filter or other errs or warnings
        self.__failed     = False       # True=publishing failed
        self.__removed    = False       # True=doc removed from cg (future use)
        self.__published  = False       # True=doc was output
        self.__recorded   = recorded    # Row exists in pub_proc_doc

    # Accessors
    def getDocId(self):      return self.__docId
    def getVersion(self):    return self.__version
    def getDocTypeStr(self): return self.__docTypeStr
    def getMsgs(self):       return self.__msgs
    def getFailed(self):     return self.__failed
    def getRemoved(self):    return self.__removed
    def getPublished(self):  return self.__published
    def getRecorded(self):   return self.__recorded

    # Setters.
    #   Id, version, type, can't change
    #   Booleans once switched on, stay on
    def setFailed(self):     self.__failed    = True
    def setRemoved(self):    self.__removed   = True
    def setPublished(self):  self.__published = True
    def setRecorded(self):   self.__recorded  = True

    # Msgs are appended to
    def addMsg(self, msg):
        if not self.__msgs:
            self.__msgs = msg
        else:
            self.__msgs += msg

#-----------------------------------------------------------------------
# class: Publish
#    This class encapsulates the publishing data and methods.
#    There is one public method, publish().
#-----------------------------------------------------------------------
class Publish:

    # Used as optional argument to __publishDoc() for query-selected docs.
    STORE_ROW_IN_PUB_PROC_DOC_TABLE = 1

    # Used as optional argument to __addDocMessages().
    SET_FAILURE_FLAG = "Y"

    # Job status values.
    SUCCESS    = "Success"
    FAILURE    = "Failure"
    WAIT       = "Waiting user approval"
    RUN        = "In process"
    INIT       = "Init"
    READY      = "Ready"
    START      = "Started"
    VERIFYING  = "Verifying"
    STALLED    = "Stalled"

    # Output flavors.
    FILE       = 4
    DOCTYPE    = 5
    DOC        = 6

    # Server cacheing to speedup publishing
    CACHETYPE = "pub"

    # class private variables.
    __timeOut  = 3000
    __cdrEmail = "NCIPDQoperator@mail.nih.gov"
    __pd2cg    = "Push_Documents_To_Cancer.Gov"
    __cdrHttp  = "https://%s/cgi-bin/cdr" % TIER.hosts['APPC']
    __interactiveMode   = 0
    __pushAllDocs       = False
    ## __includeLinkedDocs = 0
    __reportOnly        = 0
    __validateDocs      = 0
    __logDocModulus     = LOG_MODULUS

    # Document types provided to licensees but not to Cancer.gov
    __excludeDocTypes   = ('Country',)

    # Used in SQL SELECT statements to exclude documents of those types.
    __excludeDT         = ",".join(["'%s'" % t for t in __excludeDocTypes])

    # List of Docs to be published
    __docs = []

    # Next doc to be published.  Threads synchronize to use this
    __nextDoc = 0

    # Total published documents, updated after each subset is published
    __totalPubDocs = 0

    # Thread locking objects.
    # All threads share this one instance of a Publish object.
    __lockNextDoc  = threading.Lock()  # Get next doc id to pub from queue
    __lockLog      = threading.Lock()  # Writing a log message
    __lockManifest = threading.Lock()  # Append to a manifest of media
    __lockDb       = threading.Lock()  # Server access to blob change date
    __lockMakeDir  = threading.Lock()  # Creating or renaming a directory

    # Publish this many docs in parallel
    __numThreads  = PUB_THREADS

    # An error in any thread updates this
    # Other threads will see it and exit
    __threadError = None

    # Set this flag if a thread fails, tells other threads not to continue
    __cleanupFailureCalled = False

    # Rows to be written to the image manifest file.
    __mediaManifest = []

    #---------------------------------------------------------------
    # Hash __dateFirstPub is specifically designed to solve the issue
    # that DateFirstPublished element in vendor documents must be
    # present at the first time when the document is published. We
    # don't want to embed the DATE information in the document tuple
    # lists such as __userDocList, where it may not always be useful.
    # We make this hash global and access it whenever we need it.
    #
    # The hash key is the document ID, and the value is an empty string
    # or a Date that is from first_pub column of document table or
    # vendor_job start time for newly published documents, with XML
    # Date format YYYYY-MM-DD.
    #---------------------------------------------------------------
    __dateFirstPub = {}

    #---------------------------------------------------------------
    # Load the job settings from the database.  User-specified
    # documents will already have been recorded in the pub_proc_doc
    # table, but other documents can be added through SQL or XQL
    # queries.
    #---------------------------------------------------------------
    def __init__(self, jobId):

        # Make sure a port is available for publising.
        self.__pubPort = cdr.getPubPort()

        # Initialize a few values used for error processing.
        self.__errorCount           = 0
        self.__errorsBeforeAborting = 0
        self.__warningCount         = 0
        self.__publishIfWarnings    = "No"
        self.__perDoctypeErrors     = {}
        self.__perDoctypeMaxErrors  = {}

        # Cacheing not yet turned on
        self.__cacheingOn = 0

        # Keep a copy of the job ID.
        self.__jobId = jobId
        self.__debugLog("Publishing job processing commenced.")

        # Connect to the CDR database.  Exception is raised on failure.
        self.__getConn()
        cursor = self.__conn.cursor()

        # Retrieve the basic settings for the job from the database.
        sql = """\
            SELECT p.pub_system,
                   p.pub_subset,
                   p.usr,
                   p.output_dir,
                   p.email,
                   p.started,
                   p.no_output,
                   u.name
              FROM pub_proc p
              JOIN usr u
                ON u.id     = p.usr
             WHERE p.id     = ?
               AND p.status = ?"""
        try:
            cursor.execute(sql, (self.__jobId, Publish.START))
            row = cursor.fetchone()
            if not row:
                msg = "%s: Unable to retrieve information for job %d" % (
                                                               time.ctime(),
                                                               self.__jobId)
                self.__debugLog(msg)
                raise Exception(msg)
        except cdrdb.Error, info:
            msg  = "%s: Database failure retrieving information " % time.ctime()
            msg += "for job %d: %s" % (self.__jobId, info[1][0])
            self.__debugLog(msg)
            raise Exception(msg)

        self.__ctrlDocId   = row[0]
        self.__subsetName  = row[1]
        self.__userId      = row[2]
        self.__outputDir   = row[3]

        # If the email parameter has been saved as a list we'll have to
        # convert the string selected from the DB back to a list.
        # -------------------------------------------------------------
        try:
            self.__email   = eval(row[4])
            cdr.logwrite('email = %s' % self.__email, LOG, tback=0)
        except:
            self.__email   = row[4]
            cdr.logwrite('email = %s' % self.__email, LOG, tback=0)

        self.__jobTime     = row[5]
        self.__withOutput  = row[6] == "N"
        self.__userName    = row[7]
        self.__credentials = cdr.login(self.__userName,
                                       cdr.getpw(self.__userName) or "",
                                       port = self.__pubPort)

        # Load user-supplied list of document IDs.
        self.__userDocList = []
        try:
            cursor.execute("""\
                SELECT pub_proc_doc.doc_id,
                       pub_proc_doc.doc_version,
                       doc_type.name
                  FROM pub_proc_doc
                  JOIN doc_version
                    ON doc_version.id = pub_proc_doc.doc_id
                   AND doc_version.num = pub_proc_doc.doc_version
                  JOIN doc_type
                    ON doc_type.id = doc_version.doc_type
                 WHERE pub_proc_doc.pub_proc = ?""",  self.__jobId)

            row = cursor.fetchone()
            while row:
                self.__userDocList.append(Doc(row[0], row[1], row[2],
                                          recorded=True))
                row = cursor.fetchone()
        except cdrdb.Error, info:
            msg  = "%s: Failure retrieving documents for " % time.ctime()
            msg += "job %d: %s<BR>" % (self.__jobId, info[1][0])
            self.__updateStatus(Publish.FAILURE, msg)
            raise Exception(msg)

        # Load the job parameters from the database.  The server
        # will have merged parameters explicitly set for this job
        # invocation with defaults for the publication system subset
        # for parameters not explicitly set for this job.
        self.__params = {}
        try:
            cursor.execute("""\
                SELECT parm_name,
                       parm_value
                  FROM pub_proc_parm
                 WHERE pub_proc = ?""", self.__jobId)
            row = cursor.fetchone()
            while row:
                # Runtime parameter replacement of value macro
                # This replaces a value that may be in parameter list
                #  rather than in the query
                # It handles the double indirection, used in:
                #   ParmName='?MaxDocUpdatedDate?'
                if row[0]=='MaxDocUpdatedDate' and row[1]=='JobStartDateTime':
                    self.__debugLog("Setting JobStartDateTime='%s'" % \
                                     self.__jobTime)
                    row[1] = self.__jobTime

                # Insert into parameter dictionary
                self.__params[row[0]] = row[1]
                self.__debugLog("Parameter %s='%s'." % (row[0], row[1]))
                row = cursor.fetchone()
        except cdrdb.Error, info:
            msg  = "Failure retrieving parameters for " % time.ctime()
            msg += "job %d: %s<BR>" % (self.__jobId, info[1][0])
            self.__updateStatus(Publish.FAILURE, msg)
            raise Exception(msg)


        # Initialize the hash __dateFirstPub. The hash key is the document
        # ID, and the value is a Date that is an empty string if
        # ('Y' <> first_pub_knowable), or first_pub or vendor_job start
        # time for newly published documents, with XML Date format
        # YYYYY-MM-DD. It took about 10 secs to build the hash for 65,000
        # documents.
        try:
            cursor.execute("""\
                SELECT d.id, d.first_pub_knowable, d.first_pub
                  FROM document d, doc_type t
                 WHERE d.doc_type = t.id
                   AND NOT t.name IN ('Mailer', 'Citation')
                           """
                          )
            row = cursor.fetchone()
            while row:
                knowable = ('Y' == row[1])
                date = row[2] or ''
                if knowable and not date:
                    date = self.__jobTime
                if len(date) > 10 and date[10] == ' ':
                    date = date[:10]
                self.__dateFirstPub[row[0]] = date

                row = cursor.fetchone()

        except cdrdb.Error, info:
            msg  = "%s: Failure building hash __dateFirstPub " % time.ctime()
            msg += "for job %d: %s<BR>" % (self.__jobId, info[1][0])
            self.__updateStatus(Publish.FAILURE, msg)
            raise Exception(msg)

        # Reset some class private variables based on user input.
        #
        # Note: The 'IncludeLinkedDocs' parameter has been removed
        #       from the publishing document since it has never
        #       and shouldn't be used anymore but caused confusions
        #       when publishing had to be run by "backup publishers"
        # ----------------------------------------------------------
        ## if self.__params.has_key("IncludeLinkedDocs"):
        ##     self.__includeLinkedDocs = \
        ##         self.__params["IncludeLinkedDocs"] == "Yes"
        if self.__params.has_key("InteractiveMode"):
            self.__interactiveMode = \
                self.__params["InteractiveMode"] == "Yes"
        if self.__params.has_key("PushAllDocs"):
            self.__pushAllDocs = \
                self.__params["PushAllDocs"] == "Yes"
        if self.__params.has_key("ReportOnly"):
            self.__reportOnly = \
                self.__params["ReportOnly"] == "Yes"
        if self.__params.has_key("ValidateDocs"):
            self.__validateDocs = \
                self.__params["ValidateDocs"] == "Yes"

        # Prevent collisions resulting from mismatch between database
        # and file system (typically caused by a database refresh from
        # prod on the lower tiers). Let failures throw exceptions.
        # See https://tracker.nci.nih.gov/browse/OCECDR-3895.
        if not self.__isCgPushJob() and self.__outputDir:
            for path in glob.glob(self.__outputDir + "*"):
                if os.path.isdir(path) and "-" not in os.path.basename(path):
                    stat = os.stat(path)
                    stamp = time.strftime("%Y%m%d%H%M%S",
                                          time.localtime(stat.st_mtime))
                    os.rename(path, "%s-%s" % (path, stamp))

    #---------------------------------------------------------------
    # This is the major public entry point to publishing.
    #---------------------------------------------------------------
    def publish(self):

        try:

            # Get the destination directory.
            dest_base = self.__outputDir
            dest = dest_base + ".InProcess"

            # Record the fact that the job is in process.
            self.__updateStatus(Publish.RUN)

            # Load the publishing system's control document from the DB.
            docElem = self.__getCtrlDoc()

            # Extract the DOM node for this job's publishing system subset.
            # Set __sysName needed by Cancer.gov as a side-effect.
            subset = self.__getSubSet(docElem)

            # Invoke an external process script, if any.  Will not return
            # if an external script is attached to this publishing system
            # subset.
            self.__invokeProcessScript(subset)

            # Get the subset specifications node.
            self.__specs = self.__getSpecs(subset)

            # Get the name-value pairs of options.  Error handling set here.
            # OCECDR-3951: also extract per document type error thresholds.
            options = self.__getOptions(subset)

            # Get the destination type.
            destType = self.__getDestinationType(options)
            if destType == Publish.FILE:
                self.__fileName = self.__getDestinationFile(options)

            # Hotfix-Export requirement and implementation have
            # changed. User-selected docs will not be processed as
            # it was supposed to be. User will enter some documents
            # and we will find all the linked documents in the backend
            # when specified by user input. The linked documents will
            # be inserted into table pub_proc_doc and appended to list
            # __userDocList.
            #
            ### Hotfix-Export requirements changed again.
            ### The parameter IncludeLinkedDocs has been removed and is
            ### not being used anymore.  __addLinkedDocsToPPD will not
            ### be called.
            ##if self.__isPrimaryJob() and self.__subsetName == "Hotfix-Export":
            ##    if self.__includeLinkedDocs:
            ##        self.__addLinkedDocsToPPD()

            # Turn on cacheing.
            # This is an optimization that, at the time of writing
            # caches Term document upcoding and denormalization.
            # Other cacheing optimizations might also be used in
            # the future if experience shows a need and opportunity.
            # Must turn cacheing off before end, even if exception raised.
            cdr.cacheInit(self.__credentials,
                          cacheOn=1, cacheType=Publish.CACHETYPE,
                          port=self.__pubPort)
            self.__cacheingOn = 1

            # Two passes through the subset specification are required.
            # The first pass publishes documents specified by the user, and
            # builds the list of filters used for each specification.
            # The second pass publishes the documents selected by queries
            # (XML or XQL), skipping documents which have already been
            # published by the job (either because they were picked up by
            # another earlier query, or because they appeared in the
            # user-specified list).
            #
            # In the first pass a document on the user's list is published
            # only once, for the first specification which allows the user to
            # list documents of that document's type.
            self.__alreadyPublished = set()
            specFilters             = []
            specSubdirs             = []
            userListedDocsRemaining = len(self.__userDocList)
            self.__debugLog("Processing user-listed documents.")

            # Number of documents have been filtered and validated.
            numDocs = 0
            self.__updateMessage(
                "%s: Start filtering/validating<BR>" % time.ctime())

            for spec in self.__specs.childNodes:

                if spec.nodeName == "SubsetSpecification":

                    # Gather together filters (with parms) used for this SS.
                    filters = self.__getFilters(spec)
                    specFilters.append(filters)

                    # Get Subdirectory for this SS.
                    # Empty string is returned if not exist.
                    subdir = self.__getSubdir(spec)
                    specSubdirs.append(subdir)

                    # That's all we have to do in this pass if there are no
                    # user-listed documents remaining to be published.
                    if not userListedDocsRemaining:
                        continue

                    # Find out if this subset specification allows user
                    # doc lists.
                    docTypesAllowed = self.__getAllowedDocTypes(spec)
                    if docTypesAllowed is None:
                        continue

                    # See which user-listed documents we can publish here.
                    for doc in self.__userDocList:
                        if doc.getDocId() in self.__alreadyPublished:
                            continue
                        if docTypesAllowed and \
                           doc.getDocTypeStr() not in docTypesAllowed:
                            continue

                        # Don't want to use subdir for user-listed docs
                        # due to complexity of specifying the multiple
                        # subdirs when calling cdr.publish(). This may
                        # change in release XX.
                        # XXX - Not multi-threading these
                        # XXX - Might want to to have everything done the
                        #       same way, if not for performance.
                        self.__publishDoc(doc, filters, destType, dest)
                        numDocs += 1
                        if numDocs % self.__logDocModulus == 0:
                            msg = "%s: Filtered/validated %d docs (HF). " % (
                                                                 time.ctime(),
                                                                 numDocs)
                            self.__updateMessage(msg)
                            numFailures = self.__getFailures()
                            msg = "%d docs failed so far.<BR>" % numFailures
                            self.__updateMessage(msg)

                        self.__alreadyPublished.add(doc.getDocId())
                        userListedDocsRemaining -= 1
                        if not userListedDocsRemaining: break

            # End of first loop through SubsetSpecification nodes.

            # Make sure all the user-listed documents are accounted for.
            for doc in self.__userDocList:
                if doc.getDocId() not in self.__alreadyPublished:
                    self.__checkProblems(doc,
                                "User-specified document CDR%010d "
                                "has document type %s which is "
                                "not allowed for this publication "
                                "type" %
                             (doc.getDocId(), doc.getDocTypeStr()), "")

            # Update the pub_proc_doc table with any generated messages
            self.__addPubProcDocMsgs()

            # Now walk through the specifications again executing queries.
            self.__debugLog("Processing document-selection queries.")
            i = 0
            for spec in self.__specs.childNodes:

                if spec.nodeName == "SubsetSpecification":

                    for specChild in spec.childNodes:
                        if specChild.nodeName == "SubsetSelection":
                            # Fetch all doc id/ver/doctype tuples
                            self.__docs = self.__selectQueryDocs(specChild)

                            # Reset counter for this subset
                            self.__nextDoc = 0

                            # Launch a bunch of threads
                            self.__launchPubThreads(specFilters[i],
                                                    destType, dest,
                                                    specSubdirs[i])

                            # Back from running all threads
                            # Add all the docs to pub_proc_doc table
                            # NOTE: If an exception is raised in
                            #       self.__launchPubThreads(), this update
                            #       will not occur.
                            # XXX Do we need to update pub_proc_doc table
                            #     after each successful pub, or is this
                            #     sufficient?
                            self.__addPubProcDocRows(specSubdirs[i])
                    i += 1

            # Display a list of documents published for this job
            # --------------------------------------------------
            sql = """\
                SELECT count(*)
                  FROM pub_proc_doc
                 WHERE pub_proc = ?
                            """
            cursor.execute(sql, (self.__jobId ))
            row = cursor.fetchone()
            if not row:
                venumDocs = 999
            else:
                venumDocs = row[0]

            ### # End of the second loop through SubsetSpecification nodes.
            ### msg = "%s: Finished filtering/validating all %d docs<BR>" % (
            ###                                                    time.ctime(),
            ###                                                    numDocs)
            ### self.__updateMessage(msg)

            if not self.__isCgPushJob():
                # New Count VEVEVE
                msg = "%s: Finished filtering/validating all %s docs<BR>" % (
                                                     time.ctime(), venumDocs)
                self.__updateMessage(msg)

            numFailures = self.__getFailures()
            if numFailures > 0:
                #   href="%s/PubStatus.py?id=%d&type=FilterFailure">Check
                msg = """%s: Total of %d docs failed.
                    <A style='text-decoration: underline;'
                    href="PubStatus.py?id=%d&type=FilterFailure">Check
                    the failure details.</A><BR>""" % (time.ctime(),
                                                       numFailures,
                                                       # self.__cdrHttp,
                                                       self.__jobId)
                self.__updateMessage(msg)
            else:
                msg = "%s: Total of 0 docs failed<BR>" % time.ctime()
                self.__updateMessage(msg)


            # Rename the output directory from its working name.
            # Create a pushing job if it is a vendor job; or push
            # filtered documents to CG if it is a cg job.
            # Here's where the final (for this run) job statuses get set.
            # NB: the __updateStatus() method will refuse to change the
            # status of the job if the current status is Success (see
            # comments on that method below). This means if you set the
            # status to Success you better be sure you're not going to
            # want to subsequently check any conditions which might
            # alter that status!

            # This block handles most publishing jobs (export jobs which
            # produce output and pushing jobs).
            if self.__withOutput or self.__isCgPushJob():

                # Rename the output directory from JobNNNNN.InProcess
                # to JobNNNNN (dest_base will be empty for push jobs).
                # Note: The value for dest_base is always set to a directory
                #       path in the server for every job even though a 
                #       directory does not get created for push jobs.  
                #       The value will be reset to an empty string in the 
                #       database for push jobs downstream.
                if dest_base and not self.__isCgPushJob():
                    try:
                        os.rename(dest, dest_base)
                    except Exception, e:
                        try:
                            msg = ("renaming %s to %s: %s" %
                                   (repr(dest), repr(dest_base), e))
                            self.__debugLog(msg)
                        except:
                            pass

                # If this is a QC filter publishing job (or any job
                # type not controlled by the primary publishing control
                # document), there's no such thing as failure.
                if not self.__isPrimaryJob():
                    self.__updateStatus(Publish.SUCCESS)

                # Mark job as failed if it wasn't a live job. This makes
                # sure that logic to find the most recent live job of a
                # specified type won't find this one.
                elif self.__reportOnly:
                    self.__updateStatus(Publish.FAILURE,
                                        "The job status is set to Failure "
                                        "because it was running for pre-"
                                        "publishing reports.<BR>")

                # OCECDR-3951: check doctype-specific thresholds.
                elif self.__perDoctypeErrorThresholdExceeded():
                    self.__updateStatus(Publish.FAILURE,
                            "Error threshold(s) exceeded.<br>")

                # If we've made it this far without setting the job
                # status to Success or Failure, this is a push job,
                # or an export job for which a push job would be
                # created.
                elif self.__anotherPushJobPending():

                    # It is a failure for the separately started pushing
                    # job, not for the vendor job.
                    if self.__isCgPushJob():
                        self.__updateStatus(Publish.FAILURE)
                    else:
                        self.__updateStatus(Publish.SUCCESS)

                # Push docs or create a pushing job.
                else:

                    # Is it a pushing job?
                    if self.__isCgPushJob():

                        # Make sure output_dir is reset to "" for pushing
                        # jobs, no matter whether user has forgot to check
                        # the "Message only" box.
                        # Note: The value for output_dir is set to an empty
                        #       string (''), not set to NULL as the name
                        #       of the method suggests.
                        self.__nullOutputDir()

                        # Get the vendor_job and vendor_dest from
                        # the appropriate subset.
                        vendorInfo    = self.__findVendorData()
                        vendor_job    = vendorInfo[0]
                        vendor_dest   = vendorInfo[1]

                        if not vendor_job:
                            self.__updateStatus(Publish.FAILURE,
                                "Not enough vendor info found.<BR>")
                        else:

                            # A long pushing job of many hours starts!
                            self.__pushDocsToCG(vendor_job, vendor_dest)

                            # There is no exception thrown.
                            self.__updateStatus(Publish.VERIFYING)

                    # It is a vendor job. Create a pushing job and let
                    # it run in its own way.
                    else:
                        self.__updateStatus(Publish.SUCCESS)

                        pushSubsetName = "%s_%s" % (self.__pd2cg,
                                                    self.__subsetName)
                        msg = ""

                        # A few parameters are passed to the push job
                        # --------------------------------------------
                        # Not all Subset types have the GKPushJobDescription
                        # parameter.  Need to check for existance.
                        if self.__params.has_key('GKPushJobDescription'):
                            parms = ([('GKPushJobDescription',
                                    self.__params['GKPushJobDescription'])])
                        else:
                            parms = []

                        parms.append(('GKServer',
                                      self.__params['GKServer']))
                        parms.append(('GKPubTarget',
                                      self.__params['GKPubTarget']))
                        resp = cdr.publish(self.__credentials,
                            "Primary",
                            pushSubsetName,
                            parms = parms,
                            email = self.__email,
                            noOutput = 'Y',
                            port = self.__pubPort)
                        if not resp[0]:
                            msg += "%s: <B>Failed:</B><BR>" % time.ctime()
                            msg += "<I>%s</I><BR>"          % resp[1]
                            msg += "%s: Please run job "    % time.ctime()
                            msg += "%s separately.<BR>"     % pushSubsetName
                        else:
                            msg += "%s: Pushing filtered "    % time.ctime()
                            msg += "documents to Cancer.gov is in "
                            msg += "progress with job %s<BR>" % resp[0]
                            msg += "%s: You will receive a "  % time.ctime()
                            msg += "second email when it is done.<BR>"

                        self.__updateMessage(msg)
            else:

                # Non-push jobs with no output always succeed.
                self.__updateStatus(Publish.SUCCESS)

        except SystemExit:
            # Handlers for try block opened all the way at the top of
            # the publish() method start here.
            #
            # __invokeProcessScript() can exit, which raises SystemExit
            # Since we catch all exceptions below, we would catch this one
            #   in the general Except clause and report an
            #   "Unexpected failure", which we don't want to do.
            # Turn off cacheing before we leave
            if (self.__cacheingOn):
                try:
                    cdr.cacheInit(self.__credentials,
                                  cacheOn=0, cacheType=Publish.CACHETYPE,
                                  port=self.__pubPort)
                except:
                    pass
            sys.exit(0)

        except Exception, arg:
            self.__cleanupFailure(dest, dest_base, str(arg))

        except:
            self.__cleanupFailure(dest, dest_base,
                                  "Unexpected failure, unhandled exception. ")

        # Turn cacheing off
        if (self.__cacheingOn):
            try:
                cdr.cacheInit(self.__credentials,
                              cacheOn=0, cacheType=Publish.CACHETYPE,
                              port=self.__pubPort)
            except:
                pass
            self.cacheingOn = 0

        # Update first_pub in all_docs table.
        self.__updateFirstPub()

        # If any images were published, write out a manifest file.
        if self.__outputDir and os.path.isdir(self.__outputDir):

            if self.__mediaManifest:
                import csv
                filename = os.path.join(self.__outputDir, "media_catalog.txt")
                manifestFile = file(filename, "wb")
                csvWriter = csv.writer(manifestFile)
                self.__mediaManifest.sort(lambda a,b: cmp(a[1], b[1]))
                csvWriter.writerows(self.__mediaManifest)
                manifestFile.close()

        # Send email to notify user of job status.
        self.__sendMail()

    #------------------------------------------------------------------
    # Clean up in case of failed publishing job
    #------------------------------------------------------------------
    def __cleanupFailure(self, dest, dest_base, msg):
        """
        If an exception is caught, it is necessary to do some cleanup
        before exiting.  This subroutine can be called to perform that
        cleanup so we don't have to repeat the same code under each
        possible exception.

        Any exception raided during cleanup is ignored.  We keep
        trying the rest of cleanup before returning.

        Pass:
            dest      - Output filename
            dest_base - Base (directory) of output filename
            msg       - Message to log.

        Return:
            Void.
        """
        try:
            # Synchronize this with other threads that may call this
            #   or attempt to create a directory
            self.__lockMakeDir.acquire(1)

            # Don't do this twice
            if self.__cleanupFailureCalled:
                return

            # Log message
            msg = "publish: %s<BR>" % msg
            try:
                self.__debugLog(msg, tb=1)
            except:
                pass

            # Set status in database
            try:
                self.__updateStatus(Publish.FAILURE, msg)
            except:
                pass

            # Rename output directory to indicate failure
            if self.__withOutput:
                try:
                    os.rename(dest, dest_base + ".FAILURE")
                except:
                    pass

            # Cleanup is done
            self.__cleanupFailureCalled = True
        except:
            pass

        finally:
            # End synchronization
            self.__lockMakeDir.release()

        return


    #------------------------------------------------------------------
    # Update first_pub to vendor_job started when appropriate.
    #------------------------------------------------------------------
    def __updateFirstPub(self):
        try:
            conn = cdrdb.connect("cdr")
            conn.setAutoCommit(1)
            cursor = conn.cursor()
            cursor.execute("""\
                    UPDATE document
                       SET first_pub = pp.started
                      FROM pub_proc pp,
                           pub_proc_doc ppd,
                           document d
                     WHERE d.id = ppd.doc_id
                       AND ppd.pub_proc = pp.id
                       AND pp.id = %d
                       AND pp.status = '%s'
                       AND ppd.removed != 'Y'
                       AND (
                            ppd.failure IS NULL
                           OR
                            ppd.failure <> 'Y'
                           )
                       AND d.first_pub IS NULL
                       AND d.first_pub_knowable = 'Y'
                           """ % (self.__jobId, Publish.SUCCESS),
                           timeout = self.__timeOut
                           )
            rowsAffected = cursor.rowcount
            self.__updateMessage(
                "%s: Updated first_pub for %d documents.<BR>" % (
		                                    time.ctime(), rowsAffected))
        except cdrdb.Error, info:
            self.__updateMessage("Failure updating first_pub for job %d: %s" \
                % (self.__jobId, info[1][0]))

    #------------------------------------------------------------------
    # Allow only one pushing job to run.
    # Return True if there is a pending pushing job.
    #------------------------------------------------------------------
    def __anotherPushJobPending(self):
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""\
                    SELECT TOP 1 id
                      FROM pub_proc
                     WHERE NOT status IN ('%s', '%s')
                       AND pub_subset LIKE '%s%%'
                       AND pub_system = %d
                       AND NOT id = %d
                  ORDER BY id DESC
                           """ % (Publish.SUCCESS, Publish.FAILURE,
                                  self.__pd2cg, self.__ctrlDocId,
                                  self.__jobId)
                           )
            row = cursor.fetchone()
            if row:
                msg  = "%s: Pushing job %d is pending. Please " % \
                        (time.ctime(), row[0])
                msg += "push again later.<BR>"
                self.__updateMessage(msg)
                return True

        except cdrdb.Error, info:
            raise Exception("Failure finding pending pushing jobs "
                            "for job %d: %s" % (self.__jobId, info[1][0]))
        return False

    #------------------------------------------------------------------
    # Determine whether this is a job to push documents to Cancer.gov.
    #------------------------------------------------------------------
    def __isCgPushJob(self):
        return self.__subsetName.startswith(self.__pd2cg)

    #------------------------------------------------------------------
    # Determine whether this is a primary publication job.
    #------------------------------------------------------------------
    def __isPrimaryJob(self):
        return self.__sysName == "Primary"

    #------------------------------------------------------------------
    # Find the vendor job and destination directory based on the
    # parameter value of SubSetName belonging to this job.
    #------------------------------------------------------------------
    def __findVendorData(self):
        if not self.__params.has_key('SubSetName'):
            return [None, None]
        else:
            subsetName = self.__params['SubSetName']
            try:
                cursor = self.__conn.cursor()
                cursor.execute("""\
                    SELECT TOP 1 id, output_dir
                      FROM pub_proc
                     WHERE status = '%s'
                       AND pub_subset = '%s'
                       AND pub_system = %d
                  ORDER BY id DESC
                  """ % (Publish.SUCCESS, subsetName, self.__ctrlDocId))
                row = cursor.fetchone()
                if not row:
                    raise Exception("Corresponding vendor job does not "
                                    "exist.<BR>")

                # XXX is this really a doc ID? [RMK 2004-12-17]
                docId = row[0]
                dest  = row[1]

                prevId = self.__getLastJobId(subsetName)
                if prevId > docId:
                    raise Exception("This same job has been previously "
                                    "successfully done by job %d." % prevId)

                return [docId, dest]

            except cdrdb.Error, info:
                raise Exception("Failure finding vendor job and vendor "
                                "destination for job %d: %s" %
                                (self.__jobId, info[1][0]))

    #------------------------------------------------------------------
    # Push documents of a specific vendor_job to Cancer.gov using cdr2gk
    # module.
    # We handle different pubTypes with different functions for clarity.
    # Raise a standard error when failed.
    #------------------------------------------------------------------
    def __pushDocsToCG(self, vendor_job, vendor_dest):

        # Get the value of pubType for this cg_job.
        if self.__params.has_key('PubType'):
            pubType = self.__params['PubType']
            if not cdr.PUBTYPES.has_key(pubType):
                msg = """The value of parameter PubType, %s, is unsupported.
                       <BR>Please modify the control document or the source
                       code.<BR>""" % pubType
                raise Exception(msg)
        else:
            msg = "There is no parameter PubType in the control document.<BR>"
            raise Exception(msg)

        try:
            cursor = self.__conn.cursor()

            # If pubType is "Full Load", clean up pub_proc_cg table.
            if pubType == "Full Load":
                msg = "%s: Deleting pub_proc_cg<BR>" % time.ctime()
                self.__updateMessage(msg)
                try: cursor.execute("DELETE pub_proc_cg",
                                    timeout = self.__timeOut)
                except cdrdb.Error, info:
                    msg = "Deleting pub_proc_cg failed: %s<BR>" % info[1][0]
                    raise Exception(msg)

            # Create a working table pub_proc_cg_work to hold information
            # on transactions to Cancer.gov.
            msg = "%s: Creating pub_proc_cg_work<BR>" % time.ctime()
            self.__updateMessage(msg)
            cgWorkLink = self.__cdrHttp + "/PubStatus.py?id=%d&type=CgWork" % self.__jobId
            link = \
"""%s: <A style='text-decoration: underline;' href='%s'>
Check pushed docs</A> (of most recent publishing job)<BR>""" % (time.ctime(),
                                                                 cgWorkLink)


            if pubType in ("Full Load", "Export", "Reload"):
                # Note: For SubSetName='Interim-Export', PubType='Export'

                # If the push of a Full Load failed after the temporary
                # tables have been created and populated we want to skip
                # populating the tables again when we rerun the push job.
                # This can be achieved by setting the parameter
                # RerunFailedPush = Yes but only as long as no other push
                # job had been submitted in the meantime.
                # In order for this to work we need to update the cg_job
                # column in pub_prog_cg_work with the current jobID.
                # ------------------------------------------------------
                #if (pubType in ("Export", "Reload") or
                #    pubType == "Full Load" and
                ##if (pubType in ("Export", "Reload", "Full Load") and
                ##     self.__params['RerunFailedPush'] == 'No'):
                if self.__params['RerunFailedPush'] == 'No':
                    self.__createWorkPPC(vendor_job, vendor_dest)
                else:
                    try:
                        msg  = "%s: " % time.ctime()
                        msg += "Reusing pub_proc_cg_work data "
                        msg += "from previous job...<BR>"
                        self.__updateMessage(msg)
                        msg  = "%s: " % time.ctime()
                        msg += "Updating pub_proc_cg_work with "
                        msg += "current jobID...<BR>"
                        self.__updateMessage(msg)
                        cursor.execute ("""
                          UPDATE pub_proc_cg_work SET cg_job = ?
                                """, self.__jobId)
                    except cdrdb.Error, info:
                        raise Exception("Updating pub_proc_cg_work failed: "
                                        "%s<BR>" % info[1][0])

                pubTypeCG = pubType
                self.__updateMessage(link)

                # Job description for automated jobs is passed via
                # the PushJobDescription parameter.
                # For all other jobs the description is entered when
                # the job is released from the WAIT stage.
                # ----------------------------------------------------
                if (self.__params['SubSetName'] == 'Interim-Export' or
                    self.__params['SubSetName'] == 'Export'):
                    # Create automated job description.
                    #self.__updateJobDescription()
                    pass
                else:
                    # Stop to enter job description.
                    self.__waitUserApproval()
            elif pubType == "Hotfix (Remove)":
                self.__createWorkPPCHR(vendor_job)
                pubTypeCG = "Hotfix"
                self.__updateMessage(link)

                # Stop to enter job description.
                self.__waitUserApproval()
            elif pubType == "Hotfix (Export)":
                self.__createWorkPPCHE(vendor_job, vendor_dest)
                pubTypeCG = "Hotfix"
                self.__updateMessage(link)

                # Stop to enter job description.
                self.__waitUserApproval()
            else:
                raise Exception("pubType %s not supported." % pubType)

            docNum  = 1
            numDocs = 0
            cursor.execute ("""
                SELECT count(*)
                  FROM pub_proc_cg_work
                            """)
            row = cursor.fetchone()
            if row and row[0]:
                numDocs = row[0]

            if numDocs == 0:
                msg = "%s: No documents to be pushed to Cancer.gov.<BR>" % \
                        time.ctime()
                self.__updateStatus(Publish.SUCCESS, msg)
                return

            # Get last successful cg_jobId. GateKeeper does not
            # care which subset it belongs to.
            # Returns 0 if there is no previous success.
            # Raise an exception when failed.
            lastJobId = self.__getLastCgJob()

            # Get the required job description.
            if self.__params.has_key('GKPushJobDescription'):
                cgJobDesc = self.__params['GKPushJobDescription']
            else:
                cgJobDesc = self.__getCgJobDesc()

            if not cgJobDesc:
                self.__updateMessage(msg)
                raise Exception("<BR>Missing required job description.")

            msg = "%s: Initiating request with pubType=%s, \
                   lastJobId=%d ...<BR>" % (time.ctime(), pubTypeCG, lastJobId)

            # Set the GK target provided for this pubType
            CG_PUB_TARGET = self.__params['GKPubTarget']

            # Override the GK Server value if specified
            opts = dict(host=self.__params.get("GKServer"))

            # See if the GateKeeper is awake.
            response = cdr2gk.initiateRequest(pubTypeCG, CG_PUB_TARGET, **opts)

            if response.type != "OK":
                # Construct messages for the pub_proc database record
                msg += "GateKeeper: %s: %s<BR>" % \
                        (response.type, response.message)
                if response.fault:
                    msg += "%s: %s<BR>" % (response.fault.faultcode,
                                           response.fault.faultstring)

                # And also log what happened (may also be in cdr2gk.log)
                self.__debugLog("Aborting after GateKeeper response:\n %s" %\
                    str(response))

                # Hint for the operator
                if response.message.startswith("Error (-4)"):
                    abort = "cdr2gk.py abort --job-id JOB-ID"
                    msg += "Operator may need to run {}".format(abort)

                # Can't continue
                raise Exception(msg)

            # What does GateKeeper think the last job ID was
            gkLastJobId = response.details.lastJobId;
            if gkLastJobId != lastJobId:

                # Record this
                msg += "%s: Last job ID from server: %d<BR>" % (
                       time.ctime(), lastJobId)
                self.__debugLog("Our lastJobId=%s, Gatekeeper's=%s" % \
                                (lastJobId, gkLastJobId))

                # In test mode, we just rewire our view of the
                #   lastJobId.  In production, maybe not.
                #
                # The lastJobId reported by Gatekeeper isn't necessarily
                # a successful job in the CDR sense.  For Gatekeeper any
                # job that arrives at the server is successful and
                # therefore Gatekeeper reports the *last* JobId (success
                # or not).  For the CDR only a job for which at least
                # one document has been successfully processed by
                # Gatekeeper is considered a success.
                # This creates a mismatch if a push job (for instance a
                # hot-fix) is submitted but none of the documents can be
                # processed on Gatekeeper.
                # We're creating an overwrite to allow pushing a job
                # after it has been determined that it's OK to do so.
                # -------------------------------------------------------
                ### if cdr.isProdHost():   # for testing only
                if not cdr.isProdHost():
                    self.__debugLog(\
                      "For test, switching to GateKeeper lastJobId")
                    msg += """%s: Switching to Gatekeeper lastJobId""" % (
                           time.ctime())
                    lastJobId = response.details.lastJobId
                else:
                    if self.__params['IgnoreGKJobIDMismatch'] == 'No':
                        raise Exception("Aborting on lastJobId CDR / CG mismatch")
                    else:
                        self.__debugLog(\
                          "Overwrite lastJobId CDR / CG mismatch")
                        msg += """%s: Overwrite lastJobId mismatch""" % (
                               time.ctime())
                        lastJobId = response.details.lastJobId

            # Prepare the server for a list of documents to send.
            msg += """%s: Sending data prolog with jobId=%d, pubType=%s,
                    lastJobId=%d, numDocs=%d ...<BR>""" % (time.ctime(),
                               self.__jobId, pubTypeCG, lastJobId, numDocs)

            # Tell cancer.gov GateKeeper what's coming
            args = cgJobDesc, self.__jobId, pubTypeCG, CG_PUB_TARGET, lastJobId
            opts = dict(host=self.__params.get("GKServer"))
            response = cdr2gk.sendDataProlog(*args, **opts)
            if response.type != "OK":
                msg += "%s: %s<BR>" % (response.type, response.message)

                # Hint for the operator
                if response.message.startswith("Error (-4)"):
                    abort = "cdr2gk.py abort --job-id JOB-ID"
                    msg += "Operator may need to run {}".format(abort)

                raise Exception(msg)

            msg += "%s: Pushing documents starts<BR>" % time.ctime()
            self.__updateMessage(msg)
            msg = ""

            # Compute all CG group numbers for the added and
            #   updated docs
            # These tell CG what the dependencies are.  If one doc
            #   fails, what other docs should fail
            # This can take awhile because it has to parse docs looking
            #   for cross references between them
            self.__debugLog("Starting assignment of group numbers")

            # Uncomment the following for group number debugging with
            #   "AssignGroupNums jobnum"
            # sys.exit(1)
            groupNums = AssignGroupNums.GroupNums(self.__jobId)
            self.__debugLog("Finished assignment of group numbers")

            # Send all new and updated documents.
            addCount = 0
            XmlDeclLine = re.compile("<\?xml.*?\?>\s*", re.DOTALL)
            DocTypeLine = re.compile("<!DOCTYPE.*?>\s*", re.DOTALL)

            qry = """
                SELECT id, num, doc_type, xml
                  FROM pub_proc_cg_work
                 WHERE NOT xml IS NULL
                   AND doc_type NOT IN (%s)""" % self.__excludeDT
            self.__debugLog(qry)

            cursor.execute (qry, timeout = self.__timeOut)
            #cursor.execute ("""
            #    SELECT id, num, doc_type, xml
            #      FROM pub_proc_cg_work
            #     WHERE NOT xml IS NULL
            #                """, timeout = self.__timeOut)

            row = cursor.fetchone()
            while row:
                docId   = row[0]
                version = row[1]
                docType = row[2]
                if docType == "InScopeProtocol":
                    docType = "Protocol"
                elif docType == "GlossaryTermName":
                    docType = "GlossaryTerm"
                elif docType == "Person":
                    docType = "GeneticsProfessional"
                xml = row[3].encode('utf-8')
                xml = XmlDeclLine.sub("", xml)
                xml = DocTypeLine.sub("", xml)

                # Reverse comments to turn grouping on/off
                grpNum = groupNums.getDocGroupNum(docId)
                # grpNum = groupNums.genNewUniqueNum()

                #self.__debugLog("DocType: %s  DocId: %d" % (docType, docId))
                args = [self.__jobId, docNum, "Export", docType, docId,
                        version, grpNum, xml]
                opts = dict(host=self.__params.get("GKServer"))
                response = cdr2gk.sendDocument(*args, **opts)

                # DEBUG
                self.__debugLog("Pushed %s: %d group=%d" % (docType,
                                                            docId, grpNum))
                if response.type != "OK":
                    msg += "sending document %d failed. %s: %s<BR>" % \
                            (docId, response.type, response.message)
                    self.__debugLog(msg)
                    raise Exception(msg)
                docNum  = docNum + 1
                if docNum % 1000 == 0:
                    msg += "%s: Pushed %d documents<BR>" % (time.ctime(),
                                                            docNum)
                    self.__updateMessage(msg)
                    msg = ""
                addCount += 1
                row = cursor.fetchone()
            msg += "%s: %d documents pushed to Cancer.gov.<BR>" % (
                                          time.ctime(), addCount)
            self.__updateMessage(msg)
            msg = ""

            # Remove all the removed documents.
            qry = """
                SELECT id, num, doc_type
                  FROM pub_proc_cg_work
                 WHERE xml IS NULL
                   AND doc_type NOT IN (%s)""" % self.__excludeDT

            cursor.execute (qry)
            #cursor.execute ("""
            #    SELECT id, num, doc_type
            #      FROM pub_proc_cg_work
            #     WHERE xml IS NULL
            #                """)
            rows = cursor.fetchall()
            removeCount = len(rows)
            for row in rows:
                docId     = row[0]
                version   = row[1]
                docType   = row[2]
                if docType == "InScopeProtocol":
                    docType = "Protocol"
                elif docType == "GlossaryTermName":
                    docType = "GlossaryTerm"
                elif docType == "Person":
                    docType = "GeneticsProfessional"
                args = [self.__jobId, docNum, "Remove", docType, docId,
                        version, groupNums.genNewUniqueNum()]
                opts = dict(host=self.__params.get("GKServer"))
                response = cdr2gk.sendDocument(*args, **opts)
                if response.type != "OK":
                    msg += "deleting document %d failed. %s: %s<BR>" % (docId,
                            response.type, response.message)
                    raise Exception(msg)
                docNum  = docNum + 1
                if docNum % 1000 == 0:
                    msg += "%s: Pushed %d documents<BR>" % (time.ctime(),
                                                            docNum)
                    self.__updateMessage(msg)
                    msg = ""
            msg += "%s: %d documents removed from Cancer.gov.<BR>" % (
	                                    time.ctime(), removeCount)
            msg += "%s: Pushing done<BR>" % time.ctime()
            self.__updateMessage(msg)
            msg = ""

            # We're done with cancer.gov Gatekeeper link
            # Complete and close the connection
            args = self.__jobId, pubTypeCG, addCount + removeCount, "complete"
            opts = dict(host=self.__params.get("GKServer"))
            response = cdr2gk.sendJobComplete(*args, **opts)
            if response.type != "OK":
                msg = "Error response from job completion:<BR>" + \
                       str(response.message)
                raise Exception(msg)

            # Before we claim success, we will have to update
            # pub_proc_cg and pub_proc_doc from pub_proc_cg_work.
            # These transactions must succeed! Failure will cause
            # a mismatch between PPC/D and Cancer.gov database.
            if pubType in ("Full Load", "Export", "Reload"):
                self.__updateFromPPCW()
            elif pubType == "Hotfix (Remove)":
                self.__updateFromPPCWHR()
            elif pubType == "Hotfix (Export)":
                self.__updateFromPPCWHE()
            else:
                raise Exception("pubType %s not supported." % pubType)

            msg += "%s: Updating PPC/PPD tables done<BR>" % time.ctime()
            self.__updateMessage(msg)
            msg = ""

        except cdrdb.Error, info:
            self.__debugLog("Caught database error in __pushDocsToCG: %s" %
                            str(info))
            msg = "__pushDocsToCG() failed: %s<BR>" % info[1][0]
            raise Exception(msg)
        except Exception, arg:
            self.__debugLog("Caught Exception in __pushDocsToCG: %s" %
                            str(arg))
            raise Exception(str(arg))
        except:
            msg = "Unexpected failure in __pushDocsToCG.<BR>"
            raise Exception(msg)

    #------------------------------------------------------------------
    # Create rows in the working pub_proc_cg_work table before updating
    # pub_proc_cg and pub_proc_doc tables. After successfully sending
    # documents to CG, we can update PPC and PPD in an atomic transaction.
    # Note that all docs in PPCW are partitioned into 3 parts: updated,
    # new, and removed, although we don't distiguish the first two in the
    # table.
    #------------------------------------------------------------------
    def __createWorkPPC(self, vendor_job, vendor_dest):

        cg_job = self.__jobId
        cursor = self.__conn.cursor()
        cursor2 = self.__conn.cursor()
        cursor3 = self.__conn.cursor()

        # Wipe out all rows in pub_proc_cg_work. Only one job can be run
        # for Cancer.gov transaction. This is guaranteed by calling
        # __anotherPushJobPending().
        try:
            cursor.execute("""
                DELETE pub_proc_cg_work
                           """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            raise Exception("Deleting pub_proc_cg_work failed: %s<BR>" %
                            info[1][0])
        msg = "%s: Finished deleting pub_proc_cg_work<BR>" % time.ctime()
        self.__updateMessage(msg)

        # Insert updated documents into pub_proc_cg_work. Updated documents
        # are those in both pub_proc_cg and pub_proc_doc belonging to this
        # vendor_job. This is slow. We compare the XML document content to
        # see if it needs updating. If needed, we insert a row into
        # pub_proc_cg_work with xml set to the new document.
        try:
            # XXX Why is the subselect needed? [RMK 2004-12-17]
            qry = """
                SELECT ppc.id, t.name, ppc.xml, ppd2.subdir, ppd2.doc_version,
                       ppc.force_push
                  FROM pub_proc_cg ppc
                  JOIN pub_proc_doc ppd2
                    ON ppd2.doc_id = ppc.id
                  JOIN doc_version d
                    ON d.id = ppc.id
                   AND d.num = ppd2.doc_version
                  JOIN doc_type t
                    ON t.id = d.doc_type
                 WHERE ppd2.pub_proc = %d
                   AND t.name NOT IN (%s)
                   AND EXISTS (
                           SELECT *
                             FROM pub_proc_doc ppd
                            WHERE ppd.doc_id = ppc.id
                              AND ppd.pub_proc = %d
                              AND ppd.failure IS NULL
                              )
                  """ % (vendor_job, self.__excludeDT, vendor_job)
                #and ppc.id in (
            cursor.execute(qry, timeout = self.__timeOut)
            row = cursor.fetchone()
            idsInserted = {}

            # Regexp for normalizing whitespace for compares
            spNorm = re.compile (ur"\s\s+")

            # Setting parameters to slow down the writing and prevent
            # SQL connection failure.
            # -------------------------------------------------------
            ibrake = 1
            ifailure = 0
            brakeLevel = 500
            brakeTime  = 30

            # Fetch each doc
            while row:
                docId  = row[0]
                #self.__debugLog("*** processing %d ..." % docId)
                if idsInserted.has_key(docId):
                    row = cursor.fetchone()
                    continue
                idsInserted[docId] = 1
                dType              = row[1]
                xml                = row[2]
                subdir             = row[3]
                ver                = row[4]
                needsPush          = row[5] == 'Y'
                if dType == 'Media':
                    fileTxt = self.__getCgMediaDoc(vendor_dest, subdir,
                                                   docId)
                else:
                    path    = "%s/%s/CDR%d.xml" % (vendor_dest, subdir,
                                                   docId)
                    fileTxt = open(path, "rb").read()
                    fileTxt = unicode(fileTxt, 'utf-8')

                # The pub_proc_doc table may say push is not forced,
                # but the user may have overridden that in a job parameter.
                if self.__pushAllDocs:
                    needsPush = True

                # If push is not forced, decide by comparing the newly
                #  exported doc to the last published version.
                if not needsPush:
                    # Whitespace-normalized compare to stored file
                    if spNorm.sub(u" ", xml) != spNorm.sub(u" ", fileTxt):
                        needsPush = True

                if needsPush:
                    # If many small documents are being inserted (i.e.
                    # GlossaryTerms we need to slow down the process
                    # of inserting rows or the job will fail.
                    # ------------------------------------------------
                    if ibrake > brakeLevel:
                        self.__debugLog("Processed %d documents" % (ibrake - 1))
                        self.__debugLog("Resting %d seconds" % brakeTime)
                        time.sleep(brakeTime)
                        ibrake = 1

                    try:
                        # New xml is different or CG wants it sent anyway
                        #self.__debugLog("*** Inserting  %d ..." % docId)
                        cursor2.execute("""
                            INSERT INTO pub_proc_cg_work (id, vendor_job,
                                            cg_job, doc_type, xml, num)
                                 VALUES (?, ?, ?, ?, ?, ?)
                                        """, (docId, vendor_job, cg_job, dType,
                                              fileTxt, ver),
                                             timeout = self.__timeOut
                                       )
                        ibrake += 1
                        # self.__debugLog("*** Getting Next ID")
                    except cdrdb.Error, info:
                        # If the INSERT failed take a brake and try again
                        # after a few seconds.  If the second insert fails
                        # again exit, otherwise continue.
                        # ------------------------------------------------
                        ifailure += 1
                        self.__debugLog("*** DB Insert Error for %d" % docId)
                        self.__debugLog("*** Written %d records" % ibrake)
                        self.__debugLog("*** Retry after %d seconds" %
                                                                 brakeTime)
                        time.sleep(brakeTime)
                        try:
                            self.__debugLog("*** inserting %d ..." % docId)
                            cursor3.execute("""
                                INSERT INTO pub_proc_cg_work (id, vendor_job,
                                                cg_job, doc_type, xml, num)
                                     VALUES (?, ?, ?, ?, ?, ?)
                                            """, (docId, vendor_job, cg_job,
                                                  dType, fileTxt, ver),
                                                 timeout = self.__timeOut
                                           )
                            self.__debugLog("*** inserting %d OK" % docId)
                        except cdrdb.Error, info:
                            self.__debugLog("*** Second failure inserting")
                            raise Exception("Second Failure inserting %d to "
                                            "PPCG<BR>" % info[1][0])
                        except:
                            self.__debugLog("*** Non-DB Failure at second insert")
                            raise Exception("Non-DB Failure at second"
                                            "insertion")
                    except:
                        self.__debugLog("*** Some Non-DB Error")
                        #pass

                row = cursor.fetchone()

        except cdrdb.Error, info:
            self.__debugLog("*** Failure at %d" % docId)
            raise Exception("Setting U to pub_proc_cg_work failed: %s<BR>" %
                            info[1][0])
        except:
            raise Exception("Unexpected failure in setting U to "
                            "pub_proc_cg_work.")
        msg = "%s: Finished insertion for updating<BR>" % time.ctime()
        self.__updateMessage(msg)

        # Insert new documents into pub_proc_cg_work. New documents are
        # those in pub_proc_doc belonging to vendor_job, but not in
        # pub_proc_cg.
        try:
            cursor.execute ("""
                     SELECT DISTINCT ppd.doc_id, t.name, ppd.subdir,
                            ppd.doc_version
                       FROM pub_proc_doc ppd
                       JOIN doc_version d
                         ON d.id = ppd.doc_id
                        AND d.num = ppd.doc_version
                       JOIN doc_type t
                         ON d.doc_type = t.id
                      WHERE ppd.pub_proc = %d
                        AND ppd.failure IS NULL
                        AND t.name NOT IN (%s)
                        AND NOT EXISTS (
                                SELECT *
                                  FROM pub_proc_cg ppc
                                 WHERE ppc.id = ppd.doc_id
                                       )
                            """ % (vendor_job, self.__excludeDT),
                            timeout = self.__timeOut
                           )
            row = cursor.fetchone()
            while row:
                docId  = row[0]
                dType  = row[1]
                subdir = row[2]
                ver    = row[3]
                if dType == 'Media':
                    xml = self.__getCgMediaDoc(vendor_dest, subdir, docId)
                else:
                    path   = "%s/%s/CDR%d.xml" % (vendor_dest, subdir, docId)
                    xml    = open(path, "rb").read()
                    xml    = unicode(xml, 'utf-8')
                try:
                    cursor2.execute("""
                    INSERT INTO pub_proc_cg_work (id, vendor_job, cg_job,
                                                  doc_type, xml, num)
                         VALUES (?, ?, ?, ?, ?, ?)
                                    """,
                                    (docId, vendor_job, cg_job, dType,xml,ver),
                                    timeout = self.__timeOut
                                   )
                except:
                    raise Exception("Inserting CDR%d to PPCW failed." % docId)

                row = cursor.fetchone()

        except cdrdb.Error, info:
            raise Exception("Setting A to pub_proc_cg_work failed: %s<BR>" %
                            info[1][0])
        except Exception, arg:
            raise Exception(str(arg))
        except:
            raise Exception("Unexpected failure in setting A to "
                            "pub_proc_cg_work.")
        msg = "%s: Finished insertion for adding<BR>" % time.ctime()
        self.__updateMessage(msg)

        # Insert removed documents into pub_proc_cg_work.
        #
        # Removed documents are those in pub_proc_cg but which
        #   no longer have active status.
        # Media are not included just in case they are used by some
        #   other application on cancer.gov (at least this is my [ahm]
        #   current reconstruction of why I recommended that.)
        # Note: This is simpler, faster, and safer than older technique
        #   that removed all docs that weren't in this publication job.
        #
        # Removed documents must have a doc_type belonging to this
        #   Export subset [e.g., Protocol, Summary, Term, etc.].
        # Subsets Export-Protocol and Export-Summary need this special
        #   treatment.
        # Get a list of docType IDs such as "18,19,11".
        docTypes = self.__getSubsetDocTypes(vendor_job)

        if docTypes:
            try:
                qry = """
                    INSERT INTO pub_proc_cg_work (id, num, vendor_job,
                                                  cg_job, doc_type)
                         SELECT DISTINCT ppc.id, ppd_cg.doc_version,
                                %d, %d, t.name
                           FROM pub_proc_cg ppc, doc_type t, all_docs d,
                                pub_proc_doc ppd_cg
                          WHERE d.id = ppc.id
                            AND d.doc_type = t.id
                            AND d.doc_type IN (%s)
                            AND ppd_cg.doc_id = ppc.id
                            AND ppd_cg.pub_proc = ppc.pub_proc
                            AND d.active_status <> 'A'
                            AND t.name <> 'Media'
                            AND t.name NOT IN (%s)
                            AND ppc.id NOT IN (SELECT id
                                                 FROM pub_proc_cg_work)
                      """ % (vendor_job, cg_job, docTypes, self.__excludeDT)
                cursor.execute(qry, timeout = self.__timeOut)
            except cdrdb.Error, info:
                raise Exception("Setting D to pub_proc_cg_work failed: %s<BR>"
                                % info[1][0])
            msg = "%s: Finished insertion for deleting<BR>" % time.ctime()
        else:
            msg = ("%s: No remove transactions because no docTypes published!"
                   "<BR>" % time.ctime())
        self.__updateMessage(msg)

    #------------------------------------------------------------------
    # Return a string of doc type IDs to be used in query.
    #------------------------------------------------------------------
    def __getSubsetDocTypes(self, vendor_job):

        try:
            cursor = self.__conn.cursor()
            cursor.execute("""
                SELECT DISTINCT d.doc_type
                  FROM document d, pub_proc_doc p
                 WHERE d.id = p.doc_id
                   AND p.pub_proc = %d
                           """ % vendor_job
                          )
            # Return comma separated list of doc_type IDs, or "" if none
            return ",".join([str(row[0]) for row in cursor.fetchall()])

        except cdrdb.Error, info:
            msg = "Failure executing query to find doc types " \
                  "for job %d: %s" % (vendor_job, info[1][0])
            raise Exception(msg)

    #------------------------------------------------------------------
    # Generate the XML to be sent to Cancer.gov for a media document.
    #------------------------------------------------------------------
    def __getCgMediaDoc(self, vendorDest, subdir, docId):
        names = glob.glob("%s/%s/CDR%010d.*" % (vendorDest, subdir, docId))
        if not names:
            raise Exception("Failure locating media file for CDR%d" % docId)
        name = names[0]
        if name.endswith('.jpg'):
            mediaType = 'image/jpeg'
        elif name.endswith('.gif'):
            mediaType = 'image/gif'
        elif name.endswith('.mp3'):
            mediaType = 'audio/mpeg'
        else:
            raise Exception("Unsupported media type: %s" % name)
        fp = file(name, 'rb')
        bytes = fp.read()
        fp.close()
        return u"""\
<Media Type='%s' Size='%d' Encoding='base64'>
%s</Media>
""" % (mediaType, len(bytes), base64.encodestring(bytes))


    #------------------------------------------------------------------
    # Different version of __createWorkPPC for Hotfix (Remove)
    #------------------------------------------------------------------
    def __createWorkPPCHR(self, vendor_job):

        cg_job = self.__jobId
        cursor = self.__conn.cursor()

        # Wipe out all rows in pub_proc_cg_work. Only one job can be run
        # for Cancer.gov transaction. This is guaranteed by calling
        # __anotherPushJobPending().
        try:
            cursor.execute("""
                DELETE pub_proc_cg_work
                           """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            raise Exception("Deleting pub_proc_cg_work failed: %s<BR>" %
                            info[1][0])
        msg = "%s: Finished deleting pub_proc_cg_work<BR>" % time.ctime()
        self.__updateMessage(msg)

        # Insert removed documents into pub_proc_cg_work.
        # Removed documents are those in vendor_job. We will later
        # set the removed column in PPD and remove the PPC rows if
        # exist (indeed, it must exist).
        try:
            qry = """
                INSERT INTO pub_proc_cg_work (id, num, vendor_job,
                                              cg_job, doc_type)
                     SELECT DISTINCT ppd.doc_id, ppd.doc_version,
                            %d, %d, t.name
                       FROM pub_proc_doc ppd, doc_type t, document d
                      WHERE d.id = ppd.doc_id
                        AND d.doc_type = t.id
                        AND ppd.pub_proc = %d
                        AND t.name NOT IN (%s)
                  """ % (vendor_job, cg_job, vendor_job, self.__excludeDT)
            cursor.execute(qry, timeout = self.__timeOut)
        except cdrdb.Error, info:
            raise Exception("Setting D to pub_proc_cg_work failed (HFR): "
                            "%s<BR>" % info[1][0])
        msg = "%s: Finished inserting D to PPCW<BR>" % time.ctime()
        self.__updateMessage(msg)

    #------------------------------------------------------------------
    # Different version of __createWorkPPC for Hotfix (Export)
    # (XXX need to explain how it's different [RMK 2007-05-02])
    #------------------------------------------------------------------
    def __createWorkPPCHE(self, vendor_job, vendor_dest):

        cg_job = self.__jobId
        cursor = self.__conn.cursor()
        cursor2 = self.__conn.cursor()

        # Wipe out all rows in pub_proc_cg_work. Only one job can be run
        # for Cancer.gov transaction. This is guaranteed by calling
        # __anotherPushJobPending().
        try:
            cursor.execute("""
                DELETE pub_proc_cg_work
                           """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            raise Exception("Deleting pub_proc_cg_work failed: %s<BR>" %
                            info[1][0])

        # Insert updated documents into pub_proc_cg_work. Updated documents
        # are those that are in both pub_proc_cg and pub_proc_doc belonging
        # to this vendor_job. This is slow. We compare the XML document
        # content to see if it needs updating. If needed, we insert a row
        # into pub_proc_cg_work with xml set to the new document.
        try:
            qry = """
                SELECT ppc.id, t.name, ppc.xml, ppd2.subdir, ppd2.doc_version,
                       ppc.force_push
                  FROM pub_proc_cg ppc
                  JOIN pub_proc_doc ppd2
                    ON ppd2.doc_id = ppc.id
                  JOIN doc_version d
                    ON d.id = ppd2.doc_id
                   AND d.num = ppd2.doc_version
                  JOIN doc_type t
                    ON d.doc_type = t.id
                 WHERE ppd2.pub_proc = %d
                   AND t.name NOT IN (%s)
                   AND EXISTS (
                           SELECT *
                             FROM pub_proc_doc ppd
                            WHERE ppd.doc_id = ppc.id
                              AND ppd.pub_proc = %d
                              AND ppd.failure IS NULL
                              )
                  """ % (vendor_job, self.__excludeDT, vendor_job)
            cursor.execute(qry, timeout = self.__timeOut)
            row = cursor.fetchone()
            idsInserted = {}
            while row:
                docId  = row[0]
                if idsInserted.has_key(docId):
                    row = cursor.fetchone()
                    continue
                idsInserted[docId] = 1
                dType              = row[1]
                xml                = row[2]
                subdir             = row[3]
                ver                = row[4]
                needsPush          = row[5] == 'Y'
                if dType == 'Media':
                    fileTxt = self.__getCgMediaDoc(vendor_dest, subdir, docId)
                else:
                    path   = "%s/%s/CDR%d.xml" % (vendor_dest, subdir, docId)
                    fileTxt   = open(path, "rb").read()
                    fileTxt   = unicode(fileTxt, 'utf-8')

                # The pub_proc_doc table may say push is not forced,
                # but the user may have overridden that in a job parameter.
                if self.__pushAllDocs:
                    needsPush = True

                # XXX Why aren't we doing the same normalization here as
                #     in the original __createWorkPPC() method? [RMK
                #     2004-12-17]
                if not needsPush:
                    if xml != fileTxt:
                        needsPush = True
                if needsPush:
                    cursor2.execute("""
                        INSERT INTO pub_proc_cg_work (id, vendor_job,
                                        cg_job, doc_type, xml, num)
                             VALUES (?, ?, ?, ?, ?, ?)
                                    """, (docId, vendor_job, cg_job, dType,
                                          fileTxt, ver)
                                   )

                row = cursor.fetchone()

        except cdrdb.Error, info:
            raise Exception("Setting U to pub_proc_cg_work (HE) failed: %s<BR>" %
                            info[1][0])
        except:
            raise Exception("Unexpected failure in setting U to "
                            "pub_proc_cg_work (HE).")
        msg = "%s: Finished insertion for updating.<BR>" % time.ctime()
        self.__updateMessage(msg)

        # Insert new documents into pub_proc_cg_work. New documents are
        # those in pub_proc_doc belonging to vendor_job, but not in
        # pub_proc_cg.
        try:
            cursor.execute ("""
                     SELECT DISTINCT ppd.doc_id, t.name, ppd.subdir,
                            ppd.doc_version
                       FROM pub_proc_doc ppd, doc_type t, document d
                      WHERE ppd.pub_proc = ?
                        AND d.id = ppd.doc_id
                        AND d.doc_type = t.id
                        AND ppd.failure IS NULL
                        AND t.name NOT IN (%s)
                        AND NOT EXISTS (
                                SELECT *
                                  FROM pub_proc_cg ppc
                                 WHERE ppc.id = ppd.doc_id
                                       )
                            """ % self.__excludeDT,
                            vendor_job, timeout = self.__timeOut
                           )
            row = cursor.fetchone()
            while row:
                docId  = row[0]
                dType  = row[1]
                subdir = row[2]
                ver    = row[3]
                if dType == 'Media':
                    xml = self.__getCgMediaDoc(vendor_dest, subdir, docId)
                else:
                    # the open method fails if the subdir does not exist
                    # --------------------------------------------------
                    if subdir == '':
                       path   = "%s/CDR%d.xml" % (vendor_dest, docId)
                    else:
                       path   = "%s/%s/CDR%d.xml" % (vendor_dest, subdir, docId)

                    xml    = open(path, "rb").read()
                    xml    = unicode(xml, 'utf-8')
                cursor2.execute("""
                    INSERT INTO pub_proc_cg_work (id, vendor_job, cg_job,
                                                  doc_type, xml, num)
                         VALUES (?, ?, ?, ?, ?, ?)
                                """,
                                (docId, vendor_job, cg_job, dType, xml, ver),
                                timeout = self.__timeOut
                               )
                row = cursor.fetchone()

        except cdrdb.Error, info:
            raise Exception("Setting A to pub_proc_cg_work failed: %s<BR>" %
                            info[1][0])
        except:
            raise Exception("Unexpected failure in setting A to "
                            "pub_proc_cg_work.")
        msg = "%s: Finished insertion for adding<BR>" % time.ctime()
        self.__updateMessage(msg)

    #------------------------------------------------------------------
    # Update pub_proc_cg and pub_proc_doc from pub_proc_cg_work.
    # These transactions have to be successful or we have to review
    # related tables to find out what is wrong.
    # Note that the order of execution for PPC is critical: delete,
    # update, and insert.
    # It takes less than 20 minutes.
    #------------------------------------------------------------------
    def __updateFromPPCW(self):

        self.__conn.setAutoCommit(0)
        cursor = self.__conn.cursor()

        # Remove documents in PPC. The IN clause used should be OK.
        try:
            cursor.execute ("""
                DELETE pub_proc_cg
                 WHERE id IN (
                    SELECT ppcw.id
                      FROM pub_proc_cg_work ppcw
                     WHERE ppcw.xml IS NULL
                             )
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise Exception("Deleting from pub_proc_cg_work failed: %s<BR>" %
                            info[1][0])

        # Insert rows into PPD for removed documents of cg_job.
        try:
            cursor.execute ("""
                INSERT INTO pub_proc_doc (doc_id, doc_version, pub_proc,
                                          removed)
                     SELECT ppcw.id, ppcw.num, ppcw.cg_job, 'Y'
                       FROM pub_proc_cg_work ppcw
                      WHERE ppcw.xml IS NULL
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise Exception("Inserting D into pub_proc_doc failed: %s<BR>" %
                            info[1][0])

        # Update a document, if its id is in both PPC and PPD.
        # Update rows in PPC for updated documents of cg_job.
        try:
            cursor.execute ("""
                    UPDATE pub_proc_cg
                       SET xml = ppcw.xml, pub_proc = ppcw.cg_job,
                           force_push = 'N', cg_new = 'N'
                      FROM pub_proc_cg ppc, pub_proc_cg_work ppcw
                     WHERE ppc.id = ppcw.id
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise Exception("Updating xml, vendor_job from PPCW to PPC "
                            "failed: %s<BR>" % info[1][0])

        # Insert rows into PPD for updated documents of cg_job.
        try:
            cursor.execute ("""
               INSERT INTO pub_proc_doc (doc_id, doc_version, pub_proc)
                    SELECT ppcw.id, ppcw.num, ppcw.cg_job
                      FROM pub_proc_cg_work ppcw, pub_proc_cg ppc
                     WHERE ppcw.id = ppc.id
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise Exception("Inserting U into pub_proc_doc failed: %s<BR>" %
                            info[1][0])

        # Insert a document to both PPD and PPC.
        # Add new documents into PPD first.
        try:
            cursor.execute ("""
                INSERT INTO pub_proc_doc (doc_id, doc_version, pub_proc)
                     SELECT ppcw.id, ppcw.num, ppcw.cg_job
                       FROM pub_proc_cg_work ppcw
                      WHERE NOT ppcw.xml IS NULL
                        AND NOT EXISTS ( SELECT id
                                           FROM pub_proc_cg ppc
                                          WHERE ppc.id = ppcw.id
                                        )
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise Exception("Inserting A into pub_proc_doc failed: %s<BR>" %
                            info[1][0])

        # Add new documents into PPC last.
        try:

            cursor.execute ("""
                INSERT INTO pub_proc_cg (id, pub_proc, xml)
                     SELECT ppcw.id, ppcw.cg_job, ppcw.xml
                       FROM pub_proc_cg_work ppcw
                      WHERE NOT ppcw.xml IS NULL
                        AND NOT EXISTS ( SELECT id
                                           FROM pub_proc_cg ppc
                                          WHERE ppc.id = ppcw.id
                                        )
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise Exception("Inserting A into pub_proc_cg failed: %s<BR>" %
                            info[1][0])

        self.__conn.commit()
        self.__conn.setAutoCommit(1)

    #------------------------------------------------------------------
    # Different version of __updateFromPPCW for Hotfix (Remove)
    #------------------------------------------------------------------
    def __updateFromPPCWHR(self):

        self.__conn.setAutoCommit(0)
        cursor = self.__conn.cursor()

        # Remove documents. The IN clause used should be OK.
        try:
            cursor.execute ("""
                DELETE pub_proc_cg
                 WHERE id IN (
                    SELECT ppcw.id
                      FROM pub_proc_cg_work ppcw
                             )
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise Exception("Deleting PPC from PPCW failed: %s<BR>" %
                            info[1][0])

        # Insert rows in PPD for removed documents of cg_job.
        try:
            cursor.execute ("""
                INSERT INTO pub_proc_doc (doc_id, doc_version, pub_proc,
                                          removed)
                     SELECT ppcw.id, ppcw.num, ppcw.cg_job, 'Y'
                       FROM pub_proc_cg_work ppcw
                      WHERE ppcw.xml IS NULL
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise Exception("Inserting D into pub_proc_doc failed: %s<BR>" %
                            info[1][0])

        self.__conn.commit()
        self.__conn.setAutoCommit(1)

    #------------------------------------------------------------------
    # Different version of __updateFromPPCW for Hotfix (Export)
    #------------------------------------------------------------------
    def __updateFromPPCWHE(self):

        self.__conn.setAutoCommit(0)
        cursor = self.__conn.cursor()

        # Update a document, if its id is in both PPC and PPD.
        # Update rows in PPD for updated documents of cg_job.
        try:
            cursor.execute ("""
                    UPDATE pub_proc_cg
                       SET xml = ppcw.xml, pub_proc = ppcw.cg_job,
                           force_push = 'N', cg_new = 'N'
                      FROM pub_proc_cg ppc, pub_proc_cg_work ppcw
                     WHERE ppc.id = ppcw.id
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise Exception("Updating xml, vendor_job from PPCW to PPC "
                            "failed: %s<BR>" % info[1][0])

        # Insert rows into PPC for updated documents of cg_job.
        try:
            cursor.execute ("""
               INSERT INTO pub_proc_doc (doc_id, doc_version, pub_proc)
                    SELECT ppcw.id, ppcw.num, ppcw.cg_job
                      FROM pub_proc_cg_work ppcw, pub_proc_cg ppc
                     WHERE ppcw.id = ppc.id
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise Exception("Inserting U into pub_proc_doc failed: %s<BR>" %
                            info[1][0])

        # Insert a document to both PPD and PPC.
        # Add new documents into PPD first.
        try:
            cursor.execute ("""
                INSERT INTO pub_proc_doc (doc_id, doc_version, pub_proc)
                     SELECT ppcw.id, ppcw.num, ppcw.cg_job
                       FROM pub_proc_cg_work ppcw
                      WHERE NOT ppcw.xml IS NULL
                        AND NOT EXISTS ( SELECT id
                                           FROM pub_proc_cg ppc
                                          WHERE ppc.id = ppcw.id
                                        )
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise Exception("Inserting A into pub_proc_doc failed: %s<BR>" %
                            info[1][0])

        # Add new documents into PPC last.
        try:

            cursor.execute ("""
                INSERT INTO pub_proc_cg (id, pub_proc, xml)
                     SELECT ppcw.id, ppcw.cg_job, ppcw.xml
                       FROM pub_proc_cg_work ppcw
                      WHERE NOT ppcw.xml IS NULL
                        AND NOT EXISTS ( SELECT id
                                           FROM pub_proc_cg ppc
                                          WHERE ppc.id = ppcw.id
                                        )
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise Exception("Inserting A into pub_proc_cg failed: %s<BR>" %
                            info[1][0])

        self.__conn.commit()
        self.__conn.setAutoCommit(1)

    #------------------------------------------------------------------
    # Return the last successful cg_job for this vendor_job subset.
    #------------------------------------------------------------------
    def __getLastJobId(self, subsetName):

        jobId = 0

        try:
            cursor = self.__conn.cursor()
            # -- Must use "LIKE" instead of "=" for parm_value because
            # -- SQLServer won't test equality of NTEXT columns
            cursor.execute("""
                    SELECT MAX(pp.id)
                      FROM pub_proc pp, pub_proc_parm ppp
                     WHERE pp.status = ?
                       AND pp.pub_subset = ?
                       AND pp.pub_system = ?
                       AND ppp.pub_proc = pp.id
                       AND ppp.parm_name = 'SubSetName'
                       AND ppp.parm_value LIKE ?
                           """, (Publish.SUCCESS,
                                 "%s_%s" % (self.__pd2cg, subsetName),
                                 self.__ctrlDocId,
                                 subsetName)
                          )
            row = cursor.fetchone()

            if row and row[0]:
                return row[0]
            # else: return jobId 0 for the first job.

        except cdrdb.Error, info:
            msg = """Failure executing query to find last successful
                     jobId for subset %s: %s""" % (subsetName, info[1][0])
            raise Exception(msg)

        return jobId

    #------------------------------------------------------------------
    # Return a string describing what this job is for.
    #------------------------------------------------------------------
    def __getCgJobDesc(self):

        msgExpr = re.compile("<CgJobDesc>(.*?)</CgJobDesc>", re.DOTALL)
        CgJobDesc = ""

        try:
            cursor = self.__conn.cursor()
            cursor.execute("""
                    SELECT messages
                      FROM pub_proc
                     WHERE id = %d
                           """ % self.__jobId
                          )
            row = cursor.fetchone()
            msg = row[0]
            match = msgExpr.search(msg)
            if match:
                CgJobDesc = match.group(1)
                savedJobDesc = "%s: <B>JobDesc:</B> %s<BR>" % (time.ctime(),
                                                               CgJobDesc)
                message = msgExpr.sub(savedJobDesc, msg)
                cursor.execute("""
                    UPDATE pub_proc
                       SET messages = ?
                     WHERE id = ?
                               """, (message, self.__jobId)
                          )
            return CgJobDesc

        except cdrdb.Error, info:
            msg = """Failure in __getCgJobDesc for %d: %s""" % (
                self.__jobId, info[1][0])
            raise Exception(msg)

    #------------------------------------------------------------------
    # Return the last cg_job for any successful pushing jobs.
    #
    # Note: The picture of what the CDR sees as a 'successful' push job
    #       and what Gatekeeper sees as a successful push job may not
    #       always match.  If a push job does *not* include any document
    #       for Gaterkeeper, the CDR still identifies this job as a
    #       success (it did not fail) but Gatekeeper won't know about
    #       this job (it never received anything).  The next push job
    #       will therefore fail since both system's successful JobIDs
    #       will mismatch.
    #       This is fixed by only picking up JobIDs for which documents
    #       have been send to Gatekeeper.
    #------------------------------------------------------------------
    def __getLastCgJob(self):
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""
                    SELECT MAX(pp.id)
                      FROM pub_proc pp
                     WHERE pp.status = ?
                       AND pp.pub_subset LIKE ?
                       AND pp.pub_system = ?
                       AND (SELECT count(*)
                              FROM pub_proc_doc
                             WHERE pub_proc = pp.id) > 0
                           """, (Publish.SUCCESS,
                                 "%s%%" % self.__pd2cg,
                                 self.__ctrlDocId)
                          )
            row = cursor.fetchone()

            if row and row[0]:
                return row[0]
            else:
                return 0

        except cdrdb.Error, info:
            msg = """Failure executing query to find last successful
                     cg_job: %s<BR>""" % info[1][0]
            raise Exception(msg)

    #------------------------------------------------------------------
    # Launch some number of publishing threads.
    #
    # Waits until all threads are done, then returns.
    #
    # Attempts to detect if an error occurred in a thread, and raises
    # it on up the stack (hopefully) causing the program to exit and
    # abort all running threads.
    #
    #   filters     list of filter sets, each set with its own parm list
    #   destType    FILE, DOCTYPE, or DOC
    #   destDir     directory in which to write output
    #   subDir      subdirectory to store a subset of vendor docs
    #------------------------------------------------------------------
    def __launchPubThreads(self, filters, destType, destDir, subDir):

        # List of threading.Thread objects
        thrds = []

        for i in range(self.__numThreads):

            # Create a thread, passing all our arguments
            # Note use of threadId.  Thread object is not exposed to the
            #   thread and hasn't been created yet, so I can't use Thread.name
            thrd = threading.Thread(target=self.__publishDocList,
                                    kwargs={'threadId': "Thread-%d" % i,
                                            'filters': filters,
                                            'destType': destType,
                                            'destDir': destDir,
                                            'subDir': subDir})
            # Save object for later use
            thrds.append(thrd)

            # Launch it
            thrd.start()
            self.__debugLog("Started thread")

        # Wait for error or end of all threads
        done = 0
        while not done:
            done = 1
            for i in range(self.__numThreads):
                if thrds[i].isAlive():
                    # At least one thread is alive
                    done = 0
                else:
                    # In my tests, exceptions raised in a thread
                    #   weren't caught outside the thread where I
                    #   hoped to catch them.
                    # So I catch them in the thread, set self.__threadError
                    #   = thread id, log the error, and exit the thread.
                    # Here in the parent I check for this and raise the
                    #   exception in main - aborting if any thread failed.
                    # NOTE: Each thread must also check this and abort,
                    #   aborting the parent does _not_ kill the children.
                    chkError = "Thread-%d" % i
                    if self.__threadError == chkError:
                        # Abort
                        msg = "Aborting on error in thread %d, see logfile" % i
                        self.__debugLog(msg)
                        raise Exception(msg)

            # Wait awhile before checking again
            time.sleep(2)


    #------------------------------------------------------------------
    # Publish all of the documents in the list of docs, in a thread.
    #
    # This method may be called multiple times to run multiple threads
    # of publishing execution.
    #
    # Arguments are mostly the same as for __publishDoc() except that
    # there is no "doc" arg.  We get document id/version information
    # from self.__docs, synchronizing multiple threads on a lock object
    # to avoid two threads working on the same doc.
    #
    #   threadId    identifier string for logging
    #   filters     list of filter sets, each set with its own parm list
    #   destType    FILE, DOCTYPE, or DOC
    #   destDir     directory in which to write output
    #   subDir      subdirectory to store a subset of vendor docs
    #------------------------------------------------------------------
    def __publishDocList(self, threadId, filters, destType, destDir,
                          subDir=''):

        # Flags indicating completion and error status
        done  = 0
        error = 0

        # This is a test:
        # Publishing fails frequently while processing Media/Audio
        # documents and it's suspected that the OS is running out of
        # file handles.  We're repeating the process of slowing down
        # the system for processing these small MP3 audio files in the
        # same way as done for adding small documents to pub_proc_cg_work
        # ---------------------------------------------------------------
        t0 = time.clock()
        t1 = 0
        ibrake = 1
        mult = 1
        brakeAtDocCount = 200
        brakeTime = 30         # number of seconds to pause
        maxDocsPerSecond = 4   # don't allow more docs per second to be
                               # processed without the occational breather
                               # for the OS

        while not done:
            # Get another document id to publish
            self.__lockNextDoc.acquire(1)
            if len(self.__docs) >= self.__nextDoc + 1:
                doc = self.__docs[self.__nextDoc]
                self.__nextDoc      += 1
                self.__totalPubDocs += 1

                # Is it time to log progress?
                # This holds up other threads, but only happens
                #   once every logDocModulus documents
                if self.__totalPubDocs % self.__logDocModulus == 0:
                    try:
                        numFailures = self.__getFailures()

			msg = "%s: Filtered/validated %d docs. %d docs " % (
                                                           time.ctime(),
							   self.__totalPubDocs,
                                                           numFailures)
			msg += "failed so far.<BR>"
                        self.__updateMessage(msg)
                    except:
                        pass
            else:
                done = 1
            self.__lockNextDoc.release()

            # If we got one, publish it
            # Try to handle exceptions gracefully, then get out
            if not done:
                # Check how fast we are processing documents.  If we're too
                # fast, Windows OS starts choking without telling anyone
                # ---------------------------------------------------------
                if ibrake > brakeAtDocCount:
                    tNow = time.clock()
                    tDelta = tNow - t1
                    # This is the first call of the new thread.  t0 is set
                    # to zero and tDelta is incorrect.  Setting tDelta to 999
                    # -------------------------------------------------------
                    if tDelta == tNow and t0 > 1: tDelta = 999999
                    self.__debugLog("Secs this thread: %d" % tNow)
                    self.__debugLog("Docs this thread: %d" % (
                                                      mult * brakeAtDocCount))
                    self.__debugLog("Last %d docs processed in %d secs" % (
                                                          ibrake - 1, tDelta))
                    self.__debugLog("Current Ratio Docs/sec: %.3f" % (
                                                      brakeAtDocCount/tDelta))

                    # We only need to take a breather and pause the program
                    # if documents are being processes very fast.  For
                    # larger documents (almost all but audio files) we don't
                    # need to take a break
                    # ------------------------------------------------------
                    if brakeAtDocCount / tDelta > maxDocsPerSecond:
                        self.__debugLog("Allowed Ratio Docs/sec: %.3f ***" % (
                                                      maxDocsPerSecond))
                        self.__debugLog("Pausing for %d secs" % brakeTime)
                        time.sleep(brakeTime)

                    # Keep track of how often we came here and reset t1
                    # -------------------------------------------------
                    mult += 1
                    ibrake = 1
                    t1 = tNow

                try:
                    self.__publishDoc (doc, filters, destType, destDir, subDir)
                    ibrake += 1
                except cdrdb.Error, info:
                    self.__debugLog(
                        "Database error publishing doc %d ver %d in %s:\n  %s"
                        % (doc.getDocId(), doc.getVersion(), threadId,
                           info[1][0]), tb=1)
                    error = 1
                except:
                    self.__debugLog(
                        "Exception publishing doc %d ver %d in %s"
                        % (doc.getDocId(), doc.getVersion(), threadId), tb=1)
                    error = 1

            # If error occurred, signal it
            if error:
                self.__threadError = threadId

            # Error in this or any other thread causes thread exit
            # Otherwise an error in one thread won't stop others
            if self.__threadError:
                self.__debugLog("Exiting thread %s due to error in thread %s" %
                               (threadId, self.__threadError))
                done = 1

        # Done with thread
        sys.exit()


    #------------------------------------------------------------------
    # Publish one document.
    #
    #   doc         tuple containing doc ID, doc version, and doc type
    #               string
    #   filters     list of filter sets, each set with its own parm list
    #   destType    FILE, DOCTYPE, or DOC
    #   destDir     directory in which to write output
    #   recordDoc   flag indicating whether to add row to pub_proc_doc
    #               table
    #   subDir      subdirectory to store a subset of vendor docs
    #------------------------------------------------------------------
    def __publishDoc(self, doc, filters, destType, destDir, subDir = ''):

        docId = doc.getDocId()
        self.__debugLog("Publishing CDR%010d." % docId)

        # Keep track of problems encountered during filtering.
        warnings = ""
        errors   = ""
        invalDoc = ""
        date     = None

        # Save blob, not XML, for Media docs.
        if doc.getDocTypeStr() == "Media":
            name = "CDR%010d" % docId
            try:
                cdrDoc = cdr.getDoc('guest', docId,
                                    version = str(doc.getVersion()),
                                    blob = 'Y', getObject = True,
                                    port = self.__pubPort)
                name = cdrDoc.getPublicationFilename()
                self.__saveDoc(cdrDoc.blob, destDir + '/' + subDir, name, "wb")
                self.__lockDb.acquire(1)
                args = "guest", docId, doc.getVersion()
                opts = dict(conn=self.__conn)
                lastChange = cdr.getVersionedBlobChangeDate(*args, **opts)
                self.__lockDb.release()
                title = cdrDoc.ctrl.get('DocTitle', '').strip()
                title = title.replace('\r', '').replace('\n', ' ')
                self.__lockManifest.acquire(1)
                self.__mediaManifest.append((name, lastChange, title))
                self.__lockManifest.release()
            except Exception, e:
                errors = "Failure writing %s: %s" % (name, str(e))
            except:
                errors = "Failure writing %s" % name

        # Standard processing for non-Media documents.
        else:

            # Apply each filter set to the document.
            filteredDoc = None
            for filterSet in filters:

                # Substitute parameter value of DateFirstPub.
                paramList = []
                for pair in filterSet[1]:
                    if pair[0] == 'DateFirstPub':
                        date = self.__dateFirstPub[docId]
                        paramList.append((pair[0], date))

                    else:
                        paramList.append((pair[0], pair[1]))

                # Pass a parameter pubProcDate to the filter
                # This is needed to create a non-empty verification
                # date for all documents with a first_pub_knowable
                # (see creation of hash __dateFirstPub; the date
                #  is empty if first_pub_knowable = 'N', it is None
                #  for non-publishing jobs, i.e. QC batches)
                # =================================================
                if date != '':
                    paramList.append(('pubProcDate', self.__jobTime[:10]))

                # Set limiting date-time for documents
                if self.__params.has_key('MaxDocUpdatedDate'):
                    maxDocDate = self.__params['MaxDocUpdatedDate']
                else:
                    maxDocDate = None
                # XXX Need to add filterDate param in future
                maxFilterDate = None

                # First filter set is run against document from database.
                if not filteredDoc:
                    result = cdr.filterDoc(self.__credentials, filterSet[0],
                                           docId = docId,
                                           docVer = doc.getVersion(),
                                           parm = paramList,
                                           docDate = maxDocDate,
                                           filterDate = maxFilterDate,
                                           port = self.__pubPort)

                # Subsequent filter sets are applied to previous results.
                else:
                    result = cdr.filterDoc(self.__credentials, filterSet[0],
                                           doc = filteredDoc, parm = paramList,
                                           docDate = maxDocDate,
                                           filterDate = maxFilterDate,
                                           port = self.__pubPort)

                if type(result) not in (type([]), type(())):
                    errors = result or "Unspecified failure filtering document"
                    filteredDoc = None
                    break

                filteredDoc = result[0]
                if result[1]: warnings += result[1]

            # Validate the filteredDoc against Vendor DTD.
            if self.__validateDocs and filteredDoc:
                pdqdtd = str(os.path.join(cdr.PDQDTDPATH,
                                          self.__params['DTDFileName']))
                errObj = validateDoc(filteredDoc, docId = docId,
                                                        dtd = pdqdtd)

                for error in errObj:
                    if error.level_name == 'ERROR':
                        dtdClass = 'DTDerror'
                    else:
                        dtdClass = 'DTDwarning'

                    errors += "<span class='%s'>%s</span><BR>%s<BR>" % (
                                              dtdClass, error.level_name,
                                              error.message)
                    invalDoc = "InvalidDocs"

            # Save the output as instructed.
            if self.__withOutput and filteredDoc:
                try:
                    if invalDoc:
                        subDir = invalDoc
                    destDir = destDir + "/" + subDir
                    if destType == Publish.FILE:
                        self.__saveDoc(filteredDoc, destDir,
                                       self.__fileName, "a")
                    # Removed because not threadsafe and never used
                    # elif destType == Publish.DOCTYPE:
                    #     self.__saveDoc(filteredDoc, destDir,
                    #                    doc.getDocTypeStr(), "a")
                    else:
                        self.__saveDoc(filteredDoc, destDir,
                                       "CDR%d.xml" % docId)
                except:
                    # It appears that the system fails occasionally to
                    # write one of the first two documents processed
                    # to disk even though all other disk writes finish
                    # successfully.  If we run into a failure we want
                    # to wait a moment and try again.
                    try:
                        warnings  = '<LI class="warning">Failure writing '
                        warnings += 'document CDR%010d<BR/>' % docId
                        warnings += 'Trying again...'
                        time.sleep(2)
                        # We always use destType = Publish.DOC for
                        # publishing.  So I ignore the fact that a
                        # single file holding all publishing results
                        # may fail to write as well and we just retry
                        # writing that single document.
                        self.__saveDoc(filteredDoc, destDir,
                                       "CDR%d.xml" % docId)
                    except:
                        errors  = 'Failed twice to write document '
                        errors += 'CDR%010d' % docId

        # Handle errors and warnings.
        self.__checkProblems(doc, errors, warnings)

    #------------------------------------------------------------------
    # Handle errors and warnings.  Value of -1 for __errorsBeforeAborting
    # means never abort no matter how many errors are encountered.
    # If __publishIfWarnings has a value other than "No" we record the
    # warnings and keep going.
    #------------------------------------------------------------------
    def __checkProblems(self, doc, errors, warnings):

        # If no errors or warnings, no synchronization needed
        if not (errors or warnings):
            return

        # If not None, we have an abort situation
        msg = None

        # Check errors
        if errors:
            # OCECDR-3951: keep track of error by document type
            doctype = doc.getDocTypeStr()
            count = self.__perDoctypeErrors.get(doctype, 0)
            self.__perDoctypeErrors[doctype] = count + 1
            self.__addDocMessages(doc, errors, Publish.SET_FAILURE_FLAG)
            self.__errorCount += 1
            if self.__errorsBeforeAborting != -1:
                if self.__errorCount > self.__errorsBeforeAborting:
                    if self.__errorsBeforeAborting:
                      msg  = "Aborting: too many errors detected "
                      msg += "at CDR%d.<BR>" % doc.getDocId()
                    else:
                      msg = "Aborting: too many errors encountered"

                    # This message is being printed twice. Why? VE, 2009-12-09
                    dbMsg = "Aborting: more than %d errors encountered<BR>" % \
                             self.__errorsBeforeAborting
                    self.__updateMessage(dbMsg)

        # Check warnings
        if warnings:
            self.__addDocMessages(doc, warnings)
            self.__warningCount += 1
            if self.__publishIfWarnings == "No":
                msg = "Aborting on warning(s) detected in CDR%010d.<BR>" % \
                      doc.getDocId()

        # Did we get an abort message?
        if msg:
            self.__debugLog("checkProblems raises Exception, msg=%s" % msg)
            raise Exception(msg)

    #------------------------------------------------------------------
    # Record warning or error messages for the job.
    #------------------------------------------------------------------
    def __addJobMessages(self, messages):
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""\
                SELECT messages
                  FROM pub_proc
                 WHERE id = ?""", self.__jobId)
            row = cursor.fetchone()
            if not row:
                raise Exception("Failure reading messages for job %d" %
                                self.__jobId)
            if row[0]:
                messages = row[0] + "|" + messages
            cursor.execute("""\
                UPDATE pub_proc
                   SET messages = ?
                WHERE id = ?""", (messages, self.__jobId))
        except cdrdb.Error, info:
            msg = 'Failure recording message for job %d: %s' % \
                  (self.__jobId, info[1][0])
            raise Exception(msg)

    #------------------------------------------------------------------
    # Record warning or error messages for a document.
    #------------------------------------------------------------------
    def __addDocMessages(self, doc, messages, failure = None):

        # Just update the Doc object.  Database update comes later
        doc.addMsg(messages)
        if failure:
            doc.setFailed()

    #------------------------------------------------------------------
    # Record the publication of all of the documents.
    #------------------------------------------------------------------
    def __addPubProcDocRows(self, subDir):

        # All selected docs
        # This might be optimized if we decide to write an optimized
        #   cdrdb.executemany()
        cursor = self.__conn.cursor()
        for doc in (self.__docs):
            try:
                if not doc.getRecorded():
                    cursor.execute("""\
                        INSERT INTO pub_proc_doc
                        (
                                    pub_proc,
                                    doc_id,
                                    doc_version,
                                    messages,
                                    failure,
                                    subdir
                        )
                             VALUES
                        (
                                    ?, ?, ?, ?, ?, ?
                        )""", (self.__jobId,
                               doc.getDocId(),
                               doc.getVersion(),
                               doc.getMsgs(),
                               doc.getFailed() and 'Y' or None,
                               subDir))
                    doc.setRecorded()
                else:
                    msg = "Internal error, asked to create PPD row for doc" \
                          " that's already recorded: jobId/docId=%d/%d" % \
                           (self.__jobId, doc.getDocId())
                    raise Exception(msg)

            except cdrdb.Error, info:
                msg = 'Failure adding or updating row for document %d-%d: %s' % \
                      (doc.getDocId(), doc.getVersion(), info[1][0])
                #msg='Failure adding or updating row for document %d: %s' % \
                      #(self.__jobId, info[1][0])
                raise Exception(msg)

    #------------------------------------------------------------------
    # For docs already in the pub_proc_doc table, add any messages
    # and failure codes.
    #------------------------------------------------------------------
    def __addPubProcDocMsgs(self):

        # All user listed docs
        cursor = self.__conn.cursor()
        for doc in (self.__userDocList):
            try:
                # Row already exists, set messages if there are any
                # Previous versions would retrieve messages and failure
                #   codes from the database first, to be sure we don't
                #   overwrite them.
                # But now we do all updates in this one spot, so it
                #   isn't necessary to examine what's already there.
                if doc.getMsgs():
                    cursor.execute("""\
                        UPDATE pub_proc_doc
                           SET messages=?, failure=?
                         WHERE pub_proc=? AND doc_id=?""",
                         (doc.getMsgs(), doc.getFailed() and 'Y' or None,
                          self.__jobId, doc.getDocId()))

            except cdrdb.Error, info:
                msg = 'Failure updating row for document %d: %s' % \
                      (self.__jobId, info[1][0])
                raise Exception(msg)

    #------------------------------------------------------------------
    # Build a set of documents which match the queries for a subset
    # specification.  This version does not include any optimizations
    # which might be achieved using temporary tables to collapse
    # multiple queries into one.  XXX XQL queries not yet supported.
    #------------------------------------------------------------------
    def __selectQueryDocs(self, specNode):

        # Start with an empty list.
        docs = []

        # Walk through the specification looking for queries to execute.
        for child in specNode.childNodes:

            # Gather documents selected by SQL query.
            if child.nodeName == "SubsetSQL":

                try:
                    cursor = self.__conn.cursor()
                    sql = self.__repParams(cdr.getTextContent(child))
                    self.__debugLog("Selecting docs using:\n%s" % sql)
                    # cdr.callOrExec(cursor, sql, timeout=self.__timeOut)
                    # cursor.execute(sql, timeout = self.__timeOut)
                    # XXX FOLLOWING IS EXPERIMENTAL
                    # XXX NOT WORKING AT PRESENT WITH MULTIPART
                    # XXX Interim-Export NON-ACTIVE PROTOCOL QUERY
                    # XXX IT APPARENTLY DOES NOT CREATE A cursor.description
                    # XXX SAME PROBLEM WHEN INVOKED AS execute OR callproc.
                    if sql.strip().upper().startswith("EXEC"):
                        tempsql = sql.strip().upper()
                        if tempsql.startswith("EXECUTE "):
                            tempsql = tempsql[8:]
                        else:
                            tempsql = tempsql[5:]
                        self.__debugLog("Calling proc '%s'" % tempsql)
                        cursor.callproc(tempsql, ())
                    else:
                        cursor.execute(sql, timeout = self.__timeOut)

                    # Sanity checks for the query.
                    if not cursor.description:
                        raise Exception(u"Result set not returned for "
                                        u"SQL query: %s" % sql)
                    if len(cursor.description) < 1:
                        raise Exception(u"SQL query must return at least "
                                        u"one column (containing a "
                                        u"document ID): %s" % sql)

                    # See if we have a version column.
                    haveVersion = len(cursor.description) > 1

                    rows = cursor.fetchall()

                    self.__debugLog("Selected %d documents." % len(rows))
                    self.__debugLog("Searching pub versions.")
                    for row in rows:
                        oneId = row[0]
                        if oneId in self.__alreadyPublished: continue
                        ver = haveVersion and row[1] or None

                        try:
                            doc = self.__findPublishableVersion(oneId, ver)
                        except Exception, arg:

                            # Can't record this in the pub_proc_doc table,
                            # because we don't really have a versioned
                            # document.
                            self.__errorCount += 1
                            threshold = self.__errorsBeforeAborting
                            if threshold != -1:
                                if self.__errorCount > threshold:
                                    raise
                            self.__addJobMessages(unicode(arg))

                            # XXX Why are we falling through to the following
                            #     code if the call to get doc fails???
                        docs.append(Doc(doc[0], doc[1], doc[2]))
                        self.__alreadyPublished.add(oneId)

                except cdrdb.Error, info:
                    msg = 'Failure retrieving document IDs for job %d: %s' % \
                          (self.__jobId, info[1][0])
                    raise Exception(msg)

            # Handle XQL queries.
            elif child.nodeName == "SubsetXQL":
                xql = self.__repParams(cdr.getTextContent(child))
                resp = cdr.search(self.__credentials, xql)
                if type(resp) in (type(""), type(u"")):
                    raise Exception("XQL failure: %s" % resp)
                for queryResult in resp:
                    oneId  = queryResult.docId
                    # dType = queryResult.docType # Currently unused
                    digits = re.sub('[^\d]', '', oneId)
                    oneId     = int(digits)
                    if oneId in self.__alreadyPublished: continue
                    try:
                        doc = self.__findPublishableVersion(oneId)
                    except Exception, arg:
                        self.__errorCount += 1
                        threshold = self.__errorsBeforeAborting
                        if threshold != -1:
                            if self.__errorCount > threshold:
                                raise
                        self.__addJobMessages(arg[0])
                    docs.append(Doc(doc[0], doc[1], doc[2]))
                    self.__alreadyPublished.add(oneId)

        self.__debugLog("SubsetSpecification queries selected %d documents."
                        % len(docs))
        return docs

    #------------------------------------------------------------------
    # Find the requested publishable version of a specified document.
    #------------------------------------------------------------------
    def __findPublishableVersion(self, id, version = None):
        if version:
            sql = """\
                SELECT d.id,
                       d.num,
                       t.name
                  FROM doc_version d
                  JOIN doc_type    t
                    ON t.id          = d.doc_type
                  JOIN document    d2
                    ON d2.id         = d.id
                 WHERE d.id          = %d
                   AND d.num         = %d
                   AND d.publishable = 'Y'
                   AND d.val_status  = 'V'""" % (id, version)
        else:
            sql = """\
                SELECT d.id,
                       MAX(d.num),
                       t.name
                  FROM doc_version d
                  JOIN doc_type    t
                    ON t.id          = d.doc_type
                  JOIN document    d2
                    ON d2.id         = d.id
                 WHERE d.id          = %d
                   AND d.publishable = 'Y'
                   AND d.val_status  = 'V'
                   AND d.dt         <= '%s'
              GROUP BY d.id,
                       t.name""" % (id, self.__jobTime)
        try:
            cursor = self.__conn.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            if not row:
                if version:
                    raise Exception("Version %d for document CDR%010d "
                                    "is not publishable or does not "
                                    "exist" % (version, id))
                else:
                    raise Exception("Unable to find publishable version "
                                    "for document CDR%010d" % id)
        except cdrdb.Error, info:
            msg = "Failure executing query to find publishable version " \
                  "for CDR%010d: %s" % (self.__jobId, info[1][0])
            raise Exception(msg)
        return tuple(row)

    #------------------------------------------------------------------
    # Inform the user that the job has completed.
    # XXX Add code to notify list of standard users for publishing
    # job notification.
    #------------------------------------------------------------------
    def __sendMail(self):

        try:
            if self.__email and self.__email != "Do not notify":
                self.__debugLog("Sending mail to %s." % self.__email)
                sender = self.__cdrEmail
                subject = "CBIIT-%s: CDR Publishing Job Status" % TIER.name
                if isinstance(self.__email, basestring):
                    receivers = self.__email.replace(";", ",").split(",")
                elif type(self.__email) is list:
                    receivers = self.__email

                message   = """
Job %d has completed or changed status.  You can view a status report for this job at:

    %s/PubStatus.py?id=%d

Please do not reply to this message.
""" % (self.__jobId, self.__cdrHttp, self.__jobId)
                cdr.sendMail(sender, receivers, subject, message)
        except:
            msg = "failure sending email to %s: %s" % \
                (self.__email, cdr.exceptionInfo())
            self.__debugLog(msg)
            raise Exception(msg)

    #----------------------------------------------------------------
    # Set up a connection to CDR.  Processing of the publishing job
    # is likely to take long enough that we can't afford to keep
    # locks on the publishing tables during the whole job, so we
    # avoid wrapping the whole job in a single transaction by turning
    # on auto commit mode.
    #----------------------------------------------------------------
    def __getConn(self):
        try:
            self.__conn = cdrdb.connect("CdrPublishing")
            self.__conn.setAutoCommit()
        except cdrdb.Error, info:
            self.__conn = None
            msg = 'Database connection failure: %s' % info[1][0]
            self.__debugLog(msg)
            raise Exception(msg)

    #----------------------------------------------------------------
    # Return the document for the publishing control system from
    # the database.  Be sure to retrieve the version corresponding
    # to the date/time of the publication job.
    #----------------------------------------------------------------
    def __getCtrlDoc(self):

        try:
            cursor = self.__conn.cursor()

            # Do this in two queries to work around an ADODB bug.
            cursor.execute("""\
                    SELECT MAX(num)
                      FROM doc_version
                     WHERE id  = ?
                       AND dt <= ?""", (self.__ctrlDocId, self.__jobTime))
            row = cursor.fetchone()
            if not row:
                raise Exception("Unable to find version of document "
                                "CDR%010d created on or before %s" %
                                (self.__ctrlDocId, self.__jobTime))
            cursor.execute("""\
                    SELECT xml
                      FROM doc_version
                     WHERE id  = ?
                       AND num = ?""", (self.__ctrlDocId, row[0]))
            row = cursor.fetchone()
            if not row or not row[0]:
                raise Exception("Failure retrieving xml for control "
                                "document CDR%010d" % self.__ctrlDocId)
        except cdrdb.Error, info:
            raise Exception("Failure retrieving version of control "
                            "document CDR%010d on or before %s: %s" %
                            (self.__ctrlDocId, self.__jobTime, info[1][0]))

        xml = row[0]

        # XXX Latin 1 may not be adequate for all documents!
        return xml.encode('latin-1')

    #----------------------------------------------------------------
    # Return a SubSet node based on __subsetName.
    # Set __sysName needed by Cancer.gov as a side-effect.
    # Don't need to check nodeType since the schema is known
    #    and __subsetName is unique.
    # Error checking: node not found.
    #----------------------------------------------------------------
    def __getSubSet(self, docElem):
        pubSys = xml.dom.minidom.parseString(docElem).documentElement
        for node in pubSys.childNodes:
            if node.nodeName == "SystemName":
                self.__sysName = cdr.getTextContent(node)
            if node.nodeName == "SystemSubset":
                for n in node.childNodes:
                    if n.nodeName == "SubsetName":
                        for m in n.childNodes:
                            if m.nodeValue == self.__subsetName:
                                return node

        # not found
        msg = "Failed in __getSubSet. SubsetName: %s." % self.__subsetName
        raise Exception(msg)

    #----------------------------------------------------------------
    # Replace ?Name? with values in the parameter list.
    #----------------------------------------------------------------
    def __repParams(self, str):
        ret = str
        for name in self.__params.keys():
            ret = re.sub(r"\?%s\?" % name, self.__params[name], ret)

        # Not sure this is ever used.  Looks like a broken re to me.
        # I'm leaving it in, but see JobStartDateTime above which does
        #   this the right way I think. - AHM
        ret = re.sub(r"\?$JobDateTime\?", self.__jobTime, ret)

        return ret

    #----------------------------------------------------------------
    # Get a list of options from the subset.
    # The options specify what to do about publishing results or
    #     processing errors.
    #----------------------------------------------------------------
    def __getOptions(self, subset):
        options = {}
        abortOnError = "Yes"
        for node in subset.childNodes:
            if node.nodeName == "SubsetOptions":
                for n in node.childNodes:
                    if n.nodeName == "SubsetOption":
                        name = None
                        value = ""
                        for m in n.childNodes:
                            if m.nodeName == "OptionName":
                                name = cdr.getTextContent(m)
                            elif m.nodeName == "OptionValue":
                                value = cdr.getTextContent(m)
                        if not name:
                            raise Exception("SubsetOption missing "
                                            "required OptionName element")
                        if name in options and options[name] != value:
                            raise Exception("Duplicate option '%s'" % name)
                        options[name] = value
                        self.__debugLog("Option %s='%s'." % (name, value))
                        if name == "AbortOnError":
                            abortOnError = value
                            if self.__params.has_key('AbortOnError'):
                                abortOnError = self.__params['AbortOnError']
                        elif name == "PublishIfWarnings":
                            if value not in ("Yes", "No"):
                                raise Exception("Invalid value for "
                                                "PublishIfWarnings: %s" %
                                                value)
                            self.__publishIfWarnings = value
                if abortOnError:
                    if abortOnError == "Yes": self.__errorsBeforeAborting = 0
                    elif abortOnError == "No": self.__errorsBeforeAborting = -1
                    else:
                        try:
                            self.__errorsBeforeAborting = int(abortOnError)
                        except:
                            raise Exception("Invalid value for AbortOnError: "
                                            "%s" % abortOnError)

            # Extract error thresholds specific to document types.
            elif node.nodeName == "PerDoctypeErrorThresholds":
                for n in node.childNodes:
                    if n.nodeName == "PerDoctypeErrorThreshold":
                        doctype = threshold = None
                        for m in n.childNodes:
                            if m.nodeName == "Doctype":
                                doctype = cdr.getTextContent(m)
                            elif m.nodeName == "MaxErrors":
                                threshold = cdr.getTextContent(m)
                        try:
                            threshold = int(threshold)
                        except:
                            raise Exception("invalid error threshold %s" %
                                            repr(threshold))
                        if not doctype:
                            raise Exception("PerDoctypeErrorThreshold missing "
                                            "required Doctype element")
                        if doctype in self.__perDoctypeMaxErrors:
                            existing = self.__perDoctypeMaxErrors[doctype]
                            if existing != threshold:
                                raise Exception("Conflicting error thresh"
                                                "holds for %s" % repr(doctype))
                        self.__perDoctypeMaxErrors[doctype] = threshold
                        self.__debugLog("%d errors allowed for %s documents" %
                                        (threshold, doctype))

        return options

    #----------------------------------------------------------------
    # Get the list of filter sets for this subset specification.
    # There must be at least one filter set, and each filter set
    # and each filter set must have at least one filter.  Each
    # filter set has a possibly empty list of parameters.
    #----------------------------------------------------------------
    def __getFilters(self, spec):
        filterSets = []
        for node in spec.childNodes:
            if node.nodeName == "SubsetFilters":
                filterSets.append(self.__getFilterSet(node))
        if filterSets:
            return filterSets
        raise Exception("Subset specification has no filters")

    #----------------------------------------------------------------
    # Extract a set of filters and associated parameters.
    #----------------------------------------------------------------
    def __getFilterSet(self, node):
        filters = []
        parms   = []
        for child in node.childNodes:
            if child.nodeName == "SubsetFilter":
                filters.append(self.__getFilter(child))
            elif child.nodeName == "SubsetFilterParm":
                parms.append(self.__getFilterParm(child))
        if not filters:
            raise Exception("SubsetFilters element must have at least "
                            "one SubsetFilter child element")
        return (filters, parms)

    #----------------------------------------------------------------
    # Extract the document ID or title for a filter.
    #----------------------------------------------------------------
    def __getFilter(self, node):
        for child in node.childNodes:
            if child.nodeName == "SubsetFilterName":
                nameOrSet = cdr.getTextContent(child)
                self.__debugLog('Fetching filter named "%s"' % nameOrSet)
                if nameOrSet.find("set:") == 0:
                    return nameOrSet
                else:
                    return "name:%s" % nameOrSet
            elif child.nodeName == "SubsetFilterId":
                filterId = cdr.getTextContent(child)
                self.__debugLog('Fetching filter id "%s"' % filterId)
                return filterId
        raise Exception("SubsetFilter must contain SubsetFilterName "
                        "or SubsetFilterId")

    #----------------------------------------------------------------
    # Extract the name/value pair for a filter parameter.  Substitute
    # any job parameters for ?name? placeholders as appropriate.
    #----------------------------------------------------------------
    def __getFilterParm(self, node):
        parmName  = None
        parmValue = ""
        for child in node.childNodes:
            if child.nodeName == "ParmName":
                parmName = cdr.getTextContent(child)
            elif child.nodeName == "ParmValue":
                parmValue = cdr.getTextContent(child)
                parmValue = self.__repParams(parmValue)
        if not parmName:
            raise Exception("Missing ParmName in SubsetFilterParm")
        return (parmName, parmValue)

    #----------------------------------------------------------------
    # Extract the Subdirectory value. Return "" if not found.
    #----------------------------------------------------------------
    def __getSubdir(self, spec):
        for node in spec.childNodes:
            if node.nodeName == "Subdirectory":
                return cdr.getTextContent(node)

        return ""

    #----------------------------------------------------------------
    # Find out which document types the user can list individual
    # documents for.  Return None if this subset doesn't allow
    # listing of individual documents.  Return an empty list if
    # no document type restrictions are imposed on user-supplied
    # document ID lists for this subset.  Otherwise, return a
    # list of document type names.
    #----------------------------------------------------------------
    def __getAllowedDocTypes(self, node):
        for child in node.childNodes:
            if child.nodeName == "SubsetSelection":
                for grandchild in child.childNodes:
                    if grandchild.nodeName == "UserSelect":
                        docTypes = []
                        for dt_name in grandchild.childNodes:
                            if dt_name.nodeName == "UserSelectDoctype":
                                docTypes.append(cdr.getTextContent(dt_name))
                        return docTypes
        return None

    #----------------------------------------------------------------
    # Get the destination type. The type determines how to store the
    #    results: a single file for all documents, a single file
    #    for each document type, or a single file for each document.
    #----------------------------------------------------------------
    def __getDestinationType(self, options):
        if "DestinationType" in options:
            value = options["DestinationType"]
            if value   == "File"   : return Publish.FILE
            elif value == "DocType": return Publish.DOCTYPE
        return Publish.DOC

    #----------------------------------------------------------------
    # Get the destination file. A fileName for all documents.
    #----------------------------------------------------------------
    def __getDestinationFile(self, options):
        if "DestinationFileName" in options:
            return options["DestinationFileName"]
        else:
            return "PublicationOutput.xml"

    #----------------------------------------------------------------
    # Get the subset specifications node.
    #----------------------------------------------------------------
    def __getSpecs(self, subset):
        for node in subset.childNodes:
            if node.nodeName == "SubsetSpecifications":
                return node
        return None

    #----------------------------------------------------------------
    # Save the document in the temporary subdirectory.
    #----------------------------------------------------------------
    def __saveDoc(self, document, dir, fileName, mode = "w"):
        if not os.path.isdir(dir):
            # Create output subdirectory but don't create it twice
            self.__lockMakeDir.acquire(1)

            # Another thread might have created the directory while we
            #   waited for the lock
            # Or another thread might have renamed it to .FAILURE
            #   while we waited
            try:
                if not os.path.isdir(dir) and not self.__cleanupFailureCalled:
                    os.makedirs(dir)
            finally:
                self.__lockMakeDir.release()

        if not self.__cleanupFailureCalled:
            fileObj = open(dir + "/" + fileName, mode)
            fileObj.write(document)
            fileObj.close()

    #----------------------------------------------------------------
    # Handle process script, if one is specified, in which case
    # control is not returned to the caller.
    # XXX (RMK 2016-04-07) Why isn't the job status set?
    #----------------------------------------------------------------
    def __invokeProcessScript(self, subset):
        scriptName = ""
        for node in subset.childNodes:
            if node.nodeName == "ProcessScript":
                scriptName = cdr.getTextContent(node)
        if scriptName:
            if not os.path.isabs(scriptName):
                scriptName = cdr.BASEDIR + "/" + scriptName
            if not os.path.isfile(scriptName):
                msg = "Processing script '%s' not found" % scriptName
                raise Exception(msg)
            cmd = scriptName + " %d" % self.__jobId
            self.__debugLog("Publishing command '%s' invoked." % cmd)
            os.system(cmd)
            sys.exit(0)

    #----------------------------------------------------------------------
    # Set job status (with optional message) in pub_proc table.
    #----------------------------------------------------------------------
    def __updateStatus(self, status, message = None):

        # There is a question about whether updates are always committed
        # This may helf us find out
        autoCom = 'off'
        if self.__conn.getAutoCommit():
            autoCom = 'on'

        self.__debugLog("Updating job status to %s.  Autocommit is %s." % \
                        (status, autoCom))
        if message: self.__debugLog(message)

        date = "NULL"
        if status in (Publish.SUCCESS, Publish.FAILURE, Publish.VERIFYING):
            date = "GETDATE()"
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""
                SELECT messages
                  FROM pub_proc
                 WHERE id = %d
                           """ % self.__jobId
                          )
            row     = cursor.fetchone()
            message = (row and row[0] or '') + (message or '')

            # We only allow the status to be updated if it is not
            # already set to 'Success'.  This will prevent a push
            # job from setting the status to 'Verifying' if the
            # push job didn't actually send any data because the CDR
            # and Cancer.gov versions are identical.
            # ------------------------------------------------------
            cursor.execute("""
                UPDATE pub_proc
                   SET status    = ?,
                       messages  = ?,
                       completed = %s
                 WHERE id = ?
                   AND status != 'Success'""" % date,
                                               (status, message, self.__jobId))
        except cdrdb.Error, info:
            msg = 'Failure updating status: %s' % info[1][0]
            self.__debugLog(msg)
            raise Exception(msg)

    #----------------------------------------------------------------------
    # Update message in pub_proc table.
    #----------------------------------------------------------------------
    def __updateMessage(self, message):

        try:
            # Relies on DBMS for synchronization
            cursor = self.__conn.cursor()
            cursor.execute("""
                SELECT messages
                  FROM pub_proc
                 WHERE id = %d
                           """ % self.__jobId
                          )
            row     = cursor.fetchone()
            message = (row and row[0] or '') + message

            cursor.execute("""
                UPDATE pub_proc
                   SET messages  = ?
                 WHERE id        = ?""", (message, self.__jobId))
        except cdrdb.Error, info:
            msg = 'Failure updating message: %s' % info[1][0]
            self.__debugLog(msg)
            raise Exception(msg)

    # ---------------------------------------------------------------------
    # Create the mandatory CG Job Description for the automated Interim
    # jobs send to Gatekeeper.
    # *** This function is not needed anymore: VE, 2007-05-04. ***
    # ---------------------------------------------------------------------
    def __updateJobDescription(self):
        cgJobDescription = "<CgJobDesc>Automated updates<BR></CgJobDesc>"
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""
                SELECT messages
                  FROM pub_proc
                 WHERE id = %d""" % self.__jobId)
            row = cursor.fetchone()
            msg = (row and row[0] or '') + cgJobDescription

            cursor.execute("""
                UPDATE pub_proc
                   SET messages  = ?
                 WHERE id        = ?""", (msg, self.__jobId))
            self.__debugLog(
                 'Interim-Export updated pub_proc messages with "%s"'
                 %  msg)
        except cdrdb.Error, info:
            msg = 'Failure updating message: %s' % info[1][0]
            raise Exception(msg)


    #----------------------------------------------------------------------
    # Set output_dir to "" in pub_proc table.
    # This method is named incorrectly.  The value for output_dir is set
    # to an empty string instead of setting it to NULL. Big difference!!!
    #----------------------------------------------------------------------
    def __nullOutputDir(self):
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""
                UPDATE pub_proc
                   SET output_dir = ''
                 WHERE id = %d
                           """ % self.__jobId
                          )
        except cdrdb.Error, info:
            msg = 'Failure setting output_dir to "": %s' % info[1][0]
            raise Exception(msg)

    #----------------------------------------------------------------------
    # Get the number of failed documents in pub_proc_doc table.
    #----------------------------------------------------------------------
    def __getFailures(self):
        jbId = self.__jobId
        try:
            # Synchronization at database level should be sufficient.
            cursor = self.__conn.cursor()
            cursor.execute("""
                SELECT count(*)
                  FROM pub_proc_doc
                 WHERE (failure = 'Y' OR messages IS NOT NULL)
                   AND pub_proc = %d
                           """ % jbId
                          )
            row = cursor.fetchone()
            if row and row[0]:
                return row[0]
            else:
                return 0

        except cdrdb.Error, info:
            msg = 'Failure getting failed docs for job %d: %s' % (jbId,
                                                            info[1][0])
            self.__debugLog(msg)
            raise Exception(msg)

    #----------------------------------------------------------------------
    # Wait for user's approval to proceed.
    #----------------------------------------------------------------------
    def __waitUserApproval(self):

        # Put a stop there.
        status = Publish.WAIT
        now = time.ctime()
        msg =  "%s: Job is waiting for user's approval<BR>" % now
        msg += "%s: Change the publishing job status using the " % now
        msg += "menu item <BR>"
        msg += "%s: &nbsp;&nbsp;&nbsp;&nbsp;<I>Manage " % now
        msg += "Publishing Job Status</I><BR>"
        msg += "%s: from the Publishing Menu<BR>" % now
        self.__updateStatus(status, msg)
        msg += "<BR>"
        self.__sendMail()

        # Wait until user does something.
        while 1:
            try:
                cursor = self.__conn.cursor()
                cursor.execute("""
                    SELECT status
                      FROM pub_proc
                     WHERE id = %d
                               """ % self.__jobId,
                               timeout = self.__timeOut
                              )
                row = cursor.fetchone()
                if row and row[0]:
                    status = row[0]
                else:
                    msg = 'No status for job %d' % self.__jobId
                    raise Exception(msg)

            except cdrdb.Error, info:
                msg = 'Failure getting status for job %d: %s' % (
                            self.__jobId, info[1][0])
                raise Exception(msg)

            # Wait another 10 seconds.
            now = time.ctime()
            if status == Publish.WAIT:
                time.sleep(10)
            elif status == Publish.RUN:
                self.__updateMessage(
                    "<BR>%s: Job is resumed by user<BR>" % now)
                return
            elif status == Publish.FAILURE:
                raise Exception("%s: Job %d is killed by user<BR>"
                                % (now, self.__jobId))
            else:
                msg = "Unexpected status: %s for job %d.<BR>" \
                    % (status, self.__jobId)
                raise Exception(msg)

    #----------------------------------------------------------------------
    # Log debugging message to /cdr/log/publish.log
    #----------------------------------------------------------------------
    def __debugLog(self, line, tb=0):

        # Synchronize to avoid messages clobbering each other
        self.__lockLog.acquire(1)

        # Output, with optional traceback
        if LOG is not None:
            msg = "Job %d: %s" % (self.__jobId, line)
            if LOG == "":
                msg = "%s\n" % msg
                sys.stderr.write(msg)
            else:
                # open(LOG, "a").write(msg)
                cdr.logwrite(msg, LOG, tback=tb)

        # End critical section
        self.__lockLog.release()

    #----------------------------------------------------------------------
    # Check to see if any error thresholds specific to individual document
    # types have been exceeded. If so, log the problem and return True.
    #----------------------------------------------------------------------
    def __perDoctypeErrorThresholdExceeded(self):
        answer = False
        for doctype in self.__perDoctypeErrors:
            if doctype in self.__perDoctypeMaxErrors:
                allowed = self.__perDoctypeMaxErrors[doctype]
                actual = self.__perDoctypeErrors[doctype]
                if actual > allowed:
                    msg = ("%s %s failures allowed; %s encountered" %
                           (allowed, doctype, actual))
                    self.__debugLog(msg)
                    self.__updateMessage(msg + "<br>")
                    answer = True
        return answer

#-----------------------------------------------------------------------
# class: ErrObject
#    This class encapsulates the DTD validating errors.
#-----------------------------------------------------------------------
class ErrObject:
    def __init__(self, Warnings=None, Errors=None):
        self.Warnings  = Warnings or []
        self.Errors    = Errors or []

#-----------------------------------------------------------------------
# class: ErrHandler
#    This class encapsulates the error handler for XML parser.
#-----------------------------------------------------------------------
class ErrHandler:
    def __init__(self, loc):        self.locator = loc
    def set_locator(self, loc):     self.fulminator = loc
    def get_locator(self):          return self.locator
    def set_sysid(self, sysid):     self.__sysid = sysid
    def set_errobj(self, errObj):   self.__errObj = errObj
    def warning(self, msg):         self.__output("W:", msg)
    def error(self, msg):           self.__output("E:", msg)
    def fatal(self, msg):           self.__output("F:", msg)
    def __output(self, prefix, msg):
        where = self.locator.get_current_sysid()
        if where == 'Unknown': where = self.__sysid
        xmlString = self.locator.get_raw_construct()
        if prefix == "W:":
            self.__errObj.Warnings.append("%s:%d:%d: %s (%s)\n" % (where,
                                         self.locator.get_line(),
                                         self.locator.get_column(),
                                         msg,
                                         xmlString))
        else:
            self.__errObj.Errors.append("%s:%d:%d: %s (%s)\n" % (where,
                                         self.locator.get_line(),
                                         self.locator.get_column(),
                                         msg,
                                         xmlString))


#----------------------------------------------------------------------
# Validate a given document against its DTD.
#----------------------------------------------------------------------
def validateDoc(filteredDoc, docId = 0, dtd = cdr.DEFAULT_DTD):

    # Loading the DTD file
    fp     = open(dtd)
    pdqDtd = lxml.etree.DTD(fp)
    fp.close()

    # Load and validate the document passed to us
    docXml = lxml.etree.XML(filteredDoc)
    pdqDtd.validate(docXml)

    # Extract any validation errors from the error object
    errLog = pdqDtd.error_log.filter_from_errors()

    return errLog

#----------------------------------------------------------------------
# Find all the linked documents for a given hash of docId/docVer pairs.
# Return a message and a hash of docId/docVer pairs excluding the input.
# Make this public in case that other programs can easily call it.
#----------------------------------------------------------------------
def findLinkedDocs(docPairList):

    try:
        msg = "%s: Starting building link_net hash<BR>" % time.ctime()
        conn = cdrdb.connect('CdrGuest')
        cursor = conn.cursor()
        cursor.execute("""
                SELECT DISTINCT ln.source_doc,
                                ln.target_doc,
                                MAX(v.num)
                           FROM link_net ln
                           JOIN doc_version v
                             ON v.id = ln.target_doc
                           JOIN document d
                             ON d.id = v.id
                           JOIN doc_type t
                             ON t.id = v.doc_type
                          WHERE t.name <> 'Citation'
                            AND v.val_status = 'V'
                            AND v.publishable = 'Y'
                       GROUP BY ln.target_doc,
                                ln.source_doc
                       """)

        # Build a hash in memory that reflects link_net.
        row = cursor.fetchone()
        links = {}
        nRows = 0
        while row:
            if not links.has_key(row[0]):
                links[row[0]] = []
            links[row[0]].append(row[1:])
            row = cursor.fetchone()
            nRows += 1
        msg += "%s: Finishing link_net hash for %d linking docs<BR>" % (
                                                             time.ctime(),
                                                             len(links))

        # Seed the hash with documents passed in.
        linkedDocHash = {}
        for key in docPairList.keys():
            linkedDocHash[key] = docPairList[key]

        # Find all linked docs recursively.
        done = 0
        passNum = 0
        while not done:
            done = 1
            passNum += 1
            docIds = linkedDocHash.keys()
            for docId in docIds:
                if links.has_key(docId):
                    for targetPair in links[docId]:
                        if not linkedDocHash.has_key(targetPair[0]):
                            linkedDocHash[targetPair[0]] = targetPair[1]
                            done = 0

        # Delete all elements in the input hash.
        for key in docPairList.keys():
            del linkedDocHash[key]

        msg += "%s: Found all %d linked documents in %d passes<BR>" % (
                                                          time.ctime(),
                                                          len(linkedDocHash),
                                                          passNum)

        # Return what we have got.
        return [msg, linkedDocHash]

    except Exception, e:
        raise Exception("Failure finding linked docs: %s<BR>" % e)

#----------------------------------------------------------------------
# Test driver.
#----------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write("usage: cdrpub.py job-id {--debug}\n")
        sys.exit(1)
    if len(sys.argv) > 2 and sys.argv[2] == "--debug":
        cdr2gk.DEBUGLEVEL = 1
    LOG = ""
    p = Publish(int(sys.argv[1]))
    p.publish()
