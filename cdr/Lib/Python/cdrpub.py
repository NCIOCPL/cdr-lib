#----------------------------------------------------------------------
#
# $Id: cdrpub.py,v 1.54 2003-07-10 22:27:45 ameyer Exp $
#
# Module used by CDR Publishing daemon to process queued publishing jobs.
#
# $Log: not supported by cvs2svn $
# Revision 1.53  2003/04/02 22:16:50  pzhang
# Added feature "set:" for Filter Set enhancement.
#
# Revision 1.52  2003/03/05 17:36:13  pzhang
# Updated __addDocMessages() and __checkProblems to have both
# error and warning recorded in messages column.
#
# Revision 1.51  2003/03/05 16:19:58  pzhang
# Counted non-null messages as a failure. It could be set due to
# XSLT message instruction with terminate='no'.
#
# Revision 1.50  2003/02/14 20:10:13  pzhang
# Dropped document type and added job description at prolog level.
#
# Revision 1.49  2003/01/29 21:52:09  pzhang
# Fixed the bug in creating the pushing job. The new logic is:
#     1) Vendor job will create a pushing job and then finish
#        itself without waiting for the pushing job.
#     2) Pushing job will be picked up by CdrPublishing service
#        as a separate new job after it was created by the vendor
#        job.
# Dropped all newJobId parameters in functions due to the new
#     logic above.
#
# Revision 1.48  2003/01/28 20:37:25  pzhang
# Added comment on documents with filter failures.
# These documents are not regarded as "removed" or "blocked".
#
# Revision 1.47  2003/01/24 22:03:44  pzhang
# Added code to make sure that distinct rows are inserted
# into pub_proc_cg_work table.
#
# Revision 1.46  2003/01/23 20:37:33  pzhang
# Added code to generate DateFirstPublished element
# in a vendor document when it is first published.
#
# Revision 1.45  2003/01/13 17:56:48  pzhang
# Fixed code for Hotfix-Export based on code review.
#
# Revision 1.44  2003/01/08 22:36:15  pzhang
# Revised the code in the following areas:
#  1) Fixed SQL in __updateFromPPCW() with Bob's queries.
#  2) Used fetchone() instead of fetchall().
#  3) Set output_dir for pushing job to None.
#  4) Used cg_job instead of vendor_job in pub_proc_cg.
#  5) Added parameter newJobId into __debugLog().
#
# Revision 1.43  2002/12/20 23:27:55  pzhang
# Updated pub_proc_doc with cg_job, not vendor_job after pushing is done.
#
# Revision 1.42  2002/12/05 14:38:51  pzhang
# Added timeOut for update PPCW.
#
# Revision 1.41  2002/12/03 21:10:54  pzhang
# Added a fewer words to Check PushedDocs link.
#
# Revision 1.40  2002/11/20 16:38:20  pzhang
# Fixed a bug in missing updating pub_proc from PPCW to PPC.
# Added more try blocks to detect why Setting A to PPCW failed.
#
# Revision 1.39  2002/11/14 20:21:46  pzhang
# Added version infor for CG team.
#
# Revision 1.38  2002/11/07 23:12:58  pzhang
# Changed default values for subset parameters.
#
# Revision 1.37  2002/11/06 21:56:40  pzhang
# Allowed semicolon as an email address separator.
#
# Revision 1.36  2002/11/05 16:05:51  pzhang
# Enhanced interface per Eileen's input.
#
# Revision 1.35  2002/11/01 19:16:43  pzhang
# Used binary read and utf-8 instead of latin-1.
#
# Revision 1.34  2002/10/11 20:43:59  pzhang
# Enhanced message display. Allowed validateDoc() call by non-primary job.
#
# Revision 1.33  2002/09/26 20:41:07  ameyer
# Caught SystemExit from __invokeProcessScript() to be sure that the
# exit actually takes place and isn't blocked by the catchall "except"
# clause at the end of publish().
#
# Revision 1.32  2002/09/17 21:17:00  pzhang
# Added __updateFirstPub().
#
# Revision 1.31  2002/09/11 20:47:09  pzhang
# Added a global __timeOut variable.
# Encoded file to unicode after it is read from file system.
# Added __getSubsetDocTypes() to support publishing individual doc type.
# Updated Hotfix-Export code so that it will compare files for new and
#     updated documents as in Export.
#
# Revision 1.30  2002/09/06 21:49:20  pzhang
# Added __canPush().
# Added timeout parameter to cursor.execute().
#
# Revision 1.29  2002/09/05 16:26:01  pzhang
# Changed default value of __interactiveMode to 0.
#
# Revision 1.28  2002/09/05 14:41:45  pzhang
# Added port parameter to cdr.py function calls.
# Made __reportOnly default to 0 so that pushing job will have value 0.
#
# Revision 1.27  2002/09/03 21:54:13  pzhang
# Changed default values from 'No' to 'Yes'.
# Enabled output_dir for ReportOnly.
#
# Revision 1.26  2002/08/30 20:14:01  pzhang
# Fixed a couple of minor bugs.
#
# Revision 1.25  2002/08/30 19:40:46  pzhang
# Merged control parameter PushToCancerGov to ReportOnly.
# Put invalid filtered documents in InvalidDocs subdirectory.
#
# Revision 1.24  2002/08/20 22:07:31  pzhang
# Added many control parameters.
#
# Revision 1.23  2002/08/16 21:36:36  pzhang
# Added __waitUserApproval function.
#
# Revision 1.22  2002/08/16 20:19:30  pzhang
# Added new implementation for Hotfix-Export.
#
# Revision 1.21  2002/08/15 17:39:16  pzhang
# Added checking failure details link
#
# Revision 1.20  2002/08/13 23:07:50  pzhang
# Added more filtering time messages.
# Added getLastCgJob to match CG's definition of lastJob.
#
# Revision 1.19  2002/08/12 16:49:18  pzhang
# Added updateMessage function to help trace publishing process.
#
# Revision 1.18  2002/08/08 22:43:38  pzhang
# Added AbortOnError as a parameter so that user can change this value.
# Made comparison of xml and file work for updating documents.
#
# Revision 1.17  2002/08/08 17:00:07  pzhang
# Encoded xml value out of pub_proc_cg_work to UTF-8
# before sending it to CG.
#
# Revision 1.16  2002/08/08 15:18:21  pzhang
# Don't push vendor documents that are not good.
#
# Revision 1.15  2002/08/07 19:47:38  pzhang
# Added code to handle Subdirectory of SubsetSpecification.
#
# Revision 1.14  2002/08/07 14:41:49  pzhang
# Added features to push documents to Cancer.gov. It is far from the
# final version and contains many bugs. Save this version before
# the changes are lost or out of control.
#
# Revision 1.13  2002/08/01 15:52:26  pzhang
# Used socket to get HOST instead of hard-coded mmdb2.
# Added validateDoc module public method.
#
# Revision 1.12  2002/04/09 13:12:32  bkline
# Plugged in support for XQL queries.
#
# Revision 1.11  2002/04/04 18:31:43  bkline
# Fixed status value in query for pub_proc row; fixed query placeholder typo.
#
# Revision 1.10  2002/04/04 15:31:38  bkline
# Cleaned up some of the obsolete log entries (see CVS logs for full history).
#
# Revision 1.9  2002/04/04 15:24:06  bkline
# Rewrote module to match Mike's design spec more closely.  Split out
# CGI support to a separate module.
#
#----------------------------------------------------------------------

import cdr, cdrdb, os, re, string, sys, xml.dom.minidom
import socket, cdr2cg, time
from xml.parsers.xmlproc import xmlval, xmlproc

#-----------------------------------------------------------------------
# Value for controlling debugging output.  None means no debugging
# output is generated.  An empty string means debugging output is
# written to the standard error file.  Any other string is used as
# the pathname of the logfile to which to write debugging output.
#-----------------------------------------------------------------------
LOG = "d:/cdr/log/publish.log"

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

    # Output flavors.
    FILE       = 4
    DOCTYPE    = 5
    DOC        = 6

    # class private variables.
    __timeOut  = 3000
    __cdrEmail = "cdr@%s.nci.nih.gov" % socket.gethostname()
    __pd2cg    = "Push_Documents_To_Cancer.Gov"
    __cdrHttp  = "http://%s.nci.nih.gov/cgi-bin/cdr" % socket.gethostname()
    __interactiveMode = 0
    __checkPushedDocs  = 0
    __includeLinkedDocs = 0
    __reportOnly        = 0
    __validateDocs      = 0

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
                   u.name,
                   u.password
              FROM pub_proc p
              JOIN usr u
                ON u.id     = p.usr
             WHERE p.id     = ?
               AND p.status = ?"""
        try:
            cursor.execute(sql, (self.__jobId, Publish.START))
            row = cursor.fetchone()
            if not row:
                msg = "Unable to retrieve information for job %d" % self.__jobId
                self.__debugLog(msg)
                raise StandardError(msg)
        except cdrdb.Error, info:
            msg = "Database failure retrieving information for job %d: %s" % \
                (self.__jobId, info[1][0])
            self.__debugLog(msg)
            raise StandardError(msg)

        self.__ctrlDocId   = row[0]
        self.__subsetName  = row[1]
        self.__userId      = row[2]
        self.__outputDir   = row[3]
        self.__email       = row[4]
        self.__jobTime     = row[5]
        self.__no_output   = row[6]
        self.__userName    = row[7]
        self.__passWord    = row[8]
        self.__credentials = cdr.login(self.__userName,
                                       self.__passWord,
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
                self.__userDocList.append((row[0], row[1], row[2]))
                row = cursor.fetchone()
        except cdrdb.Error, info:
            msg = 'Failure retrieving documents for job %d: %s' % \
                  (self.__jobId, info[1][0])
            self.__updateStatus(Publish.FAILURE, msg)
            raise StandardError(msg)

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
                self.__params[row[0]] = row[1]
                self.__debugLog("Parameter %s='%s'." % (row[0], row[1]))
                row = cursor.fetchone()
        except cdrdb.Error, info:
            msg = 'Failure retrieving parameters for job %d: %s' % \
                  (self.__jobId, info[1][0])
            self.__updateStatus(Publish.FAILURE, msg)
            raise StandardError(msg)


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
            msg = 'Failure building hash __dateFirstPub for job %d: %s' % \
                  (self.__jobId, info[1][0])
            self.__updateStatus(Publish.FAILURE, msg)
            raise StandardError(msg)

        # Reset some class private variables based on user input.
        if self.__params.has_key("IncludeLinkedDocs"):
            self.__includeLinkedDocs = \
                self.__params["IncludeLinkedDocs"] == "Yes"
        if self.__params.has_key("InteractiveMode"):
            self.__interactiveMode = \
                self.__params["InteractiveMode"] == "Yes"
        if self.__params.has_key("CheckPushedDocs"):
            self.__checkPushedDocs = \
                self.__params["CheckPushedDocs"] == "Yes"
        if self.__params.has_key("ReportOnly"):
            self.__reportOnly = \
                self.__params["ReportOnly"] == "Yes"
        if self.__params.has_key("ValidateDocs"):
            self.__validateDocs = \
                self.__params["ValidateDocs"] == "Yes"

    #---------------------------------------------------------------
    # This is the major public entry point to publishing.
    #---------------------------------------------------------------
    def publish(self):

        try:

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

            # Get the destination directory.
            dest_base = self.__outputDir
            dest = dest_base + ".InProcess"

            # Get the subset specifications node.
            self.__specs = self.__getSpecs(subset)

            # Get the name-value pairs of options.  Error handling set here.
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
            if self.__sysName == "Primary" and \
                self.__subsetName == "Hotfix-Export":
                if self.__includeLinkedDocs:
                    self.__addLinkedDocsToPPD()

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
            self.__alreadyPublished = {}
            specFilters             = []
            specSubdirs             = []
            userListedDocsRemaining = len(self.__userDocList)
            self.__debugLog("Processing user-listed documents.")

            # Number of documents have been filtered and validated.
            numDocs = 0
            self.__updateMessage(
                "Start filtering/validating at %s.<BR>" % time.ctime())

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
                        if doc[0] in self.__alreadyPublished:
                            continue
                        if docTypesAllowed and doc[2] not in docTypesAllowed:
                            continue

                        # Don't want to use subdir for user-listed docs
                        # due to complexity of specifying the multiple
                        # subdirs when calling cdr.publish(). This may
                        # change in release XX.
                        self.__publishDoc(doc, filters, destType, dest)
                        numDocs += 1
                        if numDocs % 1000 == 0:
                            self.__updateMessage(
                                "Filtered/validated %d docs at %s.<BR>" % (
                                            numDocs, time.ctime()))
                            numFailures = self.__getFailures()
                            self.__updateMessage("""%d docs failed so
                                far.<BR>""" % numFailures)

                        self.__alreadyPublished[doc[0]] = 1
                        userListedDocsRemaining -= 1
                        if not userListedDocsRemaining: break

            # Make sure all the user-listed documents are accounted for.
            for doc in self.__userDocList:
                if doc[0] not in self.__alreadyPublished:
                    self.__checkProblems(doc,
                                "User-specified document CDR%010d "
                                "has document type %s which is "
                                "not allowed for this publication "
                                "type" % (doc[0], doc[2]), "")

            # Now walk through the specifications again executing queries.
            self.__debugLog("Processing document-selection queries.")
            i = 0
            for spec in self.__specs.childNodes:

                if spec.nodeName == "SubsetSpecification":

                    for specChild in spec.childNodes:
                        if specChild.nodeName == "SubsetSelection":
                            docs = self.__selectQueryDocs(specChild)
                            for doc in docs:
                                self.__publishDoc(doc, specFilters[i],
                                      destType, dest,
                                      Publish.STORE_ROW_IN_PUB_PROC_DOC_TABLE,
                                      specSubdirs[i])
                                numDocs += 1
                                if numDocs % 1000 == 0:
                                    numFailures = self.__getFailures()
                                    self.__updateMessage("""Filtered/validated
                                        %d docs at %s, and %d docs failed so
                                        far.<BR>""" % (numDocs, time.ctime(),
                                                       numFailures))
                    i += 1

            self.__updateMessage("""Finish filtering/validating all %d docs
                                    at %s.<BR>""" % (numDocs, time.ctime()))
            numFailures = self.__getFailures()
            if numFailures > 0:
                msg = """Total of %d docs failed.
                    <A style='text-decoration: underline;'
                    href="%s/PubStatus.py?id=%d&type=FilterFailure">Check
                    the failure details.</A><BR>""" % (numFailures,
                    self.__cdrHttp, self.__jobId)
                self.__updateMessage(msg)
            else:
                self.__updateMessage("Total of 0 docs failed.<BR>")

            if self.__publishIfWarnings == "Ask" and self.__warningCount:
                self.__updateStatus(Publish.WAIT, "Warnings encountered")

            # Rename the output directory from its working name.
            # Create a pushing job if it is a vendor job; or push
            # filtered documents to CG if it is a cg job.
            else:
                # Pushing job could have "Message only" checked in theory.
                lenPd2Cg = len(self.__pd2cg)
                if self.__no_output != "Y" or \
                    (self.__subsetName)[0:lenPd2Cg] == self.__pd2cg:

                    try:
                        os.rename(dest, dest_base)
                    except:
                        pass

                    if self.__sysName != "Primary" or \
                        self.__reportOnly:
                        self.__updateStatus(Publish.SUCCESS)
                    elif not self.__canPush():
                        # It is a failure for the separately started pushing
                        # job, not for the vendor job.
                        if (self.__subsetName)[0:lenPd2Cg] == self.__pd2cg:
                            self.__updateStatus(Publish.FAILURE)
                        else:
                            self.__updateStatus(Publish.SUCCESS)

                    # Push docs or create a pushing job.
                    else:

                        # It is a pushing job.
                        if (self.__subsetName)[0:lenPd2Cg] == self.__pd2cg:

                            # Make sure output_dir is reset to "" for pushing
                            # jobs, no matter whether user has forgot to check
                            # the "Message only" box.
                            self.__nullOutputDir()

                            # Get the vendor_job and vendor_dest from
                            # the appropriate subset.
                            vendorInfo    = self.__findVendorData()
                            vendor_job    = vendorInfo[0]
                            vendor_dest   = vendorInfo[1]

                            if not vendor_job:
                                self.__updateStatus(Publish.FAILURE,
                                    "No enough vendor info found.<BR>")
                            else:

                                # A long pushing job of many hours starts!
                                self.__pushDocsToCG(vendor_job, vendor_dest)

                                # There is no exception thrown.
                                self.__updateStatus(Publish.SUCCESS)

                        # It is a vendor job. Create a pushing job and let
                        # it run in its own way.
                        else:
                            self.__updateStatus(Publish.SUCCESS)

                            pushSubsetName = "%s_%s" % (self.__pd2cg,
                                                self.__subsetName)
                            msg = ""
                            resp = cdr.publish(self.__credentials,
                                "Primary",
                                pushSubsetName,
                                email = self.__email,
                                noOutput = 'Y',
                                port = self.__pubPort)
                            if not resp[0]:
                                msg += "<B>Failed:</B> %s\n" % resp[1]
                                msg += """<BR>Please run job %s
                                    separately.<BR>""" % pushSubsetName
                            else:
                                msg += """Pushing filtered documents to
                                    Cancer.gov is in progress with job
                                    %s. You will receive a second email
                                    when it is done.<BR>""" % resp[0]

                            self.__updateMessage(msg)
                else:
                    self.__updateStatus(Publish.SUCCESS)

        except SystemExit:
            # __invokeProcessScript() can exit, which raises SystemExit
            # Since we catch all exceptions below, we would catch this one
            #   in the general Except clause and report an
            #   "Unexpected failure", which we don't want to do.
            sys.exit(0)

        except StandardError, arg:
            self.__updateStatus(Publish.FAILURE, arg[0])
            if self.__no_output != "Y":
                try:
                    os.rename(dest, dest_base + ".FAILURE")
                except:
                    pass
        except:
            self.__updateStatus(Publish.FAILURE, "Unexpected failure")
            cdr.logwrite ("Unhandled exception in publish:", LOG, tback=1)
            if self.__no_output != "Y":
                try:
                    os.rename(dest, dest_base + ".FAILURE")
                except:
                    pass

        # Send email to notify user of job status.
        if self.__reportOnly:
            self.__updateStatus(Publish.FAILURE, """The job status is
                set to Failure because it was running for pre-publishing
                reports.<BR>""")

        # Update first_pub in all_docs table.
        self.__updateFirstPub()

        self.__sendMail()

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
                "<BR>Updated first_pub for %d documents.<BR>" % rowsAffected)
        except cdrdb.Error, info:
            self.__updateMessage("Failure updating first_pub for job %d: %s" \
                % (self.__jobId, info[1][0]))

    #------------------------------------------------------------------
    # Allow only one pushing job to run.
    # Return 0 if there is a pending pushing job.
    #------------------------------------------------------------------
    def __canPush(self):
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
                msg = """Pushing job %d is pending. Please push again
                         later.<BR>""" % row[0]
                self.__updateMessage(msg)
                return 0

        except cdrdb.Error, info:
            raise StandardError("""Failure finding pending pushing jobs
                        for job %d: %s""" % (self.__jobId, info[1][0]))
        return 1

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
                    raise StandardError(
                        "Corresponding vendor job does not exist.<BR>")

                id   = row[0]
                dest = row[1]

                prevId = self.__getLastJobId(subsetName)
                if prevId > id:
                    raise StandardError("""This same job has been previously
                        successfully done by job %d.""" % prevId)

                return [id, dest]

            except cdrdb.Error, info:
                raise StandardError("""Failure finding vendor job and vendor
                        destination for job %d: %s""" % (self.__jobId,
                                                         info[1][0]))

    #------------------------------------------------------------------
    # Push documents of a specific vendor_job to Cancer.gov using cdr2cg
    # module.
    # We handle different pubTypes with different functions for clarity.
    # Raise a standard error when failed.
    #------------------------------------------------------------------
    def __pushDocsToCG(self, vendor_job, vendor_dest):

        # Get the value of pubType for this cg_job.
        if self.__params.has_key('PubType'):
            pubType = self.__params['PubType']
            if not cdr2cg.PUBTYPES.has_key(pubType):
                msg = """The value of parameter PubType, %s, is unsupported.
                       <BR>Please modify the control document or the source
                       code.<BR>""" % pubType
                raise StandardError(msg)
        else:
            msg = "There is no parameter PubType in the control document.<BR>"
            raise StandardError(msg)

        try:
            cursor = self.__conn.cursor()

            # If pubType is "Full Load", clean up pub_proc_cg table.
            if pubType == "Full Load":
                msg = "Deleting pub_proc_cg at %s.<BR>" % time.ctime()
                self.__updateMessage(msg)
                try: cursor.execute("DELETE pub_proc_cg",
                                    timeout = self.__timeOut)
                except cdrdb.Error, info:
                    msg = "Deleting pub_proc_cg failed: %s<BR>" % info[1][0]
                    raise StandardError(msg)

            # Create a working table pub_proc_cg_work to hold information
            # on transactions to Cancer.gov.
            msg = "Creating pub_proc_cg_work at %s.<BR>" % time.ctime()
            self.__updateMessage(msg)
            cgWorkLink = self.__cdrHttp + "/PubStatus.py?id=1&type=CgWork"
            link = "<A style='text-decoration: underline;' href='%s'> \
                Check pushed docs</A>(<B>inaccurate after next pushing \
                job has started or this job succeeds</B>).<BR>" % cgWorkLink
            if pubType == "Full Load" or pubType == "Export":
                self.__createWorkPPC(vendor_job, vendor_dest)
                pubTypeCG = pubType
                self.__updateMessage(link)

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
                raise StandardError("pubType %s not supported." % pubType)

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
                msg = "No documents to be pushed to Cancer.gov.<BR>"
                self.__updateStatus(Publish.SUCCESS, msg)
                return

            # Get last successful cg_jobId. GateKeeper does not
            # care which subset it belongs to.
            # Returns 0 if there is no previous success.
            # Raise an exception when failed.
            lastJobId = self.__getLastCgJob()

            # Get the required job description.
            cgJobDesc = self.__getCgJobDesc()
            if not cgJobDesc:
                self.__updateMessage(msg)
                raise StandardError("<BR>Missing required job description.")

            # See if the GateKeeper is awake.
            msg = "Initiating request with pubType=%s, \
                   lastJobId=%d ...<BR>" % (pubTypeCG, lastJobId)
            response = cdr2cg.initiateRequest(cgJobDesc, pubTypeCG, lastJobId)
            if response.type != "OK":
                msg += "%s: %s<BR>" % (response.type, response.message)
                if response.fault:
                    msg += "%s: %s<BR>" % (response.fault.faultcode,
                                           response.fault.faultstring)
                    raise StandardError(msg)
                # Keep sending documents in this case and contact
                # CG for detail. This is useful in testing pushes
                # where a mismatched lostJobId is often expected.
                elif response.details:
                    lastJobId = response.details.lastJobId
                    msg += "Last job ID from server: %d<BR>" % lastJobId

            # Prepare the server for a list of documents to send.
            msg += """Sending data prolog with jobId=%d, pubType=%s,
                    lastJobId=%d, numDocs=%d ...<BR>""" % (self.__jobId,
                    pubTypeCG, lastJobId, numDocs)
            response = cdr2cg.sendDataProlog(cgJobDesc, self.__jobId,
                                             pubTypeCG, lastJobId, numDocs)
            if response.type != "OK":
                msg += "%s: %s<BR>" % (response.type, response.message)
                raise StandardError(msg)

            msg += "Pushing documents starts at %s.<BR>" % time.ctime()
            self.__updateMessage(msg)
            msg = ""

            # Send all new and updated documents.
            addCount = 0
            XmlDeclLine = re.compile("<\?xml.*?\?>\s*", re.DOTALL)
            DocTypeLine = re.compile("<!DOCTYPE.*?>\s*", re.DOTALL)
            cursor.execute ("""
                SELECT id, num, doc_type, xml
                  FROM pub_proc_cg_work
                 WHERE NOT xml IS NULL
                            """, timeout = self.__timeOut)
            row = cursor.fetchone()
            while row:
                id      = row[0]
                version = row[1]
                docType = row[2]
                if docType == "InScopeProtocol":
                    docType = "Protocol"
                xml = row[3].encode('utf-8')
                xml = XmlDeclLine.sub("", xml)
                xml = DocTypeLine.sub("", xml)

                response = cdr2cg.sendDocument(self.__jobId, docNum,
                            "Export", docType, id, version, xml)
                if response.type != "OK":
                    msg += "sending document %d failed. %s: %s<BR>" % \
                            (id, response.type, response.message)
                    raise StandardError(msg)
                docNum  = docNum + 1
                if docNum % 1000 == 0:
                    msg += "Pushed %d documents at %s.<BR>" % (docNum,
                                                               time.ctime())
                    self.__updateMessage(msg)
                    msg = ""
                addCount += 1
                row = cursor.fetchone()
            msg += "%d documents pushed to Cancer.gov.<BR>" % addCount
            self.__updateMessage(msg)
            msg = ""

            # Remove all the removed documents.
            cursor.execute ("""
                SELECT id, num, doc_type
                  FROM pub_proc_cg_work
                 WHERE xml IS NULL
                            """)
            rows = cursor.fetchall()
            for row in rows:
                id        = row[0]
                version   = row[1]
                docType   = row[2]
                if docType == "InScopeProtocol":
                    docType = "Protocol"
                response = cdr2cg.sendDocument(self.__jobId, docNum, "Remove",
                                               docType, id, version)
                if response.type != "OK":
                    msg += "deleting document %d failed. %s: %s<BR>" % (id,
                            response.type, response.message)
                    raise StandardError(msg)
                docNum  = docNum + 1
                if docNum % 1000 == 0:
                    msg += "Pushed %d documents at %s.<BR>" % (docNum,
                            time.ctime())
                    self.__updateMessage(msg)
                    msg = ""
            msg += "%d documents removed from Cancer.gov.<BR>" % len(rows)
            msg += "Pushing done at %s.<BR>" % time.ctime()
            self.__updateMessage(msg)
            msg = ""

            # Before we claim success, we will have to update
            # pub_proc_cg and pub_proc_doc from pub_proc_cg_work.
            # These transactions must succeed! Failure will cause
            # a mismatch between PPC/D and Cancer.gov database.
            if pubType == "Full Load" or pubType == "Export":
                self.__updateFromPPCW()
            elif pubType == "Hotfix (Remove)":
                self.__updateFromPPCWHR()
            elif pubType == "Hotfix (Export)":
                self.__updateFromPPCWHE()
            else:
                raise StandardError("pubType %s not supported." % pubType)

            msg += "Updating PPC/PPD tables done at %s.<BR>" % time.ctime()
            self.__updateMessage(msg)
            msg = ""

        except cdrdb.Error, info:
            msg = "__pushDocsToCG() failed: %s<BR>" % info[1][0]
            raise StandardError(msg)
        except StandardError, arg:
            raise StandardError(arg[0])
        except:
            msg = "Unexpected failure in __pushDocsToCG.<BR>"
            raise StandardError(msg)

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

        # Wipe out all rows in pub_proc_cg_work. Only one job can be run
        # for Cancer.gov transaction. This is guaranteed by calling
        # __canPush().
        try:
            cursor.execute("""
                DELETE pub_proc_cg_work
                           """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            raise StandardError(
                "Deleting pub_proc_cg_work failed: %s<BR>" % info[1][0])
        msg = "Finished deleting pub_proc_cg_work at %s.<BR>" % time.ctime()
        self.__updateMessage(msg)

        # Insert updated documents into pub_proc_cg_work. Updated documents
        # are those in both pub_proc_cg and pub_proc_doc belonging to this
        # vendor_job. This is slow. We compare the XML document content to
        # see if it needs updating. If needed, we insert a row into
        # pub_proc_cg_work with xml set to the new document.
        try:
            qry = """
                SELECT ppc.id, t.name, ppc.xml, ppd2.subdir, ppd2.doc_version
                  FROM pub_proc_cg ppc, doc_type t, document d,
                       pub_proc_doc ppd2
                 WHERE d.id = ppc.id
                   AND d.doc_type = t.id
                   AND ppd2.doc_id = d.id
                   AND ppd2.pub_proc = %d
                   AND EXISTS (
                           SELECT *
                             FROM pub_proc_doc ppd
                            WHERE ppd.doc_id = ppc.id
                              AND ppd.pub_proc = %d
                              AND ppd.failure IS NULL
                              )
                  """ % (vendor_job, vendor_job)
            cursor.execute(qry, timeout = self.__timeOut)
            row = cursor.fetchone()
            idsInserted = {}

            # Regexp for normalizing whitespace for compares
            spNorm = re.compile (ur"\s\s+")

            # Fetch each doc
            while row:
                id     = row[0]
                if idsInserted.has_key(id):
                    row = cursor.fetchone()
                    continue
                idsInserted[id] = 1
                type   = row[1]
                xml    = row[2]
                subdir = row[3]
                ver    = row[4]
                path   = "%s/%s/CDR%d.xml" % (vendor_dest, subdir, id)
                file   = open(path, "rb").read()
                file   = unicode(file, 'utf-8')

                # Whitespace-normalized compare to stored file
                if spNorm.sub(u" ", xml) != spNorm.sub(u" ", file):
                    # New xml is different, save it for cancer.gov
                    cursor2.execute("""
                        INSERT INTO pub_proc_cg_work (id, vendor_job,
                                        cg_job, doc_type, xml, num)
                             VALUES (?, ?, ?, ?, ?, ?)
                                    """, (id, vendor_job, cg_job, type,
                                          file, ver),
                                         timeout = self.__timeOut
                                   )

                row = cursor.fetchone()

        except cdrdb.Error, info:
            raise StandardError(
                "Setting U to pub_proc_cg_work failed: %s<BR>" % info[1][0])
        except:
            raise StandardError(
                "Unexpected failure in setting U to pub_proc_cg_work.")
        msg = "Finished insertion for updating at %s.<BR>" % time.ctime()
        self.__updateMessage(msg)

        # Insert new documents into pub_proc_cg_work. New documents are
        # those in pub_proc_doc belonging to vendor_job, but not in
        # pub_proc_cg.
        try:
            cursor.execute ("""
                     SELECT DISTINCT ppd.doc_id, t.name, ppd.subdir,
                            ppd.doc_version
                       FROM pub_proc_doc ppd, doc_type t, document d
                      WHERE ppd.pub_proc = %d
                        AND d.id = ppd.doc_id
                        AND d.doc_type = t.id
                        AND ppd.failure IS NULL
                        AND NOT EXISTS (
                                SELECT *
                                  FROM pub_proc_cg ppc
                                 WHERE ppc.id = ppd.doc_id
                                       )
                            """ % vendor_job, timeout = self.__timeOut
                           )
            row = cursor.fetchone()
            while row:
                id     = row[0]
                type   = row[1]
                subdir = row[2]
                ver    = row[3]
                path   = "%s/%s/CDR%d.xml" % (vendor_dest, subdir, id)
                xml    = open(path, "rb").read()
                xml    = unicode(xml, 'utf-8')
                try:
                    cursor2.execute("""
                    INSERT INTO pub_proc_cg_work (id, vendor_job, cg_job,
                                                  doc_type, xml, num)
                         VALUES (?, ?, ?, ?, ?, ?)
                                    """,
                                    (id, vendor_job, cg_job, type, xml, ver),
                                    timeout = self.__timeOut
                                   )
                except:
                    raise StandardError(
                        "Inserting CDR%d to PPCW failed." % id)

                row = cursor.fetchone()

        except cdrdb.Error, info:
            raise StandardError(
                "Setting A to pub_proc_cg_work failed: %s<BR>" % info[1][0])
        except StandardError, arg:
            raise StandardError(arg[0])
        except:
            raise StandardError(
                "Unexpected failure in setting A to pub_proc_cg_work.")
        msg = "Finished insertion for adding at %s.<BR>" % time.ctime()
        self.__updateMessage(msg)

        # Insert removed documents into pub_proc_cg_work.
        # Removed documents are those in pub_proc_cg, but not in
        # pub_proc_doc belonging to vendor_job. The document version number
        # is obtained from the cg_job in pub_proc_cg. If we want the most
        # recent version, we will either reconstruct the query or update
        # pub_proc column in pub_proc_cg for each job.
        # We don't regard filter failure as "removed or blocked", so
        # every document in pub_proc_doc belonging to this vendor_job
        # is considered as good although the failed ones were not used
        # for update comparison.

        # Removed documents must have a doc_type belonging to this
        # Export subset [e.g., Protocol, Summary, Term, etc.].
        # Subsets Export-Protocol and Export-Summary need this special
        # treatment.
        # Get a list of docType IDs such as "18, 19, 11".
        docTypes = self.__getSubsetDocTypes(vendor_job)

        try:
            qry = """
                INSERT INTO pub_proc_cg_work (id, num, vendor_job,
                                              cg_job, doc_type)
                     SELECT DISTINCT ppc.id, ppd_cg.doc_version,
                            %d, %d, t.name
                       FROM pub_proc_cg ppc, doc_type t, document d,
                            pub_proc_doc ppd_cg
                      WHERE d.id = ppc.id
                        AND d.doc_type = t.id
                        AND d.doc_type IN (%s)
                        AND ppd_cg.doc_id = ppc.id
                        AND ppd_cg.pub_proc = ppc.pub_proc
                        AND NOT EXISTS (
                                SELECT *
                                  FROM pub_proc_doc ppd
                                 WHERE ppd.doc_id = ppc.id
                                   AND ppd.pub_proc = %d
                                       )
                  """ % (vendor_job, cg_job, docTypes, vendor_job)
            cursor.execute(qry, timeout = self.__timeOut)
        except cdrdb.Error, info:
            raise StandardError(
                "Setting D to pub_proc_cg_work failed: %s<BR>" % info[1][0])
        msg = "Finished insertion for deleting at %s.<BR>" % time.ctime()
        self.__updateMessage(msg)

    #------------------------------------------------------------------
    # Return a string of doc type IDs to be used in query.
    #------------------------------------------------------------------
    def __getSubsetDocTypes(self, vendor_job):

        docTypes = ""

        try:
            cursor = self.__conn.cursor()
            cursor.execute("""
                SELECT DISTINCT d.doc_type
                  FROM document d, pub_proc_doc p
                 WHERE d.id = p.doc_id
                   AND p.pub_proc = %d
                           """ % vendor_job
                          )
            rows = cursor.fetchall()
            for row in rows:
                docTypes += "%d," % row[0]
            return docTypes[:-1]

        except cdrdb.Error, info:
            msg = "Failure executing query to find doc types " \
                  "for job %d: %s" % (vendor_job, info[1][0])
            raise StandardError(msg)

    #------------------------------------------------------------------
    # Different version of __createWorkPPC for Hotfix (Remove)
    #------------------------------------------------------------------
    def __createWorkPPCHR(self, vendor_job):

        cg_job = self.__jobId
        cursor = self.__conn.cursor()

        # Wipe out all rows in pub_proc_cg_work. Only one job can be run
        # for Cancer.gov transaction. This is guaranteed by calling
        # __canPush().
        try:
            cursor.execute("""
                DELETE pub_proc_cg_work
                           """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            raise StandardError(
                "Deleting pub_proc_cg_work failed: %s<BR>" % info[1][0])
        msg = "Finished deleting pub_proc_cg_work at %s.<BR>" % time.ctime()
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
                  """ % (vendor_job, cg_job, vendor_job)
            cursor.execute(qry, timeout = self.__timeOut)
        except cdrdb.Error, info:
            raise StandardError(
                "Setting D to pub_proc_cg_work failed: %s<BR>" % info[1][0])
        msg = "Finished inserting D to PPCW at %s.<BR>" % time.ctime()
        self.__updateMessage(msg)

    #------------------------------------------------------------------
    # Different version of __createWorkPPC for Hotfix (Export)
    #------------------------------------------------------------------
    def __createWorkPPCHE(self, vendor_job, vendor_dest):

        cg_job = self.__jobId
        cursor = self.__conn.cursor()
        cursor2 = self.__conn.cursor()

        # Wipe out all rows in pub_proc_cg_work. Only one job can be run
        # for Cancer.gov transaction. This is guaranteed by calling
        # __canPush().
        try:
            cursor.execute("""
                DELETE pub_proc_cg_work
                           """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            raise StandardError(
                "Deleting pub_proc_cg_work failed: %s<BR>" % info[1][0])

        # Insert updated documents into pub_proc_cg_work. Updated documents
        # are those that are in both pub_proc_cg and pub_proc_doc belonging
        # to this vendor_job. This is slow. We compare the XML document
        # content to see if it needs updating. If needed, we insert a row
        # into pub_proc_cg_work with xml set to the new document.
        try:
            qry = """
                SELECT ppc.id, t.name, ppc.xml, ppd2.subdir, ppd2.doc_version
                  FROM pub_proc_cg ppc, doc_type t, document d,
                       pub_proc_doc ppd2
                 WHERE d.id = ppc.id
                   AND d.doc_type = t.id
                   AND ppd2.doc_id = d.id
                   AND ppd2.pub_proc = %d
                   AND EXISTS (
                           SELECT *
                             FROM pub_proc_doc ppd
                            WHERE ppd.doc_id = ppc.id
                              AND ppd.pub_proc = %d
                              AND ppd.failure IS NULL
                              )
                  """ % (vendor_job, vendor_job)
            cursor.execute(qry, timeout = self.__timeOut)
            row = cursor.fetchone()
            idsInserted = {}
            while row:
                id     = row[0]
                if idsInserted.has_key(id):
                    row = cursor.fetchone()
                    continue
                idsInserted[id] = 1
                type   = row[1]
                xml    = row[2]
                subdir = row[3]
                ver    = row[4]
                path   = "%s/%s/CDR%d.xml" % (vendor_dest, subdir, id)
                file   = open(path, "rb").read()
                file   = unicode(file, 'utf-8')

                if xml != file:
                    cursor2.execute("""
                        INSERT INTO pub_proc_cg_work (id, vendor_job,
                                        cg_job, doc_type, xml, num)
                             VALUES (?, ?, ?, ?, ?, ?)
                                    """, (id, vendor_job, cg_job, type,
                                          file, ver)
                                   )

                row = cursor.fetchone()

        except cdrdb.Error, info:
            raise StandardError(
                "Setting U to pub_proc_cg_work failed: %s<BR>" % info[1][0])
        except:
            raise StandardError(
                "Unexpected failure in setting U to pub_proc_cg_work.")
        msg = "Finished insertion for updating at %s.<BR>" % time.ctime()
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
                        AND NOT EXISTS (
                                SELECT *
                                  FROM pub_proc_cg ppc
                                 WHERE ppc.id = ppd.doc_id
                                       )
                            """, (vendor_job), timeout = self.__timeOut
                           )
            row = cursor.fetchone()
            while row:
                id     = row[0]
                type   = row[1]
                subdir = row[2]
                ver    = row[3]
                path   = "%s/%s/CDR%d.xml" % (vendor_dest, subdir, id)
                xml    = open(path, "rb").read()
                xml    = unicode(xml, 'utf-8')
                cursor2.execute("""
                    INSERT INTO pub_proc_cg_work (id, vendor_job, cg_job,
                                                  doc_type, xml, num)
                         VALUES (?, ?, ?, ?, ?, ?)
                                """,
                                (id, vendor_job, cg_job, type, xml, ver),
                                timeout = self.__timeOut
                               )
                row = cursor.fetchone()

        except cdrdb.Error, info:
            raise StandardError(
                "Setting A to pub_proc_cg_work failed: %s<BR>" % info[1][0])
        except:
            raise StandardError(
                "Unexpected failure in setting A to pub_proc_cg_work.")
        msg = "Finished insertion for adding at %s.<BR>" % time.ctime()
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
            raise StandardError(
                "Deleting from pub_proc_cg_work failed: %s<BR>" % info[1][0])

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
            raise StandardError(
                "Inserting D into pub_proc_doc failed: %s<BR>" % info[1][0])

        # Update a document, if its id is in both PPC and PPD.
        # Update rows in PPD for updated documents of cg_job.
        try:
            cursor.execute ("""
                    UPDATE pub_proc_cg
                       SET xml = ppcw.xml, pub_proc = ppcw.cg_job
                      FROM pub_proc_cg ppc, pub_proc_cg_work ppcw
                     WHERE ppc.id = ppcw.id
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise StandardError(
                "Updating xml, vendor_job from PPCW to PPC failed: %s<BR>" % \
                info[1][0])

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
            raise StandardError(
                "Inserting U into pub_proc_doc failed: %s<BR>" % info[1][0])

        # Insert a document to both PPD and PPC.
        # Add new documents into PPD first.
        try:
            cursor.execute ("""
                INSERT INTO pub_proc_doc (doc_id, doc_version, pub_proc)
                     SELECT ppcw.id, ppcw.num, ppcw.cg_job
                       FROM pub_proc_cg_work ppcw
                      WHERE NOT ppcw.xml IS NULL
                        AND NOT EXISTS ( SELECT *
                                           FROM pub_proc_cg ppc
                                          WHERE ppc.id = ppcw.id
                                        )
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise StandardError(
                "Inserting A into pub_proc_doc failed: %s<BR>" % info[1][0])

        # Add new documents into PPC last.
        try:

            cursor.execute ("""
                INSERT INTO pub_proc_cg (id, pub_proc, xml)
                     SELECT ppcw.id, ppcw.cg_job, ppcw.xml
                       FROM pub_proc_cg_work ppcw
                      WHERE NOT ppcw.xml IS NULL
                        AND NOT EXISTS ( SELECT *
                                           FROM pub_proc_cg ppc
                                          WHERE ppc.id = ppcw.id
                                        )
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise StandardError(
                "Inserting A into pub_proc_cg failed: %s<BR>" % info[1][0])

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
            raise StandardError(
                "Deleting PPC from PPCW failed: %s<BR>" % info[1][0])

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
            raise StandardError(
                "Inserting D into pub_proc_doc failed: %s<BR>" % info[1][0])

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
                       SET xml = ppcw.xml, pub_proc = ppcw.cg_job
                      FROM pub_proc_cg ppc, pub_proc_cg_work ppcw
                     WHERE ppc.id = ppcw.id
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise StandardError(
                "Updating xml, vendor_job from PPCW to PPC failed: %s<BR>" % \
                info[1][0])

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
            raise StandardError(
                "Inserting U into pub_proc_doc failed: %s<BR>" % info[1][0])

        # Insert a document to both PPD and PPC.
        # Add new documents into PPD first.
        try:
            cursor.execute ("""
                INSERT INTO pub_proc_doc (doc_id, doc_version, pub_proc)
                     SELECT ppcw.id, ppcw.num, ppcw.cg_job
                       FROM pub_proc_cg_work ppcw
                      WHERE NOT ppcw.xml IS NULL
                        AND NOT EXISTS ( SELECT *
                                           FROM pub_proc_cg ppc
                                          WHERE ppc.id = ppcw.id
                                        )
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise StandardError(
                "Inserting A into pub_proc_doc failed: %s<BR>" % info[1][0])

        # Add new documents into PPC last.
        try:

            cursor.execute ("""
                INSERT INTO pub_proc_cg (id, pub_proc, xml)
                     SELECT ppcw.id, ppcw.cg_job, ppcw.xml
                       FROM pub_proc_cg_work ppcw
                      WHERE NOT ppcw.xml IS NULL
                        AND NOT EXISTS ( SELECT *
                                           FROM pub_proc_cg ppc
                                          WHERE ppc.id = ppcw.id
                                        )
                            """, timeout = self.__timeOut)
        except cdrdb.Error, info:
            self.__conn.setAutoCommit(1)
            raise StandardError(
                "Inserting A into pub_proc_cg failed: %s<BR>" % info[1][0])

        self.__conn.commit()
        self.__conn.setAutoCommit(1)

    #------------------------------------------------------------------
    # Return the last successful cg_job for this vendor_job subset.
    #------------------------------------------------------------------
    def __getLastJobId(self, subsetName):

        jobId = 0

        try:
            cursor = self.__conn.cursor()
            cursor.execute("""
                    SELECT MAX(pp.id)
                      FROM pub_proc pp, pub_proc_parm ppp
                     WHERE pp.status = ?
                       AND pp.pub_subset = ?
                       AND pp.pub_system = ?
                       AND ppp.pub_proc = pp.id
                       AND ppp.parm_name = 'SubSetName'
                       AND ppp.parm_value = ?
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
            raise StandardError(msg)

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
                savedJobDesc = "<B>JobDesc:</B> %s<BR>" % CgJobDesc
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
            raise StandardError(msg)

    #------------------------------------------------------------------
    # Return the last cg_job for any successful pushing jobs.
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
            raise StandardError(msg)

    #------------------------------------------------------------------
    # Update pub_proc_doc table and __userDocList with all linked
    # documents.
    #------------------------------------------------------------------
    def __addLinkedDocsToPPD(self):

        # Build the input hash.
        docPairList = {}
        for doc in self.__userDocList:
            docPairList[doc[0]] = doc[1]

        # Find all the linked documents.
        resp = findLinkedDocs(docPairList)
        msg  = resp[0]
        hash = resp[1]
        self.__updateMessage(msg)

        # Insert all pairs into PPD. Because these linked documents are
        # not in PPD yet, it should succeed with all insertions.
        try:
            cursor = self.__conn.cursor()
            for id in hash.keys():

                # Update the PPD table.
                cursor.execute ("""
                    INSERT INTO pub_proc_doc
                                (pub_proc, doc_id, doc_version)
                         VALUES  (?, ?, ?)
                                """, (self.__jobId, id, hash[id])
                               )

                # Update the __useDocList.
                cursor.execute ("""
                         SELECT t.name
                           FROM doc_type t, document d
                          WHERE d.doc_type = t.id
                            AND d.id = ?
                                """, id
                               )
                row = cursor.fetchone()
                if row and row[0]:
                    self.__userDocList.append((id, hash[id], row[0]))
                else:
                    msg = "Failed in adding docs to __userDocList.<BR>"
                    raise StandardError(msg)
        except cdrdb.Error, info:
            msg = 'Failure adding linked docs to PPD for job %d: %s' % (
                  self.__jobId, info[1][0])
            raise StandardError(msg)

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
    def __publishDoc(self, doc, filters, destType, destDir,
                     recordDoc = 0, subDir = ''):

        self.__debugLog("Publishing CDR%010d." % doc[0])

        # Keep track of problems encountered during filtering.
        warnings = ""
        errors   = ""
        invalDoc = ""

        # Apply each filter set to the document.
        filteredDoc = None
        for filterSet in filters:

            # Substitute parameter value of DateFirstPub.
            paramList = []
            for pair in filterSet[1]:
                if pair[0] == 'DateFirstPub':
                    date = self.__dateFirstPub[doc[0]]
                    paramList.append((pair[0], date))
                else:
                    paramList.append((pair[0], pair[1]))

            # First filter set is run against document from database.
            if not filteredDoc:
                result = cdr.filterDoc(self.__credentials, filterSet[0],
                                       docId = doc[0], docVer = doc[1],
                                       parm = paramList,
                                       port = self.__pubPort)

            # Subsequent filter sets are applied to previous results.
            else:
                result = cdr.filterDoc(self.__credentials, filterSet[0],
                                       doc = filteredDoc, parm = paramList,
                                       port = self.__pubPort)
            if type(result) not in (type([]), type(())):
                errors = result or "Unspecified failure filtering document"
                filteredDoc = None
                break

            filteredDoc = result[0]
            if result[1]: warnings += result[1]

        # Validate the filteredDoc against Vendor DTD.
        if self.__validateDocs and filteredDoc:
            errObj = validateDoc(filteredDoc, docId = doc[0])
            for error in errObj.Errors:
                errors += "%s<BR>" % error
                invalDoc = "InvalidDocs"
            for warning in errObj.Warnings:
                warnings += "%s<BR>" % warning
                invalDoc = "InvalidDocs"

        # Save the output as instructed.
        if self.__no_output != 'Y' and filteredDoc:
            try:
                subDir = invalDoc or subDir
                destDir = destDir + "/" + subDir
                if destType == Publish.FILE:
                    self.__saveDoc(filteredDoc, destDir, self.__fileName, "a")
                elif destType == Publish.DOCTYPE:
                    self.__saveDoc(filteredDoc, destDir, doc[2], "a")
                else:
                    self.__saveDoc(filteredDoc, destDir, "CDR%d.xml" % doc[0])
            except:
                errors = "Failure writing document CDR%010d" % doc[0]
        if recordDoc:
            self.__addPubProcDocRow(doc, subDir)

        # Handle errors and warnings.
        self.__checkProblems(doc, errors, warnings)

    #------------------------------------------------------------------
    # Handle errors and warnings.  Value of -1 for __errorsBeforeAborting
    # means never abort no matter how many errors are encountered.
    # If __publishIfWarnings has the value "Ask" we record the warnings
    # and keep going.
    #------------------------------------------------------------------
    def __checkProblems(self, doc, errors, warnings):
        if errors:
            self.__addDocMessages(doc, errors, Publish.SET_FAILURE_FLAG)
            self.__errorCount += 1
            if self.__errorsBeforeAborting != -1:
                if self.__errorCount > self.__errorsBeforeAborting:
                    if self.__errorsBeforeAborting:
                        msg = "Aborting on error detected in CDR%010d.<BR>" \
                                % doc[0]
                    else:
                        msg = "Aborting: too many errors encountered"
                    raise StandardError(msg)
        if warnings:
            self.__addDocMessages(doc, warnings)
            self.__warningCount += 1
            if self.__publishIfWarnings == "No":
                msg = "Aborting on warning(s) detected in CDR%010d.<BR>" \
                    % doc[0]
                raise StandardError(msg)

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
                raise StandardError("Failure reading messages for job %d" %
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
            raise StandardError(msg)

    #------------------------------------------------------------------
    # Record warning or error messages for a document.
    #------------------------------------------------------------------
    def __addDocMessages(self, doc, messages, failure = None):

        try:
            cursor = self.__conn.cursor()
            cursor.execute("""
                SELECT messages, failure
                  FROM pub_proc_doc
                 WHERE doc_id = ?
                   AND pub_proc = ?
                           """, (doc[0], self.__jobId)
                          )
            row  = cursor.fetchone()
            msg  = (row and row[0] or '') + messages

            # Don't reset column failure if the input is None!
            fail = failure or (row and row[1])

            cursor.execute("""\
                UPDATE pub_proc_doc
                   SET messages = ?,
                       failure  = ?
                 WHERE pub_proc = ?
                   AND doc_id   = ?""", (msg, fail,
                                         self.__jobId, doc[0]))
        except cdrdb.Error, info:
            msg = 'Failure recording message for document %d: %s' % \
                  (doc[0], info[1][0])
            raise StandardError(msg)

    #------------------------------------------------------------------
    # Record the publication of the specified document.
    #------------------------------------------------------------------
    def __addPubProcDocRow(self, doc, subDir):
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""\
                INSERT INTO pub_proc_doc
                (
                            pub_proc,
                            doc_id,
                            doc_version,
                            subdir
                )
                     VALUES
                (
                            ?,
                            ?,
                            ?,
                            ?
                )""", (self.__jobId, doc[0], doc[1], subDir))
        except cdrdb.Error, info:
            msg = 'Failure adding row for document %d: %s' % \
                  (self.__jobId, info[1][0])
            raise StandardError(msg)

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
                    cursor.execute(sql, timeout = self.__timeOut)

                    # XXX Dependency on result set column names is fragile
                    # and non-portable, relying on (among other things)
                    # DBMS treatment of case of object names.  Consider
                    # replacing this with a convention which uses the
                    # column count and order in the result set.
                    idCol = -1
                    verCol = -1
                    if not cursor.description:
                        raise StandardError("Result set not returned for "
                                            "SQL query: %s" % sql)
                    i = 0
                    for field in cursor.description:
                        if i == 0: # field[0] == "id":
                            idCol = i
                        elif i == 1: # field[0] == "version":
                            verCol = i
                        i += 1
                    if idCol == -1:
                        raise StandardError("SQL query does not return an "
                                            "'id' column: %s" % sql)
                    row = cursor.fetchone()
                    while row:
                        id = row[idCol]
                        if id in self.__alreadyPublished: continue
                        ver = verCol != -1 and row[verCol] or None

                        try:
                            doc = self.__findPublishableVersion(id, ver)
                        except StandardError, arg:

                            # Can't record this in the pub_proc_doc table,
                            # because we don't really have a versioned document.
                            self.__errorCount += 1
                            threshold = self.__errorsBeforeAborting
                            if threshold != -1:
                                if self.__errorCount > threshold:
                                    raise
                            self.__addJobMessages(arg[0])
                        docs.append(doc)
                        self.__alreadyPublished[id] = 1
                        row = cursor.fetchone()

                except cdrdb.Error, info:
                    msg = 'Failure retrieving document IDs for job %d: %s' % \
                          (self.__jobId, info[1][0])
                    raise StandardError(msg)

            # Handle XQL queries.
            elif child.nodeName == "SubsetXQL":
                xql = self.__repParams(cdr.getTextContent(child))
                resp = cdr.search(self.__credentials, xql)
                if type(resp) in (type(""), type(u"")):
                    raise StandardError("XQL failure: %s" % resp)
                for queryResult in resp:
                    id     = queryResult.docId
                    type   = queryResult.docType
                    digits = re.sub('[^\d]', '', id)
                    id     = int(digits)
                    if id in self.__alreadyPublished: continue
                    try:
                        doc = self.__findPublishableVersion(id)
                    except StandardError, arg:
                        self.__errorCount += 1
                        threshold = self.__errorsBeforeAborting
                        if threshold != -1:
                            if self.__errorCount > threshold:
                                raise
                        self.__addJobMessages(arg[0])
                    docs.append(doc)
                    self.__alreadyPublished[id] = 1

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
                    raise StandardError("Version %d for document CDR%010d "
                                        "is not publishable or does not "
                                        "exist" % (id, version))
                else:
                    raise StandardError("Unable to find publishable version "
                                        "for document CDR%010d" % id)
        except cdrdb.Error, info:
            msg = "Failure executing query to find publishable version " \
                  "for CDR%010d: %s" % (self.__jobId, info[1][0])
            raise StandardError(msg)
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
                sender    = self.__cdrEmail
                subject   = "CDR Publishing Job Status"
                receivers = string.replace(self.__email, ";", ",")
                receivers = string.split(receivers, ",")
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
            raise StandardError(msg)

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
            raise StandardError(msg)

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
                raise StandardError("Unable to find version of document "
                                    "CDR%010d created on or before %s" %
                                    (self.__ctrlDocId, self.__jobTime))
            cursor.execute("""\
                    SELECT xml
                      FROM doc_version
                     WHERE id  = ?
                       AND num = ?""", (self.__ctrlDocId, row[0]))
            row = cursor.fetchone()
            if not row or not row[0]:
                raise StandardError("Failure retrieving xml for control "
                                    "document CDR%010d" % self.__ctrlDocId)
        except cdrdb.Error, info:
            raise StandardError("Failure retrieving version of control "
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
        raise StandardError(msg)

    #----------------------------------------------------------------
    # Replace ?Name? with values in the parameter list.
    #----------------------------------------------------------------
    def __repParams(self, str):
        ret = str
        for name in self.__params.keys():
            ret = re.sub(r"\?%s\?" % name, self.__params[name], ret)
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
                            raise StandardError("SubsetOption missing "
                                                "required OptionName element")
                        if name in options and options[name] != value:
                            raise StandardError("Duplicate option '%s'" % name)
                        options[name] = value
                        self.__debugLog("Option %s='%s'." % (name, value))
                        if name == "AbortOnError":
                            abortOnError = value
                            if self.__params.has_key('AbortOnError'):
                                abortOnError = self.__params['AbortOnError']
                        elif name == "PublishIfWarnings":
                            if value not in ["Yes", "No", "Ask"]:
                                raise StandardError("Invalid value for "
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
                            raise StandardError("Invalid value for "
                                                "AbortOnError: %s" %
                                                abortOnError)
                break

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
        raise StandardError("Subset specification has no filters")

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
            raise StandardError("SubsetFilters element must have at least " \
                                "one SubsetFilter child element")
        return (filters, parms)

    #----------------------------------------------------------------
    # Extract the document ID or title for a filter.
    #----------------------------------------------------------------
    def __getFilter(self, node):
        for child in node.childNodes:
            if child.nodeName == "SubsetFilterName":
                nameOrSet = cdr.getTextContent(child)
                if nameOrSet.find("set:") == 0:
                    return nameOrSet
                else:
                    return "name:%s" % nameOrSet
            elif child.nodeName == "SubsetFilterId":
                return cdr.getTextContent(child)
        raise StandardError("SubsetFilter must contain SubsetFilterName " \
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
            raise StandardError("Missing ParmName in SubsetFilterParm")
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
            os.makedirs(dir)
        fileObj = open(dir + "/" + fileName, mode)
        fileObj.write(document)
        fileObj.close()

    #----------------------------------------------------------------
    # Handle process script, if one is specified, in which case
    # control is not returned to the caller.
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
                raise StandardError(msg)
            cmd = scriptName + " %d" % self.__jobId
            self.__debugLog("Publishing command '%s' invoked." % cmd)
            os.system(cmd)
            sys.exit(0)

    #----------------------------------------------------------------------
    # Set job status (with optional message) in pub_proc table.
    #----------------------------------------------------------------------
    def __updateStatus(self, status, message = None):

        self.__debugLog("Updating job status to %s." % status)
        if message: self.__debugLog(message)

        date = "NULL"
        if status in (Publish.SUCCESS, Publish.FAILURE):
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

            cursor.execute("""
                UPDATE pub_proc
                   SET status    = ?,
                       messages  = ?,
                       completed = %s
                 WHERE id = ?""" % date, (status, message, self.__jobId))
        except cdrdb.Error, info:
            msg = 'Failure updating status: %s' % info[1][0]
            self.__debugLog(msg)
            raise StandardError(msg)

    #----------------------------------------------------------------------
    # Update message in pub_proc table.
    #----------------------------------------------------------------------
    def __updateMessage(self, message):

        try:
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
            raise StandardError(msg)

    #----------------------------------------------------------------------
    # Set output_dir to "" in pub_proc table.
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
            raise StandardError(msg)

    #----------------------------------------------------------------------
    # Get the number of failed documents in pub_proc_doc table.
    #----------------------------------------------------------------------
    def __getFailures(self):
        id = self.__jobId
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""
                SELECT count(*)
                  FROM pub_proc_doc
                 WHERE (failure = 'Y' OR messages IS NOT NULL)
                   AND pub_proc = %d
                           """ % id
                          )
            row = cursor.fetchone()
            if row and row[0]:
                return row[0]
            else:
                return 0

        except cdrdb.Error, info:
            msg = 'Failure getting failed docs for job %d: %s' % (id,
                                                            info[1][0])
            self.__debugLog(msg)
            raise StandardError(msg)

    #----------------------------------------------------------------------
    # Wait for user's approval to proceed.
    #----------------------------------------------------------------------
    def __waitUserApproval(self):

        # Put a stop there.
        status = Publish.WAIT
        msg = "Job is waiting for user's approval at %s.<BR>" % time.ctime()
        msg += "Change the publishing job status using Manage Publishing " \
               "Job Status link under Publishing. <BR>"
        self.__updateStatus(status, msg)
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
                    raise StandardError(msg)

            except cdrdb.Error, info:
                msg = 'Failure getting status for job %d: %s' % (
                            self.__jobId, info[1][0])
                raise StandardError(msg)

            # Wait another 10 seconds.
            now = time.ctime()
            if status == Publish.WAIT:
                time.sleep(10)
            elif status == Publish.RUN:
                self.__updateMessage(
                    "Job is resumed by user at %s.<BR>" % now)
                return
            elif status == Publish.FAILURE:
                raise StandardError("Job %d is killed by user at %s.<BR>" \
                    % (self.__jobId, now))
            else:
                msg = "Unexpected status: %s for job %d.<BR>" \
                    % (status, self.__jobId)
                raise StandardError(msg)

    #----------------------------------------------------------------------
    # Log debugging message to d:/cdr/log/publish.log
    #----------------------------------------------------------------------
    def __debugLog(self, line):
        if LOG is not None:
            msg = "Job %d: %s\n" % (self.__jobId, line)
            if LOG == "":
                sys.stderr.write(msg)
            else:
                open(LOG, "a").write(msg)

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
# Set a parser instance to validate filtered documents.
#----------------------------------------------------------------------
__parser     = xmlval.XMLValidator()
__app        = xmlproc.Application()
__errHandler = ErrHandler(__parser)
__parser.set_application(__app)
__parser.set_error_handler(__errHandler)

#----------------------------------------------------------------------
# Validate a given document against its DTD.
#----------------------------------------------------------------------
def validateDoc(filteredDoc, docId = 0):

    errObj      = ErrObject()
    docTypeExpr = re.compile(r"<!DOCTYPE\s+(.*?)\s+.*?>", re.DOTALL)
    docType     = """<!DOCTYPE %s SYSTEM "%s">
                  """

    match = docTypeExpr.search(filteredDoc)
    if match:
        topElement = match.group(1)
        docType    = docType % (topElement, cdr2cg.PDQDTD)
        doc        = docTypeExpr.sub(docType, filteredDoc)
    else:
        errObj.Errors.append(
            "%d.xml:0:0:DOCTYPE declaration is missing." % docId)
        return errObj

    __errHandler.set_sysid("%d.xml" % docId)
    __errHandler.set_errobj(errObj)
    __parser.feed(doc)
    __parser.reset()

    return errObj

#----------------------------------------------------------------------
# Find all the linked documents for a given hash of docId/docVer pairs.
# Return a message and a hash of docId/docVer pairs excluding the input.
# Make this public in case that other programs can easily call it.
#----------------------------------------------------------------------
def findLinkedDocs(docPairList):

    try:
        msg = "Starting building link_net hash at %s.<BR>" % time.ctime()
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
        msg += "Finishing link_net hash for %d linking docs at %s.<BR>" % (
                        len(links), time.ctime())

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
            for id in docIds:
                if links.has_key(id):
                    for targetPair in links[id]:
                        if not linkedDocHash.has_key(targetPair[0]):
                            linkedDocHash[targetPair[0]] = targetPair[1]
                            done = 0

        # Delete all elements in the input hash.
        for key in docPairList.keys():
            del linkedDocHash[key]

        msg += "Found all %d linked documents in %d passes at %s.<BR>" % (
                        len(linkedDocHash), passNum, time.ctime())

        # Return what we have got.
        return [msg, linkedDocHash]

    except:
        raise StandardError("Failure finding linked docs.<BR>")


#----------------------------------------------------------------------
# Test driver.
#----------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write("usage: cdrpub.py job-id\n")
        sys.exit(1)
    LOG = ""
    p = Publish(int(sys.argv[1]))
    p.publish()
