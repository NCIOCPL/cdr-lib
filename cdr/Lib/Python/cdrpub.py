#----------------------------------------------------------------------
#
# $Id: cdrpub.py,v 1.13 2002-08-01 15:52:26 pzhang Exp $
#
# Module used by CDR Publishing daemon to process queued publishing jobs.
#
# $Log: not supported by cvs2svn $
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
import socket, cdr2cg
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

    # class private variables
    __cdrEmail = "cdr@%s.nci.nih.gov" % socket.gethostname()

    #---------------------------------------------------------------
    # Load the job settings from the database.  User-specified
    # documents will already have been recorded in the pub_proc_doc
    # table, but other documents can be added through SQL or XQL 
    # queries.
    #---------------------------------------------------------------
    def __init__(self, jobId):
        
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
        self.__credentials = cdr.login(self.__userName, self.__passWord)

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
            userListedDocsRemaining = len(self.__userDocList)
            self.__debugLog("Processing user-listed documents.")
            for spec in self.__specs.childNodes:

                if spec.nodeName == "SubsetSpecification":

                    # Gather together filters (with parms) used for this SS.
                    filters = self.__getFilters(spec)
                    specFilters.append(filters)

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
                        self.__publishDoc(doc, filters, destType, dest)
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
                                      Publish.STORE_ROW_IN_PUB_PROC_DOC_TABLE)
                    i += 1

            if self.__publishIfWarnings == "Ask" and self.__warningCount:
                self.__updateStatus(Publish.WAIT, "Warnings encountered")

            # Rename the output directory from its working name if appropriate.
            else:
                self.__updateStatus(Publish.SUCCESS)
                if self.__no_output != "Y":
                    try:
                        os.rename(dest, dest_base)
                    except:
                        pass


        except StandardError, arg:
            self.__updateStatus(Publish.FAILURE, arg[0])
            if self.__no_output != "Y":
                try:
                    os.rename(dest, dest_base + ".FAILURE")
                except:
                    pass
        except:
            self.__updateStatus(Publish.FAILURE, "Unexpected failure")
            if self.__no_output != "Y":
                try:
                    os.rename(dest, dest_base + ".FAILURE")
                except:
                    pass

        # Send email to notify user of job status.
        self.__sendMail()

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
    #------------------------------------------------------------------
    def __publishDoc(self, doc, filters, destType, destDir, recordDoc = 0):

        self.__debugLog("Publishing CDR%010d." % doc[0])

        # Keep track of problems encountered during filtering.
        warnings = ""
        errors   = ""

        # Apply each filter set to the document.
        filteredDoc = None
        for filterSet in filters:

            # First filter set is run against document from database.
            if not filteredDoc:
                result = cdr.filterDoc(self.__credentials, filterSet[0],
                                       docId = doc[0], docVer = doc[1],
                                       parm = filterSet[1])

            # Subsequent filter sets are applied to previous results.
            else:
                result = cdr.filterDoc(self.__credentials, filterSet[0],
                                       doc = filteredDoc, parm = filterSet[1])
            if type(result) not in (type([]), type(())):
                errors = result or "Unspecified failure filtering document"
                filteredDoc = None
                break

            filteredDoc = result[0]
            if result[1]: warnings += result[1]

        # Save the output as instructed.
        if self.__no_output != 'Y' and filteredDoc:
            try:
                if destType == Publish.FILE:
                    self.__saveDoc(filteredDoc, destDir, self.__fileName, "a")
                elif destType == Publish.DOCTYPE:
                    self.__saveDoc(filteredDoc, destDir, doc[2], "a")
                else:
                    self.__saveDoc(filteredDoc, destDir, "CDR%d.xml" % doc[0])
            except:
                errors = "Failure writing document CDR%010d" % doc[0]
        if recordDoc:
            self.__addPubProcDocRow(doc)

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
                        msg = "Aborting on error detected in CDR%010d" % doc[0]
                    else:
                        msg = "Aborting: too many errors encountered"
                    self.__updateStatus(Publish.FAILURE, msg)
                    raise StandardError(msg)
        elif warnings:
            self.__addDocMessages(doc, warnings)
            self.__warningCount += 1
            if self.__publishIfWarnings == "No":
                msg = "Aborting on warning(s) detected in CDR%010d" % doc[0]
                self.__updateStatus(Publish.FAILURE, msg)
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
            cursor.execute("""\
                UPDATE pub_proc_doc
                   SET messages = ?,
                       failure  = ?
                 WHERE pub_proc = ?
                   AND doc_id   = ?""", (messages, failure, 
                                         self.__jobId, doc[0]))
        except cdrdb.Error, info:
            msg = 'Failure recording message for document %d: %s' % \
                  (doc[0], info[1][0])
            raise StandardError(msg)

    #------------------------------------------------------------------
    # Record the publication of the specified document.
    #------------------------------------------------------------------
    def __addPubProcDocRow(self, doc):
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""\
                INSERT INTO pub_proc_doc
                (
                            pub_proc,
                            doc_id,
                            doc_version
                )
                     VALUES
                (
                            ?,
                            ?,
                            ?
                )""", (self.__jobId, doc[0], doc[1]))
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
                    cursor.execute(sql)

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
                        if field[0] == "id":
                            idCol = i
                        elif field[0] == "version":
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
                    id     = string.atoi(digits)
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
                 WHERE d.id          = ?
                   AND d.num         = ?
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
                receivers = string.split(self.__email, ",")
                message   = """\
Job %d has completed.  You can view a status report for this job at:

    http://%s.nci.nih.gov/cgi-bin/cdr/PubStatus.py?id=%d

Please do not reply to this message.
""" % (self.__jobId, socket.gethostname(), self.__jobId)
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
    # Don't need to check nodeType since the schema is known
    #    and __subsetName is unique.
    # Error checking: node not found.
    #----------------------------------------------------------------
    def __getSubSet(self, docElem):
        pubSys = xml.dom.minidom.parseString(docElem).documentElement
        for node in pubSys.childNodes:
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
                return "name:%s" % cdr.getTextContent(child)
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
        id = self.__jobId
        date = "NULL"
        if status in (Publish.SUCCESS, Publish.FAILURE):
            date = "GETDATE()"
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""\
                UPDATE pub_proc
                   SET status    = ?,
                       messages  = ?,
                       completed = %s
                 WHERE id        = ?""" % date, (status, message, id))
        except cdrdb.Error, info:
            msg = 'Failure setting status for job %d: %s' % (id, info[1][0])
            self.__debugLog(msg)
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
# Test driver.
#----------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write("usage: cdrpub.py job-id\n")
        sys.exit(1)
    LOG = ""
    p = Publish(int(sys.argv[1]))
    p.publish()
