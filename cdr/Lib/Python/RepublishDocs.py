#----------------------------------------------------------------------
#
# $Id: RepublishDocs.py,v 1.4 2007-05-11 16:06:46 bkline Exp $
#
# Module for republishing a set of documents, regardless of whether
# what we would send to Cancer.gov is identical with what we sent
# for the last push job.
#
# $Log: not supported by cvs2svn $
# Revision 1.3  2007/05/09 18:39:24  bkline
# Added parameter indicating that only failed documents should be
# re-published for a specified job.
#
# Revision 1.2  2007/05/07 01:16:20  bkline
# Added test driver, substantial logging and debugging instrumentation,
# and significant optimizations for the parts that were taking so long
# that it would not have been possible to invoke the module from a CGI
# script.
#
# Revision 1.1  2007/05/04 19:34:29  bkline
# New module for nightly publishing project.
#
#----------------------------------------------------------------------
import cdr, cdr2gk, cdrdb, cdrcgi, time

# Extra output to standard error file.
DEBUG = False

class CdrRepublisher:

    """
    Object which knows how to send a set of documents to Cancer.gov
    in such a way that suppresses the optimization which avoids
    re-sending a document which is identical to what was last sent
    to Cancer.gov for that document.  This functionality will be
    useful not only for facilitating testing Cancer.gov's GateKeeper
    software, but also for re-publishing documents which failed
    processing after a previous push to Cancer.gov.

    Instantiation of the object requires that a CDR session has
    been established using a login account with sufficient permission
    levels for publishing the documents.

    Usage:
        import cdr
        from RepublishDocs import CdrRepublisher
        session = cdr.login('username', 'password')
        cr = CdrRepublisher(session)
        docs = (493702, 63708, 149912)
        try:
            cr.republish(addNewLinkedDocuments = True,
                         docList = docs,
                         email = '***REMOVED***')
        except Exception, e:
            cdr.logwrite('republish() failure: %s' % e)
            reportFailure(...)
        reportSuccess(...)
    """
    
    class Doc:

        """
            Object which carries enough information about a CDR document
            which needs to be re-published that the appropriate adjustments
            can be made to the pub_proc_cg table prior to submitting the
            job request for the publication.  Requests to convert the
            object to string representation return the format suitable
            for inclusion in the sequence passed as the docList parameter
            to the cdr.publish() call.

            Public attributes:

                docId       - integer for the CDR unique document ID
                docVersion  - integer for the latest publishable version
                              of the document
                isNew       - True if the document is not available on
                              Cancer.gov in both the live and preview
                              stages
        """
        
        def __init__(self, docId, docVersion, isNew):
            self.docId      = docId
            self.docVersion = docVersion
            self.isNew      = isNew
        def __str__(self):
            return "CDR%010d/%d" % (self.docId, self.docVersion)

    def __init__(self, credentials, host = cdr.DEFAULT_HOST,
                 port = cdr.DEFAULT_PORT):

        """
            Instatiates a new object for republishing CDR documents
            to Cancer.gov.

            Pass:

                credentials    - public session identifier for a CDR
                                 login with sufficient permissions to
                                 create a publishing job, or a tuple
                                 with two members: the CDR account ID
                                 and password with which such a
                                 session can be created
                host           - optional string identifying the CDR
                                 server on which the re-publishing
                                 job is to be submitted; defaults to
                                 'localhost'
                port           - optional integer parameter identifying
                                 the TCP/IP port to be used in communicating
                                 with the CDR Server; defaults to 2019
        """
        
        self.__credentials = credentials
        self.__host        = host
        self.__port        = port
        self.__conn        = cdrdb.connect(dataSource = host)
        self.__cursor      = self.__conn.cursor()
        self.__onCG        = self.__getDocsOnCG()
    
    def republish(self, addNewLinkedDocuments,
                  docList = None, jobList = None, docType = None,
                  docTypeAll = False, failedOnly = True, email = ''):


        """
            Requests that a set of documents be sent to Cancer.gov,
            avoiding the optimization which blocks sending the same
            version of a document twice in succession.

            Pass:

                addNewLinkedDocuments - True if the method should
                                        recursively look for and add
                                        to the job any new documents
                                        linked by any other document
                                        in the set to be re-published;
                                        otherwise False (required
                                        parameter)
                docList               - sequence of integers each
                                        identifying with its unique
                                        document identifier a CDR
                                        document to be republished
                                        (optional parameter); can
                                        be None (the default) or an
                                        empty sequence if documents
                                        will be identified by job or
                                        document type
                jobList               - sequence of integers each
                                        identifying a publishing job
                                        for which each of the documents
                                        were successfully exported
                                        are to be included in the new
                                        republishing job (optional
                                        parameter); can be None (the
                                        default) or an empty sequence
                                        if documents to be republished
                                        will be identified by document ID
                                        or document type
                docType               - string identifying the document
                                        type for which all publishable
                                        (or published -- see docTypeAll
                                        parameter below) documents are
                                        to be re-published (optional
                                        parameter); can be None (the
                                        default) or an empty string
                                        if documents to be republished
                                        will be identified by document
                                        ID and/or job ID; the document
                                        type 'Protocol' is mapped to
                                        'InScopeProtocol'
                docTypeAll            - True if all publishable documents
                                        of the type specified by the
                                        docType parameter should be
                                        included in the re-publishing
                                        job; False or None if the job
                                        should only send documents which
                                        are currently in the pub_proc_cg
                                        table as having been sent to
                                        Cancer.gov already (optional
                                        parameter, defaulting to False);
                                        ignored if the docType parameter
                                        is not specified
                failedOnly            - True if only documents with failure
                                        set to 'Y' in the pub_proc_doc
                                        table are to be included when
                                        collecting documents for
                                        specified publishing jobs; otherwise
                                        all documents are included for
                                        the publishing jobs specified
                                        (optional parameter, defaulting
                                        to True); ignored if no job IDs
                                        are specified
                email                 - optional string containing the
                                        address to which an email message
                                        is to be sent when the publishing
                                        job completes; also used for
                                        reporting failures if this
                                        method hits an exception

            Returns:

                integer representing the unique ID of the newly
                created export job

            An exception is raised in the event of a failure
            to create the new job.
            
        """

        # Record the request.
        cdr.logwrite("republish(): %d doc IDs, %d job IDs, docType: %s" %
                     (docList and len(docList) or 0,
                      jobList and len(jobList) or 0,
                      docType or "None"), cdr.PUBLOG)

        # Gather the documents from the list of individual document IDs
        self.__docs = {}
        if docList:
            for docId in docList:
                self.__addDocumentToSet(docId)

        # Add to the list documents identified by previous publishing job
        if jobList:
            for jobId in jobList:
                self.__cursor.execute("""\
                    SELECT doc_id, failure
                      FROM pub_proc_doc
                     WHERE pub_proc = ?
                       AND (removed IS NULL or removed = 'N')""",
                                      jobId, timeout = 300)
                rows = self.__cursor.fetchall()
                for docId, failure in rows:
                    if not failedOnly or failure == 'Y':
                        self.__addDocumentToSet(docId)

        # Collect all documents of a specified document type if requested.
        if docType:

            # InScopeProtocol documents are know to Cancer.gov as 'Protocol'.
            if docType == 'Protocol':
                docType == 'InScopeProtocol'

            # Get all publishable documents of the specified document type ...
            if docTypeAll:
                self.__cursor.execute("""\
                    SELECT DISTINCT v.id
                      FROM doc_version v
                      JOIN doc_type t
                        ON t.id = v.doc_type
                     WHERE v.publishable = 'Y'
                       AND v.val_status = 'V'
                       AND t.name = ?""", docType, timeout = 300)

            # ... or just those already sent to Cancer.gov, as requested.
            else:
                self.__cursor.execute("""\
                    SELECT d.id
                      FROM document d
                      JOIN pub_proc_cg c
                        ON c.id = d.id
                      JOIN doc_type t
                        ON t.id = d.doc_type
                     WHERE t.name = ?""", docType, timeout = 300)
            rows = self.__cursor.fetchall()
            for row in rows:
                self.__addDocumentToSet(row[0])

        # Sanity check.
        if not self.__docs:
            raise Exception("republish(): no documents to publish")
        
        # Record the number of documents collected directly.
        cdr.logwrite("republish(): %d documents collected" % len(self.__docs),
                     cdr.PUBLOG)

        # If requested, include new docs linked to by the ones we will publish.
        if addNewLinkedDocuments:
            numOriginalDocs = len(self.__docs)
            self.__addNewLinkedDocuments()
            cdr.logwrite("republish(): %d new linked documents added to set" %
                         (len(self.__docs) - numOriginalDocs),
                         cdr.PUBLOG)

        try:

            # Make sure we don't optimize away the push of any of these docs.
            self.__adjustPubProcCgTable()
            cdr.logwrite("republish(): pub_proc_cg table adjusted", cdr.PUBLOG)

            # Use the publishing job type appropriate for republishing.
            pubSystem = 'Primary'
            pubSubset = 'Republish-Export'

            # Create a sequence of strings in the form doc-id/version-number.
            docs = [str(self.__docs[docId]) for docId in self.__docs]

            # Create the export job, which in turn creates the follow-on push
            # job.
            resp = cdr.publish(self.__credentials, pubSystem, pubSubset,
                               parms = None, docList = docs, email = email,
                               host = self.__host, port = self.__port)

            # Make sure the job creation succeeded.
            jobId, errors = resp
            if jobId:
                jobId = int(jobId)
                cdr.logwrite("republish(): new publishing job %d created" %
                             jobId, cdr.PUBLOG)
                return jobId
            else:
                self.__cleanupPubProcCgTable()
                raise Exception("republish(): %s" % errors)

        # Clean up in the event of failure, including resetting the
        # force_push and cg_new columns back to 'N'.  If we have an
        # email address, use it to notify the requestor of the bad news.
        except Exception, e:
            try:
                cdr.logwrite("republish(): %s" % e, cdr.PUBLOG, True)
            except:
                pass
            if email:
                try:
                    sender  = "cdr@%s" % cdrcgi.WEBSERVER
                    subject = "Republication failure on %s" % self.__host
                    body    = "Failure republishing CDR documents:\n%s\n" % e
                    cdr.sendMail(sender, [email], subject, body)
                    cdr.logwrite("republish(): sent failure notification "
                                 "to %s" % email, cdr.PUBLOG)
                except:
                    pass
            try:
                self.__cleanupPubProcCgTable()
                cdr.logwrite("republish(): pub_proc_cg table cleaned up",
                             cdr.PUBLOG)
            except:
                pass
            raise

    #------------------------------------------------------------------
    # Find the most recent publishing version for the specified
    # document and ask Cancer.gov if they already have it.  If
    # there is a publishable version, and we don't already have
    # the document in the pile to be published, insert a Doc object
    # into the dictionary representing that set.
    #------------------------------------------------------------------
    def __addDocumentToSet(self, docId):
        if docId not in self.__docs:
            docVersion = self.__findLatestPubVersion(docId)
            if docVersion:
                isNew = not self.__isOnCG(docId)
                doc = CdrRepublisher.Doc(docId, docVersion, isNew)
                self.__docs[docId] = doc

    #------------------------------------------------------------------
    # Look in the doc_version table to find the most recent valid
    # publishable version for the specified document.
    #------------------------------------------------------------------
    def __findLatestPubVersion(self, docId):
        self.__cursor.execute("""\
            SELECT MAX(num)
              FROM doc_version
             WHERE id = ?
               AND publishable = 'Y'
               AND val_status = 'V'""", docId)
        rows = self.__cursor.fetchall()
        return rows and rows[0][0] or None

    #------------------------------------------------------------------
    # Find documents which should be included with the set to be
    # published because one or more documents already in that set
    # link to them.  Patterned after a similar function in cdrpub.py,
    # but in this case, we only need to pick up documents which
    # aren't already on Cancer.gov.  This method is significantly
    # faster than the one in cdrpub.py.
    #------------------------------------------------------------------
    def __addNewLinkedDocuments(self):

        # Debugging instrumentation.
        if DEBUG:
            start = time.time()
            passes = 0

        # Create a temporary table with link pairs of interest (omitting
        # document types we don't send to Cancer.gov).
        self.__cursor.execute("""\
            CREATE TABLE #links
              (source_id INTEGER NOT NULL,
               target_id INTEGER NOT NULL,
             doc_version INTEGER NOT NULL)""")
        self.__conn.commit()
        self.__cursor.execute("""\
                INSERT INTO #links
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
                      WHERE t.name NOT IN ('Citation', 'Person', 'Country',
                                           'Documentation', 'Mailer',
                                           'MiscellaneousDocument',
                                           'SupplementaryInfo')
                        AND v.val_status = 'V'
                        AND v.publishable = 'Y'
                        AND d.active_status = 'A'
                   GROUP BY ln.target_doc,
                            ln.source_doc""", timeout = 300)
        if DEBUG:
            cdr.logwrite("republish(): populated #links table "
                         "with %d rows in %.3f seconds" %
                         (self.__cursor.rowcount, (time.time() - start)),
                         cdr.PUBLOG)

        # Seed a second temporary table with documents we already have
        # in the set to be published.
        self.__cursor.execute("""\
            CREATE TABLE #docs
                     (id INTEGER NOT NULL UNIQUE,
             doc_version INTEGER     NULL)""")
        self.__conn.commit()
        for docId in self.__docs:
            self.__cursor.execute("INSERT INTO #docs (id) VALUES(?)", docId)
        self.__conn.commit()
        if DEBUG:
            cdr.logwrite("republish(): added %d rows to #docs table; "
                         "elapsed: %.3f seconds" %
                         (len(self.__docs), (time.time() - start)),
                         cdr.PUBLOG)

        # Find linked documents to be added to the original set.
        done = False
        while not done:
            if DEBUG:
                passes += 1
                sys.stderr.write("pass %d\n" % passes)
            self.__cursor.execute("""\
                INSERT INTO #docs
            SELECT DISTINCT links.target_id, links.doc_version
                       FROM #links links
                       JOIN #docs docs
                         ON docs.id = links.source_id
                      WHERE links.target_id NOT IN (SELECT id FROM #docs)""")
            self.__conn.commit()
            if not self.__cursor.rowcount:
                done = True
        if DEBUG:
            cdr.logwrite("republish(): done adding linked documents; "
                         "elapsed: %.3f seconds; passes: %d" %
                         (time.time() - start, passes), cdr.PUBLOG)

        # Get the rows that were added to the table for linked documents.
        self.__cursor.execute("""\
            SELECT id, doc_version
              FROM #docs
             WHERE doc_version IS NOT NULL""")
        linkedDocs = self.__cursor.fetchall()
        if DEBUG:
            cdr.logwrite("republish(): added %d documents to #docs table; "
                         "elapsed: %.3f seconds" %
                         (len(linkedDocs), (time.time() - start)),
                         cdr.PUBLOG)

        # Pick up the linked documents that Cancer.gov doesn't already have.
        for docId, docVer in linkedDocs:
            if docId not in self.__docs and not self.__isOnCG(docId):
                self.__docs[docId] = CdrRepublisher.Doc(docId, docVer, True)
        if DEBUG:
            cdr.logwrite("republish(): set now contains %d documents; "
                         "elapsed: %.3f seconds" %
                         (len(self.__docs), time.time() - start), cdr.PUBLOG)

    #------------------------------------------------------------------
    # Find out whether the specified document is already on Cancer.gov.
    # In this context (according to Olga Rosenbaum, in a meeting held
    # 2007-05-01), this means present both on the "Live" site and on
    # the "Preview" stage.
    #------------------------------------------------------------------
    def __isOnCG(self, docId):
        return docId in self.__onCG

    #------------------------------------------------------------------
    # Collect the documents which are already on Cancer.gov.
    # XXX Assumes we set job status to something other than 'Success'
    # for a push job until we verify that all of the documents arrived
    # safely at their destination.
    #------------------------------------------------------------------
    def __getDocsOnCG(self):

        # Debugging instrumentation.
        if DEBUG:
            start = time.time()

        # Get the latest push job id for each document on or after the
        # last successful full load.
        self.__cursor.execute("""\
            CREATE TABLE #pub_docs
                     (id INTEGER NOT NULL,
                 pub_job INTEGER NOT NULL)""")
        self.__conn.commit()
        self.__cursor.execute("""\
            INSERT INTO #pub_docs
                 SELECT d.doc_id, MAX(p.id)
                   FROM pub_proc_doc d
                   JOIN pub_proc p
                     ON p.id = d.pub_proc
                  WHERE	p.id >= (SELECT MAX(id)
                                   FROM pub_proc
                                  WHERE pub_subset = 'Push_Documents_'
                                                   + 'To_Cancer.Gov_'
                                                   + 'Full-Load'
                                    AND completed IS NOT NULL
                                    AND status = 'Success')
                    AND p.completed IS NOT NULL
                    AND p.status = 'Success'
                    AND d.failure IS NULL
                    AND p.pub_subset LIKE 'Push_Documents_To_Cancer.Gov%'
               GROUP BY d.doc_id""", timeout = 300)
        self.__conn.commit()

        # Get the ones for which the last action wasn't removal.
        self.__cursor.execute("""\
            SELECT p.id
              FROM #pub_docs p
              JOIN pub_proc_doc d
                ON d.doc_id = p.id
               AND d.pub_proc = p.pub_job
             WHERE d.removed = 'N'""")
        onCG = set([row[0] for row in self.__cursor.fetchall()])
        if DEBUG:
            cdr.logwrite("republish(): found %d documents on Cancer.gov in "
                         "%.3f seconds" % (len(onCG), time.time() - start),
                         cdr.PUBLOG)
        return onCG
                    
    #------------------------------------------------------------------
    # Update all of the rows in the pub_proc_cg table for documents
    # being published by this job, setting force_push to 'Y' (which
    # avoids the optimization suppressing resending of the same version
    # of the same document twice in a row), and setting cg_new to
    # reflect whether Cancer.gov already has the document.
    #------------------------------------------------------------------
    def __adjustPubProcCgTable(self):
        for docId in self.__docs:
            doc = self.__docs[docId]
            self.__cursor.execute("""\
                UPDATE pub_proc_cg
                   SET force_push = 'Y',
                       cg_new = ?
                 WHERE id = ?""", (doc.isNew and 'Y' or 'N', docId))
            self.__conn.commit()

    #------------------------------------------------------------------
    # Undo the adjustments we made earlier to the pub_proc_cg table.
    # Invoked in the case of failure.
    #------------------------------------------------------------------
    def __cleanupPubProcCgTable(self):
        try:
            self.__cursor.execute("""\
                UPDATE pub_proc_cg
                   SET force_push = 'N',
                       cg_new = 'N'""")
            self.__conn.commit()
        except:
            pass

#----------------------------------------------------------------------
# Test driver.
#----------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    DEBUG = True
    if len(sys.argv) < 4:
        sys.stderr.write("""\
usage: %s uid pwd arg ...

where arg is one of:
   doc=<CDR document ID>
   job=<CDR publishing job ID>
   type=<CDR document type name>
   email=<email address>
   add
   all
""" % sys.argv[0])
        sys.exit(1)
    uid     = sys.argv[1]
    pwd     = sys.argv[2]
    docs    = []
    jobs    = []
    all     = False
    add     = False
    docType = None
    email   = ''
    for arg in sys.argv[3:]:
        if arg.startswith('doc='):
            docs.append(int(arg[4:]))
        elif arg.startswith('job='):
            jobs.append(int(arg[4:]))
        elif arg.startswith('type='):
            if docType:
                raise Exception("only one document type may be specified")
            else:
                docType = arg[5:]
        elif arg == "all":
            all = True
        elif arg == "add":
            add = True
        elif arg.startswith("email="):
            email = arg[6:]
        else:
            raise Exception("invalid argument %s" % arg)
    session = cdr.login(uid, pwd)
    try:
        republisher = CdrRepublisher(session)
        jobId = republisher.republish(addNewLinkedDocuments = add,
                                      docList = docs,
                                      jobList = jobs,
                                      docType = docType,
                                      docTypeAll = all,
                                      email = email)
        cdr.logout(session)
    except Exception, e:
        cdr.logout(session)
        raise
