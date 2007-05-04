#----------------------------------------------------------------------
#
# $Id: RepublishDocs.py,v 1.1 2007-05-04 19:34:29 bkline Exp $
#
# Module for republishing a set of documents, regardless of whether
# what we would send to Cancer.gov is identical with what we sent
# for the last push job.
#
# $Log: not supported by cvs2svn $
#----------------------------------------------------------------------
import cdr, cdr2gk, cdrdb, cdrcgi

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
                isNew       - True if the document is not avaiable on
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
    
    def republish(addNewLinkedDocuments,
                  docList = None, jobList = None, docType = None,
                  docTypeAll = False, email = ''):


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
                email                 - optional string containing the
                                        address to which an email message
                                        is to be sent when the publishing
                                        job completes; also used for
                                        reporting failures if this
                                        method hits an exception
        """

        # Gather the documents from the list of individual document IDs
        self.__docs = {}
        for docId in docList:
            self.__addDocumentToSet(docId)

        # Add to the list documents identified by previous publishing job
        for jobId in jobList:
            self.__cursor.execute("""\
                SELECT doc_id
                  FROM pub_proc_doc
                 WHERE (failure IS NULL OR failure = 'N')
                   AND (removed IS NULL or removed = 'N')""",
                                  timeout = 300)
            rows = self.__cursor.fetchall()
            for row in rows:
                self.__addDocumentToSet(row[0])

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

        # If requested, include new docs linked to by the ones we will publish.
        if addNewLinkedDocuments:
            self.__addNewLinkedDocuments()

        # Sanity check.
        if not self.__docs:
            raise Exception("republish(): no documents to publish")
        
        try:

            # Make sure we don't optimize away the push of any of these docs.
            self.__adjustPubProcCgTable()

            # Use the publishing job type appropriate for republishing.
            pubSystem = 'Primary'
            pubSubset = 'Republish-Export'

            # Create a sequence of strings in the form doc-id/version-number.
            docs = [str(self.__docs[docId]) for docId in self.__docs]

            # Create the export job, which in turn creates the follow-on push
            # job.
            resp = publish(self.__credentials, pubSystem, pubSubset,
                           parms = None, docList = docs, email = email,
                           host = self.__host, port = self.__port)

            # Make sure the job creation succeeded.
            jobId, errors = resp
            if jobId:
                return jobId
            else:
                self.__cleanupPubProcCgTable()
                raise Exception("republish(): %s" % errors)

        # Clean up in the event of failure, including resetting the
        # force_push and cg_new columns back to 'N'.  If we have an
        # email address, use it to notify the requestor of the bad news.
        except Exception, e:
            if email:
                try:
                    sender  = "cdr@%s" % cdrcgi.WEBSERVER
                    subject = "Republication failure on %s" % self.__host
                    body    = "Failure republishing CDR documents:\n%s\n" % e
                    cdr.sendMail(sender, [email], subject, body)
                except:
                    pass
            try:
                self.__cleanupPubProcCgTable()
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
    # aren't already on Cancer.gov.
    #------------------------------------------------------------------
    def __addNewLinkedDocuments(self):

        # Build a hash in memory that reflects link_net.
        self.__cursor.execute("""
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
                            ln.source_doc""", timeout = 300)
        row = cursor.fetchone()
        links = {}
        while row:
            sourceId, targetId, targetVersion = row
            if sourceId not in links:
                links[sourceId] = []
            links[sourceId].append((targetId, targetVersion))
            row = cursor.fetchone()

        # Find all linked docs recursively.
        done = False
        alreadyChecked = set()
        Doc = CdrRepublisher.Doc
        while not done:
            done = True
            newDocs = {}
            for docId in self.__docs:
                if docId not in alreadyChecked and docId in links:
                    for targetId, targetVersion in links[docId]:
                        if targetId not in self.__docs:
                            if targetId not in newDocs:
                                if not self.__isOnCG(targetId):
                                    doc = Doc(targetId, targetVersion, True)
                                    newDocs[targetId] = doc
                    alreadyChecked.add(docId)
            for docId in newDocs:
                self.__docs[docId] = newDocs[docId]
                done = False

    #------------------------------------------------------------------
    # Find out whether the specified document is already on Cancer.gov.
    # In this context (according to Olga Rosenbaum, in a meeting held
    # 2007-05-01), this means present both on the "Live" site and on
    # the "Preview" stage.
    #------------------------------------------------------------------
    def __isOnCG(self, docId):
        try:
            response = cdr2gk.requestStatus("SingleDocument", docId)
            locations = response.details.docs[0]
            liveJob = locations.liveJobId
            previewJob = locations.previewJobId
            if not liveJob or liveJob == 'Not Present':
                return False
            if not previewJob or previewJob == 'Not Present':
                return False
            return True
        except:
            return False

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
