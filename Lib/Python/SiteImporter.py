#----------------------------------------------------------------------
#
# $Id: SiteImporter.py,v 1.30 2008-05-29 20:32:55 bkline Exp $
#
# Base class for importing protocol site information from external sites.
#
# $Log: not supported by cvs2svn $
# Revision 1.29  2008/05/06 13:37:15  bkline
# Refinements in manifest parser; mapping of new status value.
#
# Revision 1.28  2007/06/04 10:22:47  bkline
# Converted incoming xml from utf-8 before storing or processing.
#
# Revision 1.27  2007/05/16 22:46:37  bkline
# Escaped status string reserved characters.
#
# Revision 1.26  2007/05/11 22:30:42  bkline
# Added some more status mapping at Sheri's request (still #3244).
#
# Revision 1.25  2007/05/11 03:50:36  bkline
# Added missing parens to call to upper().
#
# Revision 1.24  2007/05/11 03:43:49  bkline
# Mapped 'Temporarily Closed to Accrual' to 'Temporarily closed' (request
# 3244).
#
# Revision 1.23  2007/04/20 21:30:10  bkline
# Passed in parameters to 'Insert External Sites' filter for adjusting
# the lead org's protocol status when appropriate.
#
# Revision 1.22  2007/04/17 13:35:01  bkline
# Fixed typo ('AND' for 'WHERE') in SQL query.
#
# Revision 1.21  2007/04/16 15:20:00  bkline
# Enhancements needed for Oncore imports.
#
# Revision 1.20  2005/11/17 14:06:01  bkline
# Separated logs for imports from different sources.
#
# Revision 1.19  2005/10/20 14:24:59  bkline
# Restricted processing of pending docs to the ones for this source.
#
# Revision 1.18  2005/10/17 21:09:42  bkline
# Modified logic for reporting on missing docs.
#
# Revision 1.17  2005/09/02 17:41:12  bkline
# Split out missing trials for which we've never received any site
# information from those which have been unexpectedly dropped.
#
# Revision 1.16  2005/08/27 22:09:04  bkline
# Added code to block import of trials which don't have at least one lead
# organization with a matching UpdateMode.  Modified email report so that
# it now is sent out even with test reports (but with an extra header
# identifying the test nature of the job).  Added parameter for new
# DateLastModified value.
#
# Revision 1.15  2005/08/22 16:10:06  bkline
# Added parameter for invoking validation in test mode.
#
# Revision 1.14  2005/06/20 16:44:14  bkline
# Added more flexibility with the name of the manifest file; added more
# robust handling of database code.
#
# Revision 1.13  2005/06/10 12:34:29  bkline
# Set job's __id member to None at top of constructor.
#
# Revision 1.12  2005/06/03 15:18:08  bkline
# Fixed pychecker warnings.
#
# Revision 1.11  2005/06/03 15:14:09  bkline
# Fixed typo in variable name; add more exception handling.
#
# Revision 1.10  2005/06/01 04:18:17  bkline
# Fixed spacing in email report group name.  Adjusted processing logic for
# test mode.
#
# Revision 1.9  2005/05/26 23:45:07  bkline
# Fix to create output directory in test mode for SiteImporter subclass.
#
# Revision 1.8  2005/05/24 21:10:27  bkline
# Added email report at end of job.
#
# Revision 1.7  2005/05/10 21:21:47  bkline
# Converted @@IDENTITY value from string to integer (for some reason the
# ADO interface converts it from an integer to a string); added code to
# strip the doctype declaration from the stored RSS XML document.
#
# Revision 1.6  2005/05/05 13:06:35  bkline
# Added sweep to process pending trials left by previous jobs; added more
# robust failure handling.
#
# Revision 1.5  2005/05/03 12:13:48  bkline
# Added check for ambiguous source IDs.
#
# Revision 1.4  2005/04/28 12:52:32  bkline
# Added splitlines() to loop that walks through lines in manifest file.
#
# Revision 1.3  2005/04/18 13:07:56  bkline
# Fixed typo (header for headers in sendRequest() method).
#
# Revision 1.2  2005/03/30 14:35:53  bkline
# Increased timeout for first database query; added 'source' parameter
# to invocation of filter to insert external sites into the protocol
# document.
#
# Revision 1.1  2005/03/15 21:12:32  bkline
# Base class for jobs that import protocol site information from outside.
#
#----------------------------------------------------------------------
import cdr, cdrdb, httplib, sys, time, zipfile, ModifyDocs, socket
import xml.dom.minidom, xml.sax.saxutils

TEST_MODE  = False
UID        = "ExternalImporter"
PWD        = "***REMOVED***"
DEVELOPER  = '***REMOVED***' # for error reports
TEST_LIMIT = 10

#----------------------------------------------------------------------
# Extracts XML elements from a SOAP server's response, or the zipfile
# containing the participating site information, depending on what
# we got.
#----------------------------------------------------------------------
class SoapResponse:
    def __init__(self, bytes):
        self.bytes = bytes
        self.zipfile = bytes.startswith("PK") and bytes or ""
        self.status = ""
        self.statusText = ""
        if not self.zipfile:
            dom = xml.dom.minidom.parseString(bytes)
            for node in dom.documentElement.childNodes:
                if node.nodeName == "MESSAGE_PAYLOAD":
                    for child in node.childNodes:
                        if child.nodeName == "STATUS":
                            self.status = cdr.getTextContent(child)
                        elif child.nodeName == "STATUS_TEXT":
                            self.statusText = cdr.getTextContent(child)
            if not self.status:
                raise Exception("Malformed response: missing STATUS")
            if self.status not in ("SUCCESS", "IN_PROGRESS"):
                raise Exception("Unexpected STATUS: %s (%s)" % 
                                (self.status, self.statusText))

class ImportJob(ModifyDocs.Job):

    def __init__(self, source, comment, fileName = None, validate = False):

        ModifyDocs.Job.__init__(self, UID, PWD, None, None, comment,
                                testMode = TEST_MODE, validate = validate,
                                logFile = "d:/cdr/log/%sImport.log" % source)
        
        self.log("SiteImporter: source=%s" % source)

        self.__id         = None
        self.__conn       = cdrdb.connect()
        self.__cursor     = self.__conn.cursor()
        self.__source     = source
        self.__sourceId   = self.__loadSourceId()
        self.__loMatches  = self.__getTrialsWithMatchingLeadOrgs()
        self.__dispIds    = self.__loadDispositionIds()
        self.__fileName   = fileName
        self.__docs       = []
        self.__manifest   = None
        self.__siteFilter = open('ImportedProtocolSites.xsl').read()
        self.__sourceIds  = self.__loadSourceIdMap()
        self.__cutoff     = self.__getCutoffDate()
        self.__file       = self.__loadArchiveFile()
        self.__manifest   = self.__loadManifest()
        self.__id         = self.__createJob()
        self.__dropped    = {}
        self.__numTrials  = 0
        self.__newTrials  = 0
        self.__updTrials  = 0
        self.__unchanged  = 0
        self.__duplicates = {}
        self.__unmapped   = {}
        self.__locked     = {}
        self.__noLoMatch  = {}
        docsWithCdrId     = 0
        for name in self.__file.namelist():
            if name.lower().endswith('.xml'):
                doc = self.loadImportDoc(name)
                self.__docs.append(doc)
                if doc.cdrId:
                    docsWithCdrId += 1
                    if TEST_MODE and docsWithCdrId >= TEST_LIMIT: break
        self.log("found %d matches with CDR protocols" % docsWithCdrId)

    #------------------------------------------------------------------
    # Derived class must override this method to retrieve a compressed
    # file of participating site information.
    #------------------------------------------------------------------
    def downloadSites(self):
        raise Exception("Don't know how to download the sites file")
    
    #------------------------------------------------------------------
    # Derived class must override this method to return the string
    # used to identify protocol IDs used by this source (e.g.,
    # "CTEP ID").
    #------------------------------------------------------------------
    def getSourceIdType(self):
        raise Exception("Don't know the source ID type string")

    #------------------------------------------------------------------
    # Derived class must override this method to provide the filter
    # parameters used for preparing the XML site fragment for insertion
    # into the InScopeProtocol document.
    #------------------------------------------------------------------
    def getFilterParameters(self):
        raise Exception("Don't know what the filter parameters are")
    
    #------------------------------------------------------------------
    # Allows job types that need to modify the behavior of ImportDoc
    # to be able to derive a class from that base class and use it
    # by overriding this method.
    #------------------------------------------------------------------
    def loadImportDoc(self, name):
        try:
            return ImportDoc(self, name)
        except Exception, e:
            self.log("loadImportDoc(%s): %s" % (name, str(e)))
            self.log("job aborting")
            raise
            sys.exit(1)

    def run(self):
        if TEST_MODE:
            self.createOutputDir()
        else:
            self.log("SiteImporter running in real mode")
        for doc in self.__docs:
            self.__processDoc(doc)
        if not TEST_MODE:
            self.__processPendingDocs()
            self.__markDroppedDocs()
            self.__setJobStatus('Success')

    def lookupCdrId(self, sourceId):
        key = ImportJob.normalizeSourceId(sourceId)
        cdrIds = self.__sourceIds.get(key)
        if cdrIds:
            if len(cdrIds) > 1:
                idStrings = [("CDR%d" % cdrId) for cdrId in cdrIds]
                idsString = "; ".join(idStrings)
                self.log("ambiguous source id %s: %s" % (key, idsString))
                self.__duplicates[sourceId] = idsString
                return None
            return cdrIds[0]
        self.__unmapped[sourceId] = True
        return None

    def getFileName(self):     return self.__fileName
    def getSiteFilter(self):   return self.__siteFilter
    def getArchiveFile(self):  return self.__file
    def getConnection(self):   return self.__conn
    def getCursor(self):       return self.__cursor
    def getId(self):           return self.__id
    def getSourceId(self):     return self.__sourceId
    def getSource(self):       return self.__source
    def getCutoff(self):       return self.__cutoff
    def getDispId(self, name): return self.__dispIds.get(name)

    def sendRequest(self, host, app, body = None, method = "POST"):

        headers = {}
        if body:
            if TEST_MODE:
                f = file('soap-request.xml', 'a')
                f.write(body)
                f.close()
            headers = {
                "Content-type": "text/xml",
                "User-agent": "CDR Python Client"
            }
        try:
            conn = httplib.HTTPSConnection(host)
        except Exception, e:
            self.log("HTTPSConnection: %s" % str(e))
            self.__setJobStatus("Failure")
            sys.exit(1)
        try:
            conn.request(method, app, body, headers)
        except Exception, e:
            conn.close()
            self.log("%s: %s" % (method, str(e)))
            self.__setJobStatus("Failure")
            sys.exit(1)
        try:
            resp = conn.getresponse()
        except Exception, e:
            conn.close()
            self.log("getresponse: %s" % str(e))
            self.__setJobStatus("Failure")
            sys.exit(1)
        if resp.status != 200:
            self.log("HTTP response status: %s" % resp.status)
            self.__setJobStatus("Failure")
            sys.exit(1)
        try:
            data = resp.read()
        except Exception, e:
            self.log("HTTP read: %s" % str(e))
            conn.close()
            self.__setJobStatus("Failure")
            sys.exit(1)
        conn.close()
        try:
            resp = SoapResponse(data)
        except Exception, e:
            self.log("Response:\n%s\n" % data)
            self.log("%s\n" % str(e))
            sys.exit(1)
        if resp.status:
            self.log("SOAP status: %s (%s)" % (resp.status, resp.statusText))
        else:
            self.log("retrieved %d-byte zipfile" % len(resp.zipfile))
        return resp

    def normalizeSourceId(sourceId):
        return sourceId.strip().upper()
    normalizeSourceId = staticmethod(normalizeSourceId)
    
    #----------------------------------------------------------------------
    # Mail a report to the specified recipient list.
    #----------------------------------------------------------------------
    def sendReport(self, includeDeveloper = False):
        group   = 'Protocol Import Reviewers'
        recips  = self.__getEmailRecipients(group, includeDeveloper)
        server  = socket.gethostname()
        source  = self.__source
        pattern = "%s sites downloaded %%Y-%%m-%%d on %s" % (source, server)
        subject = time.strftime(pattern)
        sender  = "cdr@%s.nci.nih.gov" % server
        body    = self.__getEmailReportBody()
        cdr.sendMail(sender, recips, subject, body)

    #----------------------------------------------------------------------
    # Determine whether the protocol has a lead org with an UpdateMode
    # matching the job's source.
    #----------------------------------------------------------------------
    def hasMatchingLeadOrg(self, docId):
        if docId in self.__loMatches:
            return True
        if docId:
            self.log("no matching lead org for %s" % docId)
        return False

    #----------------------------------------------------------------------
    # Build a list of lead orgs wih UpdateMode matching source.
    #----------------------------------------------------------------------
    def __getTrialsWithMatchingLeadOrgs(self):
        self.__cursor.execute("""\
    SELECT m.doc_id, s.value
      FROM query_term m
      JOIN query_term t
        ON m.doc_id = t.doc_id
       AND LEFT(m.node_loc, 12) = LEFT(t.node_loc, 12)
      JOIN query_term s
        ON s.doc_id = m.doc_id
     WHERE m.path  = '/InScopeProtocol/ProtocolAdminInfo'
                   + '/ProtocolLeadOrg/UpdateMode'
       AND t.path  = '/InScopeProtocol/ProtocolAdminInfo'
                   + '/ProtocolLeadOrg/UpdateMode/@MailerType'
       AND t.value = 'Protocol_SandP'
       AND m.value = ?
       AND s.path  = '/InScopeProtocol/ProtocolAdminInfo'
                   + '/CurrentProtocolStatus'""", self.__source,
                              timeout = 500)
        trials = {}
        for docId, status in self.__cursor.fetchall():
            trials[docId] = status
        self.log("%d active trials found with UpdateMode of %s" %
                 (len(trials), self.__source))
        return trials
    
    #----------------------------------------------------------------------
    # Build the report email message body.
    #----------------------------------------------------------------------
    def __getEmailReportBody(self):
        testText = TEST_MODE and "*** TEST RUN ***\n\n" or ""
        body = """\
%sTrials in manifest file: %d

Trials with initial external sites imported: %d
Updated trials: %d
Total trials imported/updated: %d

Skipped unchanged trials: %d
Skipped duplicate trials: %d
Skipped unmapped trials: %d
Skipped locked trials: %d
Skipped trials with no lead orgs having update mode of %s: %d
Total trials skipped: %d

Trials dropped: %d

""" % (testText,
       len(self.__manifest),
       self.__newTrials,
       self.__updTrials,
       self.__newTrials + self.__updTrials,
       self.__unchanged,
       len(self.__duplicates),
       len(self.__unmapped),
       len(self.__locked),
       self.__source, len(self.__noLoMatch),
       self.__unchanged + len(self.__unmapped) + len(self.__duplicates) +
       len(self.__locked) + len(self.__noLoMatch),
       len(self.__dropped))
        if self.__dropped:
            for trialId in self.__dropped:
                body += """\
Trial %s was dropped by %s
""" % (trialId, self.__source)
            body += "\n"
        if self.__duplicates:
            for duplicate in self.__duplicates:
                body += """\
Trial ID %s matched by %s
""" % (duplicate, self.__duplicates[duplicate])
            body += "\n"
        if self.__locked:
            for locked in self.__locked:
                body += """\
CDR%d locked by %s
""" % (locked, self.__locked[locked])
            body += "\n"
        if self.__noLoMatch:
            for noLoMatch in self.__noLoMatch:
                body += """\
CDR%d has no lead org with update mode of %s
""" % (noLoMatch, self.__source)
            body += "\n"
        if self.__unmapped:
            for unmapped in self.__unmapped:
                body += """\
Trial ID %s not matched by any CDR document
""" % unmapped
            body += "\n"
        newLine = ""
        missingDocs = self.__getMissingDocs()
        for cdrId, dropped in missingDocs:
            if not dropped:
                newLine = "\n"
                body += """\
CDR%d has lead org(s) with UpdateMode of %s but no site info ever received
""" % (cdrId, self.__source)
        body += newLine
        for cdrId, dropped in missingDocs:
            if dropped:
                body += """\
CDR%d has lead org(s) with UpdateMode of %s but trial has been dropped
""" % (cdrId, self.__source)
        return body
       
    #----------------------------------------------------------------------
    # Compile a list of documents we should have gotten but didn't.
    #----------------------------------------------------------------------
    def __getMissingDocs(self):
        self.__cursor.execute("""\
            SELECT cdr_id, dropped
              FROM import_doc
             WHERE source = ?
               AND cdr_id IS NOT NULL""", self.__sourceId)
        receivedDocs = {}
        for row in self.__cursor.fetchall():
            receivedDocs[row[0]] = row[1]
        missingDocs = []
        for cdrId in self.__loMatches:
            if self.__loMatches[cdrId].upper() == 'ACTIVE':
                if cdrId not in receivedDocs:
                    missingDocs.append((cdrId, False))
                elif receivedDocs[cdrId]:
                    missingDocs.append((cdrId, True))
        missingDocs.sort()
        return missingDocs

    #----------------------------------------------------------------------
    # Gather a list of email recipients for reports.
    #----------------------------------------------------------------------
    def __getEmailRecipients(self, recipGroup, includeDeveloper = False):
        try:
            self.__cursor.execute("""\
                SELECT u.email
                  FROM usr u
                  JOIN grp_usr gu
                    ON gu.usr = u.id
                  JOIN grp g
                    ON g.id = gu.grp
                 WHERE g.name = ?
                   AND u.expired IS NULL
                   AND u.email IS NOT NULL
                   AND u.email <> ''""", recipGroup)
            recips = [row[0] for row in self.__cursor.fetchall()]
            if includeDeveloper and DEVELOPER not in recips:
                recips.append(DEVELOPER)
            return recips
        except:
            if includeDeveloper:
                return [DEVELOPER]
            else:
                return []

    def __createJob(self):
        if not TEST_MODE:
            self.__cursor.execute("""\
                INSERT INTO import_job (dt, source, status)
                     VALUES (GETDATE(), ?, 'In progress')""", self.__sourceId)
            self.__conn.commit()
            self.__cursor.execute("SELECT @@IDENTITY")
            return int(self.__cursor.fetchall()[0][0])
        return None

    def __setJobStatus(self, status):
        if not TEST_MODE and self.__id:
            self.__cursor.execute("""\
                UPDATE import_job
                   SET status = ?
                 WHERE id = ?""", (status, self.__id))
            self.__conn.commit()

    def __loadArchiveFile(self):
        if self.__fileName:
            self.log("loading sites from %s" % self.__fileName)
        else:
            source          = self.__source.replace(' ', '').lower()
            pattern         = "%Y%m%d%H%M%S"
            pattern         = "imported-sites/%s-%s.zip" % (source, pattern)
            self.__fileName = time.strftime(pattern)
            self.log("Downloading %s" % self.__fileName)
            sites           = self.downloadSites()
            try:
                fp = file(self.__fileName, "wb")
                fp.write(sites)
                fp.close()
                self.log("saved %s" % self.__fileName)
            except Exception, e:
                self.log("saving zipfile: %s" % str(e))
                sys.exit(1)
        try:
            return zipfile.ZipFile(self.__fileName)
        except Exception, e:
            self.log("zipfile: %s" % str(e))
            sys.exit(1)

    def __loadManifest(self):
        manifest = {}
        manifestName = 'manifest.txt'
        for name in self.__file.namelist():
            if name.lower() == 'manifest.txt':
                manifestName = name
        try:
            for line in self.__file.read(manifestName).splitlines():
                line = line.strip()
                try:
                    id, status = line.split(' , ', 1)
                except:
                    try:
                        id, status = line.split('\t', 1)
                    except:
                        id, status = line, None
                if not status or status.upper() != 'CLOSED':
                    manifest[id.upper()] = status
        except Exception, e:
            self.log("__loadManifest(%s) failure: %s" % (manifestName, str(e)))
        return manifest

    def __loadSourceIdMap(self):
        idMap = {}
        sourceIdType = self.getSourceIdType()
        if sourceIdType == "CDR":
            self.__cursor.execute("""\
                SELECT d.id
                  FROM document d
                  JOIN doc_type t
                    ON t.id = d.doc_type
                 WHERE t.name = 'InScopeProtocol'""")
            for row in self.__cursor.fetchall():
                idMap[u"CDR%010d" % row[0]] = [row[0]]
            return idMap
        institutionPrefix = "@Institution="
        if sourceIdType.startswith(institutionPrefix):
            institution = sourceIdType[len(institutionPrefix):]
            self.__cursor.execute("""\
            SELECT DISTINCT i.doc_id, i.value
                       FROM query_term i
                       JOIN query_term t
                         ON i.doc_id = t.doc_id
                        AND LEFT(i.node_loc, 8) = LEFT(t.node_loc, 8)
                       JOIN query_term o
                         ON o.doc_id = t.doc_id
                        AND LEFT(o.node_loc, 8) = LEFT(t.node_loc, 8)
                      WHERE t.path = '/InScopeProtocol/ProtocolIDs/OtherID'
                                   + '/IDType'
                        AND i.path = '/InScopeProtocol/ProtocolIDs/OtherID'
                                   + '/IDString'
                        AND o.path = '/InScopeProtocol/ProtocolIDs/OtherID'
                                   + '/@Institution'
                        AND t.value = 'Institutional/Original'
                        AND o.value = ?""", institution, timeout = 300)
        else:
            self.__cursor.execute("""\
            SELECT DISTINCT i.doc_id, i.value
                       FROM query_term i
                       JOIN query_term t
                         ON i.doc_id = t.doc_id
                        AND LEFT(i.node_loc, 8) = LEFT(t.node_loc, 8)
                      WHERE t.path = '/InScopeProtocol/ProtocolIDs/OtherID'
                                   + '/IDType'
                        AND i.path = '/InScopeProtocol/ProtocolIDs/OtherID'
                                   + '/IDString'
                        AND t.value = ?""", sourceIdType, timeout = 300)
        for cdrId, sourceId in self.__cursor.fetchall():
            key = ImportJob.normalizeSourceId(sourceId)
            if key in idMap:
                cdrIds = idMap[key]
                if cdrId not in cdrIds:
                    cdrIds.append(cdrId)
            else:
                idMap[key] = [cdrId]
        if TEST_MODE:
            sys.stderr.write("ID map loaded with %d IDs\n" % len(idMap))
        return idMap

    def __getCutoffDate(self):
        if self.__fileName:
            return ""
        self.__cursor.execute("""\
            SELECT MAX(dt)
              FROM import_job
             WHERE source = ?
               AND status = 'Success'""", self.__sourceId)
        rows = self.__cursor.fetchall()
        if not (rows and rows[0][0]):
            self.log("no previous successful jobs recorded")
            return ""
        dt = str(rows[0][0])
        cutoff = dt[:10]
        self.log("cutoff from database is %s" % cutoff)
        return cutoff

    def __markDroppedDocs(self):    
        self.__cursor.execute("""\
            SELECT id, source_id
              FROM import_doc
             WHERE source = ?
               AND dropped IS NULL""", self.__sourceId)
        rows = self.__cursor.fetchall()
        for id, sourceId in rows:
            if sourceId.upper() not in self.__manifest:
                self.__dropped[sourceId] = True
                self.__cursor.execute("""\
                    UPDATE import_doc
                       SET dropped = GETDATE()
                     WHERE id = ?""", id)
                self.__conn.commit()

    def __processPendingDocs(self):
        self.__cursor.execute("""\
            SELECT id, source_id, cdr_id, downloaded, changed
              FROM import_doc
             WHERE disposition = %d
               AND source = %d
               AND id NOT IN (SELECT doc
                                FROM import_event
                               WHERE job = %d)
               AND dropped IS NULL""" % (self.getDispId('pending'),
                                         self.__sourceId,
                                         self.__id))
        rows = self.__cursor.fetchall()
        docs = []
        savedComment = self.comment
        for importDocId, sourceId, cdrId, downloaded, changed in rows:
            when = changed or downloaded
            self.comment = ("%s (delayed import from sites document "
                            "downloaded %s)" % (savedComment, when))
            docs.append(ImportDoc(self,
                                  sourceId = sourceId,
                                  importDocId = importDocId,
                                  oldCdrId = cdrId))
        self.comment = savedComment
        if docs:
            self.log("Processing %d docs left over from previous jobs" %
                     len(docs))
            for doc in docs:
                self.__processDoc(doc)
        
    def __loadSourceId(self):
        self.__cursor.execute("""\
            SELECT id
              FROM import_source
             WHERE name = ?""", self.__source)
        return self.__cursor.fetchall()[0][0]

    def __loadDispositionIds(self):
        self.__cursor.execute("SELECT id, name FROM import_disposition")
        dispositions = {}
        for id, name in self.__cursor.fetchall():
            dispositions[name.lower()] = id
        return dispositions

    def __findLocker(self, cdrId):
        self.__cursor.execute("""\
            SELECT u.name
              FROM usr u
              JOIN checkout c
                ON c.usr = u.id
             WHERE c.id = ?
               AND c.dt_in IS NULL""", cdrId)
        rows = self.__cursor.fetchall()
        if rows:
            return rows[0][0]
        return "unidentified user"

    def __processDoc(self, doc):
        if not doc.cdrId:
            self.log("No match for %s" % doc.sourceId)
        else:
            try:
                if doc.locked:
                    self.__locked[doc.cdrId] = self.__findLocker(doc.cdrId)
                    self.log("Doc %s locked for %s" % (doc.cdrId,
                                                       doc.sourceId))
                    if not TEST_MODE:
                        doc.recordEvent()
                elif not doc.loMatch:
                    self.__noLoMatch[doc.cdrId] = doc
                    self.log("Doc %s has no lead org with %s update mode" %
                             (doc.cdrId, self.__source))
                    if not TEST_MODE:
                        doc.recordEvent()
                elif doc.cdrDoc:
                    if (TEST_MODE or doc.new or doc.changed or
                        doc.newCdrId or doc.pending):
                        
                        self.log("Updating %s from %s" % (doc.cdrId,
                                                          doc.sourceId))
                        doc.cdrDoc.saveChanges(self.__cursor, logger = self)
                        if not TEST_MODE:
                            doc.recordEvent()
                        if doc.new:
                            self.__newTrials += 1
                        else:
                            self.__updTrials += 1
                    else:
                        self.__unchanged += 1
                        self.log("No changes for %s" % doc.sourceId)
            except Exception, e:
                self.log("__processDoc(): %s" % str(e))
            if not doc.locked and doc.cdrDoc:
                try:
                    cdr.unlock(self.session, "CDR%010d" % doc.cdrDoc.id)
                except:
                    pass

class ImportDoc:
    
    def __init__(self, importJob, name = None, sourceId = None,
                 importDocId = None, oldCdrId = None):
        self.impJob   = importJob
        self.name     = name
        self.sourceId = sourceId or name and name[:-4] or None
        self.new      = False  # Never seen this external protocol ID before
        self.newCdrId = False  # External ID mapped to a different CDR doc
        self.changed  = False
        self.sites    = None
        self.locked   = False
        self.errMsg   = None
        self.cdrId    = importJob.lookupCdrId(self.sourceId)
        self.loMatch  = importJob.hasMatchingLeadOrg(self.cdrId)
        self.cdrDoc   = None
        self.pending  = importDocId and True or False
        self.siteXml  = self.loadSiteDocXml(name, importDocId)
        self.status   = self.extractProtocolStatus(self.siteXml)
        if self.cdrId:
            if TEST_MODE:
                sys.stderr.write("matched %s with %s\n" % (self.sourceId,
                                                           self.cdrId))
            self.sites = self.filterSiteXml()
            try:
                self.cdrDoc = ModifyDocs.Doc(self.cdrId, importJob.session,
                                             self, importJob.comment)
            except ModifyDocs.DocumentLocked, e:
                self.locked = True
                self.errMsg = str(e)
            except Exception, e:
                self.errMsg = str(e)
            if self.errMsg:
                importJob.log("CDR %s: %s" % (self.cdrId, self.errMsg))
        if not TEST_MODE:
            self.importDocId = self.getImportDocId(importDocId, oldCdrId)

    def run(self, docObj):
        status = self.status
        normalizedStatus = status.upper().strip()
        if normalizedStatus == 'TEMPORARILY CLOSED TO ACCRUAL':
            status = 'Temporarily closed'
        elif normalizedStatus.startswith('CLOSED TO ACCRUAL'):
            status = 'Closed'
        elif normalizedStatus == 'COMPLETE':
            status = 'Completed'
        elif normalizedStatus == 'APPROVED':
            status = 'Approved-not yet active'
        else:
            status = status or ''
            
        parms = (('source', self.impJob.getSource()),
                 ('lastModified', time.strftime("%Y-%m-%d")),
                 ('status', status),
                 ('user', UID))
        newXml = cdr.filterDoc('guest', ['name:Insert External Sites'],
                               doc = docObj.xml, parm = parms)
        if type(newXml) in (type(""), type(u"")):
            self.impJob.log("CDR%d: %s" % (self.cdrId, newXml))
            return docObj.xml
        if newXml[1]:
            self.impJob.log("CDR%d: %s" % (self.cdrId, newXml[1]))
        return newXml[0].replace("@@EXTERNALSITES@@", self.sites)
    
    def loadSiteDocXml(self, name = None, importDocId = None):
        if name:
            self.rawXml = unicode(self.impJob.getArchiveFile().read(name),
                                  'utf-8')
        else:
            cursor = self.impJob.getCursor()
            cursor.execute("""\
                SELECT xml
                  FROM import_doc
                 WHERE id = ?""", importDocId)
            self.rawXml = cursor.fetchall()[0][0]
        lines = self.rawXml.split(u"\n")
        if lines[1].find('DOCTYPE') != -1 and lines[1].find(".dtd") != -1:
            lines[1:2] = []
        return u"\n".join(lines).encode('utf-8')

    def extractProtocolStatus(self, siteXml):
        dom = xml.dom.minidom.parseString(siteXml)
        for node in dom.documentElement.childNodes:
            if node.nodeName == 'Protocol_Status':
                return cdr.getTextContent(node).strip()
        
    def filterSiteXml(self):
        resp = cdr.filterDoc('guest', self.impJob.getSiteFilter(),
                             doc = self.siteXml, inline = True,
                             parm = self.impJob.getFilterParameters())
        if type(resp) in (type(""), type(u"")):
            raise Exception(u"filterSiteXml(): %s" % resp)
        return self.addContact(resp[0])

    def addContact(self, docXml):
        if type(docXml) != type(u""):
            docXml = unicode(docXml, "utf-8")
        openDelim = u"@@ctepId="
        closeDelim = u"@@"
        pos = docXml.find(openDelim)
        while pos != -1:
            end = docXml.find(closeDelim, pos + 1)
            if end == -1:
                raise Exception("addContact(): missing terminating delimiter")
            id = docXml[pos + len(openDelim) : end]
            if not id:
                print "addContact(): empty CTEP ID"
                rows = []
            else:
                try:
                    self.impJob.getCursor().execute("""\
                SELECT r.element, r.value
                  FROM external_map_rule r
                  JOIN external_map m
                    ON m.id = r.map_id
                  JOIN external_map_usage u
                    ON u.id = m.usage
                 WHERE u.name = 'CTEP_Institution_Code'
                   AND m.value = ?""", id)
                    rows = self.impJob.getCursor().fetchall()
                except Exception, e:
                    print "addContact(): %s" % str(e)
                    print "id=[%s]" % repr(id)
                    #print "pos=%d end=%d len(id)=%d" % (pos, end, len(id))
                    rows = []
            title = u""
            phone = u""
            for row in rows:
                elem = u"<%s>%s</%s>" % (row[0], row[1], row[0])
                if row[0] == u"ExternalSiteContactTitle":
                    title = elem
                elif row[0] == u"ExternalSiteContactPhone":
                    phone = elem
            if title or phone:
                replacement = (u"<ExternalSiteContact>%s%s"
                               u"</ExternalSiteContact>" % (title, phone))
            else:
                replacement = u""
            end += len(closeDelim)
            docXml = docXml[:pos] + replacement + docXml[end:]
            pos = docXml.find(openDelim)
        return docXml.encode('utf-8')

    #------------------------------------------------------------------
    # Create or update a row in the import_doc table, returning the
    # primary key for the row.  For the loop through the documents
    # contained in the downloaded archive, the importDocId and
    # oldCdrId parameters will be None.  For the pass to process
    # documents left hanging from previous jobs (disposition left
    # as 'pending' and dropped column NULL), the parameters will
    # be populated with real values.
    #------------------------------------------------------------------
    def getImportDocId(self, importDocId = None, oldCdrId = None):
        
        cursor = self.impJob.getCursor()
        conn   = self.impJob.getConnection()
        disp   = self.cdrId and 'pending' or 'unmatched'
        dispId = self.impJob.getDispId(disp)

        # Handle documents just pulled from the downloaded file here.
        if not importDocId:

            # Have we seen this document before?
            cursor.execute("""\
                SELECT id, xml, cdr_id, dropped
                  FROM import_doc
                 WHERE source = ?
                   AND source_id = ?""", (self.impJob.getSourceId(),
                                          self.sourceId))
            rows = cursor.fetchall()

            # No: create new row in import_doc table.
            if not rows:
                self.new = True
                cursor.execute("""\
        INSERT INTO import_doc (source, source_id, xml, downloaded,
                                disposition, disp_dt, cdr_id)
             VALUES (?, ?, ?, GETDATE(), ?, GETDATE(), ?)""",
                               (self.impJob.getSourceId(),
                                self.sourceId, self.rawXml, dispId,
                                self.cdrId))
                conn.commit()
                cursor.execute("SELECT @@IDENTITY")
                importDocId = cursor.fetchall()[0][0]

            # Yes: update the row as appropriate.
            else:
                (importDocId, oldXml, oldCdrId, dropped) = rows[0]
                self.changed = oldXml != self.rawXml
                if self.cdrId and self.cdrId != oldCdrId:
                    self.newCdrId = True

                # Was a formerly dropped document re-sent?
                if dropped:
                    cursor.execute("""\
                        UPDATE import_doc
                           SET dropped = NULL
                         WHERE id = ?""", importDocId)
                    conn.commit()

                # If the XML changed, store the new information.
                if self.changed:
                    cursor.execute("""\
                        UPDATE import_doc
                           SET xml = ?,
                               changed = GETDATE(),
                               disposition = ?,
                               disp_dt = GETDATE()
                         WHERE id = ?""", (self.rawXml, dispId, importDocId))
                    conn.commit()

        # We already have the importDocId (pending doc from previous job).
        else:
            # Do we still have a matching CDR document?
            if self.cdrId:
                if self.cdrId != oldCdrId:
                    self.newCdrId = True

            # No: replace 'pending' disposition with 'unmatched'.
            else:
                cursor.execute("""\
                    UPDATE import_doc
                       SET disposition = ?
                     WHERE id = ?""", (dispId, importDocId))
                conn.commit()

        # Handle case where CDR ID changed.
        if self.newCdrId:
            if oldCdrId:
                self.impJob.log("New CDR ID %d for %s; old ID was %d" %
                                (self.cdrId, self.sourceId, oldCdrId))
            cursor.execute("""\
                UPDATE import_doc
                   SET cdr_id = ?,
                       disposition = ?,
                       disp_dt = GETDATE()
                 WHERE id = ?""", (self.cdrId, dispId, importDocId))
            conn.commit()

        # Give the caller the primary key for the import_doc row.
        return importDocId

    def recordEvent(self):
        cursor     = self.impJob.getCursor()
        conn       = self.impJob.getConnection()
        versions   = cdr.lastVersions('guest', "CDR%010d" % self.cdrId)
        pubVersion = (self.cdrDoc and
                      (versions[1] != self.cdrDoc.versions[1]) and 'Y' or 'N')
        new        = self.new and 'Y' or 'N'
        locked     = self.locked and 'Y' or 'N'
        dispId     = self.impJob.getDispId('imported')
        cursor.execute("""\
            INSERT INTO import_event (job, doc, locked, new, pub_version)
                 VALUES (?, ?, ?, ?, ?)""", (self.impJob.getId(),
                                             self.importDocId,
                                             locked, new, pubVersion),
                       timeout = 300)
        conn.commit()
        if self.loMatch and not self.locked:
            cursor.execute("""\
                UPDATE import_doc
                   SET disposition = ?,
                       disp_dt = GETDATE()
                 WHERE id = ?""", (dispId, self.importDocId), timeout = 300)
            conn.commit()
