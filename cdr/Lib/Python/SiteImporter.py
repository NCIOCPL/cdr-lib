#----------------------------------------------------------------------
#
# $Id: SiteImporter.py,v 1.1 2005-03-15 21:12:32 bkline Exp $
#
# Base class for importing protocol site information from external sites.
#
# $Log: not supported by cvs2svn $
#----------------------------------------------------------------------
import cdr, cdrdb, httplib, sys, time, zipfile, ModifyDocs

TEST_MODE = False
UID       = "ExternalImporter"
PWD       = "***REMOVED***"

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
            import xml.dom.minidom
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

    def __init__(self, source, comment, fileName = None):

        ModifyDocs.Job.__init__(self, UID, PWD, None, None, comment,
                                testMode = TEST_MODE)
        
        self.__conn       = cdrdb.connect()
        self.__cursor     = self.__conn.cursor()
        self.__source     = source
        self.__sourceId   = self.__loadSourceId()
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
        docsWithCdrId     = 0
        for name in self.__file.namelist():
            if name.lower().endswith('.xml'):
                doc = self.loadImportDoc(name)
                self.__docs.append(doc)
                if doc.cdrId:
                    docsWithCdrId += 1
                    if TEST_MODE and docsWithCdrId >= 10: break
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
        return ImportDoc(self, name)

    def run(self):
        for doc in self.__docs:
            if doc.cdrId:
                if doc.locked:
                    self.log("Doc %s locked for %s" % (doc.cdrId,
                                                       doc.sourceId))
                    if not TEST_MODE:
                        doc.recordEvent()
                elif doc.cdrDoc:
                    if doc.new or doc.changed or doc.newCdrId:
                        self.log("Updating %s from %s" % (doc.cdrId,
                                                          doc.sourceId))
                        doc.cdrDoc.saveChanges(self)
                        if not TEST_MODE:
                            doc.recordEvent()
                    else:
                        self.log("No changes for %s" % doc.sourceId)
                    cdr.unlock(self.session, "CDR%010d" % doc.cdrDoc.id)
            else:
                self.log("No match for %s" % doc.sourceId)
        if not TEST_MODE:
            self.__markDroppedDocs()
            self.__setJobStatus('Success')

    def lookupCdrId(self, sourceId):
        return self.__sourceIds.get(ImportJob.normalizeSourceId(sourceId))

    def getFileName(self):     return self.__fileName
    def getSiteFilter(self):   return self.__siteFilter
    def getArchiveFile(self):  return self.__file
    def getConnection(self):   return self.__conn
    def getCursor(self):       return self.__cursor
    def getId(self):           return self.__id
    def getSourceId(self):     return self.__sourceId
    def getCutoff(self):       return self.__cutoff
    def getDispId(self, name): return self.__dispIds.get(name)

    def sendRequest(self, host, app, body = None, method = "POST"):

        header = {}
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
    
    def __createJob(self):
        if not TEST_MODE:
            self.__cursor.execute("""\
                INSERT INTO import_job (dt, source, status)
                     VALUES (GETDATE(), ?, 'In progress')""", self.__sourceId)
            self.__conn.commit()
            self.__cursor.execute("SELECT @@IDENTITY")
            return self.__cursor.fetchall()[0][0]

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
        try:
            for line in self.__file.read('manifest.txt'):
                try:
                    id, status = line.strip().split(' , ', 1)
                except:
                    id, status = line.strip(), None
                manifest[id] = status
        except:
            pass
        return manifest

    def __loadSourceIdMap(self):
        idMap = {}
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
                        AND t.value = ?""", self.getSourceIdType())
        for cdrId, sourceId in self.__cursor.fetchall():
            idMap[ImportJob.normalizeSourceId(sourceId)] = cdrId
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
        self.log("cutoff is %s" % cutoff)
        return cutoff

    def __markDroppedDocs(self):    
        self.__cursor.execute("""\
            SELECT id, source_id
              FROM import_doc
             WHERE source = ?
               AND dropped IS NOT NULL""", self.__sourceId)
        rows = self.__cursor.fetchall()
        for id, sourceId in rows:
            if sourceId not in self.__manifest:
                self.__cursor.execute("""\
                    UPDATE import_doc
                       SET dropped = GETDATE()
                     WHERE id = ?""", id)
                self.__conn.commit()

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

class ImportDoc:
    
    def __init__(self, importJob, name):
        self.impJob   = importJob
        self.name     = name
        self.sourceId = name[:-4]
        self.new      = False  # Never seen this external protocol ID before
        self.newCdrId = False  # External ID mapped to a different CDR doc
        self.changed  = False
        self.sites    = None
        self.locked   = False
        self.errMsg   = None
        self.cdrId    = importJob.lookupCdrId(self.sourceId)
        self.siteXml  = self.loadSiteDocXml(name)
        self.cdrDoc   = None
        if self.cdrId:
            self.sites = self.filterSiteXml()
            if TEST_MODE:
                sys.stderr.write("matched %s with %s\n" % (self.sourceId,
                                                           self.cdrId))
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
            self.importDocId = self.getImportDocId()

    def run(self, docObj):
        newXml = cdr.filterDoc('guest', ['name:Insert External Sites'],
                               doc = docObj.xml)
        if type(newXml) in (type(""), type(u"")):
            self.impJob.log("CDR%d: %s" % (self.cdrId, newXml))
            return docObj.xml
        if newXml[1]:
            self.impJob.log("CDR%d: %s" % (self.cdrId, newXml[1]))
        return newXml[0].replace("@@EXTERNALSITES@@", self.sites)
    
    def loadSiteDocXml(self, name):
        self.rawXml = self.impJob.getArchiveFile().read(name)
        lines = self.rawXml.split("\n")
        if lines[1].find('DOCTYPE') != -1 and lines[1].find(".dtd") != -1:
            lines[1:2] = []
        doc = "\n".join(lines)
        return doc

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
    
    def getImportDocId(self):
        
        cursor = self.impJob.getCursor()
        conn   = self.impJob.getConnection()
        cursor.execute("""\
            SELECT id, xml, cdr_id, dropped
              FROM import_doc
             WHERE source = ?
               AND source_id = ?""", (self.impJob.getSourceId(),
                                      self.sourceId))
        rows   = cursor.fetchall()
        disp   = self.cdrId and 'pending' or 'unmatched'
        dispId = self.impJob.getDispId(disp)
        if rows:
            (id, siteXml, cdrId, dropped) = rows[0]
            self.changed                  = siteXml != self.rawXml
            if self.cdrId and self.cdrId != cdrId:
                self.newCdrId = True
            if self.changed:
                cursor.execute("""\
                    UPDATE import_doc
                       SET xml = ?,
                           changed = GETDATE(),
                           disposition = ?,
                           disp_dt = GETDATE()
                     WHERE id = ?""", (self.rawXml, dispId, id))
                conn.commit()
            if self.newCdrId:
                if cdrId:
                    self.impJob.log("New CDR ID %d for %s; old ID was %d" %
                                    (self.cdrId, self.sourceId, cdrId))
                cursor.execute("""\
                    UPDATE import_doc
                       SET cdr_id = ?,
                           disposition = ?,
                           disp_dt = GETDATE()
                     WHERE id = ?""", (self.cdrId, dispId, id))
                conn.commit()
            if dropped:
                cursor.execute("""\
                    UPDATE import_doc
                       SET dropped = NULL
                     WHERE id = ?""", id)
                conn.commit()
        else:
            self.new = True
            cursor.execute("""\
                INSERT INTO import_doc (source, source_id, xml, downloaded,
                                        disposition, disp_dt, cdr_id)
                     VALUES (?, ?, ?, GETDATE(), ?, GETDATE(), ?)""",
                           (self.impJob.getSourceId(),
                            self.sourceId, self.rawXml, dispId, self.cdrId))
            conn.commit()
            cursor.execute("SELECT @@IDENTITY")
            id = cursor.fetchall()[0][0]
        return id
                       
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
                                             locked, new, pubVersion))
        conn.commit()
        if not self.locked:
            cursor.execute("""\
                UPDATE import_doc
                   SET disposition = ?,
                       disp_dt = GETDATE()
                 WHERE id = ?""", (dispId, self.importDocId))
            conn.commit()
