#----------------------------------------------------------------------
#
# $Id: ModifyDocs.py,v 1.10 2005-03-04 19:19:45 bkline Exp $
#
# Harness for one-off jobs to apply a custom modification to a group
# of CDR documents.
#
# $Log: not supported by cvs2svn $
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
import cdr, cdrdb, cdrglblchg, sys, time, re

LOGFILE = 'd:/cdr/log/ModifyDocs.log'
ERRPATT = re.compile(r"<Err>(.*?)</Err>", re.DOTALL)
DEBUG   = 0

#----------------------------------------------------------------------
# Custom exception indicating that we can't check out a document.
#----------------------------------------------------------------------
class DocumentLocked(Exception): pass

#----------------------------------------------------------------------
# Module level variables (statics)
#----------------------------------------------------------------------
_testMode  = True   # True=Output to files only, not database
_outputDir = None   # Files go in this directory

#----------------------------------------------------------------------
# Class for one modification job.
#----------------------------------------------------------------------
class Job:

    def __init__(self, uid, pwd, filter, transform, comment, testMode=True,
                 logFile = LOGFILE):
        """
        Create a new one-off job to apply a custom modification to
        a group of CDR documents.

        Pass:
            uid        - CDR user ID of operator
            pwd        - password for CDR account
            filter     - object with method to get document IDs to be
                         processed; must have method getDocIds()
            transform  - object which knows how to take the old XML
                         for a CDR document and transform it using
                         the algorithm appropriate to this job; the
                         name of this method must be run()
            comment    - string to be stored with new versions
            testMode   - True = write output to files, do not update database.
                         False = Modify the database.
            logFile    - optional path for logfile
        """
        global _testMode

        self.logFile   = open(logFile, 'a')
        self.uid       = uid
        self.pwd       = pwd
        self.filter    = filter
        self.transform = transform
        self.comment   = comment
        self.conn      = cdrdb.connect('CdrGuest')
        self.cursor    = self.conn.cursor()
        self.session   = cdr.login(uid, pwd)
        _testMode      = testMode
        error          = cdr.checkErr(self.session)
        if error:
            raise Exception("Failure logging into CDR: %s" % error)

    def run(self):
        # In test mode, create output directory for files
        global _testMode, _outputDir
        if _testMode:
            # Raises exception to exit program if fails
            _outputDir = cdrglblchg.createOutputDir()
            self.log("Running in test mode.  Output to: %s" % _outputDir)
        else:
            self.log("Running in real mode.  Updating the database")

        # Change all docs
        ids = self.filter.getDocIds()
        self.log("%d documents selected" % len(ids))
        for docId in ids:
            try:
                self.log("Processing CDR%010d" % docId)
                doc = Doc(docId, self.session, self.transform, self.comment)
                doc.saveChanges(self)
            except Exception, info:
                self.log("Document %d: %s" % (docId, str(info)))
                if DEBUG:
                    raise
            cdr.unlock(self.session, "CDR%010d" % docId)

    #------------------------------------------------------------------
    # Log processing/error information with a timestamp.
    #------------------------------------------------------------------
    def log(self, what):
        what = "%s: %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), what)
        self.logFile.write(what)
        sys.stderr.write(what)

    def __del__(self):
        try:
            self.logFile.close()
        except:
            pass

#----------------------------------------------------------------------
# Class for a CDR document.
#----------------------------------------------------------------------
class Doc:
    def __init__(self, id, session, transform, comment):
        self.id        = id
        self.session   = session
        self.transform = transform
        self.comment   = comment
        self.versions  = cdr.lastVersions('guest', "CDR%010d" % id)
        self.loadAndTransform()

    #------------------------------------------------------------------
    # Uses xml comparator from cdr module.
    #------------------------------------------------------------------
    def compare(self, a, b):
        return cdr.compareXmlDocs(a, b)

    #------------------------------------------------------------------
    # Check out the CWD, last version, and last publishable version,
    # and run them through the transformation for this job.
    #------------------------------------------------------------------
    def loadAndTransform(self):
        self.cwd       = cdr.getDoc(self.session, self.id, 'Y', getObject = 1)
        if type(self.cwd) in (type(""), type(u"")):
            err = cdr.checkErr(self.cwd) or self.cwd
            raise DocumentLocked("Unable to check out CWD for CDR%010d: %s" %
                                 (self.id, err))
        self.newCwd    = self.transform.run(self.cwd)
        self.lastv     = None
        self.lastp     = None
        self.newLastv  = None
        self.newLastp  = None
        (lastAny, lastPub, isChanged) = self.versions
        if lastAny > 0:
            if isChanged == 'Y':
                self.lastv = cdr.getDoc(self.session, self.id, 'Y',
                                        str(lastAny),
                                        getObject = 1)
                if type(self.lastv) in (type(""), type(u"")):
                    err = cdr.checkErr(self.lastv) or self.lastv
                    raise Exception("Failure retrieving lastv (%d) "
                                    "for CDR%010d: %s" % (lastAny,
                                                       self.id, err))
                self.newLastv = self.transform.run(self.lastv)
            else:
                self.lastv    = self.cwd
                self.newLastv = self.newCwd
        if lastPub > 0:
            if lastPub == lastAny:
                self.lastp    = self.lastv
                self.newLastp = self.newLastv
            else:
                self.lastp = cdr.getDoc(self.session, self.id, 'Y',
                                        str(lastPub),
                                        getObject = 1)
                if type(self.lastp) in (type(""), type(u"")):
                    err = cdr.checkErr(self.lastp) or self.lastp
                    raise Exception("Failure retrieving lastp (%d) "
                                    "for CDR%010d: %s" % (lastPub,
                                                       self.id, err))
                self.newLastp = self.transform.run(self.lastp)

    def saveChanges(self, job):
        """
        Saves versions for a document according to the following logic:

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
        If LPV(t) <> LPV:
            Create new PV using LPV(t)
        If LV(t) <> LV:
            Create new NPV from LV(t)
        If CWD(t) <> LS:
            Create new CWD using CWD(t)
        """
        global _testMode, _outputDir
        lastSavedXml  = None
        docId         = self.cwd.id
        everValidated = self.lastp and True or self.__everValidated(job.cursor)
        #job.log("everValidated = %s" % everValidated)

        # Only save to database in run mode
        # Only write to output files in test mode

        if _testMode:
            # Write new/original CWDs
            cdrglblchg.writeDocs(_outputDir, docId,
                                 self.cwd.xml, self.newCwd, 'cwd')
        if self.lastv and self.compare(self.cwd.xml, self.lastv.xml):
            if not _testMode:
                # Save old CWD as new version
                self.__saveDoc(str(self.cwd), ver='Y', pub='N', job=job,
                               val = (everValidated and 'Y' or 'N'),
                               logWarnings = False)
            lastSavedXml = self.lastv.xml
        if self.lastp and self.compare(self.lastp.xml, self.newLastp):
            if _testMode:
                # Write new/original last pub version
                cdrglblchg.writeDocs(_outputDir, docId,
                                     self.lastp.xml, self.newLastp, 'pub')
            else:
                # Save new last pub version
                self.lastp.xml = lastSavedXml = self.newLastp
                self.__saveDoc(str(self.lastp), ver='Y', pub='Y', job=job,
                               val = (everValidated and 'Y' or 'N'))

        #--------------------------------------------------------------
        # Note that in the very common case in which the last created
        # version and the most recent publishable version are the same
        # version, the test below will report no differences between
        # self.lastv.xml and self.newLastv.  This is because in this
        # case self.lastv and self.lastp are references to the same
        # cdr.Doc object, and the assignment above of self.newLastp
        # to self.lastp.xml also changes self.lastv.xml.  This is
        # almost too clever and tricky (it has confused the author
        # of this code on at least one occasion -- hence this comment),
        # but it's more efficient than doing deep copies of the cdr.Doc
        # objects, and (equally important) it does the Right Thing.
        #--------------------------------------------------------------
        if self.lastv and self.compare(self.lastv.xml, self.newLastv):
            if _testMode:
                # Write new/original last non-pub version
                cdrglblchg.writeDocs(_outputDir, docId,
                                     self.lastv.xml, self.newLastv, 'lastv')
            else:
                # Save new last non-pub version
                self.lastv.xml = lastSavedXml = self.newLastv
                self.__saveDoc(str(self.lastv), ver='Y', pub='N', job=job,
                               val = (everValidated and 'Y' or 'N'))
        if lastSavedXml and self.compare(self.newCwd, lastSavedXml):
            if not _testMode:
                # Save new CWD
                self.cwd.xml = self.newCwd
                self.__saveDoc(str(self.cwd), ver='N', pub='N', job=job,
                               val = (everValidated and 'Y' or 'N'))

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
    def __saveDoc(self, docStr, ver, pub, job, val = 'Y', logWarnings = True):
        job.log("saveDoc(%d, ver='%s' pub='%s' val='%s')" % (self.id, ver, 
                                                             pub, val))
        # return 1

        response = cdr.repDoc(self.session, doc = docStr, ver = ver,
                              val = val,
                              verPublishable = pub,
                              reason = self.comment, comment = self.comment,
                              showWarnings = 'Y')
        if not response[0]:
            raise Exception("Failure saving changes for CDR%010d: %s" %
                            (self.id, response[1]))
        if logWarnings and response[1]:
            warnings = ERRPATT.findall(response[1])
            if warnings:
                for warning in warnings:
                    job.log("Warning for CDR%010d: %s" % (self.id, warning))
            else:
                job.log("Warning for CDR%010d: %s" % (self.id, response[1]))
