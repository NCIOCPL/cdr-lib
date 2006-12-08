#----------------------------------------------------------------------
#
# $Id: ModifyDocs.py,v 1.17 2006-12-08 03:31:56 ameyer Exp $
#
# Harness for one-off jobs to apply a custom modification to a group
# of CDR documents.
#
# $Log: not supported by cvs2svn $
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
_validate  = False  # True=Check that change didn't invalidate valid doc
_haltOnErr = False  # True=Don't save any doc if change invalidate a version

# Controls for which versions are transformed, with defaults
_transformANY = True    # Last version of any kind
_transformPUB = True    # Last publishable version

#----------------------------------------------------------------------
# Module level setters for transform version controls
#
# The way these controls work is ONLY to gate the output, not the
# transformations.  The problem is that some complex logic occurs
# in which one version can move through to become a new version of a
# different type.  We allow ALL of that to happen without interference
# and just gate the outputs.
#
# There is no _transformCWD or setTransformCWD().
# It isn't possible to save a verwion without overwriting the current
# working document.  Therefore the program ALWAYS modifies the current
# working document.  Only the last version and/or last publishable
# version can be blocked from change.
#
# Note however that a change to the CWD will always create a new last,
# non-publishable version.  That is by design to avoid complete
# obliteration of the previous CWD.
#----------------------------------------------------------------------
def setTransformANY(setting):
    global _transformANY
    _transformANY = setting

def setTransformPUB(setting):
    global _transformPUB
    _transformPUB = setting

#----------------------------------------------------------------------
# Class for one modification job.
#----------------------------------------------------------------------
class Job:

    def __init__(self, uid, pwd, filter, transform, comment, testMode=True,
                 logFile=LOGFILE, validate=False, haltOnValErr=False):
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
        global _validate
        global _haltOnErr

        self.logFile   = open(logFile, 'a')
        self.uid       = uid
        self.pwd       = pwd
        self.filter    = filter
        self.transform = transform
        self.comment   = comment
        self.conn      = cdrdb.connect('CdrGuest')
        self.cursor    = self.conn.cursor()
        self.session   = cdr.login(uid, pwd)
        self.noStdErr  = False
        _testMode      = testMode
        _validate      = validate
        _haltOnErr     = haltOnValErr
        error          = cdr.checkErr(self.session)
        if error:
            raise Exception("Failure logging into CDR: %s" % error)

    def createOutputDir(self):
        global _outputDir

        # Raises exception to exit program if fails
        _outputDir = cdrglblchg.createOutputDir()
        self.log("Running in test mode.  Output to: %s" % _outputDir)

    def run(self):
        # In test mode, create output directory for files
        if _testMode:
            self.createOutputDir()
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

        global _testMode
        global _validate
        global _haltOnErr

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

        global _transformANY
        global _transformPUB

        # Stored versions
        self.cwd       = None
        self.lastv     = None
        self.lastp     = None
        self.newCwd    = None
        self.newLastv  = None
        self.newLastp  = None
        self.cwdVals   = None
        self.lastvVals = None
        self.lastpVals = None
        (lastAny, lastPub, isChanged) = self.versions

        # Checkout current working document to get doc and lock
        self.cwd = cdr.getDoc(self.session, self.id, 'Y', getObject = 1)
        if type(self.cwd) in (type(""), type(u"")):
            err = cdr.checkErr(self.cwd) or self.cwd
            raise DocumentLocked("Unable to check out CWD for CDR%010d: %s" %
                                 (self.id, err))

        # Run the transformation filter on current working doc
        self.newCwd = self.transform.run(self.cwd)

        # If (and only if) old version was valid, validate new version
        # cwdVal is array of error messages or empty array
        if (_validate):
            self.cwdVals = cdr.valPair(self.session, self.cwd.type,
                                       self.cwd.xml, self.newCwd)

        # If there is a last version
        if lastAny > 0:

            # And it's not the same as the CWD
            if isChanged == 'Y':
                self.lastv = cdr.getDoc(self.session, self.id, 'Y',
                                        str(lastAny),
                                        getObject = 1)
                if type(self.lastv) in (type(""), type(u"")):
                    err = cdr.checkErr(self.lastv) or self.lastv
                    raise Exception("Failure retrieving lastv (%d) "
                                    "for CDR%010d: %s" % (lastAny,
                                                       self.id, err))

                # Transform
                self.newLastv = self.transform.run(self.lastv)
                if _validate:
                    self.lastvVals= cdr.valPair(self.session, self.lastv.type,
                                                self.lastv.xml, self.newLastv)
            else:
                # Copy references lastv is now cwd
                self.lastv     = self.cwd
                self.newLastv  = self.newCwd
                self.lastvVals = self.cwdVals

        # If there is a last publishable version
        if lastPub > 0:

            # If it's not the same as the last version
            if lastPub != lastAny:
                self.lastp = cdr.getDoc(self.session, self.id, 'Y',
                                        str(lastPub),
                                        getObject = 1)
                if type(self.lastp) in (type(""), type(u"")):
                    err = cdr.checkErr(self.lastp) or self.lastp
                    raise Exception("Failure retrieving lastp (%d) "
                                    "for CDR%010d: %s" % (lastPub,
                                                       self.id, err))

                # Transform
                self.newLastp = self.transform.run(self.lastp)
                if _validate:
                    # Original plan was to validate pub version even if
                    #   last pub was invalid, but this is safe since
                    #   attempt to save an invalid pub version will cause
                    #   it to be marked non-publishable
                    self.lastpVals= cdr.valPair(self.session, self.lastp.type,
                                                self.lastp.xml, self.newLastp)

            else:
                # Copy references lastp is lastv, and maybe also cwd
                self.lastp     = self.lastv
                self.newLastp  = self.newLastv
                self.lastpVals = self.lastvVals


    def saveChanges(self, job):
        """
        In run mode, saves all versions of a document needing to be saved.
        In test mode, writes to output files, leaving the database alone.

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
        everValidated = self.lastp and True or self.__everValidated(job.cursor)
        #job.log("everValidated = %s" % everValidated)

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
            job.log(msg)

        if _testMode:
            # Write new/original CWDs
            cdrglblchg.writeDocs(_outputDir, docId,
                                 self.cwd.xml, self.newCwd, 'cwd',
                                 self.cwdVals)

        # If last version is not cwd, save cwd so it won't be lost
        if self.lastv and self.compare(self.cwd.xml, self.lastv.xml):
            # Don't save if test mode or validation failure
            if not _testMode and not vals:
                # Save old CWD as new version
                self.__saveDoc(str(self.cwd), ver='Y', pub='N', job=job,
                               val = (everValidated and 'Y' or 'N'),
                               logWarnings = False)
                lastSavedXml = self.lastv.xml

        # If publishable version changed ...
        if self.lastp and self.compare(self.lastp.xml, self.newLastp):
            if _transformPUB and _testMode:
                # Write new/original last pub version
                cdrglblchg.writeDocs(_outputDir, docId,
                                     self.lastp.xml, self.newLastp, 'pub',
                                     self.lastpVals)
            else:
                self.lastp.xml = self.newLastp
                # If not validating or passed validation,
                #   save new last pub version
                if _transformPUB and not (vals and _haltOnErr):
                    self.__saveDoc(str(self.lastp), ver='Y', pub='Y', job=job,
                                   val = (everValidated and 'Y' or 'N'))
                    lastSavedXml = self.newLastp

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
            if _transformANY and _testMode:
                # Write new/original last non-pub version results
                cdrglblchg.writeDocs(_outputDir, docId,
                                     self.lastv.xml, self.newLastv, 'lastv',
                                     self.lastvVals)
            # Reflect the changes
            self.lastv.xml = self.newLastv

            if not _testMode:
                # Save new last non-pub version
                if _transformANY and not (vals and _haltOnErr):
                    self.__saveDoc(str(self.lastv), ver='Y', pub='N', job=job,
                                   val = (everValidated and 'Y' or 'N'))
                    lastSavedXml = self.newLastv

        # If no XML has been saved, and the new and old cwd are different
        #   or
        # If last XML saved is not the same as the new cwd
        #   then
        # Save the new current working document
        if (not lastSavedXml and self.compare(self.newCwd, self.cwd.xml)) or \
               (lastSavedXml and self.compare(self.newCwd, lastSavedXml)):
            if not _testMode:
                # Save new CWD
                self.cwd.xml = self.newCwd
                if not (vals and _haltOnErr):
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
