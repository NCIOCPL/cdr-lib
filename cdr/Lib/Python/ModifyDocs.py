#----------------------------------------------------------------------
#
# $Id: ModifyDocs.py,v 1.2 2003-09-02 14:00:14 bkline Exp $
#
# Harness for one-off jobs to apply a custom modification to a group
# of CDR documents.
#
# $Log: not supported by cvs2svn $
# Revision 1.1  2003/08/21 19:29:02  bkline
# Harness for one-off global changes.
#
#----------------------------------------------------------------------
import cdr, cdrdb, sys, time, re, os

LOGFILE = 'd:/cdr/log/ModifyDocs.log'
ERRPATT = re.compile(r"<Err>(.*?)</Err>", re.DOTALL)

#----------------------------------------------------------------------
# Class for one modification job.
#----------------------------------------------------------------------
class Job:
    
    def __init__(self, uid, pwd, filter, transform, comment,
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
            logFile    - optional path for logfile
        """

        self.logFile   = open(logFile, 'a')
        self.uid       = uid
        self.pwd       = pwd
        self.filter    = filter
        self.transform = transform
        self.comment   = comment
        self.cursor    = cdrdb.connect('CdrGuest')
        self.session   = cdr.login(uid, pwd)
        error          = cdr.checkErr(self.session)
        if error:
            raise Exception("Failure logging into CDR: %s" % error)

    def run(self):
        ids = self.filter.getDocIds()
        for id in ids:
            try:
                self.log("Processing CDR%010d" % id)
                doc = Doc(id, self.session, self.transform, self.comment)
                doc.saveChanges(self)
            except Exception, info:
                self.log("Document %d: %s" % (id, str(info)))
                raise
            cdr.unlock(self.session, "CDR%010d" % id)

    #------------------------------------------------------------------
    # Log processing/error information with a timestamp.
    #------------------------------------------------------------------
    def log(self, what):
        what = "%s: %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), what)
        self.logFile.write(what)
        sys.stderr.write(what)
    
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
            raise Exception("Unable to check out CWD for CDR%010d: %s" %
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
            Create new NPV from LPV(t)
        If CWD(t) <> LS:
            Create new CWD using CWD(t)
        """

        lastSavedXml = None
        if self.lastv and self.compare(self.cwd.xml, self.lastv.xml):
            self.cwd.xml = lastSavedXml = self.lastv.xml
            lastSavedXml = self.cwd.xml
            self.saveDoc(str(self.cwd), ver = 'Y', pub = 'N', job = job,
                         logWarnings = 0)
        if self.lastp and self.compare(self.lastp.xml, self.newLastp):
            self.lastp.xml = lastSavedXml = self.newLastp
            self.saveDoc(str(self.lastp), ver = 'Y', pub = 'Y', job = job)
        if self.lastv and self.compare(self.lastv.xml, self.newLastv):
            self.lastv.xml = lastSavedXml = self.newLastv
            self.saveDoc(str(self.lastp), ver = 'Y', pub = 'N', job = job)
        if lastSavedXml and self.compare(self.newCwd, lastSavedXml):
            self.cwd.xml = self.newCwd
            self.saveDoc(str(self.cwd), ver = 'N', pub = 'N', job = job)

    #------------------------------------------------------------------
    # Invoke the CdrRepDoc command.
    #------------------------------------------------------------------
    def saveDoc(self, docStr, ver, pub, job, logWarnings = 1):
        job.log("saveDoc(%d, ver='%s' pub='%s')" % (self.id, ver, pub))
        # return 1
        response = cdr.repDoc(self.session, doc = docStr, ver = ver,
                              val = 'Y',
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
