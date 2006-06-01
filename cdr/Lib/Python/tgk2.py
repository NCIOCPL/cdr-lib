#----------------------------------------------------------------------
#
# $Id: tgk2.py,v 1.1 2006-06-01 20:08:43 bkline Exp $
#
# Test program to confirm that we can now send documents for a push
# job to the Cancer.gov GateKeeper with delays interspersed and without
# knowing in advance how many documents will be pushed by the job.
#
# There are three ways to invoke this program:
#  1. tgk2 --source=xxx "--description=first test of enhancements"
#     (where xxx is Development or Testing; used to create a new job)
#  2. tgk2 --job-id=NNNN --next-doc-num=NN doc-id [doc-id ...]
#     (where NNNN is the job ID obtained from the output of step 1,
#      and NN is the next sequence number for the first document to
#      be pushed for this job)
#  3. tgk2 --close-job --job-id=NNNN
#     (to tell GateKeeper the job is complete)
#
# $Log: not supported by cvs2svn $
#----------------------------------------------------------------------
import cdr2cg, cdr, cdrdb, re, sys, getopt

cdr2cg.debuglevel = 1

XmlDeclLine = re.compile(u"<\\?xml.*?\\?>\\s*", re.DOTALL)
DocTypeLine = re.compile(u"<!DOCTYPE.*?>\\s*", re.DOTALL)
FaultString = re.compile(u"<faultstring>(.*)</faultstring>", re.DOTALL)

class Doc:
    def __init__(self, cursor, docId):
        self.id = cdr.exNormalize(docId)[1]
        cursor.execute("""\
            SELECT p.xml, d.doc_version, t.name
              FROM pub_proc_cg p
              JOIN pub_proc_doc d
                ON d.pub_proc = p.pub_proc
               AND d.doc_id = p.id
              JOIN doc_version v
                ON v.id = p.id
               AND v.num = d.doc_version
              JOIN doc_type t
                ON t.id = v.doc_type
             WHERE p.id = ?""", self.id)
        rows = cursor.fetchall()
        if not rows:
            raise Exception(u"Document %s is not on Cancer.gov" % docId)
        docXml, self.version, self.type = rows[0]
        if self.type == u'InScopeProtocol':
            self.type = u'Protocol'
        self.xml = DocTypeLine.sub(u"", XmlDeclLine.sub(u"", docXml))
        
def usage():
    sys.stderr.write("""\
usage: tgk2 [options] [doc-id [doc-id ...]]

options:
    --close-job         tell GateKeeper the job is complete
    --job-id=N          push job ID for sending docs or closing job
    --source=name       Development or Testing (default is Development)
    --host=dns-name     default is test4.cancer.gov
    --description=text  default is "testing 'GK'"
    --next-doc-num=N    next sequence number when sending (default is 1)
""")
    sys.exit(1)

if __name__ == '__main__':
    cdr2cg.host   = 'test4.cancer.gov'
    cdr2cg.source = 'CDR Development'
    lastJobId     = 'ignore'
    docCount      = 'ignore'
    description   = "Testing 'GK'"
    closeJob      = False
    nextDocNum    = 1
    thisJobId     = None
    pubType       = 'Export'

    try:
        longopts = ["close-job", "job-id=", "source=", "host=", "description=",
                    "next-doc-num="]
        opts, args = getopt.getopt(sys.argv[1:], "", longopts)
    except getopt.GetoptError, e:
        usage()
    for o, a in opts:
        if o == '--close-job':
            closeJob = True
            sys.stderr.write("Closing job\n")
        elif o == '--job-id':
            sys.stderr.write("Job ID %s\n" % a)
            thisJobId = int(a)
        elif o == '--source':
            if a not in ('Development', 'Testing'):
                usage()
            cdr2cg.source = "CDR %s" % a
            sys.stderr.write("Source %s\n" % cdr2cg.source)
        elif o == '--host':
            cdr2cg.host = a
            sys.stderr.write("Host %s\n" % cdr2cg.host)
        elif o == '--description':
            description = a
            sys.stderr.write("Description %s\n" % description)
        elif o == '--next-doc-num':
            try:
                nextDocNum = int(a)
            except:
                usage()
            sys.stderr.write("Next doc number %d\n" % nextDocNum)
        else:
            usage()
    if not thisJobId:
        response = cdr2cg.initiateRequest(description, pubType, lastJobId)
        print response
        if response.type == 'OK':
            thisJobId = response.details.nextJobId
            if thisJobId:
                print "sending data prolog for job %s" % thisJobId
                response = cdr2cg.sendDataProlog(description, thisJobId,
                                                 pubType, lastJobId, docCount)
                print response
                print "Job ID: %s" % thisJobId
    elif closeJob:
        response = cdr2cg.sendJobComplete(thisJobId, pubType)
        print response
    else:
        conn = cdrdb.connect('CdrGuest')
        cursor = conn.cursor()
        for arg in args:
            doc = Doc(cursor, arg)
            response = cdr2cg.sendDocument(thisJobId, nextDocNum, pubType,
                                           doc.type, doc.id, doc.version,
                                           doc.xml.encode('utf-8'))
            print "sending doc %d (CDR%d)" % (nextDocNum, doc.id)
            print response
            nextDocNum += 1
        print "next-doc-num: %d" % nextDocNum
