#----------------------------------------------------------------------
#
# $Id: cdrmailcommon.py,v 1.1 2002-09-19 21:40:00 ameyer Exp $
#
# Mailer classes needed both by the CGI and by the batch portion of the
# mailer software.
#
# $Log: not supported by cvs2svn $
#----------------------------------------------------------------------

import cdrdb

# Log file for debugging - module variable, not instance
logf = None

def logwrite(msg):
    global logf
    if logf == None:
        logf = open ('d:/cdr/log/mailcommon.log', "w", 0)

    # if (type(msg) == "" or type(msg) == u""):
    logf.write (msg)
    logf.write ("\n")

#----------------------------------------------------------------------
# Class for finding documents and recipients for remailers.
#----------------------------------------------------------------------
class RemailSelector:
    """
    Find all documents in a particular class which must be remailed.

    Finds the content documents, the tracking documents for the
      mailer which received no response, and the recipients
      who did not respond.

    Stores info temporarily in database tables.
    """
    def __init__(self, conn = None, jobId = None):
        """
        Construct a RemailSelector

        Parameters:
            conn   = Database connection.
                     If none passed, we'll create one.
            jobId  = Publishing job identifier.
                     When called from cgi, this is None.
                     When called from mailer, it's the real thing.
        """
        # Create connection if needed
        if (conn == None):
            try:
                conn = cdrdb.connect("CdrPublishing")
            except cdrdb.Error, info:
                raise "database connection error: %s" % info[1][0]

        # Save connection and job id
        self.__conn  = conn
        self.__jobId = jobId


    def select(self, originalMailType, earlyDays=120, lateDays=60):
        """
        Find documents and related ids for remailing.
        Finds doc ids, associated mailer tracking doc ids, and
          ids of Person documents for people who did not respond
          to a previous mailer for this doc.

        Parameters:
            originalMailType = Original mailer types, e.g.,
              'Physician-Annual update'.

            earlyDays = Number of days to look backward for mailers that
              received no response.  This is included because, for some
              mailer types CIPS may never have sent remailers, and we
              probably don't want to follow up on old, unanswered mailers
              that are very old.

            lateDays = Number of days that must have passed since a mailer
              was sent before we'll consider the lack of response to
              require a remailing.

        Returns:
            Number of documents selected.  May be zero if none need
            remailing.

        Throws:
            Exceptions raised in cursor.execute are passed through.
        """
        # Create a temporary table to hold our results
        # Adding validation constraints is overkill on this table
        # Data is coming from tables which must already be valid
        # #remailTemp will automatically be dropped when this
        #   connection closes.
        try:
            cursor = self.__conn.cursor()
            cursor.execute ("""
                CREATE TABLE #remailTemp (
                    job         INTEGER,
                    doc         INTEGER,
                    ver         INTEGER,
                    tracker     INTEGER,
                    recipient   INTEGER)""")
        except cdrdb.Error, info:
            raise 'database error creating temporary table #remailTemp %s'\
                  % str(info[1][0])

        # Select hits into it
        # Changes needed:
        #  In future, may add pub_proc_doc failure check.
        #  Database doesn't have this info at outset
        #  Could be a major addition
        #
        try:
            qry = """
                INSERT INTO #remailTemp (doc, tracker, recipient)
                SELECT doc.int_val, mailer.doc_id, recip.int_val
                  FROM query_term mailer
                  JOIN query_term sent
                    ON sent.doc_id  = mailer.doc_id
                  JOIN query_term recip
                    ON recip.doc_id = mailer.doc_id
                  JOIN query_term doc
                    ON doc.doc_id = mailer.doc_id
                 WHERE mailer.path  = '/Mailer/Type'
                   AND mailer.value = '%s'
                   AND sent.path    = '/Mailer/Sent'
                   AND sent.value BETWEEN (GETDATE()-%d) AND (GETDATE()-%d)
                   AND recip.path   = '/Mailer/Recipient/@cdr:ref'
                   AND doc.path     = '/Mailer/Document/@cdr:ref'
                   AND NOT EXISTS (
                      SELECT *
                        FROM query_term resp
                       WHERE resp.path = '/Mailer/Response/Received'
                         AND resp.doc_id = mailer.doc_id)
                   AND NOT EXISTS (
                      SELECT *
                        FROM query_term remail
                       WHERE remail.path = '/Mailer/RemailerFor/@cdr:ref'
                         AND remail.int_val = mailer.doc_id)
                """ % (originalMailType, earlyDays, lateDays)
            cursor.execute (qry, timeout=180)

            logwrite ("Remailer query:\n%s" % qry)
            logwrite ("Hit count on query = %d" % cursor.rowcount)

            # Tell user how many hits there were
            return cursor.rowcount

        except cdrdb.Error, info:
            raise 'database error selecting remailers %s' % str(info[1][0])

    def getDocIdQuery(self):
        """
        Return a query string to retrieve doc IDs for the remailers.
        Note:  Assumes same connection is used to access temp table
               as was used to build it.
        """
        return "SELECT DISTINCT doc FROM #remailTemp"

    def getDocIdVerQuery(self):
        """
        Return a query string to retrieve tuples of doc IDs plus
        version numbers for the remailers.
        Note:  Assumes same connection is used to access temp table
               as was used to build it.
        """
        return """SELECT DISTINCT id, MAX(num) FROM doc_version
                   WHERE id IN (SELECT DISTINCT doc FROM #remailTemp)
                   GROUP BY id"""

    def fillMailerIdTable(self, jobId):
        """
        A job was initiated.
        Copy the data from the temporary table to a place where
        the batch portion of the mailer program can get at it
        """
        logwrite (str(jobId))
        try:
            cursor = self.__conn.cursor()
            cursor.execute (
              """INSERT INTO remailer_ids (job, doc, tracker, recipient)
                     SELECT %d, doc, tracker, recipient
                       FROM #remailTemp""" % jobId)

            # If we don't do this, results will be thrown away
            self.__conn.commit()

        except cdrdb.Error, info:
            raise 'database error saving remailer_ids %s' % str(info[1][0])

    def getRelatedIds(self, docId):
        """
        Return the ids of the mailer tracking documents and recipients
        for a particular document to be remailed.
        Data is returned as a sequence of tuples of:
            recipient id, tracker id
        """
        try:
            cursor = self.__conn.cursor()
            cursor.execute (
                "SELECT recipient, tracker "
                "  FROM remailer_ids "
                " WHERE job=? AND doc=?", (self.__jobId, docId))

            return cursor.fetchall()

        except cdrdb.Error, info:
            raise 'database error getting related remailer_ids: %s' \
                  % (self.__jobId, str(info[1][0]))

    def delete(self):
        """
        Delete the mailer id information from the mailer_ids table.
        Call when processing is finished.
        """
        try:
            cursor = self.__conn.cursor()
            self.__cursor.execute(
                "DELETE FROM remailer_ids WHERE job=?", self.__jobId)

            # Make object unusable
            self.__conn  = None
            self.__jobId = None

        except cdrdb.Error, info:
            raise "database error deleting job %d from remailer_ids: %s" \
                  % (self.__jobId, str(info[1][0]))

