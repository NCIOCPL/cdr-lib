#----------------------------------------------------------------------
#
# $Id: cdrmailcommon.py,v 1.2 2002-10-24 23:13:07 ameyer Exp $
#
# Mailer classes needed both by the CGI and by the batch portion of the
# mailer software.
#
# $Log: not supported by cvs2svn $
# Revision 1.1  2002/09/19 21:40:00  ameyer
# First fersion of common mailer routines.  Not yet operational.
#
#----------------------------------------------------------------------

import sys, cdrdb


# Log file for debugging - module variable, not instance
LOGFILE = "d:/cdr/log/mailer.log"

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


    def select(self, originalMailType, maxMailers=sys.maxint,
                     earlyDays=120, lateDays=60):
        """
        Find documents and related ids for remailing.
        Finds doc ids, associated mailer tracking doc ids, and
          ids of Person documents for people who did not respond
          to a previous mailer for this doc.

        Parameters:
            originalMailType = Original mailer types, e.g.,
              'Physician-Annual update'.

            maxMailers = Maximum number of documents to select for
              mailer generation.  Default is no limit.

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
            # Select latest version of document for which:
            #    A mailer was sent between
            #       Some number of days (60 is expected)
            #       and some other number (120 is expected)
            #       (e.g., examines all mailers sent between 2 and 4
            #        months ago.)
            #    The mailer is the requested type, i.e., one of the
            #       non-remailer types.
            #    No response to the mailer was received.
            #    No remailer has already been sent.
            # The selection also gathers other information, but note
            #    that although the original recipient is picked up here,
            #    the recipient must be recalculated for Organizations.
            qry = """
                INSERT INTO #remailTemp (doc, ver, tracker, recipient)
                SELECT TOP %d qdoc.int_val, MAX(doc_version.num),
                       MIN(qmailer.doc_id), qrecip.int_val
                  FROM query_term qmailer
                  JOIN doc_version
                    ON qmailer.doc_id = doc_version.id
                  JOIN query_term qsent
                    ON qsent.doc_id  = qmailer.doc_id
                  JOIN query_term qrecip
                    ON qrecip.doc_id = qmailer.doc_id
                  JOIN query_term qdoc
                    ON qdoc.doc_id = qmailer.doc_id
                 WHERE qmailer.path  = '/Mailer/Type'
                   AND qmailer.value = '%s'
                   AND qsent.path    = '/Mailer/Sent'
                   AND qsent.value BETWEEN (GETDATE()-%d) AND (GETDATE()-%d)
                   AND qrecip.path   = '/Mailer/Recipient/@cdr:ref'
                   AND qdoc.path     = '/Mailer/Document/@cdr:ref'
                   AND NOT EXISTS (
                      SELECT *
                        FROM query_term resp
                       WHERE resp.path = '/Mailer/Response/Received'
                         AND resp.doc_id = qmailer.doc_id)
                   AND NOT EXISTS (
                      SELECT *
                        FROM query_term remail
                       WHERE remail.path = '/Mailer/RemailerFor/@cdr:ref'
                         AND remail.int_val = qmailer.doc_id)
                GROUP BY qdoc.int_val, qrecip.int_val
                """ % (maxMailers, originalMailType, earlyDays, lateDays)
            cursor.execute (qry, timeout=180)

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
        try:
            self.__conn.cursor().execute(
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
                  % str(info[1][0])

    def delete(self):
        """
        Delete the mailer id information from the mailer_ids table.
        Call when processing is finished.
        """
        try:
            self.__conn.cursor().execute(
                "DELETE FROM remailer_ids WHERE job=?", self.__jobId)

            # Make object unusable
            self.__conn  = None
            self.__jobId = None

        except cdrdb.Error, info:
            raise "database error deleting job %d from remailer_ids: %s" \
                  % (self.__jobId, str(info[1][0]))

