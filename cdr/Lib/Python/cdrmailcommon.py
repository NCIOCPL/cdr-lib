#----------------------------------------------------------------------
#
# $Id: cdrmailcommon.py,v 1.3 2002-11-01 02:42:57 ameyer Exp $
#
# Mailer classes needed both by the CGI and by the batch portion of the
# mailer software.
#
# $Log: not supported by cvs2svn $
# Revision 1.2  2002/10/24 23:13:07  ameyer
# Revised selections and numerous small changes.
#
# Revision 1.1  2002/09/19 21:40:00  ameyer
# First fersion of common mailer routines.  Not yet operational.
#
#----------------------------------------------------------------------

import sys, cdrdb


# Log file for debugging - module variable, not instance
LOGFILE = "d:/cdr/log/mailer.log"

#----------------------------------------------------------------------
# Class for finding documents for remailers.
#----------------------------------------------------------------------
class RemailSelector:
    """
    Find all documents in a particular class which must be remailed.

    Finds the content documents, including the version number, and
      the tracking documents for the mailer which received no response

    Stores info temporarily in database tables.

    Selects:
       Latest publishable version of document for which:
       A mailer was sent between
          Some number of days (60 is expected)
          and some other number (120 is expected)
          (e.g., examines all mailers sent between 2 and 4
           months ago.)
       The mailer is the requested type, i.e., one of the
          non-remailer types.
       No response to the mailer was received.
       No remailer has already been sent.
    The selection also retrieves the id of the mailer tracking document
       for the previous mailer - to be used in the RemailerFor element
       in the new Mailer tracking document for this remailer.

    Several temporary tables are used, primarily to simplify and
      significantly speed up the selections.
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
        self.__conn   = conn
        self.__cursor = conn.cursor()
        self.__jobId  = jobId


    def select(self, originalMailType, maxMailers=sys.maxint,
                     earlyDays=120, lateDays=60):
        """
        Find documents and related ids for remailing.
        Finds doc ids, associated mailer tracking doc ids, and
          ids of Person documents for people who did not respond
          to a previous mailer for this doc.

        Parameters:
            originalMailType = String containing original mailer types,
              preformatted for inclusion in an IN clause, e.g.,
                "'Physician-Initial', 'Physician-Annual update'", or
                "'Organization-Annual update'"
              Note: This is not a tuple, it's a string, with SQL single
                    quotes included in the string.
              Note: It is legal to specify a set containing only one
                    element for an IN clause.

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
        # Get all the base mailers that have been sent out
        #   between earlyDays and lateDays ago and store in
        #   temp table #orig_mailers
        # This is selecting document ids of mailer tracking documents,
        #   not primary documents that are mailed.
        try:
            self.__cursor.execute ("""
             SELECT mailer.doc_id
               INTO #orig_mailers
               FROM query_term mailer
               JOIN query_term mailer_sent
                 ON mailer_sent.doc_id = mailer.doc_id
              WHERE mailer.path = '/Mailer/Type'
                AND mailer.value IN (%s)
                AND mailer_sent.path = '/Mailer/Sent'
                AND mailer_sent.value
            BETWEEN CONVERT(CHAR(10), DATEADD(DAY, -%d, GETDATE()), 121)
                AND CONVERT(CHAR(10), DATEADD(DAY, -%d,  GETDATE()), 121)
            """ % (originalMailType, earlyDays, lateDays))
        except cdrdb.Error, info:
            raise 'db error creating temporary table #orig_mailers %s'\
                  % str(info[1][0])

        # Create another temporary table containing all mailers in the
        #   above table that received a response.
        try:
            self.__cursor.execute ("""
             SELECT query_term.doc_id
               INTO #got_response
               FROM query_term
               JOIN #orig_mailers
                 ON #orig_mailers.doc_id = query_term.doc_id
              WHERE query_term.path = '/Mailer/Response/Received'
                AND query_term.value IS NOT NULL
                AND query_term.value <> ''
            """)
        except cdrdb.Error, info:
            raise 'db error creating temporary table #got_response %s'\
                  % str(info[1][0])

        # How many have we already sent out remailers for (might be some
        #   overlap with the previous set).
        try:
            self.__cursor.execute ("""
             SELECT #orig_mailers.doc_id
               INTO #already_remailed
               FROM query_term
               JOIN #orig_mailers
                 ON #orig_mailers.doc_id = query_term.int_val
              WHERE query_term.path = '/Mailer/RemailerFor/@cdr:ref'
            """)
        except cdrdb.Error, info:
            raise 'db error creating temporary table #already_remailed %s'\
                  % str(info[1][0])

        # Get the primary document and version for each mailer that
        #   isn't in either of the previous two subsets.  Remember the
        #   mailer ID, too, so we can populate the RemailerFor element.
        # XXXX - ISSUE - XXXX
        #    Do we need to look at the pub_proc.status value for the original
        #    job to make sure we aren't creating a remailer for a mailer that,
        #    in fact, never went out.
        try:
            self.__cursor.execute ("""
                 SELECT DISTINCT TOP %d document.id AS doc,
                        MAX(doc_version.num) AS ver,
                        #orig_mailers.doc_id AS tracker
                   INTO #remail_temp
                   FROM document
                   JOIN doc_version
                     ON doc_version.id = document.id
                   JOIN query_term
                     ON query_term.int_val = document.id
                   JOIN #orig_mailers
                     ON #orig_mailers.doc_id = query_term.doc_id
                  WHERE query_term.path = '/Mailer/Document/@cdr:ref'
                    AND doc_version.publishable = 'Y'
                    AND #orig_mailers.doc_id NOT IN (
                        SELECT doc_id
                          FROM #got_response
                         UNION
                        SELECT doc_id
                          FROM #already_remailed
                        )
               GROUP BY document.id, #orig_mailers.doc_id
            """ % maxMailers)

            # Tell user how many hits there were
            return self.__cursor.rowcount

        except cdrdb.Error, info:
            raise 'db error creating #remail_temp id/ver/tracker table: %s' \
                  % str(info[1][0])

    def getDocIdQuery(self):
        """
        Return a query string to retrieve doc IDs for the remailers.
        Note:  Assumes same connection is used to access temp table
               as was used to build it.
        """
        return "SELECT DISTINCT doc FROM #remail_temp"

    def getDocIdVerQuery(self):
        """
        Return a query string to retrieve tuples of doc IDs plus
        version numbers for the remailers.
        Note:  Assumes same connection is used to access temp table
               as was used to build it.
        """
        return """SELECT DISTINCT id, MAX(num) FROM doc_version
                   WHERE id IN (SELECT DISTINCT doc FROM #remail_temp)
                   GROUP BY id"""

    def fillMailerIdTable(self, jobId):
        """
        A job was initiated.
        Copy the data from the temporary table to a place where
        the batch portion of the mailer program can get at it
        """
        try:
            self.__cursor.execute(
              """INSERT INTO remailer_ids (job, doc, tracker)
                     SELECT %d, doc, tracker
                       FROM #remail_temp""" % jobId)

            # If we don't do this, results will be thrown away
            self.__conn.commit()

        except cdrdb.Error, info:
            raise 'database error saving remailer_ids %s' % str(info[1][0])

    def getRelatedIds(self, docId):
        """
        Return the ids of the mailer tracking documents
        for a particular document to be remailed.
        Data is returned as a sequence of tuples of tracker id.
        """
        try:
            self.__cursor.execute (
                "SELECT tracker "
                "  FROM remailer_ids "
                " WHERE job=? AND doc=?", (self.__jobId, docId))

            return self.__cursor.fetchall()

        except cdrdb.Error, info:
            raise 'database error getting related remailer_ids: %s' \
                  % str(info[1][0])

    def delete(self):
        """
        Delete the mailer id information from the mailer_ids table.
        Call when processing is finished.
        """
        try:
            self.__cursor.execute(
                "DELETE FROM remailer_ids WHERE job=?", self.__jobId)

            # Make object unusable
            self.__conn  = None
            self.__jobId = None

        except cdrdb.Error, info:
            raise "database error deleting job %d from remailer_ids: %s" \
                  % (self.__jobId, str(info[1][0]))

