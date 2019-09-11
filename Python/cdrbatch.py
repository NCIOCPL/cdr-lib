#----------------------------------------------------------------------
# Internal module defining a CdrBatch class for managing batch jobs.
#
# Used by CdrBatchService.py, CdrBatchInfo.py, and by individual
# batch jobs.
#
# JIRA::OCECDR-3800 - eliminated security vulnerabilities
#----------------------------------------------------------------------
import sys
import string
import cdr
from cdrapi import db
import cdrcgi

#----------------------------------------------------------------------
# Module level constants
#----------------------------------------------------------------------
# Constants for job status values
ST_QUEUED     = 'Queued'        # New job inserted, ready for daemon to start
ST_INITIATING = 'Initiating'    # Daemon is starting it
ST_IN_PROCESS = 'In process'    # Job came up and announced itself
ST_SUSPEND    = 'Suspend'       # Future
ST_SUSPENDED  = 'Suspended'     # Future
ST_RESUME     = 'Resume'        # Future
ST_STOP       = 'Stop'          # Stop cleanly if possible
ST_STOPPED    = 'Stopped'       # Job stopped cleanly before end
ST_COMPLETED  = 'Completed'     # Job ran to completion
ST_ABORTED    = 'Aborted'       # Abnormal termination due to internal error

# Process type identifiers, see sendSignal()
PROC_DAEMON   = 'daemon'        # Process is the batch job daemon
PROC_EXTERN   = 'extern'        # Process is not daemon or batch job
PROC_BATCHJOB = 'batchjob'      # Process is a batch job

# For validating proc id
PROC_VALID = (PROC_DAEMON, PROC_EXTERN, PROC_BATCHJOB)

# The batch service daemon can currently set only these values
_ST_DAEMON_VALID = (ST_INITIATING,)

# External controllers (not the batch service or the job itself)
#   can currently set only these values
_ST_EXTERN_VALID = (ST_STOP,)

# Any batch job can currently set these
_ST_JOB_VALID = (ST_IN_PROCESS, ST_STOPPED, ST_COMPLETED, ST_ABORTED)

# Others are future, or set by the queue method

# Logfile name
LF = cdr.DEFAULT_LOGDIR + "/BatchJobs.log"

#------------------------------------------------------------------
# Exception class for batch processing
#------------------------------------------------------------------
class BatchException (Exception):
    pass

#------------------------------------------------------------------
# Signal a job to do something
#------------------------------------------------------------------
def sendSignal (conn, jobId, newStatus, whoami):
    """
    Set the status of the job.  This might become a true OS signal
    in future.  For now it just raises a flag.

    As a double check, the caller has to identify himself as the
    batch job daemon, a running job, or an external program.
    This is not intended to defeat malicious programmers, only
    to help catch inadvertent errors.

    This is a static method that can be used by any program, whether
    or not it has a CdrBatch object.

    Pass:
        conn      - Active database connection.
        jobId     - ID of the job.
        newStatus - Set job status to this.
        whoami    - One of CdrBatch PROC_ values.

    Return:
        Void.  Raises error if failed.
    """
    # Is caller recognized?
    if not whoami in PROC_VALID:
        raise BatchException ("Invalid whoami parameter")

    # Is caller authorized to do what he wants to do?
    if whoami == PROC_DAEMON and newStatus not in _ST_DAEMON_VALID:
        raise BatchException (
          "The publishing daemon cannot set status = %s" % newStatus)
    if whoami == PROC_EXTERN and newStatus not in _ST_EXTERN_VALID:
        raise BatchException (
          "External programs cannot set status = %s" % newStatus)
    if whoami == PROC_BATCHJOB and newStatus not in _ST_JOB_VALID:
        raise BatchException (
          "Batch jobs cannot set status = %s" % newStatus)

    # Get the current status, raises exception if failed
    status = getJobStatus (idStr=str(jobId))[0][3]

    # Insure status makes sense in current context
    msg = None
    if newStatus == ST_INITIATING \
                and status != ST_QUEUED:
        msg = "Job must be in queued state to initiate"
    if newStatus == ST_IN_PROCESS \
                and status != ST_INITIATING \
                and status != ST_SUSPENDED:
        msg = "Job must be in initiated or suspended state to signal in_process"
    if newStatus == ST_STOP \
                and status != ST_IN_PROCESS:
        msg = "Job must be in_process before stopping it"
    if newStatus == ST_STOPPED \
                and status != ST_IN_PROCESS \
                and status != ST_SUSPENDED \
                and status != ST_STOP:
        msg = "Invalid signal %s" % newStatus
    if newStatus == ST_COMPLETED \
                and status != ST_IN_PROCESS:
        msg = "Job must have been in process in order to be completed"
    if msg:
        msg = "status=%s: %s" % (status, msg)
        cdr.logwrite (msg, LF)
        raise BatchException (msg)

    # Set the new status
    try:
        qry = """
          UPDATE batch_job
             SET status = ?, status_dt = GETDATE()
           WHERE id = ?"""
        conn.cursor().execute (qry, (newStatus, jobId))

    except Exception as e:
        msg = "Unable to update job status: %s" % e
        cdr.logwrite (msg, LF)
        raise BatchException (msg)


#------------------------------------------------------------------
# Get the current status of one or more jobs
#------------------------------------------------------------------
def getJobStatus (idStr=None, name=None, ageStr=None, status=None):
    """
    Find the current status of one or more jobs.

    Pass:
        Must pass at least one of these parms.

        jobId  - unique identifier of the job.
        ageStr - jobs in last 'age' days.
        name   - job name from table.
        status - one or more of the status values.
                 May pass either a single string, or a sequence.

    Return:
        Tuple of rows of:
            Tuple of:
                job id
                job name
                date/time queued
                status
                date/time of status set
                last progress message recorded
    """
    # Normalize values
    # CGI form may have passed blanks instead of none
    jobId     = normalCgi (idStr, 1)
    jobAge    = normalCgi (ageStr, 1)
    jobName   = normalCgi (name)
    if not status:
        jobStatus = None
    elif isinstance(status, type("")):
        jobStatus = normalCgi (status)
    else:
        # Normalize to string or sequence of normalized strings
        newStatus = []
        for stat in status:
            stat = normalCgi(stat)
            if stat:
                newStatus.append(stat)
        if len(newStatus) == 0:
            jobStatus = None
        elif len(newStatus) == 1:
            jobStatus = newStatus[0]
        else:
            jobStatus = newStatus

    # Must pass at least one arg
    if not jobId and not jobAge and not jobName and not jobStatus:
        msg = "Request for status without parameters"
        cdr.logwrite (msg, LF)
        raise BatchException (msg)

    # Create query
    fields = "id", "name", "started", "status", "status_dt", "progress"
    query = db.Query("batch_job", *fields).order("started")
    if jobId:
        query.where(query.Condition("id", jobId))
    else:
        if jobName:
            query.where(query.Condition("name", "%" + jobName + "%", "LIKE"))
        if jobAge:
            query.where("started >= DATEADD(DAY, -%d, GETDATE())" % jobAge)
        if jobStatus:
            if isinstance(jobStatus, str):
                query.where(query.Condition("status", jobStatus))
            else:
                query.where(query.Condition("status", jobStatus, "IN"))
    try:
        # Return may be empty tuple if no jobs match criteria
        return query.execute().fetchall()
    except Exception as e:
        raise BatchException("Unable to get job status: %s" % e)

#------------------------------------------------------------------
# Get an HTML display of active jobs
#------------------------------------------------------------------
def getJobStatusHTML(ageDays=1, name=None,
           status=(ST_QUEUED, ST_INITIATING, ST_IN_PROCESS, ST_SUSPEND)):
    """
    Produce a snippet of HTML displaying a list of currently active
    jobs.  This is intended for applications where a user doesn't
    want to run a resource intensive batch job when others are already
    running and so wants to see if anything is queued or running.

    This is a wrapper around getJobStatus().

    Parameters:
        ageDays = Number of days to look back.
                  The default of 1 eliminates looking at old stuff that
                  broke and never got fixed.
        name    = Job name.  None fetches all jobs.
        status  = Specific job status(es) to look for.  The default finds
                  all jobs ready to run, starting, running, or suspended.

    Return:
        String containing an HTML table.
        None if no jobs matching criteria.
    """

    # Get the data
    rows = getJobStatus(name=name, ageStr=str(ageDays), status=status)

    # No hits?
    if not rows:
        return None

    # Create table with headers
    html = """
<table border='2'>
  <tr>
   <td>JobID</td>
   <td>Name</td>
   <td>Started</td>
   <td>Status</td>
   <td>Last info</td>
   <td>Last msg</td>
  </tr>
"""
    # Add it all
    for row in rows:
        html += "  <tr>\n"
        for col in row:
            html += "   <td>%s</td>\n" % col
        html += "  </tr>\n"
    html += "</table>\n"

    return html

#------------------------------------------------------------------
# Is there an instance of a job active?
#------------------------------------------------------------------
def activeCount (jobName):
    """
    Check to see if any jobs with a certain name are in any kind of
    active state - i.e., not permanently ended.
    Only looks at the last 24 hours so that, if something crashes
    and leaves a status of in-process in the table, it will
    eventually (in 24 hours) clear itself from this query.

    Pass:
        jobName - Name of job in batch_job table.
                  Name may include SQL wildcards.
                  '%' finds any in-process batch jobs, regardless of name.

    Return:
        Number of active batch jobs.
        0 = nothing currently active.
    """

    # Are there any jobs not in one of the active statuses?
    statuses = (ST_QUEUED, ST_INITIATING, ST_IN_PROCESS)
    query = db.Query("batch_job", "COUNT(*)")
    query.where(query.Condition("status", statuses, "IN"))
    query.where(query.Condition("name", jobName, "LIKE"))
    query.where("started >= DATEADD(DAY, -1, GETDATE())")
    try:
        return query.execute().fetchone()[0]
    except Exception as e:
        raise BatchException("Unable to get batch job activity info: %s" % e)

#------------------------------------------------------------------
# Normalize input that may have come from a CGI form
#------------------------------------------------------------------
def normalCgi (cgiStr, makeInt=0):
    """
    Normalize input from a CGI form to be a string, or integer, or None.

    Pass:
        cgiStr  - String to normalize.
        makeInt - True=Convert to integer, or None if not possible.

    Return:
        Normalized value or None if no value found.
    """
    if cgiStr:
        # Strip spaces, may produce empty string
        cgiStr = cgiStr.strip()
        if len(cgiStr) == 0:
            return None

        # If we have to convert to integer
        if makeInt:
            try:
                return int (cgiStr)
            except ValueError:
                return None

    # Return original None, or normalized string
    return cgiStr


#------------------------------------------------------------------
# Parent class for batch jobs
#------------------------------------------------------------------
class CdrBatch:
    """
    Parent class for batch jobs.
    Inherit from this to get basic batch functionality.
    """

    #------------------------------------------------------------------
    # Constructor
    #------------------------------------------------------------------
    def __init__(self, jobId=None, jobName='Global Change', command=None,
                 args=None, email=None, priority=None, host='localhost'):
        """
        Constructor for base class of batch jobs.

        Pass:
            jobId - Constructor will look in the database for values.

           or pass specific values:
            jobName  - Human readable name of this job.  Probably should
                       be a generic name like "Global change".
            priority - Future use.
            command  - Name of program to run, should be .exe, .py, etc.
            args     - Tuple of argument name/value tuples.
                       Will associate these with job in db.
            email    - Tuple of email addresses to receive output.
        Throws:
            Exception if database error, or passed job id not found.
        """

        self.__jobName = None

        # Set job id to None or passed value
        self.__jobId = jobId and int(jobId) or None

        # No errors yet in new job
        self.__failure = None

        # Need access to the database for anything we do
        self.__conn = None
        try:
            self.__conn   = db.connect()
            self.__cursor = self.__conn.cursor()
        except Exception as e:
            # Job must not try to run itself
            self.fail("Unable to connect to database: %s" % e)

        # Everything autocommitted on this cursor
        try:
            self.__conn.autocommit(True)
        except Exception as e:
            self.fail ("Setting connection autocommit %s" % e)

        # Gather parms if it's a new job
        if not self.__jobId:
            # If no job id passed, take parms from caller
            self.__jobName  = jobName
            self.__command  = command
            self.__args     = {}
            self.__email    = email
            self.__priority = priority

            # Args are loaded into a dictionary - with type checking
            if args:
                if not isinstance(args, (tuple, list)):
                    self.fail("Job arguments must be passed as a sequence")

                for argPair in args:
                    if not isinstance(argPair, tuple) or len(argPair) != 2:
                        self.fail (
                            "Individual job arguments must be tuples of "
                            "(argname, argvalue)")

                    # Ensure that we have usable types
                    argKey, argVal = argPair

                    # Keys have to be strings
                    if not isinstance(argKey, str):
                        self.fail (
                          "Expecting job argument name of type string.\n" +
                          "Got keytype=%s for arg key=%s val=%s" %
                          (type(argKey), argKey, argVal))

                    # Convert values to mutable sequence
                    if isinstance(argVal, (list, tuple)):
                        values = list(argVal)
                    else:
                        values = [argVal]

                    # Components of sequence have to be strings
                    for i, value in enumerate(values):
                        if value is None:
                            values[i] = ""
                        elif isinstance(value, (int, bool, float)):
                            values[i] = str(value)
                        elif not isinstance(value, str):
                            self.fail("Got arg value of type %s for %s" %
                                      (type(value), argKey))

                    # Store key + arg list in dictionary, by name of arg
                    self.__args[argKey] = values

            # Others don't exist until job is queued and/or run
            self.__status      = None
            self.__processId   = None
            self.__started     = None
            self.__lastDt      = None
            self.__progressMsg = None
        else:
            # Job already queued, take parms from database
            self.__loadJob()

        # Ensure we have everything we need
        if not self.__jobName:
            self.fail ("Every batch job must have a name")

        if not self.__command:
            self.fail ("Every batch job must have a command")


    #------------------------------------------------------------------
    # Load a job from the database
    #------------------------------------------------------------------
    def __loadJob (self):
        """
        Load a job object from data in the batch_job and batch_job_parm
        tables.

        Called by the constructor to construct a BatchJob instance from
        a known job id.
        """
        # Get info from database
        fields = ("name", "command", "process_id", "started", "status_dt",
                  "status", "email", "progress")
        query = db.Query("batch_job", *fields)
        query.where(query.Condition("id", self.__jobId))
        try:
            rows = query.execute(self.__cursor).fetchall()
            if not rows:
                self.fail("batch job %d not found" % self.__jobId)
            if len(rows) > 1:
                self.fail("found %d batch_jobs with id: %d" % self.__jobId)
        except Exception as e:
            self.fail("DB error loading job %d: %s" % (self.__jobId, e))

        # Load all data into instance
        row = rows[0]
        (self.__jobName, self.__command, self.__processId, self.__started,
         self.__lastDt, self.__status, self.__email, self.__progressMsg) = row

        # Get job parameters
        self.__args = {}
        query = db.Query("batch_job_parm", "name", "value")
        query.where(query.Condition("job", self.__jobId))
        try:
            rows = query.execute(self.__cursor).fetchall()
        except Exception as e:
            self.fail("error loading parms for job %d: %s" % (self.__jobId, e))

        # Load parameters into dictionary
        for row in rows:
            argKey = row[0]
            argVal = row[1]

            # If this is the first value for this key, simply store it
            if argKey not in self.__args:
                self.__args[argKey] = argVal

            # If more than one, re-create the original list
            else:
                # First one was loaded as a simple key, convert it to list
                if not isinstance(self.__args[argKey], type([])):

                    # Save value, delete key, re-create as a sequence
                    firstArg = self.__args[argKey]
                    del (self.__args[argKey])
                    argSeq = []
                    argSeq.append (firstArg)
                    self.__args[argKey] = argSeq

                # Append the new value
                self.__args[argKey].append (argVal)

        # Signal object loaded and processing ready to begin
        sendSignal (self.__conn, self.__jobId, ST_IN_PROCESS, PROC_BATCHJOB)


    #------------------------------------------------------------------
    # Simple accessors for data in object
    #------------------------------------------------------------------
    def getJobId(self):     return self.__jobId
    def getJobName(self):   return self.__jobName
    def getCommand(self):   return self.__command
    def getProcessId(self): return self.__processId
    def getStarted(self):   return self.__started
    def getArgs(self):      return self.__args
    def getEmail(self):     return self.__email
    def getCursor(self):    return self.__cursor

    #------------------------------------------------------------------
    # Get the email addresses as a list
    #------------------------------------------------------------------
    def getEmailList(self):
        """
        Converts a string of email addresses of any of the forms:
            "addr1 addr2"
            "addr1, addr2"
            "addr1; addr2"
            "addr1"
        to a list of addresses, without separators.

        Return:
            List of zero or more addresses, encoded as ASCII.

        Raises:
            UnicodeError if non-ASCII in email address.
        """
        # Get emails, in ASCII, raises exception on error
        emails = self.__email.encode("ascii")

        # Convert alternative separators to spaces
        trTbl  = string.maketrans(",;", "  ")
        emails = string.translate(emails, trTbl)

        # Return them as a list
        return string.split (emails)


    #------------------------------------------------------------------
    # Complex accessor for args
    #
    # Value for an argument can be a string, or a list.
    # Data may have gone into the database as ASCII
    #   but always comes out as unicode
    # Caller should say if he wants 16 bit unicode preserved
    # Else we convert to utf-8.
    #------------------------------------------------------------------
    def getParm(self, key, ucode=False):
        if key in self.__args:
            val = self.__args[key]
            if isinstance(val, (tuple, list)) and len(val) == 1:
                val = val[0]
            if ucode:
                # Simply return what we have
                return val
            else:
                # Convert to utf-8, but may have to do it on each list item
                if isinstance(val, type([])):
                    for i in range(len(val)):
                        val[i] = val[i].encode('utf-8')
                else:
                    val = val.encode('utf-8')
                return val
        return None


    #------------------------------------------------------------------
    # Queue the job in this object for batch processing
    #------------------------------------------------------------------
    def queue(self):
        """
        Places data in the batch_job table for the daemon
        to find and initiate.
        """
        self.log ("cdrbatch: entered queue")

        # Can't queue if already queued
        if self.__status:
            self.fail ("Can't requeue job that is already queued or started")

        # Queue the job
        try:
            # This simple version just queues the job
            # We might do something more sophisticated with the start time
            #   and with regular scheduling
            self.__cursor.execute("""
                INSERT INTO batch_job (name, command, started, status_dt,
                                       status, email)
                     VALUES (?, ?, GETDATE(), GETDATE(), ?, ?)
            """, (self.__jobName, self.__command, ST_QUEUED, self.__email))

        except Exception as e:
            self.fail("Database error queueing job: %s" % e)

        # Get the job id
        try:
            self.__cursor.execute("SELECT @@IDENTITY")
            row = self.__cursor.fetchone()
            if not row:
                self.fail("Unknown database error fetching job id")
            self.__jobId = int (row[0])
        except Exception as e:
            self.fail("Database error queueing job: %s" % e)

        # If there are any arguments, save them for batch job to retrieve
        # Args are a dictionary containing pairs of:
        #    Argument name (a string)
        #    Argument values (a string or a sequence of one or more strings)
        if self.__args:
            # For each argument name (key)
            for key in self.__args.keys():

                valSeq = self.__args[key]
                if isinstance(valSeq, (str, bytes)):
                    valSeq = [valSeq]

                # For each value in the sequence of values for this key
                for val in valSeq:
                    if not isinstance(val, str):
                        val = val.decode("utf-8")
                    try:
                        self.__cursor.execute ("""
                          INSERT INTO batch_job_parm (job, name, value)
                               VALUES (?, ?, ?)
                        """, (self.__jobId, key, val))
                    except Exception as e:
                        self.fail (
                             "Database error setting parameter %s=%s: %s" %
                                   (key, val, e))


    #------------------------------------------------------------------
    # Fail a batch job
    #------------------------------------------------------------------
    def fail(self, why, exit=1, logfile=LF):
        """
        Save the reason why this job has failed and exit.

        Pass:
            why     - reason to log in logfile.
            exit    - true=exit here, else raise exception.
            logfile - if caller doesn't want the standard batch job log.
        """
        # Normalize reason, mainly for unicode
        reason = str(why)

        # Try to set status, but only if we're not already in the midst
        #   of a recursive fail
        if not self.__failure:
            # Set reason, also prevents recursion on setStatus
            self.__failure = reason

            # If we're running part of a started job, update job info in db
            if self.__jobId:
                try:
                    self.setStatus (ST_ABORTED)
                    self.setProgressMsg (reason)
                except BatchException as be:
                    self.log ("Unable to update job status on failure: %s" % \
                              str(be), logfile)

            # Can't use this job any more, close it's connection
            if self.__conn:
                try:
                    self.__cursor.close()
                except Exception as e:
                    self.log ("Unable to close cursor: %s" % e, logfile)

        # Log reason
        self.log (reason, logfile)

        # Exit here
        if exit:
            sys.exit (1)


    #------------------------------------------------------------------
    # Check current status
    #------------------------------------------------------------------
    def getStatus (self):
        """
        An instance method fronting for the class getJobStatus method.
        Sets internal status variables and returns current status.

        Return:
            Tuple of:
                status
                date/time of last status setting
        """
        try:
            # Get status from first row, there is only one with unique job id
            statusRow = getJobStatus (idStr=str(self.__jobId))[0]
            self.__status = statusRow[3]
            self.__lastDt = statusRow[4]

        except BatchException as e:
            self.fail ("Unable to get status: %s" % e)

        return (self.__status, self.__lastDt)


    #------------------------------------------------------------------
    # Set status
    #------------------------------------------------------------------
    def setStatus (self, newStatus):
        """
        An instance method fronting for the class sendSignal method.
        Sets database status.

        Pass:
            New status, must be one of allowed ones for a batch job.
        """

        # Nothing to do if we're testing.
        if not self.__jobId:
            return

        try:
            sendSignal (self.__conn, self.__jobId, newStatus, PROC_BATCHJOB)
            self.__status = newStatus
        except BatchException as e:
            self.fail ("Unable to set status: %s" % str(e))


    #------------------------------------------------------------------
    # Set the user progress message
    #------------------------------------------------------------------
    def setProgressMsg (self, newMsg):

        # Don't bother for test jobs.
        if not self.__jobId:
            return

        # The internal one
        self.__progressMsg = newMsg

        # And in the database
        if isinstance(newMsg, str):
            newMsg = newMsg.encode("utf-8")
        try:
            self.__cursor.execute ("""
              UPDATE batch_job
                 SET progress = ?, status_dt = GETDATE()
               WHERE id = ?""", (newMsg, self.__jobId))
        except Exception as e:
            self.fail ("Unable to update progress message: %s" % e)


    #------------------------------------------------------------------
    # Write a message to the batch log
    #------------------------------------------------------------------
    def log(self, msg, logfile=LF):
        """
        Call cdr.logwrite to write a message to our logfile.
        msg may be a tuple, see cdr.logwrite().

        Pass:
            msg     - message string(s) to write.
            logfile - where to write.
        """
        if self.__jobId:
            newMsg = ["Job %d '%s':" % (self.__jobId, self.__jobName)]
        else:
            newMsg = ["Job '%s':" % self.__jobName]


        # Construct a tuple of messages including job identification
        if isinstance(msg, type(())) or isinstance(msg, type ([])):
            newMsg = newMsg + list(msg)
        else:
            newMsg.append (msg)
        tupMsg = tuple(newMsg)

        cdr.logwrite (tupMsg, logfile)

    #------------------------------------------------------------------
    # Show the user how to monitor the status of the report job.
    #------------------------------------------------------------------
    def show_status_page(self, session, title, subtitle, script,
                         extra_buttons=None):
        """
        Build a cdrcgi.Page object showing a link which can be followed
        to view the status of the batch job just queued up.  Send the
        page to the user's browser.

        Pass:
            session       - message string(s) to write
            title         - string for banner and /html/head/title element
            subtitle      - string to display below the banner
            script        - handler for buttons
            extra_buttons - action buttons to be prepended to the
                            buttons for jumping to the main menu
                            or logging out; a single button can
                            be passed as a string; multiple buttons
                            are passed as a sequence of strings (optional)
        """
        buttons = extra_buttons or []
        if isinstance(buttons, bytes):
            buttons = str(buttons, "utf-8")
        if isinstance(buttons, str):
            buttons = [buttons]
        parms   = "%s=%s&jobId=%s" % (cdrcgi.SESSION, session, self.__jobId)
        url     = "getBatchStatus.py?%s" % parms
        link    = cdrcgi.Page.B.A("link", href=url)
        start   = "To monitor the status of the job, click this "
        item    = "View Batch Job Status"
        finish  = " or use the CDR Administration menu to select '%s'." % item
        page    = cdrcgi.Page(title, subtitle=subtitle, action=script,
                              buttons=buttons + [cdrcgi.MAINMENU, "Log Out"],
                              session=session)
        page.add("<fieldset>")
        page.add(page.B.H4("Report has been queued for background processing"))
        page.add(page.B.P(start, link, finish))
        page.add("</fieldset>")
        page.send()
