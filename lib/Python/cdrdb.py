"""
    cdrdb - DB-SIG compliant module for CDR database access.

    Module Interface

        Access to the database is made available through connection
        objects.  The module must provide the following factory call
        for these:

        connect(parameters...)
            Constructor for creating a connection to the database.
            Returns a Connection Object. It takes a number of parameters
            which are database dependent.  [See documentation attached
            directly to function implementation.]

        These module globals must be defined:

            apilevel
                String constant stating the supported DB API level.
                Currently only the strings '1.0' and '2.0' are allowed.
                If not given, a Database API 1.0 level interface should
                be assumed.

            threadsafety
                Integer constant stating the level of thread safety the
                interface supports. Possible values are:
                    0 = Threads may not share the module.
                    1 = Threads may share the module, but not connections.
                    2 = Threads may share the module and connections.
                    3 = Threads may share the module, connections and cursors.

                Sharing in the above context means that two threads may use
                a resource without wrapping it using a mutex semaphore to
                implement resource locking. Note that you cannot always
                make external resources thread safe by managing access
                using a mutex: the resource may rely on global variables
                or other external sources that are beyond your control.

            paramstyle
                String constant stating the type of parameter marker
                formatting expected by the interface. Possible values are:
                    'qmark' = Question mark style, e.g. '...WHERE name=?'
                    'numeric' = Numeric, positional style,
                            e.g. '...WHERE name=:1'
                    'named' = Named style, e.g. '...WHERE name=:name'
                    'format' = ANSI C printf format codes,
                            e.g. '...WHERE name=%s'
                    'pyformat' = Python extended format codes,
                            e.g. '...WHERE name=%(name)s'

            The module should make all error information available
            through these exceptions or subclasses thereof:

                Warning
                    Exception raised for important warnings like data
                    truncations while inserting, etc. It must be a subclass
                    of the Python StandardError (defined in the module
                    exceptions).

                Error
                    Exception that is the base class of all other error
                    exceptions. You can use this to catch all errors with
                    one single 'except' statement. Warnings are not
                    considered errors and thus should not use this class
                    as base. It must be a subclass of the Python
                    StandardError (defined in the module exceptions).

                InterfaceError
                    Exception raised for errors that are related to the
                    database interface rather than the database itself.
                    It must be a subclass of Error.

                DatabaseError
                    Exception raised for errors that are related to the
                    database. It must be a subclass of Error.

                DataError
                    Exception raised for errors that are due to problems
                    with the processed data like division by zero, numeric
                    value out of range, etc. It must be a subclass of
                    DatabaseError.

                OperationalError
                    Exception raised for errors that are related to the
                    database's operation and not necessarily under the
                    control of the programmer, e.g. an unexpected disconnect
                    occurs, the data source name is not found, a transaction
                    could not be processed, a memory allocation error
                    occurred during processing, etc. It must be a subclass
                    of DatabaseError.

                IntegrityError
                    Exception raised when the relational integrity of the
                    database is affected, e.g. a foreign key check fails.
                    It must be a subclass of DatabaseError.

                InternalError
                    Exception raised when the database encounters an
                    internal error, e.g. the cursor is not valid anymore,
                    the transaction is out of sync, etc. It must be a
                    subclass of DatabaseError.

                ProgrammingError
                    Exception raised for programming errors, e.g. table
                    not found or already exists, syntax error in the SQL
                    statement, wrong number of parameters specified, etc.
                    It must be a subclass of DatabaseError.

            NotSupportedError
                Exception raised in case a method or database API was used
                which is not supported by the database, e.g. requesting
                a .rollback() on a connection that does not support
                transaction or has transactions turned off.  It must be
                a subclass of DatabaseError.

        This is the exception inheritance layout:

            StandardError
            |__Warning
            |__Error
               |__InterfaceError
               |__DatabaseError
                  |__DataError
                  |__OperationalError
                  |__IntegrityError
                  |__InternalError
                  |__ProgrammingError
                  |__NotSupportedError

        Note: The values of these exceptions are not defined. They should
        give the user a fairly good idea of what went wrong though.

        See also the specific documentation for the Connection and Cursor
        classes.
"""

#----------------------------------------------------------------------
#
# $Id$
#
#----------------------------------------------------------------------

import win32com.client
import time, os
import pywintypes

# Until we do this bogus object creation, the constants are invisible.
win32com.client.Dispatch("ADODB.Connection")

# Provides name resolution for different hosts
import cdrutil

# Provides lookup of database passwords from centralized file.
import cdrpw

# Setting up the propper database source
# --------------------------------------
# Default
CBIIT_HOSTING = True

# Accounting for alternate tiers later, see def connect()
h = cdrutil.AppHost(cdrutil.getEnvironment(), cdrutil.getTier())
if h.org == 'OCE':
    CDR_DB_SERVER = 'localhost'
    CBIIT_HOSTING = False
else:
    CDR_DB_SERVER = h.host['DBWIN'][0]

# Look in the environment for override of default location of CDR database.
_cdr_db_server = os.environ.get('CDR_DB_SERVER')
if _cdr_db_server:
    CDR_DB_SERVER = _cdr_db_server

# Logging support.  Set LOGFILE to log file pathname to enable logging.
LOGFILE = os.environ.get('CDR_DB_LOGFILE') or None
def debugLog(sql = None, params = None, what = "SQL Query"):
    if LOGFILE:
        import datetime
        now = datetime.datetime.now()
        now = "%d-%02d-%-2d %02d:%02d:%02d.%03d" % (now.year, now.month,
                                                    now.day, now.hour,
                                                    now.minute, now.second,
                                                    now.microsecond / 1000)
        try:
            fp = open(LOGFILE, 'a')
            fp.write("%s: %s\n" % (now, what))
            if sql:
                fp.write("%s\nParameters:\n%s\n" % (sql, params))
            fp.close()
        except:
            pass

#----------------------------------------------------------------------
# These module constants are required by the DBSIG's API.
#----------------------------------------------------------------------
apilevel     = '2.0'
threadsafety = 1
paramstyle   = 'qmark'

#----------------------------------------------------------------------
# Standard exception classes.
#----------------------------------------------------------------------
class Warning(StandardError):           pass
class Error(StandardError):             pass
class InterfaceError(Error):            pass
class DatabaseError(Error):             pass
class DataError(DatabaseError):         pass
class OperationalError(DatabaseError):  pass
class IntegrityError(DatabaseError):    pass
class InternalError(DatabaseError):     pass
class ProgrammingError(DatabaseError):  pass
class NotSupportedError(DatabaseError): pass

#----------------------------------------------------------------------
# Build a tuple of error strings for a raised exception.
#----------------------------------------------------------------------
def buildErrorList(conn):
    e = []
    for err in conn.Errors:
        e.append(err.Description)
    return tuple(e)

#----------------------------------------------------------------------
# Cursor object, returned by Connection.cursor().
#----------------------------------------------------------------------
class Cursor:
    """
    Has the following public attributes.

        description
            This read-only attribute is a sequence of 7-item sequences.
            Each of these sequences contains information describing one
            result column: (name, type_code, display_size, internal_size,
            precision, scale, null_ok). This attribute will be None for
            operations that do not return rows or if the cursor has not
            had an operation invoked via the executeXXX() method yet.

        rowcount
            This read-only attribute specifies the number of rows that the
            last executeXXX() produced (for DQL statements like select) or
            affected (for DML statements like update or insert).  The attribute
            is -1 in case no executeXXX() has been performed on the cursor
            or the rowcount of the last operation is not determinable by
            the interface.

        arraysize
            This read/write attribute specifies the number of rows to fetch
            at a time with fetchmany(). It defaults to 1 meaning to fetch a
            single row at a time.  Implementations must observe this value
            with respect to the fetchmany() method, but are free to interact
            with the database a single row at a time. It may also be used
            in the implementation of executemany().
    """

    def __init__(self, conn):
        self.__conn           = conn
        self.__rs             = None
        self.description      = None
        self.rowcount         = -1
        self.arraysize        = 100
        self.__dateTimeObject = pywintypes.Time(0)

    def callproc(self, procname, parameters, timeout = 120):
        """
        Call a stored database procedure with the given name. The sequence
        of parameters must contain one entry for each argument that the
        procedure expects. The result of the call is returned as modified
        copy of the input sequence. Input parameters are left untouched,
        output and input/output parameters replaced with possibly new
        values.  The procedure may also provide a result set as output.
        This must then be made available through the standard fetchXXX()
        methods.
        """

        self.__rs            = None
        self.description     = None
        self.rowcount        = -1
        cmd                  = win32com.client.Dispatch("ADODB.Command")
        cmd.CommandType      = win32com.client.constants.adCmdStoredProc
        cmd.CommandText      = procname
        cmd.ActiveConnection = self.__conn
        cmd.CommandTimeout   = timeout
        #cmd.Parameters.Refresh()
        params               = cmd.Parameters
        nParams              = params.Count
        if not nParams:
            raise ProgrammingError, ("Cursor.callproc",
                                   (u"Procedure %s not cataloged" % procname,))

        # Plug in the parameters
        if type(parameters) not in (list, tuple):
            parameters = (parameters,)
        if len(parameters) != nParams - 1:
            raise ProgrammingError, ("Cursor.callproc",
                    (u"expected %d parameters, received %d" %
                     (nParams - 1, len(parameters)),))
        for i in range(len(parameters)):
            p = params.Item(i + 1)
            if p.Type in BINARY.nativeTypes:
                p.Value = buffer(parameters[i])
            else:
                p.Value = parameters[i]
        try:
            debugLog(procname, parameters, "Stored Procedure")
            self.__rs, rowsAffected = cmd.Execute()
            fields = self.__rs.Fields
            if len(fields):
                desc = []
                for field in fields:
                    desc.append(self.__getFieldDesc(field))
                self.description = desc
            else:
                self.rowcount = rowsAffected

            # XXX Won't be available yet if there's a pending result set.
            for i in range(len(parameters)):
                p = params.Item(i + 1)
                if p.Direction == win32com.client.constants.adParamOutput or \
                   p.Direction == win32com.client.constants.adParamInputOutput:
                    parameters[i] = p.Value
            return params.Item(0).Value

        except:
            errorList = buildErrorList(self.__conn)
            if errorList:
                raise Error, ("Cursor.callproc", errorList)
            raise InternalError, ("Cursor.callproc",
                    ((u"internal error in '%s'" % procname),))

    def close(self):
        """
        Close the cursor now (rather than whenever __del__ is called).
        The cursor will be unusable from this point forward; an Error
        (or subclass) exception will be raised if any operation is
        attempted with the cursor.
        """

        if self.__rs:
            debugLog(what = "closing resultset with state %s" % self.__rs.State)
            if self.__rs.State == win32com.client.constants.adStateOpen:
                self.__rs.Close()
        self.__rs        = None
        self.description = None
        self.rowcount    = -1

    def execute(self, query, params = None, timeout = 120):
        """
        Prepare and execute a database operation (query or command).
        Parameters may be provided as sequence or mapping and will be
        bound to variables in the operation. Variables are specified
        in a database-specific notation (see the module's paramstyle
        attribute for details).

        A reference to the operation will be retained by the cursor.
        If the same operation object is passed in again, then the
        cursor can optimize its behavior. This is most effective for
        algorithms where the same operation is used, but different
        parameters are bound to it (many times).

        For maximum efficiency when reusing an operation, it is best
        to use the setinputsizes() method to specify the parameter
        types and sizes ahead of time. It is legal for a parameter
        to not match the predefined information; the implementation
        should compensate, possibly with a loss of efficiency.

        The parameters may also be specified as list of tuples to e.g.
        insert multiple rows in a single operation, but this kind of
        usage is depreciated: executemany() should be used instead.
        [This is not supported by this implementation.]

        Return values are not defined.
        """

        if isinstance(query, Query):
            query = str(query)
        self.__rs            = None
        self.description     = None
        self.rowcount        = -1
        cmd                  = win32com.client.Dispatch("ADODB.Command")
        cmd.ActiveConnection = self.__conn
        cmd.CommandText      = query
        cmd.CommandType      = win32com.client.constants.adCmdText
        cmd.CommandTimeout   = timeout
        try:
            if params:
                cmdParams = cmd.Parameters
                if type(params) not in (tuple, list):
                    params = (params,)
                if len(cmdParams) != len(params):
                    raise ProgrammingError, ("Cursor.execute",
                            (u"expected %d parameters, received %d" %
                             (len(cmdParams), len(params)),))
                for i in range(len(params)):
                    p = cmdParams.Item(i)
                    if p.Type in BINARY.nativeTypes:
                        p.Value = buffer(params[i])
                    else:
                        p.Value = params[i]
            debugLog(query, params)
            self.__rs, rowsAffected = cmd.Execute()
            fields = self.__rs.Fields
            if len(fields):
                desc = []
                for field in fields:
                    desc.append(self.__getFieldDesc(field))
                self.description = desc
            else:
                self.rowcount = rowsAffected

        except:
            queryStr  = ' Query: "' + query + '"'
            if(params):
                queryStr += ' Params: ' + str(params)
            errorList = buildErrorList(self.__conn)
            if errorList:
                raise Error, ("Cursor.execute:%s" % queryStr, errorList)
            raise InternalError, ("Cursor.execute",
                    ((u"unexpected failure for query:%s" % queryStr),))

    def executemany(self, query, paramSets):
        """
        Prepare a database operation (query or command) and then execute
        it against all parameter sequences or mappings found in the
        sequence seq_of_parameters.

        Modules are free to implement this method using multiple calls
        to the execute() method or by using array operations to have the
        database process the sequence as a whole in one call.

        The same comments as for execute() also apply accordingly to this
        method.

        Return values are not defined.
        """

        for paramSet in paramSets: self.execute(query, paramSet)

    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single
        sequence, or None when no more data is available.

        An Error (or subclass) exception is raised if the previous
        call to executeXXX() did not produce any result set or no
        call was issued yet.
        """

        rows = self.fetchmany(1, 0)
        return rows and rows[0] or None

    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.

        An Error (or subclass) exception is raised if the previous call
        to executeXXX() did not produce any result set or no call was issued
        yet.
        """

        return self.fetchmany(-1, 0)

    def fetchmany(self, size = None, rememberSize = 1):
        """
        Fetch the next set of rows of a query result, returning a sequence
        of sequences (e.g. a list of tuples). An empty sequence is returned
        when no more rows are available.

        The number of rows to fetch per call is specified by the parameter.
        If it is not given, the cursor's arraysize determines the number of
        rows to be fetched. The method should try to fetch as many rows as
        indicated by the size parameter. If this is not possible due to the
        specified number of rows not being available, fewer rows may be
        returned.

        An Error (or subclass) exception is raised if the previous call to
        executeXXX() did not produce any result set or no call was issued yet.

        Note there are performance considerations involved with the size
        parameter. For optimal performance, it is usually best to use the
        arraysize attribute. If the size parameter is used, then it is best
        for it to retain the same value from one fetchmany() call to the next.
        """

        if not self.__rs or not self.description:
            raise ProgrammingError, ("Cursor.fetchmany",
                                    (u"No result set available",))
        if size == None:
            size = self.arraysize
        if rememberSize:
            self.arraysize = size
        if self.__rs.EOF: return []
        try:
            data  = self.__rs.GetRows(size)
            rows  = []
            nCols = len(data)
            nRows = len(data[0])
            for row in range(nRows):
                vals = []
                for col in range(nCols):
                    val = data[col][row]
                    if type(val) == type(self.__dateTimeObject):
                        val = val.Format("%Y-%m-%d %H:%M:%S")
                    elif self.description[col][1] == BINARY:
                        val = str(val)
                    vals.append(val)
                rows.append(vals)
            debugLog(what = "fetch(size=%s): nrows=%s rs.EOF=%s" %
                     (size, len(rows), self.__rs.EOF))
            return rows

        except:
            errorList = buildErrorList(self.__conn)
            if errorList:
                raise Error, ("Cursor.fetchmany", errorList)
            raise InternalError, ("Cursor.fetchmany",
                    (u"unexpected failure",))

    def nextset(self):
        """
        This method will make the cursor skip to the next available set,
        discarding any remaining rows from the current set.

        If there are no more sets, the method returns None. Otherwise,
        it returns a true value and subsequent calls to the fetch methods
        will return rows from the next result set.

        An Error (or subclass) exception is raised if the previous call
        to executeXXX() did not produce any result set or no call was
        issued yet.
        """

        self.description     = None
        self.rowcount        = -1
        if not self.__rs:
            raise ProgrammingError, ("Cursor.nextset",
                    (u"no record sets available",))

        try:
            self.__rs, rowsAffected = self.__rs.NextRecordset()
            debugLog(what = "rs.NextRecordset() rowsAffected=%s" % rowsAffected)
            if not self.__rs:
                return None
            fields = self.__rs.Fields
            if len(fields):
                desc = []
                for field in fields:
                    desc.append(self.__getFieldDesc(field))
                self.description = desc
            else:
                self.rowcount = rowsAffected
            return 1

        except:
            errorList = buildErrorList(self.__conn)
            if errorList:
                raise Error, ("Cursor.nextset", errorList)
            raise InternalError, ("Cursor.nextset",
                                 (u"unexpected failure",))

    def setinputsizes(self, sizes):
        """
        This can be used before a call to executeXXX() to predefine
        memory areas for the operation's parameters.

        sizes is specified as a sequence -- one item for each input
        parameter. The item should be a Type Object that corresponds
        to the input that will be used, or it should be an integer
        specifying the maximum length of a string parameter. If the
        item is None, then no predefined memory area will be reserved
        for that column (this is useful to avoid predefined areas for
        large inputs).

        This method would be used before the executeXXX() method is invoked.

        Implementations are free to have this method do nothing and users
        are free to not use it.
        """

        pass

    def setoutputsize(self, size, col = 0):
        """
        Set a column buffer size for fetches of large columns (e.g. LONGs,
        BLOBs, etc.). The column is specified as an index into the result
        sequence. Not specifying the column will set the default size for
        all large columns in the cursor.

        This method would be used before the executeXXX() method is invoked.

        Implementations are free to have this method do nothing and users
        are free to not use it.
        """

        pass

    def __nativeTypeToApiType(self, nativeType):
        if nativeType in STRING  .nativeTypes: return STRING
        if nativeType in BINARY  .nativeTypes: return BINARY
        if nativeType in NUMBER  .nativeTypes: return NUMBER
        if nativeType in DATETIME.nativeTypes: return DATETIME
        if nativeType in ROWID   .nativeTypes: return ROWID
        raise NotSupportedError, ("Cursor.__nativeTypeToApiType",
                ((u"unrecognized native type %d" % nativeType),))

    def __getFieldDesc(self, field):
        name         = field.Name
        typeCode     = self.__nativeTypeToApiType(field.Type)
        internalSize = field.DefinedSize
        displaySize  = internalSize
        internalSize = field.DefinedSize
        precision    = field.Precision
        scale        = field.NumericScale
        nullable     = (field.Attributes &
                        win32com.client.constants.adFldIsNullable)
        if typeCode in (NUMBER, DATETIME, ROWID): displaySize = 20
        return (name,
                typeCode,
                displaySize,
                internalSize,
                precision,
                scale,
                nullable)

#----------------------------------------------------------------------
# Connection object which knows how to connect to CDR database.
#----------------------------------------------------------------------
class Connection:

    def __init__(self, adoConn):
        self.__adoConn    = adoConn
        self.__inTrans    = 0
        self.__autoCommit = 1
        self.setAutoCommit(0)

    def close(self):
        """
        Close the connection now (rather than whenever __del__ is called).
        The connection will be unusable from this point forward;
        an Error (or subclass) exception will be raised if any operation
        is attempted with the connection. The same applies to all cursor
        objects trying to use the connection.
        """

        try:
            debugLog(what = "closing connection")
            self.__adoConn.Close()
        except:
            errorList = buildErrorList(self.__adoConn)
            if errorList:
                raise Error, ("Connection.close", errorList)
            raise InternalError, ("Connection.close",
                    (u"unexpected failure",))

    def commit(self):
        """
        Commit any pending transaction to the database. Note that if
        the database supports an auto-commit feature, this must be
        initially off. An interface method may be provided to turn
        it back on.
        """

        try:
            debugLog(what = "commit")
            self.__adoConn.CommitTrans()
            if self.__autoCommit: self.__inTrans = False
            else:
                self.__adoConn.BeginTrans()
                self.__inTrans = True
        except:
            errorList = buildErrorList(self.__adoConn)
            if errorList:
                raise Error, ("Connection.commit", errorList)
            raise InternalError, ("Connection.commit",
                    (u"unexpected failure",))

    def rollback(self):
        """
        Causes the database to roll back to the start of any pending
        transaction.  Closing a connection without committing the
        changes first will cause an implicit rollback to be performed.
        """

        try:
            debugLog(what = "rollback")
            self.__adoConn.RollbackTrans()
            if self.__autoCommit: self.__inTrans = False
            else:
                self.__adoConn.BeginTrans()
                self.__inTrans = True
        except:
            errorList = buildErrorList(self.__adoConn)
            if errorList:
                raise Error, ("Connection.rollback", errorList)
            raise InternalError, ("Connection.rollback",
                    (u"unexpected failure",))

    def cursor(self):
        """
        Returns a new Cursor object using this connection.
        """

        return Cursor(self.__adoConn)

    def setAutoCommit(self, on = True):
        """
        Turns autocommit on or off.  If autocommit is on, then
        implicit transactions are not used (in contradiction to
        the SQL standard, but in keeping with SQL Server's default
        behavior).

        Returns the previous state of autocommit, before any changes
        made by this routine.
        """

        # Remember what we had
        oldAutoCommit = self.__autoCommit

        debugLog(what = "setting auto-commit %s (was %s)" %
                 (on and "on" or "off", oldAutoCommit and "on" or "off"))
        try:
            if on:
                #self.__adoConn.Execute("SET IMPLICIT_TRANSACTIONS OFF")
                if self.__autoCommit: return
                self.__autoCommit = True
                if self.__inTrans:
                    self.commit()
            else:
                #self.__adoConn.Execute("SET IMPLICIT_TRANSACTIONS ON")
                if not self.__autoCommit: return
                self.__autoCommit = False
                self.__adoConn.BeginTrans()
                self.__inTrans = True
        except:
            errorList = buildErrorList(self.__adoConn)
            if errorList:
                raise Error, ("Connection.setAutoCommit", errorList)
            raise InternalError, ("Connection.setAutoCommit",
                    (u"unexpected failure",))

        return oldAutoCommit

    def getAutoCommit(self):
        """
        Returns the current value of autocommit.
        """
        return self.__autoCommit


#----------------------------------------------------------------------
# Connect to the CDR using known login account.
#----------------------------------------------------------------------
def connect(user='cdr', dataSource=CDR_DB_SERVER, db='cdr'):
    """
    Factory for creating a connection to the database.  Returns a
    Connection Object. It takes a number of parameters which are
    database dependent.  This implementation expects only the user
    name for the database login account.  The 'cdr' account is used
    for standard database activity for which the permission to alter
    data is required.  The 'CdrGuest' account has read-only access
    to the CDR data.
    """

    global CBIIT_HOSTING
    if dataSource != CDR_DB_SERVER:
        # Default server name established above
        # If it's anything else, establish the network name here
        global h
        hostInfo = h.getTierHostNames(dataSource, 'DBWIN')
        if hostInfo:
            dataSource = hostInfo.qname()

    adoConn = win32com.client.Dispatch("ADODB.Connection")
    if CBIIT_HOSTING:
        port = 55373
        if h.tier == "PROD":
            port = 55733
        if user.upper() == "CDR":
            user = "cdrsqlaccount"
    else:
        port = 32408
    password = cdrpw.password(h.org, h.tier, db, user)
    try:
        connString = ("Provider=SQLOLEDB;"
                      "Data Source=%s,%d;"
                      "Initial Catalog=%s;"
                      "User ID=%s;"
                      "Password=%s" %
                      (dataSource, port, db, user, password))
        debugLog(what="CONNECTION STRING: %s" % repr(connString))
        adoConn.Open(connString)
    except:
        raise DatabaseError, ("connect", buildErrorList(adoConn))
    return Connection(adoConn)

class Type:
    def __init__(self, *nativeTypes):
        self.nativeTypes = nativeTypes

STRING   = Type(win32com.client.constants.adBoolean,
                win32com.client.constants.adBSTR,
                win32com.client.constants.adChar,
                win32com.client.constants.adLongVarChar,
                win32com.client.constants.adLongVarWChar,
                win32com.client.constants.adVarChar,
                win32com.client.constants.adVariant,
                win32com.client.constants.adVarWChar,
                win32com.client.constants.adWChar)
BINARY   = Type(win32com.client.constants.adBinary,
                win32com.client.constants.adLongVarBinary,
                win32com.client.constants.adVarBinary)
NUMBER   = Type(win32com.client.constants.adBigInt,
                win32com.client.constants.adCurrency,
                win32com.client.constants.adDecimal,
                win32com.client.constants.adDouble,
                win32com.client.constants.adInteger,
                win32com.client.constants.adNumeric,
                win32com.client.constants.adSingle,
                win32com.client.constants.adSmallInt,
                win32com.client.constants.adTinyInt,
                win32com.client.constants.adUnsignedBigInt,
                win32com.client.constants.adUnsignedInt,
                win32com.client.constants.adUnsignedSmallInt,
                win32com.client.constants.adUnsignedTinyInt)
DATETIME = Type(win32com.client.constants.adDate,
                win32com.client.constants.adDBDate,
                win32com.client.constants.adDBTime,
                win32com.client.constants.adDBTimeStamp)
ROWID    = Type(win32com.client.constants.adGUID,)

# mandatory type helpers
def Date(year, month, day):
    return "%04d-%02d-%02d" % (year, month, day)

def Time(hour, minute, second):
    return "%02d:%02d:%02d" % (hour, minute, second)

def Timestamp(year, month, day, hour, minute, second):
    return "%04d-%02d-%02d %02d:%02d:%02d" % (year, month, day,
                                              hour, minute, second)

def DateFromTicks(ticks):
    return "%04d-%02d-%02d" % time.localtime(ticks)[:3]

def TimeFromTicks(ticks):
    return "%02d:%02d:%02d" % time.localtime(ticks)[3:6]

def TimestampFromTicks(ticks):
    return "%04d-%02d-%02d %02d:%02d:%02d" % time.localtime(ticks)[:6]

# extension helper
def strftime(format, canonicalString):
    """
    Takes canonical string form of date/time value and formats it
    using the time module's strftime().  Unfortunately the Windows
    platform doesn't have strptime(), so we have to roll our own.
    """

    # Only pull this in if we need it.
    import re

    # Assume YYYY-MM-DD HH:MM:SS format.
    match = re.match(r"(\d{4})-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)",
                     canonicalString)
    if not match:
        raise ProgrammingError("strftime()",
                               ("invalid argument", canonicalString))

    # Pull out the pieces.
    try:
        year   = int(match.group(1))
        month  = int(match.group(2))
        day    = int(match.group(3))
        hour   = int(match.group(4))
        minute = int(match.group(5))
        second = int(match.group(6))
    except:
        raise ProgrammingError("strftime()",
                               ("invalid argument", canonicalString))

    # Create a tuple we can use with time.strftime().
    time_tuple = (year, month, day, hour, minute, second, 0, 0, -1)

    # Normalize to fill out the last three (unknown) values of the tuple.
    time_tuple = time.localtime(time.mktime(time_tuple))

    return time.strftime(format, time_tuple)

class Query:

    """
    Builder for SQL select queries.

    Example usage:

        query = cdrdb.Query('t1 a', 'a.title', 'b.name AS "Type"')
        query.join('t2 b', 'b.id = a.t2')
        query.where(query.Condition('b.name', ('Foo', 'Bar'), 'IN'))

        # To see the generated SQL
        print query

        # To execute and cleanup
        cursor = query.execute()
        rows = cursor.fetchall()
        cursor.close()

        # Or alternatively if closing the cursor doesn't matter
        rows = query.execute().fetchall()
    """

    def __init__(self, table, *columns):
        """
        Initializes a SQL query builder

        Passed:

            table           table name with possible alias
            columns         one or more column names to be selected,
                            qualified with alias if necessary; a column
                            can be an expression
        """
        self._table = table
        self._columns = columns
        self._joins = []
        self._where = []
        self._group = []
        self._having = []
        self._order = []
        self._parms = []
        self._unions = []
        self._timeout = 120
        self._alias = None
        self._into = None
        self._cursor = None
        self._limit = None
        self._unique = False
        self._str = None
        self._outer = False

    def timeout(self, value):
        """
        Override the default timeout of 120 seconds with a new value.
        """
        self._timeout = int(value)
        return self

    def join(self, table, *conditions):
        """
        Join to an additional table (or view)

        Each condition can be a simple string (e.g., 't.id = d.doc_type')
        or a more complicated Condition or Or object.

        If you don't supply at least one condition, you might be
        unpleasantly surprised by the results. :-)
        """
        self._joins.append(Query.Join(table, False, *conditions))
        self._str = None
        return self

    def outer(self, table, *conditions):
        """
        Create a left outer join

        Sets the self._outer flag so the formatter knows to add
        extra left padding to the query as needed.  Otherwise works
        the same as the join() method.
        """
        self._joins.append(Query.Join(table, True, *conditions))
        self._outer = True
        self._str = None
        return self

    def where(self, condition):
        """
        Adds a condition for the query's WHERE clause

        A condition can be a simple string (e.g., 't.id = d.doc_type')
        or a more complicated Condition or Or object.
        """
        self._where.append(condition)
        self._str = None
        return self

    def group(self, *columns):
        """
        Adds one or more columns to be used in the query's GROUP BY clause

        Example usage:
            query.group('d.id', 'd.title')
        """
        for column in columns:
            Query._add_sequence_or_value(column, self._group)
        self._str = None
        return self

    def having(self, condition):
        """
        Adds a condition to the query's HAVING clause

        A condition can be a simple string (e.g., 't.id = d.doc_type')
        or a more complicated Condition or Or object.
        """
        self._having.append(condition)
        self._str = None
        return self

    def union(self, query):
        """
        Add a query to be UNIONed with this one.

        Use this when you want to apply the ORDER BY clause to the UNIONed
        queries as a whole.  Make sure only this query has an ORDER set.
        If you need each component query to maintain its own internal order,
        construct and serialize each separately, and assemble them by hand.
        For example:

        q1 = cdrdb.Query(...).join.(...).where(...).order(...)
        q2 = cdrdb.Query(...).join.(...).where(...).order(...)
        union = cdrdb.Query(q1, "*").union(cdrdb.Query(q2, "*"))
        """
        self._unions.append(query)
        self._str = None
        return self

    def order(self, *columns):
        """
        Add the column(s) to be used to sort the results

        Example usage:
            query.order('doc_type.name', 'version.dt DESC')
        """
        temp = []
        for column in columns:
            Query._add_sequence_or_value(str(column), temp)
        for column in temp:
            column = column.strip()
            words = column.split()
            if len(words) > 2:
                raise Exception("invalid order column %s" % repr(column))
            if len(words) == 2 and words[1].upper() not in ("ASC", "DESC"):
                raise Exception("invalid order column %s" % repr(column))
            self._order.append(" ".join(words))
        self._str = None
        return self

    def limit(self, limit):
        """
        Sets maximum number of rows to return
        """
        if type(limit) is not int:
            raise Exception("limit must be integer")
        self._limit = limit
        self._str = None
        return self

    def unique(self):
        """
        Requests that duplicate rows be eliminated
        """
        self._unique = True
        self._str = None
        return self

    def cursor(self, cursor):
        """
        Pass in a cursor to be used for the query.
        """
        self._cursor = cursor
        return self

    def execute(self, cursor=None, timeout=None):
        """
        Assemble and execute the SQL query, returning the cursor object

        As with the Miranda rule, if you do not supply a cursor,
        one will be provided for you.

        Note that the temporary 'sql' variable is assigned before
        invoking the cursor's execute() method, to make sure that
        the _parms sequence has been constructed.
        """
        cursor = cursor or self._cursor or connect("CdrGuest").cursor()
        sql = str(self)
        if not timeout:
            timeout = self._timeout
        cursor.execute(sql, tuple(self._parms), timeout=timeout)
        return cursor

    def alias(self, alias):
        """
        Assigns an alias for a query so that it can be used as a virtual
        table as the target of a FROM clause:

            SELECT xxx.this, xxx.that, yyy.other
              FROM (
                  SELECT this, that
                    FROM whatever
              ) AS xxx

        Example usage:

            q1 = cdrdb.Query('whatever', 'this', 'that').alias('xxx')
            q2 = cdrdb.Query(q1, 'xxx.this', 'xxx.that', 'yyy.other')
            q2.join('other_table yyy', ...)
        """
        self._alias = alias
        self._str = None
        return self

    def parms(self):
        """
        Accessor method for query parameters

        Return the list of parameters to be passed to the database
        engine for the execution of the query.  Will be in the
        correct order, matching the position of the corresponding
        placeholders in the query string.
        """

        # Make sure the parameters have been assembled.
        if self._str is None:
            dummy = str(self)
        return self._parms

    def _align(self, keyword, rest=""):
        """
        Internal helper method to make the SQL query easier to read
        """
        keyword = " " * self._indent + keyword
        return "%s %s" % (keyword[-self._indent:], rest)

    def into(self, name):
        """
        Specify name of table to be created by this query

        Prefix the name with the octothorpe character ('#') to create
        a temporary table.
        """
        self._into = name
        self._str = None
        return self

    def log(self, **parms):
        import cdr
        logfile = parms.get("logfile", cdr.DEFAULT_LOGDIR + "/query.log")
        label = parms.get("label", "QUERY")
        output = u"%s:\n%s" % (label, self)
        if self._parms:
            parms = ["PARAMETERS:"] + [repr(p) for p in self._parms]
            output += "\n" + u"\n\t".join(parms)
        cdr.logwrite(output, logfile)

    def __str__(self):
        """
        Assemble the query for execution or logging.

        The format of the query string is arranged to make reading
        by a human easier.  The assembled query is cached.

        A side effect of a call to this method is that the sequence
        of all parameters to be passed to the database engine for
        execution of the query is constructed as the '_parms' member.
        """

        # If our cached string is not stale, use it.
        if self._str:
            return self._str

        # Start with a fresh paramater list.
        self._parms = []

        # Start the select statement, and calculate needed left padding.
        select = "SELECT"
        if self._unique:
            select += " DISTINCT"
        if self._limit is not None:
            select += " TOP %d" % self._limit
        self._indent = len(select)
        for attribute, keywords in (
            (self._order, "ORDER BY"),
            (self._outer, "LEFT OUTER JOIN"),
            (self._group, "GROUP BY")):
            if attribute:
                needed = len(keywords) - self._indent
                if needed > 0:
                    self._indent += needed
        query = [self._align(select, ", ".join(self._columns))]

        # Add clause to store results in a new table if requested.
        if self._into:
            query.append(self._align("INTO", self._into))

        # Is the base table itself a query?
        if isinstance(self._table, Query):

            # Make sure it has an alias.
            alias = self._table._alias
            if not alias:
                raise Exception("Virtual tables must have an alias")

            # SQL Server won't accept placeholders here.
            sql = str(self._table)
            if self._table._parms:
                raise Exception("Placeholders not allowed in virtual table")

            # Add the indented query in parentheses.
            query.append(self._align("FROM", "("))
            query.append(Query.indent("%s) %s" % (self._table, alias)))

        # No: just a plain vanilla FROM clause.
        else:
            query.append(self._align("FROM", self._table))

        # Add JOIN clauses for any additional tables used for the query.
        for join in self._joins:
            self._serialize_join(query, join)

        # Add the conditions used to restrict the set of results.
        keyword = "WHERE"
        for condition in self._where:
            self._serialize_condition(query, keyword, condition)
            keyword = "AND"

        # If the query uses aggregates, specify column for the grouping.
        if self._group:
            query.append(self._align("GROUP BY", ", ".join(self._group)))

        # Specify any restrictions on the results based on aggregations.
        if self._having:
            keyword = "HAVING"
            for condition in self._having:
                self._serialize_condition(query, keyword, condition)
                keyword = "AND"

        # Add any queries to be spliced to this one
        for union in self._unions:
            query.append(self._align("UNION"))
            query.append(str(union))

        # Specify the sorting of the result set if requested.
        if self._order:
            query.append(self._align("ORDER BY", ", ".join(self._order)))

        # Assemble everything and cache the results.
        self._str = "\n".join(query)

        # Give the caller the resulting SQL.
        return self._str

    def _serialize_or_set(self, query, keyword, or_set, prefix, suffix):
        """
        Internal helper method for building the query string

        This method has four responsibilities:
         1. Wrap the set of OR conditions in properly balanced parentheses
         2. Connect the conditions with the "OR" keyword
         3. Hand of serialization of each condition to _serialize_condition
         4. Connect nested sequences of conditions with the "AND" keyword
        """
        open_paren = "(" + prefix
        close_paren = ""
        for i, condition in enumerate(or_set.conditions):
            last_or = (i == len(or_set.conditions) - 1)
            if type(condition) in (tuple, list):
                for j, c in enumerate(condition):
                    if last_or and j == len(condition) - 1:
                        close_paren = suffix + ")"
                    self._serialize_condition(query, keyword, c,
                                              open_paren, close_paren)
                    keyword = "AND"
                    open_paren = ""
            else:
                if last_or:
                    close_paren = suffix + ")"
                self._serialize_condition(query, keyword, condition,
                                          open_paren, close_paren)
            keyword = "OR"
            open_paren = ""

    def _serialize_condition(self, query, keyword, condition, prefix="",
                             suffix=""):
        """
        Internal helper method for building the query string.
        """

        # Hand off the work for an Or set
        if isinstance(condition, Query.Or):
            self._serialize_or_set(query, keyword, condition, prefix, suffix)
            return

        # Handle the easy cases.
        if not isinstance(condition, Query.Condition):
            query.append(self._align(keyword, prefix + condition + suffix))
            return

        # Start the test string.
        test = "%s %s%s" % (condition.column, prefix, condition.test)

        # Handle a nested query.
        if isinstance(condition.value, Query):

            # Serialize the nested query.
            nested = condition.value
            alias = nested._alias and (" %s" % nested._alias) or ""
            serialized = "%s)%s%s" % (nested, alias, suffix)

            # Finish the condition.
            query.append(self._align(keyword, test + " ("))
            query.append(Query.indent(serialized))
            self._parms += nested._parms

        # Handle a sequence of values.
        elif condition.test.upper() in ("IN", "NOT IN"):

            # Make sure we have a list.
            values = condition.value
            if type(values) not in (list, tuple):
                values = [values]

            # Must have at least one value.
            if not values:
                raise Exception("%s test with no values" %
                                repr(condition.test.upper()))

            # Add the placeholders.
            test += " (%s)" % ", ".join(["?"] * len(values))

            # Plug in the condition to the query string.
            query.append(self._align(keyword, test + suffix))

            # Add the parameters.
            self._parms += values

        # Last case: single value test.
        else:
            if type(condition.value) in (list, tuple):
                raise Exception("Unexpected sequence of values")
            query.append(self._align(keyword, "%s ?%s" % (test, suffix)))
            self._parms.append(condition.value)

    def _serialize_join(self, query, join):
        """
        Helper function for building the query string.
        """
        keyword = join.outer and "LEFT OUTER JOIN" or "JOIN"

        # Is this 'table' being constructed on the fly?
        if isinstance(join.table, Query):

            # Make sure it has been provided with an alias.
            alias = join._alias
            if not alias:
                raise Exception("resultset expression without alias")

            # SQL Server won't accept placeholders here.
            if join.parms():
                raise Exception("Placeholders not allowed in joined "
                                "resultset expression")

            # Add the table expression indented and in parentheses.
            query.append(self._align(keyword, "("))
            query.append(Query.indent("%s) %s" % (join.table, alias)))

        # No, just a named table.
        else:
            query.append(self._align(keyword, join.table))

        # Add the conditions for the join.
        keyword = "ON"
        for condition in join.conditions:
            self._serialize_condition(query, keyword, condition)
            keyword = "AND"

    @staticmethod
    def indent(block, n=4):
        """
        Indent a block containing one or more lines by a number of spaces
        """
        if isinstance(block, Query):
            block = str(block)
        padding = " " * n
        end = block.endswith("\n") and "\n" or ""
        lines = block.splitlines()
        return "\n".join(["%s%s" % (padding, line) for line in lines]) + end

    @staticmethod
    def _add_sequence_or_value(to_be_added, collection):
        if type(to_be_added) is list:
            collection += to_be_added
        elif type(to_be_added) is tuple:
            collection += list(to_be_added)
        else:
            collection.append(to_be_added)

    class Condition:
        """
        Test of a value (typically, but not necessarily a column; could
        also be an expression, or even a constant value), against a
        second value (which can be a single value, or a query which
        returns a single value, or a sequence of values in the case
        of an "IN" or "NOT IN" test).
        """

        def __init__(self, col, val, test="="):
            self.column = col
            self.value = val
            self.test = test
    C = Condition

    class Or:
        """
        Represents a set of one or more conditions the satisfaction of
        any one of which will be considered as satisfying the entire
        set.

        Simple example:

            query = cdrdb.Query('t1', 'c1', 'c2')
            first_test = 'c1 < 42'
            second_test = query.Condition('c2', get_some_values(), 'IN')
            query.where(query.Or(first_test, second_test))
        """

        def __init__(self, *conditions):
            """
            Accepts one or more conditions, each of which can be either
            a string containing a SQL expression, or a Query.Condition
            object.  Any argument can also be a sequence of SQL expressions
            and/or Query.Condition or Query.Or objects, which will all be
            ANDed together as a single unit to be ORed against the tests
            represented by the other arguments to the constructor.  There
            is no limit (other than that imposed by the computing resources
            on the client and server machines) to the level of nesting
            supported for combinations of AND and OR condition sets.
            """
            self.conditions = conditions

    class Join:
        """
        Used internally to represent a SQL JOIN clause
        """

        def __init__(self, table, outer, *conditions):
            self.table = table
            self.outer = outer
            self.conditions = []
            for condition in conditions:
                Query._add_sequence_or_value(condition, self.conditions)

    @staticmethod
    def report(test_number, query, outcome):
        print "Test %d...%s" % (test_number, outcome and "passed" or "failed")
        f = open("Query.tests", "a")
        banner = (" Test %d " % test_number).center(70, "=")
        f.write("%s\n" % banner)
        f.write("%s\n" % query)
        if query._parms:
            f.write("%s\n" % ("-" * 70))
            for p in query._parms:
                f.write("%s\n" % repr(p))
        f.close()

    @staticmethod
    def test():
        """
        Run tests to check the health of the Query class.
        """

        # Convenience aliases
        Q = Query
        C = Query.Condition
        R = Query.report

        # Create some test tables.
        c = connect("CdrGuest").cursor()
        c.execute("CREATE TABLE #t1 (i INT, n VARCHAR(32))")
        c.execute("CREATE TABLE #t2 (i INT, n VARCHAR(32))")
        c.execute("INSERT INTO #t1 VALUES(42, 'Alan')")
        c.execute("INSERT INTO #t1 VALUES(43, 'Bob')")
        c.execute("INSERT INTO #t1 VALUES(44, 'Volker')")
        c.execute("INSERT INTO #t1 VALUES(45, 'Elmer')")
        c.execute("INSERT INTO #t2 VALUES(42, 'biology')")
        c.execute("INSERT INTO #t2 VALUES(42, 'aviation')")
        c.execute("INSERT INTO #t2 VALUES(42, 'history')")
        c.execute("INSERT INTO #t2 VALUES(43, 'music')")
        c.execute("INSERT INTO #t2 VALUES(43, 'cycling')")
        c.execute("INSERT INTO #t2 VALUES(44, 'physics')")
        c.execute("INSERT INTO #t2 VALUES(44, 'volleyball')")
        c.execute("INSERT INTO #t2 VALUES(44, 'tennis')")

        # Test 1: ORDER BY with TOP.
        q = Q("#t1", "i").limit(1).order("1 DESC")
        r = q.execute(c, timeout=10).fetchall()
        R(1, q, r == [[45]])

        # Test 2: JOIN with COUNT.
        q = Q("#t1", "COUNT(DISTINCT #t1.i)").join("#t2", "#t2.i = #t1.i")
        r = q.execute(c).fetchall()
        R(2, q, r == [[3]])

        # Test 3: GROUP BY and HAVING.
        q = Q("#t2", "i", "COUNT(*)").group("i").having("COUNT(*) > 2")
        r = set([row[0] for row in q.execute(c).fetchall()])
        R(3, q, r == set([42, 44]))

        # Test 4: LEFT OUTER JOIN with IS NULL.
        q = Q("#t1 a", "a.i", "b.n").outer("#t2 b", "b.i = a.i")
        r = q.where("b.n IS NULL").execute(c).fetchall()
        R(4, q, r == [[45, None]])

        # Test 5: NESTED ORs and ANDs.
        q = Q("#t1 a", "a.n").join("#t2 b", "b.i = a.i").unique()
        q.where(Q.Or("a.n LIKE 'E%'", ("a.i < 44", "b.n LIKE '%o%'")))
        q.where("a.n <> 'Volker'")
        r = q.execute(c).fetchall()
        R(5, q, r == [['Alan']])

        # Test 6: Condition object with placeholders.
        v = ('biology', 'physics')
        q = Q("#t1 a", "a.n").join("#t2 b", "b.i = a.i").unique().order(1)
        q.where(C("b.n", v, "IN"))
        q.timeout(5)
        r = [row[0] for row in q.execute(c).fetchall()]
        R(6, q, r == ['Alan', 'Volker'])

        # Test 7: UNION.
        q = Q("#t1", "n").where("i > 44")
        q.union(Q("#t1", "n").where("i < 43"))
        r = [r[0] for r in q.order(1).execute(c).fetchall()]
        R(7, q, r == ["Alan", "Elmer"])

        # Test 8: INTO.
        Q("#t1", "*").into("#t3").execute(c)
        q = Q("#t3", "n").order(1)
        r = [r[0] for r in q.execute(c).fetchall()]
        R(8, q, r == ["Alan", "Bob", "Elmer", "Volker"])

        # Test 9: nested query.
        q = Q("#t1", "n")
        q.where(C("i", Q("#t2", "i").unique(), "NOT IN"))
        r = q.execute(c).fetchall()
        R(9, q, r == [["Elmer"]])
