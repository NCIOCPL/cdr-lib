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
# $Id: cdrdb.py,v 1.16 2004-10-22 12:20:50 bkline Exp $
#
# $Log: not supported by cvs2svn $
# Revision 1.15  2003/02/14 20:30:37  bkline
# Added extension helper function strftime().
#
# Revision 1.14  2002/09/03 12:49:19  bkline
# Added optional db parameter to connect().
#
# Revision 1.13  2002/09/02 00:38:00  bkline
# Added optional timeout parameter to callproc().
#
# Revision 1.12  2002/05/23 22:05:29  ameyer
# setAutoCommit() now returns the previous state of the autocommit flag.
# getAutoCommit() fetches the autocommit state.
#
# Revision 1.11  2002/04/23 22:54:06  bkline
# Added optional timeout parameter to Cursor.execute().
#
# Revision 1.10  2001/12/19 20:31:03  bkline
# Added optional dataSource argument to connect().
#
# Revision 1.9  2001/12/19 20:24:42  bkline
# Added date/time formatting and support for CdrPublishing login.
#
# Revision 1.8  2001/08/06 22:21:22  bkline
# Implemented commit(), rollback(), and setAutoCommit() on Connection.
#
# Revision 1.7  2001/08/06 18:03:58  bkline
# Switched hardcoded error strings to Unicode.
#
# Revision 1.6  2001/08/06 15:55:30  bkline
# Fixed a collection item notation bug in Cursor.execute().
#
# Revision 1.5  2001/08/06 14:38:26  bkline
# Enhanced exception error information; implemented nextset().
#
# Revision 1.4  2001/08/06 04:33:01  bkline
# Switched to SQLOLEDB provider.
#
# Revision 1.2  2001/08/05 20:25:03  bkline
# Fixed a couple of typos in the date/time formatting routines.
#
# Revision 1.1  2001/08/05 19:23:25  bkline
# Initial revision
#
#----------------------------------------------------------------------

import win32com.client
import time
import pywintypes

# Until we do this bogus object creation, the constants are invisible.
win32com.client.Dispatch("ADODB.Connection")

#----------------------------------------------------------------------
# These module constants are required by the DBSIG's API.
#----------------------------------------------------------------------
apilevel     = '2.0'
threadsafety = 1
paramstyle   = 'qmark'

#----------------------------------------------------------------------
# Statndard exception classes.
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

    def callproc(self, procname, parameters, timeout = 30):
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
        if type(parameters) != type([]) and type(parameters) != type(()):
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
            if self.__rs.State == win32com.client.constants.adStateOpen:
                self.__rs.Close()
        self.__rs        = None
        self.description = None
        self.rowcount    = -1

    def execute(self, query, params = None, timeout = 30):
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
                if type(params) != type(()) and type(params) != type([]):
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
            errorList = buildErrorList(self.__conn)
            if errorList:
                raise Error, ("Cursor.execute", errorList)
            raise InternalError, ("Cursor.execute",
                    ((u"unexpected failure for query '%s'" % query),))

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
            self.__adoConn.CommitTrans()
            if self.__autoCommit: self.__inTrans = 0
            else:
                self.__adoConn.BeginTrans()
                self.__inTrans = 1
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
            self.__adoConn.RollbackTrans()
            if self.__autoCommit: self.__inTrans = 0
            else:
                self.__adoConn.BeginTrans()
                self.__inTrans = 1
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

    def setAutoCommit(self, on = 1):
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

        try:
            if on:
                #self.__adoConn.Execute("SET IMPLICIT_TRANSACTIONS OFF")
                if self.__autoCommit: return
                self.__autoCommit = 1
                if self.__inTrans:
                    self.commit()
            else:
                #self.__adoConn.Execute("SET IMPLICIT_TRANSACTIONS ON")
                if not self.__autoCommit: return
                self.__autoCommit = 0
                self.__adoConn.BeginTrans()
                self.__inTrans = 1
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
def connect(user = 'cdr', dataSource = 'localhost', db = 'cdr'):
    """
    Factory for creating a connection to the database.  Returns a
    Connection Object. It takes a number of parameters which are
    database dependent.  This implementation expects only the user
    name for the database login account.  The 'cdr' account is used
    for standard database activity for which the permission to alter
    data is required.  The 'CdrGuest' account has read-only access
    to the CDR data.
    """

    adoConn = win32com.client.Dispatch("ADODB.Connection")
    if user == 'cdr': password = '***REMOVED***'
    elif user == 'CdrGuest': password = '***REDACTED***'
    elif user == 'CdrPublishing': password = '***REMOVED***'
    else: raise DatabaseError, ("connect",
            ((u"invalid login name %s" % user),))

    try:
        adoConn.Open("Provider=SQLOLEDB;\
                      Data Source=%s;\
                      Initial Catalog=%s;\
                      User ID=%s;\
                      Password=%s" % (dataSource, db, user, password))
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
