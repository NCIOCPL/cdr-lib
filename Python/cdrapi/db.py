"""
DB-SIG compliant module for CDR database access.
"""

import datetime
import logging
import unittest
import adodbapi
from cdrapi import settings


# Python 2/3 compatability
try:
    basestring
except:
    basestring = str


class TimeConverter(adodbapi.apibase.pythonDateTimeConverter):

    def DateObjectToIsoFormatString(self, obj):
        """
        Workaround for bug in adodbapi

        The package blows up when it gets a datetime object with sub-second
        precision. We side-step that bug by converting to a string with
        only three digits of that precision.
        """

        base_class = adodbapi.apibase.pythonDateTimeConverter
        s = super(base_class, self).DateObjectToIsoFormatString(obj)
        if "." not in s:
            return s
        dt, us = s.split(".", 1)
        return dt + str(round(float("." + us), 3))[1:5]


# Don't plug this in yet, as we're working around the bug further upstream
# for now. If I get time I'll see if I can fix the bug and get the package
# maintainer to accept a patch (which he has indicated he would).
# adodbapi.apibase.typeMap[datetime.datetime] = adodbapi.apibase.adc.adBSTR
# adodbapi.adodbapi.dateconverter = TimeConverter()


def connect(**opts):
    """
    Connect to the CDR database using known login account.

    Optional keyword arguments:
      user - string for database account name (default Query.CDRSQLACCOUNT)
      tier - tier name string (e.g., 'PROD') or Tier object
      database - initial db for the connection (default Query.DB)
      timeout - time to wait before giving up (default Query.DEFAULT_TIMEOUT)
    """

    tier = opts.get("tier") or settings.Tier()
    if isinstance(tier, basestring):
        tier = settings.Tier(tier)
    user = opts.get("user", Query.CDRSQLACCOUNT)
    if user == "cdr":
        user = Query.CDRSQLACCOUNT
    password = tier.password(user, Query.DB)
    if not password:
        raise Exception("user {!r} unknown on {!r}".format(user, tier.name))
    parms = {
        "Provider": "SQLOLEDB",
        "Data Source": "{},{}".format(tier.sql_server, tier.port(Query.DB)),
        "Initial Catalog": opts.get("database", Query.DB),
        "User ID": user,
        "Password": password,
        "Timeout": opts.get("timeout", Query.DEFAULT_TIMEOUT)
    }
    connection_string = ";".join(["{}={}".format(*p) for p in parms.items()])
    return adodbapi.connect(connection_string, timeout=parms["Timeout"])


class Query:

    """
    Builder for SQL select queries.

    Example usage:

        query = cdrdb.Query('t1 a', 'a.title', 'b.name AS "Type"')
        query.join('t2 b', 'b.id = a.t2')
        query.where(query.Condition('b.name', ('Foo', 'Bar'), 'IN'))

        # To see the generated SQL
        print(query)

        # To execute and cleanup
        cursor = query.execute()
        rows = cursor.fetchall()
        cursor.close()

        # Or alternatively if closing the cursor doesn't matter
        rows = query.execute().fetchall()
    """

    PLACEHOLDER = "?"
    DEFAULT_TIMEOUT = 120
    CDRSQLACCOUNT = "cdrsqlaccount"
    DB = "CDR"

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
        self._timeout = self.DEFAULT_TIMEOUT
        self._alias = None
        self._into = None
        self._cursor = None
        self._limit = None
        self._unique = False
        self._str = None
        self._outer = False
        self._logger = None

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
        if not cursor:
            if not timeout:
                timeout = self._timeout
            conn = connect(user="CdrGuest", timeout=timeout)
            cursor = conn.cursor()
        sql = str(self)
        cursor.execute(sql, tuple(self._parms))
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
        if self._logger is None:
            tier = settings.Tier()
            self._logger = tier.get_logger("db", level="DEBUG")
        label = parms.get("label", "QUERY")
        output = "%s:\n%s" % (label, self)
        if self._parms:
            parms = ["PARAMETERS:"] + [repr(p) for p in self._parms]
            output += "\n" + "\n\t".join(parms)
        self._logger.debug(output)

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
        test = "%s%s %s" % (prefix, condition.column, condition.test)

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
            test += " (%s)" % ", ".join([self.PLACEHOLDER] * len(values))

            # Plug in the condition to the query string.
            query.append(self._align(keyword, test + suffix))

            # Add the parameters.
            self._parms += values

        # Last case: single value test.
        else:
            if type(condition.value) in (list, tuple):
                raise Exception("Unexpected sequence of values")
            query.append(self._align(keyword, "%s %s%s" % (test,
                                                           self.PLACEHOLDER,
                                                           suffix)))
            self._parms.append(condition.value)

    def _serialize_join(self, query, join):
        """
        Helper function for building the query string.
        """
        keyword = join.outer and "LEFT OUTER JOIN" or "JOIN"

        # Is this 'table' being constructed on the fly?
        if isinstance(join.table, Query):

            # Make sure it has been provided with an alias.
            alias = join.table._alias
            if not alias:
                raise Exception("resultset expression without alias")

            # SQL Server won't accept placeholders here.
            if join.table.parms():
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
    def fix_datetime(value):
        """
        Workaround for adodbapi bug, which chokes on datetime parameters

        Chops off the last three microsecond digits.

        Under Python 2.7, adodbapi throws an exception when a datetime
        argument is passed which has non-zero microseconds. Under Python
        3.6, adodbapi has a different bug, which discards the sub-second
        precision altogether. :-(

        See https://sourceforge.net/p/adodbapi/bugs/17/
        """

        if not isinstance(value, datetime.datetime) or not value.microsecond:
            return value
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

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


class QueryTests(unittest.TestCase):
    """
    Run tests to check the health of the Query class.
    """

    # Convenience aliases
    Q = Query
    C = Query.Condition

    @staticmethod
    def V(rows):
        """Extract values from rows"""
        return [tuple([c for c in r]) for r in rows]

    @staticmethod
    def D(rows, cursor):
        """Get values as a dictionary"""
        keys=[d[0] for d in cursor.description]
        return [dict([(k, row[k]) for k in keys]) for row in rows]

    def setUp(self):
        """
        Create some test tables.
        """

        self.c = connect(user="CdrGuest").cursor()
        self.c.execute("CREATE TABLE #t1 (i INT, n VARCHAR(32))")
        self.c.execute("CREATE TABLE #t2 (i INT, n VARCHAR(32))")
        self.c.execute("INSERT INTO #t1 VALUES(42, 'Alan')")
        self.c.execute("INSERT INTO #t1 VALUES(43, 'Bob')")
        self.c.execute("INSERT INTO #t1 VALUES(44, 'Volker')")
        self.c.execute("INSERT INTO #t1 VALUES(45, 'Elmer')")
        self.c.execute("INSERT INTO #t2 VALUES(42, 'biology')")
        self.c.execute("INSERT INTO #t2 VALUES(42, 'aviation')")
        self.c.execute("INSERT INTO #t2 VALUES(42, 'history')")
        self.c.execute("INSERT INTO #t2 VALUES(43, 'music')")
        self.c.execute("INSERT INTO #t2 VALUES(43, 'cycling')")
        self.c.execute("INSERT INTO #t2 VALUES(44, 'physics')")
        self.c.execute("INSERT INTO #t2 VALUES(44, 'volleyball')")
        self.c.execute("INSERT INTO #t2 VALUES(44, 'tennis')")

    def test_01_order_by_with_top(self):
        q = self.Q("#t1", "i").limit(1).order("1 DESC")
        r = self.V(q.execute(self.c, timeout=10).fetchall())
        self.assertTrue(r == [(45,)])
    def test_02_join_with_count(self):
        q = self.Q("#t1", "COUNT(DISTINCT #t1.i)").join("#t2", "#t2.i = #t1.i")
        r = self.V(q.execute(self.c).fetchall())
        self.assertTrue(r == [(3,)])
    def test_03_group_by_and_having(self):
        q = self.Q("#t2", "i", "COUNT(*)").group("i").having("COUNT(*) > 2")
        r = set([row[0] for row in q.execute(self.c).fetchall()])
        self.assertTrue(r == set([42, 44]))
    def test_04_left_outer_join_with_is_null(self):
        q = self.Q("#t1 a", "a.i", "b.n").outer("#t2 b", "b.i = a.i")
        r = self.V(q.where("b.n IS NULL").execute(self.c).fetchall())
        self.assertTrue(r == [(45, None,)])
    def test_05_nested_ors_and_ands(self):
        q = self.Q("#t1 a", "a.n").join("#t2 b", "b.i = a.i").unique()
        q.where(self.Q.Or("a.n LIKE 'E%'", ("a.i < 44", "b.n LIKE '%o%'")))
        q.where("a.n <> 'Volker'")
        r = self.V(q.execute(self.c).fetchall())
        self.assertTrue(r == [('Alan',)])
    def test_06_condition_object_with_placeholders(self):
        v = ('biology', 'physics')
        q = self.Q("#t1 a", "a.n").join("#t2 b", "b.i = a.i").unique().order(1)
        q.where(self.C("b.n", v, "IN"))
        q.timeout(5)
        r = [row[0] for row in q.execute(self.c).fetchall()]
        self.assertTrue(r == ['Alan', 'Volker'])

    def test_07_union(self):
        q = self.Q("#t1", "n").where("i > 44")
        q.union(self.Q("#t1", "n").where("i < 43"))
        r = [r[0] for r in q.order(1).execute(self.c).fetchall()]
        self.assertTrue(r == ["Alan", "Elmer"])

    def test_08_into(self):
        self.Q("#t1", "*").into("#t3").execute(self.c)
        q = self.Q("#t3", "n").order(1)
        r = [r[0] for r in q.execute(self.c).fetchall()]
        self.assertTrue(r == ["Alan", "Bob", "Elmer", "Volker"])
    def test_09_nested_query(self):
        q = self.Q("#t1", "n")
        q.where(self.C("i", self.Q("#t2", "i").unique(), "NOT IN"))
        r = self.V(q.execute(self.c).fetchall())
        self.assertTrue(r == [("Elmer",)])
    def test_10_dictionary_results(self):
        c = connect(user="CdrGuest").cursor()
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
        q = self.Q("#t1", "n")
        q.where(self.C("i", self.Q("#t2", "i").unique(), "NOT IN"))
        r = self.D(q.execute(c).fetchall(), c)
        self.assertTrue(r == [{"n": "Elmer"}])


if __name__ == "__main__":
    unittest.main()
