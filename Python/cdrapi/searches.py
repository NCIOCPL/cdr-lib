"""
Search support in the CDR
"""

from cdrapi.db import Query
from cdrapi.docs import Doc


class Search:
    """

    From OCECDR-4255:

    The existing search module implements an XQL parser built with
    low-level lexical and grammar processors which are not directly
    available to Python. While it would be possible to create a
    compiled extension to replicate the existing XQL parser
    functionality, that would involve a non-trivial level of effort,
    and would compromise the goal of reducing dependencies on
    programming expertise in C and C++. An analysis of the uses of the
    search module shows that only a subset of the flexibility of the
    supported XQL syntax is ever used, and it would be possible to
    provide the required functionality without using XQL syntax. A
    replacement API was implemented using assertion test strings which
    can be easily parsed by the builtin string support in Python. Each
    valid test assertion string contains exactly three tokens:

       * a path, which can be one of

         - CdrCtl/Title

         - the xpath (starting with a single forward slash) for an
           element or attribute, with /value or /int_val appended to
           indicate which column of the query_term table should be
           used for the test

       * an operator (the same operators supported by the current XQL
         parser; e.g. =, contains, begins, gt, etc.)

       * a value to be used in the test; wildcards are added as
         appropriate if the operator is "contains" or "begins"

    The three tokens are separated by whitespace. The first two tokens
    cannot contain whitespace, but there are no whitespace restrictions
    on the value component of the test, which should  not be enclosed
    in quote marks.

    The API also supports passing a list of document types which
    can be used to narrow the results to documents of those types.
    Making the specifying of a document type be a rule was considered,
    but that would mean that you could only specify one document
    type, or that we would have to move back toward the complexity
    of XQL in order to distinguish between AND and OR groupings
    and relationships for tests. This seemed like the best compromise
    for allowing the query to pick up documents of more than one
    document type, leaving the other tests to be implicitly ANDed
    together.

    The places which currently use the search module (and which will
    therefore need to be modified) are significantly fewer than
    original anticipated:

       * Bin/UpdateSchemas.py
       * DevTools/Utilities/DiffSchemas.py
       * DevTools/Utilities/UpdateFilter.py
       * DevTools/Utilities/UpdatePubControlDoc.py
       * Inetpub/wwwroot/cgi-bin/cdr/post-schema.py
       * XMetaL/DLL/SearchDialog.cpp
       * lib/Python/RtfWriter.py
       * lib/Python/cdrpub.py (just remove the calls to cdr.search();
         XQL queries have never been used in the publishing control
         documents)
    """

    def __init__(self, session, *tests, **opts):
        """
        Capture the session and options for this search request

        Pass:
          session - required reference to `Session` object
          tests - one or more assertion strings; see explanation above
          limit - optional integer keyword argument limiting number of results
          doctypes - optional sequence of document types to be included
        """

        self.__session = session
        self.__tests = tests
        self.__opts = opts

    @property
    def cursor(self):
        """
        Give the `Search` object its own cursor
        """

        if not hasattr(self, "_cursor"):
            self._cursor = self.session.conn.cursor()
        return self._cursor

    @property
    def limit(self):
        """
        Optional throttle on the number of documents to return for the search
        """

        if not hasattr(self, "_limit"):
            self._limit = self.__opts.get("limit")
            if self._limit:
                try:
                    self._limit = int(self._limit)
                except:
                    raise Exception("limit must be integer")
        return self._limit

    @property
    def doctypes(self):
        """
        Optional sequence of document type names for restricting the search
        """

        if not hasattr(self, "_doctypes"):
            self._doctypes = self.__opts.get("doctypes") or []
            if not isinstance(self._doctypes, (list, tuple)):
                self._doctypes = [self._doctypes]
        return self._doctypes

    @property
    def tests(self):
        """
        Sequence of `Search.Test` objects containing the search logic
        """

        if not hasattr(self, "_tests"):
            tests = self.__tests or []
            if not isinstance(tests, (list, tuple)):
                tests = [tests]
            self._tests = []
            for test in tests:
                if isinstance(test, bytes):
                    test = test.decode("utf-8")
                if isinstance(test, str):
                    test = Search.Test(test)
                if not isinstance(test, Search.Test):
                    message = "Search test must be string or Test object"
                    raise Exception(message)
                self._tests.append(test)
        return self._tests

    @property
    def logger(self):
        """
        Object for recording what we do, borrowed from the `Session` object
        """

        return self.session.logger

    @property
    def query(self):
        """
        Assemble the database query object for performing the search
        """

        # Use cached `Query` object if we've already done this.
        if hasattr(self, "_query"):
            return self._query

        # Create a new `Query` object and apply any doctype filters.
        query = Query("document d", "d.id").order("d.title")
        if self.doctypes:
            query.join("doc_type t", "t.id = d.doc_type")
            if len(self.doctypes) == 1:
                query.where(query.Condition("t.name", self.doctypes[0]))
            else:
                query.where(query.Condition("t.name", self.doctypes, "IN"))

        # Apply the conditions for each of the `Test` objects.
        n = 1
        for test in self.tests:

            # If the `Test` object specifies a column use the `query_term`
            # table.
            if test.column:
                alias = f"qt{n:d}"
                n += 1
                query.join("query_term " + alias, alias + ".doc_id = d.id")
                query.where(query.Condition(alias + ".path", test.path))
                column = f"{alias}.{test.column}"

            # Otherwise, the test is looking for matching title strings.
            else:
                column = "d.title"

            # Construct and add a new `Condition` object to the query.
            query.where(query.Condition(column, test.value, test.operator))

        # If the caller doesn't want all the matching documents, apply the
        # limit requested.
        if self.limit:
            query.limit(self.limit)

        # Cache and return the `Query` object.
        self._query = query
        return query

    @property
    def session(self):
        """
        `Session` for which this `Search` object was requested
        """

        return self.__session

    def run(self):
        """
        Perform the search

        Called by:
          cdr.search()
          client XML wrapper command CdrSearch

        Return:
          possibly empty sequence of `Doc` objects
        """

        # All the heavy lifting is done in the `query` property.
        self.session.log(f"Search.run({self.__tests!r}, {self.__opts!r})")
        rows = self.query.execute(self.cursor).fetchall()
        return [Doc(self.session, id=row.id) for row in rows]


    class Test:
        """
        Assertion to be tested while looking for matching documents

        Attributes:
          path - location of what we're looking for in the documents
          operator - SQL operator to be applied for the assertion's test
          value - string we're looking for
        """

        # We support a number of aliases for the operators for backward
        # compatibility.
        OPS = {
            "eq": "=", "=": "=",
            "ne": "<>", "<>": "<>", "!=": "<>",
            "lt": "<", "<": "<", "lte": "<=", "<=": "<=",
            "gt": ">", ">": ">", "gte": ">=", ">=": ">="
        }

        def __init__(self, assertion):
            """
            Parse the test's assertion string

            Pass:
              assertion - string in the form PATH OPERATOR VALUE
                          (see `Search` class documentation above
                          for more details)
            """

            try:
                path, operator, value = assertion.split(None, 2)
            except:
                raise ValueError(f"invalid test assertion {assertion!r}")
            assert path and value, "query test must have path and value"
            if path.startswith("/"):
                self.path, self.column = path.rsplit("/", 1)
                if self.column not in ("value", "int_val"):
                    raise ValueError(f"invalid table column {self.path!r}")
            elif path == "CdrCtl/Title":
                self.path, self.column = path, None
            else:
                raise ValueError(f"unsupported path {path!r}")
            if operator == "contains":
                self.value = f"%{value}%"
                self.operator = "LIKE"
            elif operator == "begins":
                self.value = f"{value}%"
                self.operator = "LIKE"
            else:
                self.value = value
                self.operator = self.OPS.get(operator)
                if not self.operator:
                    raise ValueError(f"unsupported operator {operator!r}")


class QueryTermDef:
    """
    Identification of a portion of documents to be indexed for searching

    Attributes:
      session - reference to object representing current login
      path - string indicating a part of documents to be indexed;
             paths beginning with a single forward slash character
             are absolute paths (e.g., "/Summary/SummaryTitle");
             paths beginning with a double forward slash are relative
             (e.g., "//@cdr:ref")
      rule - string naming custom rule to be applied (if any) or None;
             as far as I know the only context in which this has been
             specified is for unit testing, and even that has not
             extended to the actual implementation of a custom rule,
             but only population of and linking to the `query_term_rule`
             table; from the documentation in tables.sql:
                 "Allows for future customization of the query support
                  mechanism, using more sophisticated index logic than
                  simply the text content of a single element. Syntax TBD."
             From the original documentation in CdrSearch.cpp:
                 "Rules cannot be created through the CDR command interface.
                  They are inserted by the programmer implementing the
                  custom software behind the rule."
             I don't really know (beyond these quotes) what the original
             programmer had in mind, or what use cases were envisioned.

    Property:
      rule_id - integer for primary key into the `query_term_rule` table
                if this definition has a custom rule to be applied;
                otherwise None
    """

    def __init__(self, session, path, rule=None):
        """
        Wrap the caller's arguments as attributes of the object

        Pass:
          session - reference to object representing current login
          path - document location of values to be indexed
          rule - name of custom indexing rule to be applied (see notes above)
        """

        self.session = session
        self.path = path
        self.rule = rule

    @property
    def rule_id(self):
        """
        Primary key of row in the `query_term_rule` table (or None)

        See notes above about the custom rule mechanism, which AFAIK
        has never been used.
        """

        if not hasattr(self, "_rule_id"):
            if not self.rule:
                self._rule_id = None
            else:
                query = Query("query_term_rule", "id")
                query.where(query.Condition("name", self.rule))
                row = query.execute(self.session.cursor).fetchone()
                if not row:
                    raise Exception(f"Unknown query term rule: {self.rule}")
                self._rule_id = row.id
        return self._rule_id

    def add(self):
        """
        Store the new query term definition

        Called by:
          cdr.addQueryTermDef()
          client XML wrapper command CdrAddQueryTermDef
        """

        self.session.log(f"QueryTermDef.add({self.path!r}, {self.rule!r})")
        if not self.session.can_do("ADD QUERY TERM DEF"):
            message = "User not authorized to add query term definitions"
            raise Exception(message)
        if not self.path:
            raise Exception("Missing required path")
        query = Query("query_term_def", "COUNT(*) AS n")
        query.where(query.Condition("path", self.path))
        if query.execute(self.session.cursor).fetchone().n > 0:
            raise Exception("Duplicate query term definition")
        names = "path, term_rule"
        values = self.path, self.rule_id
        insert = f"INSERT INTO query_term_def ({names}) VALUES (?, ?)"
        self.session.cursor.execute(insert, values)
        self.session.conn.commit()

    def delete(self):
        """
        Drop the query term definition

        Called by:
          cdr.delQueryTermDef()
          client XML wrapper command CdrDelQueryTermDef
        """

        self.session.log(f"QueryTermDef.delete({self.path!r})")
        if not self.session.can_do("DELETE QUERY TERM DEF"):
            message = "User not authorized to delete query term definitions"
            raise Exception(message)
        if not self.path:
            raise Exception("Missing required path")
        delete = "DELETE FROM query_term_def WHERE path = ?"
        self.session.cursor.execute(delete, (self.path,))
        if self.session.cursor.rowcount != 1:
            self.session.cursor.execute("ROLLBACK TRANSACTION")
            raise Exception("Query term definition not found")
        self.session.conn.commit()

    @classmethod
    def get_rules(cls, session):
        """
        Find the available query term rules

        Used by the user interface for managing search path definitions.

        See notes above in the documentation for the `QueryTermDef` class
        on the custom rule mechanism, which AFAIK has never been used.

        Required positional argument:
          session - reference to object for current login

        Called by:
          cdr.listQueryTermRules()
          client XML wrapper command CdrListQueryTermRules
        """

        session.log("QueryTermDef.get_rules()")
        query = Query("query_term_rule", "name").order("name")
        return [row.name for row in query.execute(session.cursor).fetchall()]

    @classmethod
    def get_definitions(cls, session):
        """
        Fetch the list of CDR query term definitions

        Required positional argument:
          session - reference to object for current login

        Called by:
          cdr.listQueryTermDefs()
          client XML wrapper command CdrListQueryTermDefs

        Return:
          sequence of `QueryTermDef` objects
        """

        session.log("QueryTermDef.get_definitions()")
        query = Query("query_term_def d", "d.path", "r.name")
        query.outer("query_term_rule r", "r.id = d.term_rule")
        query.order("d.path", "r.name")
        definitions = []
        for row in query.execute(session.cursor).fetchall():
            definitions.append(QueryTermDef(session, row.path, row.name))
        return definitions
