"""
Search support in the CDR
"""

from cdrapi.db import Query
from cdrapi.docs import Doc

# ----------------------------------------------------------------------
# Try to make the module compatible with Python 2 and 3.
# ----------------------------------------------------------------------
try:
    basestring
except:
    basestring = str, bytes
    unicode = str


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

        """
        self.__query = query
        if "request" in args:
            self.parse_request(args["request"])
        else:
            self.doc_types = args.get("doc_type") or []
            self.max_docs = args.get("max_docs")
            tests = args.get("test") or []
            if not isinstance(self.doc_types, list):
                self.doc_types = [self.doc_types]
            if not isinstance(tests, list):
                tests = [tests]
            self.tests = [self.Test(test) for test in tests]
        """

    @property
    def cursor(self):
        if not hasattr(self, "_cursor"):
            self._cursor = self.session.conn.cursor()
        return self._cursor

    @property
    def limit(self):
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
        if not hasattr(self, "_doctypes"):
            self._doctypes = self.__opts.get("doctypes") or []
            if not isinstance(self._doctypes, (list, tuple)):
                self._doctypes = [self._doctypes]
        return self._doctypes

    @property
    def tests(self):
        if not hasattr(self, "_tests"):
            tests = self.__tests or []
            if not isinstance(tests, (list, tuple)):
                tests = [tests]
            self._tests = []
            for test in tests:
                if isinstance(test, basestring):
                    test = Search.Test(test)
                if not isinstance(test, Search.Test):
                    message = "Search test must be string or Test object"
                    raise Exception(message)
                self._tests.append(test)
        return self._tests

    @property
    def logger(self):
        """
        Object for recording what we do
        """

        return self.session.logger

    @property
    def query(self):
        """
        Query string for the search request
        """

        return self.__query

    @property
    def session(self):
        """
        `Session` for which this `Search` object was requested
        """

        return self.__session

    """
    def parse_request(self, request):
        self.logger.debug("parsing request")
        query = request.find("Query")
        assert len(query), "Query node missing"
        self.max_docs = query.get("MaxDocs")
        self.logger.debug("max_docs: %r", self.max_docs)
        for node in query:
            if node.tag == "DocType":
                self.doc_types.append(node.text)
            elif node.tag == "Test":
                test = util.Doc.get_text(node)
                self.logger.debug("test %r", test)
                self.tests.append(self.Test(test))
    """

    def run(self):
        rows = self.query.execute(self.cursor).fetchall()
        return [Doc(self.session, id=row.id) for row in rows]
        class QueryResult:
            def __init__(self, doc_id, doc_title, doc_type):
                self.doc_id = doc_id
                self.doc_title = doc_title
                self.doc_type = doc_type
            @property
            def cdr_id(self):
                return "CDR{:010d}".format(int(self.doc_id))
        return [self.QueryResult(*row) for row in rows]

    @property
    def query(self):
        if hasattr(self, "_query"):
            return self._query
        #fields = "d.id", "d.title", "t.name"
        query = Query("document d", "d.id").order("d.title")
        if self.doctypes:
            query.join("doc_type t", "t.id = d.doc_type")
            if len(self.doctypes) == 1:
                query.where(query.Condition("t.name", self.doctypes[0]))
            else:
                query.where(query.Condition("t.name", self.doctypes, "IN"))
        n = 1
        for test in self.tests:
            if test.column:
                alias = "qt{:d}".format(n)
                n += 1
                query.join("query_term " + alias, alias + ".doc_id = d.id")
                query.where(query.Condition(alias + ".path", test.path))
                column = "{}.{}".format(alias, test.column)
            else:
                column = "d.title"
            query.where(query.Condition(column, test.value, test.operator))
        if self.limit:
            query.limit(self.limit)
        return query


    class Test:
        OPS = {
            "eq": "=", "=": "=",
            "ne": "<>", "<>": "<>", "!=": "<>",
            "lt": "<", "<": "<", "lte": "<=", "<=": "<=",
            "gt": ">", ">": ">", "gte": ">=", ">=": ">="
            }
        def __init__(self, assertion):
            try:
                path, operator, value = assertion.split(None, 2)
            except:
                message = "invalid test assertion {!r}".format(assertion)
                raise ValueError(message)
            assert path and value, "query test must have path and value"
            if path.startswith("/"):
                self.path, self.column = path.rsplit("/", 1)
                if self.column not in ("value", "int_val"):
                    message = "invalid table column {!r}".format(self.path)
                    raise ValueError(message)
            elif path == "CdrCtl/Title":
                self.path, self.column = path, None
            else:
                raise ValueError("unsupported path {!r}".format(path))
            if operator == "contains":
                self.value = "%{}%".format(value)
                self.operator = "LIKE"
            elif operator == "begins":
                self.value = "{}%".format(value)
                self.operator = "LIKE"
            else:
                self.value = value
                self.operator = self.OPS.get(operator)
                if not self.operator:
                    message = "unsupported operator {!r}".format(operator)
                    raise ValueError(message)


class QueryTermDef:
    def __init__(self, session, path, rule=None):
        self.session = session
        self.path = path
        self.rule = rule

    @property
    def rule_id(self):
        if not hasattr(self, "_rule_id"):
            if not self.rule:
                self._rule_id = None
            else:
                query = Query("query_term_rule", "id")
                query.where(query.Condition("name", self.rule))
                row = query.execute(self.session.cursor).fetchone()
                if not row:
                    message = "Unknown query term rule: {}".format(self.rule)
                    raise Exception(message)
                self._rule_id = row.id
        return self._rule_id

    def add(self):
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
        insert = "INSERT INTO query_term_def ({}) VALUES (?, ?)".format(names)
        self.session.cursor.execute(insert, values)
        self.session.conn.commit()

    def delete(self):
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
        query = Query("query_term_rule", "name").order("name")
        return [row.name for row in query.execute(session.cursor).fetchall()]

    @classmethod
    def get_definitions(cls, session):
        query = Query("query_term_def d", "d.path", "r.name")
        query.outer("query_term_rule r", "r.id = d.term_rule")
        query.order("d.path", "r.name")
        definitions = []
        for row in query.execute(session.cursor).fetchall():
            definitions.append(QueryTermDef(session, row.path, row.name))
        return definitions
