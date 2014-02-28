#----------------------------------------------------------------------
# $Id$
# Assembles information about data preserved on the CDR DEV tier.
# JIRA::OCECDR-3733
#----------------------------------------------------------------------
import glob
import os

class Table:
    """
    Holds data for a CDR table.

    Attributes:
        name   - table name
        cols   - column names (in order as stored in the database)
        values - sequence of row tuples with column values in db order
        rows   - sequence of dictionaries mapping column names to values
        names  - rows indexed by 'name' column (for tables with such a column)
        map    - rows indexed by 'id' column (for tables with an id column)
    """
    def __init__(self, name, source):
        """
        Populates a Table object with data from a CDR database table.

        Pass:
            name   - database table name
            source - either a string naming a directory or a db cursor
        """
        self.name = name
        self.path = self.cols = self.values = self.map = self.names = None
        if type(source) is str:
            path = "%s/tables/%s" % (source, name)
            self.values = [tuple(eval(row)) for row in open(path)]
            self.cols = self.values.pop(0)
        else:
            source.execute("SELECT * FROM %s" % name)
            self.cols = tuple([col[0] for col in source.description])
            self.values = [tuple(row) for row in source.fetchall()]
        self.rows = [self._row_dict(row) for row in self.values]
        if "name" in self.cols:
            names = [row["name"] for row in self.rows]
            self.names = dict(zip(names, self.rows))
            if "id" in self.cols:
                ids = [row["id"] for row in self.rows]
                self.map = dict(zip(ids, names))
        if name == "query_term_def":
            paths = [row["path"] for row in self.rows]
            self.names = dict(zip(paths, self.rows))
    def _row_dict(self, row):
        """
        Creates a dictionary for a single row, mapping column names to values

        Pass:
            row - sequence of column values for a single database table row
        Return:
            name->value mapping
                (e.g.: { "id": 32, "name": "xml", "comment": None })

        Also normalizes the 'notes' column of the filter_set table.
        """
        d = dict(zip(self.cols, row))
        if self.name == "filter_set" and d["notes"] == u"None":
            d["notes"] = u""
        return d

class DocType:
    """
    Contains documents for a single document type from one of the CDR tiers.

    Attributes:
        name - document type name (e.g., "Filter")
        docs - dictionary with keys containing normalized document
               titles (leading and trailing spaces stripped, and
               case folded), and values containing tuples of CDR
               document ID, unique document title, and document
               serialized XML; strings are Unicode
        map  - dictionary mapping CDR document IDs to document titles

    Note that preservation of documents from the DEV tier relies on
    unique document titles within each document type.  This is guaranteed
    to be true for the base control document types (Schema, Filter, and
    PublishingControl).  It is the responsibility of the developer to
    ensure that this is true for documents of any any additional document
    types which must be preserved.
    """
    def __init__(self, name, source):
        """
        Loads the documents from the file system or the database.

        Pass:
            name - document type name (e.g., "Filter")
            source - either a string naming file system directory or a
                     database cursor
        """
        self.name = name
        self.docs = {}
        self.map = {}
        if type(source) is str:
            for doc_path in glob.glob("%s/%s/*.cdr" % (source, name)):
                doc = eval(open(doc_path).read())
                key = doc[1].lower().strip()
                if key in self.docs:
                    raise Exception("too many %s docs with title %s" %
                                    (name, doc[1]))
                self.docs[key] = tuple(doc)
                self.map[doc[0]] = doc[1]
        else:
            source.execute("""\
SELECT d.id, d.title, d.xml
  FROM document d
  JOIN doc_type t
    ON t.id = d.doc_type
 WHERE t.name = ?""", name)
            row = source.fetchone()
            while row:
                doc_id, doc_title, doc_xml = row
                key = doc_title.lower().strip()
                if key in self.docs:
                    raise Exception("too many %s docs with title %s " 
                                    "in database" % (name, doc_title))
                self.docs[key] = tuple(row)
                self.map[doc_id] = doc_title
                row = source.fetchone()

class Data:
    """
    Selected documents and tables from a CDR instance

    Attributes:
        tables - dictionary of Table objects, indexed by table name
        docs   - dictionary of DocType objects, indexed by type name
    """
    def __init__(self, source, old=None):
        """
        Collects table and document information for a CDR tier.

        Pass:
            source - either a directory name or a database cursor object
            old    - optional Data object for the data from the DEV tier
                     which was preserved before a refresh of the database
                     from the production server
        """
        self.tables = {}
        self.docs = {}
        if old:
            for name in old.tables:
                try:
                    self.tables[name] = Table(name, source)
                except:
                    pass
            for name in old.docs:
                self.docs[name] = DocType(name, source)
        else:
            for path in glob.glob("%s/tables/*" % source):
                name = os.path.basename(path)
                self.tables[name] = Table(name, source)
            for path in glob.glob("%s/*" % source):
                doc_type = os.path.basename(path)
                if doc_type != "tables":
                    self.docs[doc_type] = DocType(doc_type, source)

    def filter_set_member(self, row):
        """
        Returns a tuple with denormalized filter_set_member row values
        """
        filter_name = subset = None
        if row["filter"]:
            filter_name = self.docs["Filter"].map[row["filter"]].strip()
        if row["subset"]:
            subset = self.tables["filter_set"].map[row["subset"]].strip()
        filter_set = self.tables["filter_set"].map[row["filter_set"]].strip()
        return (filter_set, filter_name, subset, row["position"])

    def grp_action(self, row):
        """
        Returns a string with denormalized grp_action row values
        """
        group = self.tables["grp"].map[row["grp"]]
        action = self.tables["action"].map[row["action"]]
        result = "permission for members of %s group to perform action %s" % (
            repr(group), repr(action))
        if row["doc_type"]:
            doc_type = self.tables["doc_type"].map[row["doc_type"]]
            result += " on %s documents" % repr(doc_type)
        return result

    def grp_usr(self, row):
        """
        Returns a string with denormalized grp_usr row values
        """
        group = self.tables["grp"].map[row["grp"]]
        user = self.tables["usr"].map[row["usr"]]
        return "%s's membership in group %s" % (user, group)

    def link_properties(self, row):
        """
        Returns a tuple with denormalized link_properties row values
        """
        return (self.tables["link_type"].map[row["link_id"]],
                self.tables["link_prop_type"].map[row["property_id"]],
                row["value"], row["comment"])

    def link_target(self, row):
        """
        Returns a tuple with denormalized link_target row values
        """
        return (self.tables["link_type"].map[row["source_link_type"]],
                self.tables["doc_type"].map[row["target_doc_type"]])

    def link_xml(self, row):
        """
        Returns a tuple with denormalized link_xml row values
        """
        return (self.tables["doc_type"].map[row["doc_type"]],
                row["element"],
                self.tables["link_type"].map[row["link_id"]])
