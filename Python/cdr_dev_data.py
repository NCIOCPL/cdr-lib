#----------------------------------------------------------------------
# Assembles information about data preserved on the CDR DEV tier.
# JIRA::OCECDR-3733
#----------------------------------------------------------------------
import datetime
import glob
import os
import re
from pathlib import Path

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

        When a cursor is passed for 'source' the documents and tables
        are read directly from the database for the local tier.  If
        a string is passed as the 'source' argument it is assumed to
        be the path (relative or absolute) for the location of the
        data captured by the PullDevData.py script (q.v.).

        The directory structure for the preserved data uses subdirectories
        for each of the document types for which documents have been
        preserved, as well as a subdirectory named 'tables' (since
        the CDR naming convention for document types always uses the
        best practice of using a singular noun, there should never
        be a conflict with the name 'tables').  Within each document
        type subdirectory is a file for each document of that type,
        with a name in the form nnnnnn.cdr, where nnnnnn is the integer
        form of the CDR document's ID.  Each such file contains a
        serialized (using Python's builtin repr() function) sequence
        of CDR document ID, document title, and document XML.  The
        'tables' subdirectory contains one file for each preserved
        table.  The file name is the table's name.  The first line in each
        file is a sequence containing the names of the columns in the
        table, in the order of the table's definition.  The subsequent
        lines each represent one row in the database table, as a
        sequence of values in the same column order as used in the
        first line.

        For example:

           DevData-20140227075603
             Filter
               100.cdr
               101.cdr
               103.cdr
               :
               :
             PublishingSystem
               176.cdr
               178.cdr
               257983.cdr
             Schema
               179.cdr
               :
               :
             tables
               action
               active_status
               :
               :
        """
        self.tables = {}
        self.docs = {}
        if old:
            for name in old.tables:
                try:
                    self.tables[name] = Table(name, source)
                except Exception as e:
                    pass
            for name in old.docs:
                self.docs[name] = DocType(name, source)
        else:
            for path in glob.glob(f"{source}/tables/*"):
                name = os.path.basename(path)
                self.tables[name] = Table(name, source)
            for path in glob.glob(f"{source}/*"):
                doc_type = os.path.basename(path)
                if doc_type != "tables":
                    if not doc_type.startswith("scheduled-jobs."):
                        self.docs[doc_type] = DocType(doc_type, source)

    def filter_set_member(self, row):
        """
        Returns a tuple with denormalized filter_set_member row values
        """
        filter_name = subset = None
        if row["filter"]:
            default = "***MISSING***"
            filter_map = self.docs["Filter"].map
            filter_name = filter_map.get(row["filter"], default).strip()
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
        result = "permission for members of %r group to perform action %r"
        result = result.format(group, action)
        if row["doc_type"]:
            doc_type = self.tables["doc_type"].map[row["doc_type"]]
            result += f" on {doc_type!r} documents"
        return result

    def grp_usr(self, row):
        """
        Returns a string with denormalized grp_usr row values
        """
        group = self.tables["grp"].map[row["grp"]]
        user = self.tables["usr"].map[row["usr"]]
        return f"{user}'s membership in group {group}"

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
        if isinstance(source, str):
            path = f"{source}/tables/{name}"
            self.values = [tuple(eval(row)) for row in open(path, encoding="utf-8")]
            self.cols = self.values.pop(0)
        else:
            source.execute(f"SELECT * FROM {name}")
            self.cols = tuple([col[0] for col in source.description])
            #self.values = [tuple(row) for row in source.fetchall()]
            self.values = []
            for row in source.fetchall():
                values = []
                for value in row:
                    if isinstance(value, datetime.datetime):
                        value = str(value)
                    values.append(value)
                self.values.append(values)
        self.rows = [self._row_dict(row) for row in self.values]
        if "name" in self.cols:
            names = [row["name"] for row in self.rows]
            self.names = dict(list(zip(names, self.rows)))
            if "id" in self.cols:
                ids = [row["id"] for row in self.rows]
                self.map = dict(list(zip(ids, names)))
        if name == "query_term_def":
            paths = [row["path"] for row in self.rows]
            self.names = dict(list(zip(paths, self.rows)))
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
        d = dict(list(zip(self.cols, row)))
        if self.name == "filter_set" and d["notes"] == "None":
            d["notes"] = ""
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
    ensure that this is true for documents of any additional document
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

        if isinstance(source, str):
            for doc_path in glob.glob(f"{source}/{name}/*.cdr"):
                text = Path(doc_path).read_text(encoding='utf-8')

                doc = eval(text)
                doc_id, title = doc[:2]
                key = title.lower().strip()

                # Summary document types could include duplicates because
                # English and Spanish docs could use identical names (i.e.
                # Delirium or 714-X).
                # The PullDevData.py script prevents those from being
                # used as test documents.
                # --------------------------------------------------------
                if key in self.docs:
                    raise Exception(f"too many {name} docs with title {title}")
                self.docs[key] = tuple(doc)
                self.map[doc_id] = title
        else:
            self.cursor = source
            source.execute("""\
SELECT d.id, d.title, d.xml
  FROM document d
  JOIN doc_type t
    ON t.id = d.doc_type
 WHERE t.name = ?""", name)
            rows = source.fetchall()

            for row in rows:
                doc_id, doc_title, doc_xml = row

                # The GTC title is build from the DefinitionText and will
                # likely contain extra spaces and newlines.  This regex
                # will strip them out to normalize the key
                # -------------------------------------------------------
                if name == 'GlossaryTermConcept':
                    key = re.sub('(\n+)( *)', ' ', doc_title.lower().strip())
                else:
                    key = doc_title.lower().strip()

                if key in self.docs and key not in self.prohibited:
                    message = "too many {} docs with title {} in database"
                    raise Exception(message.format(name, doc_title))
                self.docs[key] = tuple(row)
                self.map[doc_id] = doc_title


    @property
    def prohibited(self):
        if not hasattr(self, "_prohibited"):
            self._prohibited = set()
            self.cursor.execute("""\
               select title
                 from document d
                 join doc_type dt
                   on d.doc_type = dt.id
                where dt.name = ?
                group by title
               having count(*) > 1 """, self.name)
            _rows = self.cursor.fetchall()

            for _title, in _rows:
                self._prohibited.add(_title.lower().strip())

        return self._prohibited

