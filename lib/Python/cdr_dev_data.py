#----------------------------------------------------------------------
# $Id$
# Assembles information about data preserved on the CDR DEV tier.
# JIRA::OCECDR-3733
#----------------------------------------------------------------------
import glob
import os

class Table:
    def __init__(self, name, source):
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
        self.rows = [self.row_dict(row) for row in self.values]
        if "name" in self.cols:
            names = [row["name"] for row in self.rows]
            self.names = dict(zip(names, self.rows))
            if "id" in self.cols:
                ids = [row["id"] for row in self.rows]
                self.map = dict(zip(ids, names))
        if name == "query_term_def":
            paths = [row["path"] for row in self.rows]
            self.names = dict(zip(paths, self.rows))
    def row_dict(self, row):
        d = dict(zip(self.cols, row))
        if self.name == "filter_set" and d["notes"] == u"None":
            d["notes"] = u""
        return d

class DocType:
    def __init__(self, name, source):
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
    def __init__(self, source, old=None):
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
        filter_name = subset = None
        if row["filter"]:
            filter_name = self.docs["Filter"].map[row["filter"]].strip()
        if row["subset"]:
            subset = self.tables["filter_set"].map[row["subset"]].strip()
        filter_set = self.tables["filter_set"].map[row["filter_set"]].strip()
        return (filter_set, filter_name, subset, row["position"])

    def grp_action(self, row):
        group = self.tables["grp"].map[row["grp"]]
        action = self.tables["action"].map[row["action"]]
        doc_type = None
        if row["doc_type"]:
            doc_type = self.tables["doc_type"].map[row["doc_type"]]
        return (group, action, doc_type)

    def grp_usr(self, row):
        group = self.tables["grp"].map[row["grp"]]
        user = self.tables["usr"].map[row["usr"]]
        return "%s's membership in group %s" % (user, group)

    def link_properties(self, row):
        return (self.tables["link_type"].map[row["link_id"]],
                self.tables["link_prop_type"].map[row["property_id"]],
                row["value"], row["comment"])

    def link_target(self, row):
        return (self.tables["link_type"].map[row["source_link_type"]],
                self.tables["doc_type"].map[row["target_doc_type"]])

    def link_xml(self, row):
        return (self.tables["doc_type"].map[row["doc_type"]],
                row["element"],
                self.tables["link_type"].map[row["link_id"]])
