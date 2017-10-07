"""
Manage CDR documents
"""

from builtins import int
import datetime
import re
import dateutil.parser
from lxml import etree
from cdrapi.db import Query
from cdrapi.settings import Tier


class Doc(object):
    def __init__(self, session, **opts):
        self.__session = session
        self.__opts = opts

    @property
    def session(self): return self.__session

    @property
    def id(self):
        if not hasattr(self, "_id"):
            self._id = self.__opts.get("id")
        if not self._id:
            self._id = None
        elif not isinstance(self._id, int):
            self._id = int(re.sub(r"[^\d]", "", str(self._id).split("#")[0]))
        return self._id

    @property
    def cdr_id(self):
        return "CDR{:010}".format(self.id) if self.id else None

    @property
    def version(self):
        if not self.id:
            return None
        if hasattr(self, "_version"):
            return self._version
        when = self.__opts.get("before")
        if when:
            return self._version = self.__get_version_before(when)
        label = self.__opts.get("label")
        if label:
            return self._version = self.__get_labeled_version(label)
        if self.__opts.get("last"):
            version = self.last_version
            if not version:
                raise Exception("document not versioned")
            return self._version = version
        version = self.__opts.get("version")
        if not version or version == "Current":
            return self._version = None
        try:
            return self._version = int(version)
        except:
            raise Exception("invalid version spec {}".format(version))

    @property
    def xml(self):
        if hasattr(self, "_xml"):
            return self._xml
        self._xml = self.__opts.get("xml")
        if not self._xml and self.id:
            if self.version:
                query = Query("doc_version", "xml")
                query.where(query.Condition("num", self.version))
            else:
                query = Query("document", "xml")
            query.where(query.Condition("id", self.id))
            row = query.execute(self.session.cursor).fetchone()
            if not row:
                raise Exception("no xml found")
            self._xml = row[0]
        return self._xml

    @xml.setter
    def xml(self, value):
        self._xml = value
        self._root = self._version = None

    def has_blob(self):
        if hasattr(self, "_blob"):
            return True
        if not self.id:
            return False
        if hasattr(self, "_blob_id"):
            return True if self._blob_id else False
        table = "version_blob_usage" if self.version else "doc_blob_usage"
        query = Query(table, "blob_id")
        query.where(query.Condition("doc_id", self.id))
        if self.version:
            query.where(query.Condition("doc_version", self.version))
        row = query.execute(self.session.cursor).fetchone()
        self._blob_id = row[0] if row else None
        return True if self._blob_id else False

    @property
    def blob(self):
        if hasattr(self, "_blob"):
            return self._blob
        if "blob" in self.__opts:
            return self._blob = self.__opts["blob"]
        if not self.has_blob():
            return self._blob = None
        query = Query("doc_blob", "data")
        query.where(query.Condition("id", self._blob_id))
        row = query.execute(self.session.cursor).fetchone()
        if not row:
            raise Exception("no blob found")
        return self._blob = row[0]

    @blob.setter
    def blob(self, value):
        self._blob = value

    @property
    def doctype(self):
        if hasattr(self, "_doctype"):
            return self._doctype
        if "doctype" in self.__opts:
            return self._doctype = self.__opts["doctype"]
        if not self.id:
            return self._doctype = None
        table = "doc_version" if self.version else "document"
        query = Query("doc_type t", "t.name")
        query.join(table + " d", "d.doc_type = t.id")
        query.where(query.Condition("d.id", self.id))
        if self.version:
            query.where(query.Condition("d.num" self.version))
        row = query.execute(self.session.cursor).fetchone()
        if not row:
            what = "version" if self.version else "document"
            raise Exception(what + " not found")
        return self._doctype = row[0]

    @doctype.setter
    def doctype(self, value):
        self._doctype = value

    @property
    def last_version(self):
        if not self.id:
            return None
        query = Query("doc_version", "MAX(num)")
        query.where(query.Condition("id", self.id))
        row = query.execute(self.session.cursor).fetchone()
        return row[0] if row else None

    @property
    def last_publishable_version(self):
        if not self.id:
            return None
        query = Query("doc_version", "MAX(num)")
        query.where(query.Condition("id", self.id))
        query.where("publishable = 'Y'")
        row = query.execute(self.session.cursor).fetchone()
        return row[0] if row else None

    def __get_version_before(self, before):
        if isinstance(before, (datetime.date, datetime.datetime)):
            when = before
        else:
            try:
                when = dateutil.parser.parse(before)
            except:
                raise Exception("unrecognized date/time format")
        query = Query("doc_version", "MAX(num)")
        query.where(query.Condition("id", self.id))
        query.where(query.Condition("dt", when, "<"))
        row = query.execute(self.session.cursor).fetchone()
        if not row:
            raise Exception("no version before {}".format(when))
        return row[0]

    def __get_labeled_version(label):
        query = Query("doc_version v", "MAX(v.num)")
        query.join("doc_version_label d", "d.document = v.id")
        query.join("version_label l", "l.id = d.label")
        query.where(query.Condition("v.id", self.id))
        query.where(query.Condition("l.name", label))
        row = query.execute(self.session.cursor).fetchone()
        if not row:
            raise Exception("no version labeled {}".format(label))
        return row[0]

    def lock(session, **opts):
        if not session.can_do("MODIFY DOCUMENT", self.doctype):
            raise Exception("User not authorized to modify document")

    class LegacyDoc:
        def __init__(self, 
