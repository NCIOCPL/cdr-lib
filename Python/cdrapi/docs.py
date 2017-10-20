"""
Manage CDR documents
"""

from builtins import int
import datetime
import re
import threading
import time
import urllib.parse
import dateutil.parser
from lxml import etree
from cdrapi import db
from cdrapi.db import Query
from cdrapi.settings import Tier


try:
    basestring
except:
    basestring = unicode = str

class Doc(object):
    """
    Information about an XML document in the CDR repository

    All of the attributes for the object are implemented as properties,
    fetched as needed to optimize away potentially expensive unnecessary
    processing.

    Read-only attributes:
      session - object representing the CDR session for which the
                document information was collected
      id - primary key in the `all_docs` database table for the document
      cdr_id - standard string representation for the document's id
      version - optional integer represent version requested for the doc
      last_version - integer for the most recently created version
      last_version_date - when the last version was created
      last_publishable_version - integer for most recent pub version
      checked_out_by - string for account holding lock on the document
      last_saved - when the document was most recently saved
      has_unversioned changes - True if the all_docs table was updated
                                more recently than the doc's latest version
      creator - name of account which first created this document
      created - date/time the document was originally created
      modifier - name of account which most recently updated the doc
      modified - when the document was most recently updated
      active_status - 'A' if the document is active; 'I' if inactive
      publishable - True iff the object's version is marked publishable
      ready_for_review - True if the document has be marked review ready
      title - string for the title of this version of the document
      val_status - 'V' (valid), 'I' (invalid), or 'U' (unvalidated)
      val_date - when the version's validation status was last determined
      comment - description of this version of the document
      denormalized_xml - xml with links resolved
      highest_fragment_id - highest cdr:id attribute value in the form _\d+

    Read/write attributes:
      xml - unicode string for the serialized DOM for the document
      blob - bytes for a BLOB associated with the document (optional)
      doctype - string representing the type of the document (e.g., Term)
    """

    NS = "cips.nci.nih.gov/cdr"
    NSMAP = {"cdr": NS}
    CDR_REF = "{" + NS + "}ref"
    CDR_ID = "{" + NS + "}id"
    NOT_VERSIONED = "document not versioned"
    NO_PUBLISHABLE_VERSIONS = "no publishable version found"
    UNVALIDATED = "U"
    VALID = "V"
    INVALID = "I"

    def __init__(self, session, **opts):
        """
        Capture the session and options passed by the caller

        Required positional argument:
          session - `Session` object for which `Doc` object is made

        Optional keyword arguments
          id - optional unique identifier for existing CDR document
          xml - serialized tree for the XML document
          blob - binary large object (BLOB) for the document
          version - legal values are:
            "Current" for current working copy of document
            "LastVersion" or "last" for most recent version of docuement
            "LastPublishableVersion" or "lastp" for latest publishable ver
            "Label ..." to get version with specified label
            version number integer
            default is current working copy of document from all_docs table
          before - only consider versions created before this date
                   or date/time
        """

        self.__session = session
        self.__opts = opts

    @property
    def session(self):
        """
        `Session` for which this `Doc` object was requested
        """

        return self.__session

    @property
    def id(self):
        """
        Unique integer identifier for the CDR document
        """

        if not hasattr(self, "_id"):
            self._id = self.extract_id(self.__opts.get("id"))
        return self._id

    @staticmethod
    def extract_id(arg):
        if isinstance(arg, int):
            return arg
        return int(re.sub(r"[^\d]", "", str(arg).split("#")[0]))

    @property
    def cdr_id(self):
        """
        Canonical string form for the CDR document ID (CDR9999999999)
        """

        return "CDR{:010}".format(self.id) if self.id else None

    @property
    def version(self):
        """
        Integer for specific version of None for all_docs row
        """

        version = self.__opts.get("version")
        cutoff = self.__opts.get("before")
        if not hasattr(self, "_version"):
            if not self.id:
                self._version = None
            elif cutoff:
                lastp = str(version).startswith("lastp")
                self._version = self.__get_version_before(cutoff, lastp)
            elif not version:
                self._version = None
            elif isinstance(version, int):
                self._version = version if version > 0 else None
            else:
                try:
                    version = version.lower()
                except:
                    raise Exception("invalid version {!r}".format(version))
                if version == "current":
                    self._version = None
                elif version in ("last", "lastversion"):
                    version = self.last_version
                    if not version:
                        raise Exception(self.NOT_VERSIONED)
                    self._version = version
                elif version.startswith("lastp"):
                    version = self.last_publishable_version
                    if not version:
                        raise Exception(self.NO_PUBLISHABLE_VERSIONS)
                    self._version = version
                elif version.startswith("label "):
                    tokens = version.split(" ", 1)
                    if len(tokens) != 2:
                        error = "missing token for version specifier"
                        raise Exception(error)
                    self._version = self.__get_labeled_version(label)
                else:
                    try:
                        self._version = int(version)
                    except:
                        error = "invalid version spec {}".format(version)
                        raise Exception(error)
        return self._version

    @property
    def xml(self):
        """
        Unicode string for the serialized DOM for this version of the doc
        """

        if hasattr(self, "_xml"):
            return self._xml
        self._xml = self.__opts.get("xml")
        if self._xml:
            if not isinstance(self._xml, unicode):
                self._xml = self._xml.decode("utf-8")
        elif self.id:
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
        """
        Assign a new value to the `xml` property, coercing to Unicode

        Invalidate any parse trees or version numbers.

        Pass:
          value - new property value
        """

        self._xml = value
        if self._xml and not isinstance(self._xml, unicode):
            self._xml = self._xml.decode("utf-8")
        self._root = self._version = self._denormalized_xml = None

    @property
    def root(self):
        """
        Parsed tree for the document's XML
        """

        if not hasattr(self, "_root"):
            self._root = etree.fromstring(self.xml.encode("utf-8"))
        return self._root

    @property
    def highest_fragment_id(self):
        if not hasattr(self, "_highest_fragment_id"):
            highest = 0
            for node in self.root.xpath("//*[@cdr:id]", namespaces=self.NSMAP):
                cdr_id = node.get(self.CDR_ID)
                if cdr_id is not None and cdr_id.startswith("_"):
                    digits = cdr_id[1:]
                    if digits.isdigit():
                        highest = max(highest, int(digits))
            self._highest_fragment_id = highest
        return self._highest_fragment_id

    def has_blob(self):
        """
        Determine whether the document has a BLOB for this version

        Avoid fetching the bytes for the BLOB if it hasn't already been
        done; just get the primary key for the BLOB.
        """

        if hasattr(self, "_blob"):
            return self._blob is not None
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
        """
        Bytes for BLOB associated with this version of the document
        """

        if not hasattr(self, "_blob"):
            if "blob" in self.__opts:
                self._blob = self.__opts["blob"]
            elif not self.has_blob():
                self._blob = None
            else:
                query = Query("doc_blob", "data")
                query.where(query.Condition("id", self._blob_id))
                row = query.execute(self.session.cursor).fetchone()
                if not row:
                    raise Exception("no blob found")
                self._blob = row[0]
        return self._blob

    @blob.setter
    def blob(self, value): self._blob = value

    @property
    def doctype(self):
        """
        String representing the type of the document (e.g., 'Summary')

        We have to be careful to look in the row for the version if
        the object represents a specific version, because the document
        type can change from one version to the next.
        """

        if not hasattr(self, "_doctype"):
            if "doctype" in self.__opts:
                self._doctype = self.__opts["doctype"]
            if not self.id:
                self._doctype = None
            else:
                table = "doc_version" if self.version else "document"
                query = Query("doc_type t", "t.name")
                query.join(table + " d", "d.doc_type = t.id")
                query.where(query.Condition("d.id", self.id))
                if self.version:
                    query.where(query.Condition("d.num", self.version))
                row = query.execute(self.session.cursor).fetchone()
                if not row:
                    what = "version" if self.version else "document"
                    raise Exception(what + " not found")
                self._doctype = row[0]
        return self._doctype

    @doctype.setter
    def doctype(self, value): self._doctype = value

    def is_control_type(self):
        return self.doctype in ("Filter", "css", "schema")

    def is_content_type(self):
        return not self.is_control_type()

    @property
    def last_version(self):
        """
        Integer for the most recently saved version, if any; else None
        """

        if not hasattr(self, "_last_version"):
            if not self.id:
                self._last_version = None
            else:
                query = Query("doc_version", "MAX(num)")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.session.cursor).fetchone()
                self._last_version = (row[0] if row else None)
        return self._last_version

    @property
    def last_version_date(self):
        """
        Date/time when the last version was created, if any; else None
        """

        if not hasattr(self, "_last_version_date"):
            if not self.last_version:
                self._last_version_date = None
            else:
                query = Query("doc_version", "dt")
                query.where(query.Condition("id", self.id))
                query.where(query.Condition("num", self.last_version))
                row = query.execute(self.session.cursor).fetchone()
                self._last_version_date = row[0]
        return self._last_version_date

    @property
    def last_publishable_version(self):
        """
        Integer for the most recently created publishable version, if any
        """

        if not self.id:
            return None
        query = Query("doc_version", "MAX(num)")
        query.where(query.Condition("id", self.id))
        query.where("publishable = 'Y'")
        row = query.execute(self.session.cursor).fetchone()
        return row[0] if row else None

    def __get_version_before(self, before, publishable=None):
        """
        Find the latest version created before the specified date/time

        Pass:
          before - string or `datetime` object
          publishable - if True only look for publishable versions;
                        if False only look for unpublishable versions;
                        otherwise ignore the `publishable` column

        Return:
          integer for the version found (or None)
        """

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
        if publishable is True:
            query.where("publishable = 'Y'")
        elif publishable is False:
            query.where("publishable = 'N'")
        row = query.execute(self.session.cursor).fetchone()
        if not row:
            raise Exception("no version before {}".format(when))
        return row[0]

    def __get_labeled_version(label):
        """
        Find the version for this document with the specified label

        This feature has never been used in all the years the CDR
        has been in existence, but CIAT has requested that we preserve
        the functionality.
        """

        query = Query("doc_version v", "MAX(v.num)")
        query.join("doc_version_label d", "d.document = v.id")
        query.join("version_label l", "l.id = d.label")
        query.where(query.Condition("v.id", self.id))
        query.where(query.Condition("l.name", label))
        row = query.execute(self.session.cursor).fetchone()
        if not row:
            raise Exception("no version labeled {}".format(label))
        return row[0]

    @property
    def checked_out_by(self):
        """
        String for the name of the account holding a lock on the document

        Return None if the document is unlocked.
        """

        query = Query("usr u", "u.name")
        query.join("checkout c", "c.usr = u.id")
        query.where(query.Condition("c.id", self.id))
        query.where("c.dt_in IS NULL")
        row = query.execute(self.session.cursor).fetchone()
        return row[0] if row else None

    @property
    def last_saved(self):
        """
        Return the last time the document was saved

        Includes document creation or modification, with or without
        versioning.
        """

        if not hasattr(self, "_last_saved"):
            self._last_saved = self._modified or self._added
        return self._last_saved

    def has_unversioned_changes(self):
        """
        Determine if the document has saved after the last version
        """

        if not self.last_version_date:
            return False
        return self.last_version_date < self.last_saved

    def add(self, **opts):
        """
        """

    @property
    def errors(self):
        return self._errors if hasattr(self, "_errors") else []

    def validate(self, **opts):
        self._errors = []
        if self.is_content_type() and "schema" in opts.get("validate", []):
            self.validate_against_schema(opts.get("locators"))

    def validate_against_schema(self, use_locators=False):
        if not self.is_content_type:
            raise Exception("can't validate control document against schema")
        val_status = self.VALID
        schema_doc = self.__get_schema()
        schema = etree.XMLSchema(schema_doc)
        self.namespaces_off()
        self.strip_eids()
        if not schema.validate(self.root):
            val_status = self.INVALID
            location = None
            if use_locators:
                self.insert_eids()
                line_map = self.LineMap(self.root)
            for error in schema.error_log:
                if use_locators:
                    location = line_map.get_error_location(error)
                self.__add_error(error.message, location)
        self.__rule_sets = {}
        self.__extract_rule_sets(schema_doc)
        for name in self.__rule_sets:
            rule_sets = self.__rule_sets[name]
            if not name:
                name = "anonymous"
            for rule_set in rule_sets:
                filter_xml = etree.tostring(rule_set.get_xslt())
                result = self.__apply_filter(filter_xml, self.root)
                for node in result.doc.findall("Err"):
                    self.__add_error(node.text, node.get("cdr-eid"))
                for entry in result.error_log:
                    val_status = self.INVALID
                    self.__add_error(entry.message)
        self.__val_status = val_status
    def __add_error(self, message, location=None, **opts):
        for ncname in ("id", "href", "ref", "xref"):
            message = message.replace("cdr-" + ncname, "cdr:" + ncname)
        self._errors.append(self.Error(message, location, **opts))
    class Error:
        def __init__(self, message, location, **opts):
            self.message = re.sub(r"\s+", " ", message.strip())
            self.location = location
            self.type = opts.get("type") or Validator.Doc.VALIDATION
            self.level = opts.get("level") or Validator.Doc.ERROR
        def to_node(self):
            node = etree.Element("Err", etype=self.type, elevel=self.level)
            node.text = self.message
            if self.location:
                node.set("eref", self.location)
            return node
    def namespaces_off(self):
        NS = "{{{}}}".format(self.NS)
        for node in self.root.iter("*"):
            for name in node.attrib:
                if name.startswith(NS):
                    ncname = name.replace(NS, "cdr-")
                    node.set(ncname, node.get(name))
                    del node.attrib[name]
    def namespaces_on(self):
        NS = "{{{}}}".format(self.NS)
        for node in self.root.iter("*"):
            for name in node.attrib:
                if name.startswith("cdr-"):
                    qname = name.replace("cdr-", NS)
                    node.set(qname, node.get(name))
                    del node.attrib[name]
    def strip_eids(self):
        for node in self.root.xpath("//*[@cdr-eid]"):
            del node.attrib["cdr-eid"]
    def insert_eids(self):
        eid = 1
        for node in self.root.iter("*"):
            node.set("cdr-eid", "_%d" % eid)
            eid += 1
    def __extract_rule_sets(self, schema):
        for node in schema:
            if node.tag == self.ANNOTATION:
                for child in node.findall("{}/pattern".format(self.APPINFO)):
                    rule_set = Validator.RuleSet(child)
                    if rule_set.name not in self.rule_sets:
                        self.rule_sets[rule_set.name] = []
                    self.__rule_sets[rule_set.name].append(rule_set)
            elif node.tag == self.INCLUDE:
                name = node.get("schemaLocation")
                xml = self.get_schema_xml(name, self.session.cursor)
                self.__extract_rule_sets(etree.fromstring(xml.encode("utf-8")))
    def __get_schema(self):
        query = Query("document d", "d.title")
        query.join("doc_type t", "t.xml_schema = d.id")
        query.where(query.Condition("t.name", self.type))
        row = query.execute(self.session.cursor).fetchone()
        assert row, "no schema for document type {}".format(self.type)
        parser = etree.XMLParser()
        parser.resolvers.add(self.SchemaResolver())
        xml = self.get_schema_xml(row[0], self.session.cursor)
        return etree.fromstring(xml.encode("utf-8"), parser)

    @classmethod
    def get_schema_xml(cls, name, cursor):
        """
        TODO: fix schemas in all_docs table and use them
        """

        assert name, "get_schema(): no name for schema"
        query = Query("good_schemas s", "s.xml")
        query.join("document d", "d.id = s.id")
        query.join("doc_type t", "t.id = d.doc_type")
        query.where("t.name = 'schema'")
        query.where(query.Condition("d.title", name.replace(".xsd", ".xml")))
        try:
            return query.execute(cursor).fetchone()[0]
        except:
            raise Exception("schema {!r} not found".format(name))

    class LineMap:
        class Line:
            def __init__(self):
                self.tags = {}
                self.first = None
            def add_node(self, node):
                if not self.first:
                    self.first = node.get("cdr-eid")
                if not self.tags.get(node.tag):
                    self.tags[node.tag] = node.get("cdr-eid")
            def get_error_location(self, error):
                match = re.match("Element '([^']+)'", error.message)
                if not match:
                    return None
                tag = match.group(1) or None
                location = self.tags.get(tag)
                return location or self.first
        def __init__(self, root):
            self.lines = {}
            for node in root.iter("*"):
                line = self.lines.get(node.sourceline)
                if not line:
                    line = self.lines[node.sourceline] = self.Line()
                line.add_node(node)
        def get_error_location(self, error):
            line = self.lines.get(error.line)
            return line and line.get_error_location(error) or None

    class SchemaResolver(etree.Resolver):
        def __init__(self, cursor):
            etree.Resolver.__init__(self)
            self.__cdr_cursor = cursor
        def resolve(self, url, id, context):
            xml = Doc.get_schema_xml(url, self.__cdr_cursor)
            return self.resolve_string(xml, context)

    def unlock(**opts):
        """
        TODO implement me!
        if the document is not checked out:
            throw an exception
        if the document is checked out by another account:
            if the force flag is not set:
                throw an exception
            if the user is not allowed to perform a force unlock:
                throw an exception
        update the checkout table, optionally changing the comment column
        if the document has unversioned changes the caller want to preserve:
            create a new version (including blob if present)
        otherwise, if the force flag is set:
            add an UNLOCK row to the audit_trail table
        """

    def lock(**opts):
        """
        Add a row to the `checkout` table for this document
        """

        if not session.can_do("MODIFY DOCUMENT", self.doctype):
            raise Exception("User not authorized to modify document")
        query = Query("checkout", "usr", "dt_out")
        query.where(query.Condition("id", self.id))
        query.where("dt_in IS NULL")
        row = query.execute(self.session.cursor).fetchone()
        if row:
            user_id, checked_out = row
            if user_id == self.user_id:
                return
            if opts.get("force"):
                if not self.session.can_do("FORCE CHECKOUT", self.doctype):
                    raise Exception("User not authorized to force checkout")
                self.unlock(abandon=True, force=True)

    def legacy_doc(self, **opts):
        """
        Create a DOM tree matching what the original `cdr.Doc` object uses

        Pass:
          brief = if True, give a shorter version of the control info
          get_xml - if True, include the CdrDocXml element
          get_blob - if True, include the CdrDocBlob element if there is
                     a BLOB for this version of the document
        """

        cdr_doc = etree.Element("CdrDoc")
        cdr_doc.set("Type", self.doctype)
        cdr_doc.set("Id", self.cdr_id)
        cdr_doc.append(self.legacy_doc_control(**opts))
        if opts.get("get_xml"):
            denormalize = opts.get("denormalize")
            xml = self.denormalized_xml if denormalize else self.xml
            etree.SubElement(cdr_doc, "CdrDocXml").text = etree.CDATA(xml)
        if opts.get("get_blob") and self.has_blob:
            blob = etree.SubElement(cdr_doc, "CdrDocBlob", encoding="base64")
            blob.text = base64.encodestring(self.blob).decode("ascii")
        return cdr_doc

    @property
    def denormalized_xml(self):
        """
        Pass the document's XML through the Fast Denormalization Filter

        Don't denormalize filter, css, or schema docs.

        If filtering fails (as it will if the original XML is malformed)
        return the original XML string.
        """

        if not hasattr(self, "_denormalized_xml"):
            if self._denormalized_xml is not None:
                return self._denormalized_xml
        if self.doctype in ("Filter", "css", "schema"):
            self._denormalized_xml = self.xml
        else:
            try:
                result = self.filter("name:Fast Denormalization Filter")
                self._denormalized_xml = unicode(result.doc)
            except:
                self._denormalized_xml = self.xml
            return self._denormalized_xml

    def legacy_doc_control(self, **opts):
        """
        Create a CdrDocCtl DOM tree

        Used for assembling part of a CdrDoc DOM tree for the CdrGetDoc
        command. Also used independently of a CdrDoc document, and with
        additional child elements, for filtering callbacks.

        Pass:
          brief - if True, just include DocTitle and DocActiveStatus
          filtering - if True, include extra Create, Modify, and FirstPub
                      blocks; if False, mark all children as read-only

        Return:
          `etree.Element` object with nested children
        """

        filtering = opts.get("filtering", False)
        modified = self.make_xml_date_string(self.modified)
        val_date = self.make_xml_date_string(self.val_date)
        doc_control = etree.Element("CdrDocCtl")
        control_info = [
            ("DocTitle", self.title),
            ("DocActiveStatus", self.active_status),
            ("DocValStatus", self.val_status),
            ("DocValDate", val_date),
            ("DocVersion", self.version),
            ("DocModified", modified),
            ("DocModifier", self.modifier),
            ("DocComment", self.comment),
            ("ReadyForReview", self.ready_for_review),
        ]
        if opts.get("brief"):
            control_info = control_info[:2]
        for tag, value in control_info:
            if value:
                child = etree.SubElement(doc_control, tag)
                child.text = str(value)
                if not filtering:
                    child.set("readonly", "yes")
                if tag == "DocVersion":
                    child.set("Publishable", "Y" if self.publishable else "N")
        if filtering:
            created = self.make_xml_date_string(self.created)
            first_pub = self.make_xml_date_string(self.first_pub)
            wrapper = etree.SubElement(doc_control, "Create")
            etree.SubElement(wrapper, "Date").text = created
            etree.SubElement(wrapper, "User").text = self.creator
            wrapper = etree.SubElement(doc_control, "Modify")
            etree.SubElement(wrapper, "Date").text = modified
            etree.SubElement(wrapper, "User").text = self.modifier
            if first_pub:
                wrapper = etree.SubElement(doc_control, "FirstPub")
                etree.SubElement(wrapper, "Date").text = first_pub
        return doc_control

    @staticmethod
    def make_xml_date_string(value):
        """
        Convert date or date/time value to XML standard format

        Pass:
          string or datetime.date object or datetime.datetime object or None

        Return:
          if just a date, return YYYY-MM-DD; if a date/time, return
          YYYY-MM-DDTHH:MM:SS; otherwise None
        """

        if not value:
            return None
        return str(value)[:19].replace(" ", "T")

    @property
    def creator(self):
        """
        Name of account which first created this document
        """

        if not hasattr(self, "_creator"):
            self.__fetch_creation_info()
        return self._creator

    @property
    def created(self):
        """
        Date/time the document was originally created
        """

        if not hasattr(self, "_created"):
            self.__fetch_creation_info()
        return self._created

    @property
    def modifier(self):
        """
        Name of account which most recently updated the document

        None if the document has not been updated since being created.
        """

        if not hasattr(self, "_modifier"):
            self.__fetch_modification_info()
        return self._modifier

    @property
    def modified(self):
        """
        Date/time the document was most recently updated (if ever)
        """

        if not hasattr(self, "_modified"):
            self.__fetch_modification_info()
        return self._modified

    def __fetch_creation_info(self):
        """
        Get the account name and date/time for the document's creation
        """

        self._creator = self._created = None
        if self.id:
            query = Query("audit_trail t", "t.dt", "u.name")
            query.join("usr u", "u.id = t.usr")
            query.join("action a", "a.id = t.action")
            query.where(query.Condition("t.document", self.id))
            query.where("a.name = 'ADD DOCUMENT'")
            row = query.execute(self.session.cursor).fetchone()
            if row:
                self._created, self._creator = row

    def __fetch_modification_info(self):
        """
        Get the user and time the document was last modified
        """

        self._modifier = self._modified = None
        if self.id:
            query = Query("audit_trail t", "t.dt", "u.name").limit(1)
            query.join("usr u", "u.id = t.usr")
            query.join("action a", "a.id = t.action")
            query.where(query.Condition("t.document", self.id))
            query.where("a.name = 'MODIFY DOCUMENT'")
            row = query.order("t.dt").execute(self.session.cursor).fetchone()
            if row:
                self._modified, self._modifier = row

    @property
    def active_status(self):
        """
        'A' if the document is active; 'I' if inactive ("blocked")
        """

        if not hasattr(self, "_active_status"):
            if not self.id:
                self._first_pub = self._active_status = None
            else:
                query = Query("document", "active_status", "first_pub")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.session.cursor).fetchone()
                self._active_status, self._first_pub = row
        return self._active_status

    @property
    def first_pub(self):
        """
        Date/time the document was first published if known
        """

        if not hasattr(self, "_first_pub"):
            if not self.id:
                self._first_pub = self._active_status = None
            else:
                query = Query("document", "active_status", "first_pub")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.session.cursor).fetchone()
                self._active_status, self._first_pub = row
        return self._first_pub

    @property
    def publishable(self):
        """
        True if this is a numbered publishable version; else False
        """

        if not hasattr(self, "_publishable"):
            if not self.id or not self.version:
                self._publishable = None
            else:
                query = Query("doc_version", "publishable")
                query.where(query.Condition("id", self.id))
                query.where(query.Condition("num", self.version))
                row = query.execute(self.session.cursor).fetchone()
                if not row:
                    self._publishable = None
                else:
                    self._publishable = row[0] == "Y"
        return self._publishable

    @property
    def ready_for_review(self):
        """
        True if this is a new document which is ready for review
        """

        if not hasattr(self, "_ready_for_review"):
            query = Query("ready_for_review", "doc_id")
            query.where(query.Condition("doc_id", self.id))
            row = query.execute(self.session.cursor).fetchone()
            self._ready_for_review = True if row else False
        return self._ready_for_review

    @property
    def title(self):
        """
        String for the title of this version of the document
        """

        if not hasattr(self, "_title"):
            self.__fetch_common_properties()
        return self._title

    @property
    def val_status(self):
        """
        'V' (valid), 'I' (invalid), or 'Y' (unvalidated)
        """

        if not hasattr(self, "__val_status"):
            self.__fetch_common_properties()
        return self.__val_status

    @property
    def val_date(self):
        """
        Date/time this version of the document was last validated
        """

        if not hasattr(self, "__val_date"):
            self.__fetch_common_properties()
        return self.__val_date

    @property
    def comment(self):
        """
        String describing this version of the document
        """

        if not hasattr(self, "__comment"):
            self.__fetch_common_properties()
        return self.__comment

    def __fetch_common_properties(self):
        """
        Fetch and cache values from a single table

        If any of these values are retrieved, we might as well grab them
        all, to save multiple queries to the same table.
        """

        self._title = self._val_status = self._val_date = self._comment = None
        if self.id:
            table = "doc_version" if self.version else "document"
            query = Query(table, "title", "val_status", "val_date", "comment")
            query.where(query.Condition("id", self.id))
            if self.version:
                query.where(query.Condition("num", self.version))
            row = query.execute(self.session.cursor).fetchone()
            self._title = row[0]
            self._val_status = row[1]
            self._val_date = row[2]
            self._comnment = row[3]

    def filter(self, *filters, **opts):
        """
        Apply one or more filters to the XML for the document

        Positional arguments:
          filters - each positional argument represents a named
                    filter ("name:..."), a named filter set ("set:...")
                    or a filter's document ID

        Optional keyword arguments:
          parms - dictionary of parameters to be passed to the filtering
                  engine (parameter values indexed by parameter names)
          output - if False, only return the warning and error messages
          version - which versions of the filters to use ("last" or "lastp"
                    or a version number); a specific version number only
                    makes sense in the case of a request involving a
                    single filter, and is probably a mistake in most cases
          date - if specified, only use filters earlier than this date/time;
                 can be used in combination with `version` (for example,
                 to use the latest publishable versions created before
                 a publishing job started)
          filter - used to pass in the XML for an in-memory filter
                   instead of using filters pulled from the repository
                   (cannot be used in combination with positional
                   filter arguments)
          ctl - filter the document with the CdrDoc legacy wrapper,
                to include the document control information, if True

        Return:
          `FilterResult` object if `output` option is not False;
          otherwise, return the sequence of message strings emitted
          by the XSL/T engine
        """

        parms = opts.get("parms") or {}
        doc = self.legacy_doc(get_xml=True) if opts.get("ctl") else self.root
        messages = []
        parser = self.Parser()
        Resolver.local.docs.append(self)
        try:
            if "filter" in opts:
                filters = [Filter(None, opts.get("filter"))]
            else:
                filters = self.__assemble_filters(*filters, **opts)
            for f in filters:
                result = self.__apply_filter(f.xml, doc, parser, **parms)
                doc = result.doc
                for entry in result.error_log:
                    messages.append(entry.message)
            if opts.get("output", True):
                return self.FilterResult(doc, messages=messages)
            return messages
        finally:
            Resolver.local.docs.pop()

    def __assemble_filters(self, *filter_specs, **opts):
        """
        """
        version = opts.get("version")
        before = opts.get("before")
        opts = dict(version=version, before=before)
        filters = []
        for spec in filter_specs:
            if spec.startswith("set:"):
                name = spec.split(":", 1)[1]
                filters += self.__get_filter_set(name, **opts)
            else:
                if spec.startswith("name:"):
                    name = spec.split(":", 1)[1]
                    doc_id = Doc.id_from_title(name, self.session.cursor)
                else:
                    doc_id = spec
                filters.append(self.get_filter(doc_id, **opts))
        return filters

    def __get_filter_set(self, name, **opts):
        query = Query("filter_set", "id")
        query.where(query.Condition("name", name))
        row = query.execute(self.session.cursor).fetchone()
        if not row:
            return []
        return self.__get_filter_set_by_id(row[0], **opts)

    def __get_filter_set_by_id(self, set_id, **opts):
        depth = opts.get("depth", 0)
        if depth > Filter.MAX_FILTER_SET_DEPTH:
            raise Exception("infinite filter set recursion")
        opts = opts.copy()
        opts["depth"] = depth + 1
        with self.session.cache.filter_set_lock:
            if set_id in self.session.cache.filter_sets:
                return self.session.cache.filter_sets[set_id]
        fields = "filter", "subset"
        query = Query("filter_set_member", *fields).order("position")
        query.where(query.Condition("filter_set", set_id))
        filters = []
        rows = query.execute(self.session.cursor).fetchall()
        for filter_id, subset_id in rows:
            if filter_id:
                filters.append(self.get_filter(filter_id, **opts))
            elif subset_id:
                filters += self.__get_filter_set_by_id(subset_id, **opts)
        with self.session.cache.filter_set_lock:
            if set_id not in self.session.cache.filter_sets:
                self.session.cache.filter_sets[set_id] = filters
        return filters

    def get_filter(self, doc_id, **opts):
        """
        Fetch a possibly cached filter document

        The cache is not used for unversioned filters, because the
        current working copy of a document can change, something
        which a specific version of a document is guaranteed not
        to do.

        If a filter hasn't been used in over `SHELF_LIFE` seconds,
        we get a fresh copy for the cache.

        We have to encode spaces in the filter titles used in the
        `include` and `import` directives in order to make the URLs
        valid.

        While in development, we are using modified filters stored
        in the `good_filters` table. When we go into production we'll
        apply the same modifications to the actual filters and restore
        the use of the `doc_version` view for fetching the filters.

        Required positional argument:
          doc_id - integer for the filter's document ID

        Optional keyword options:
          version - version to fetch
          before - restrict version to one of those created before this cutoff

        Return:
          `Filter` object
        """

        doc = Doc(self.session, id=doc_id, **opts)
        key = doc.id, doc.version, self.session.tier.name
        if doc.version:
            with self.session.cache.filter_lock:
                f = self.session.filters.get(key)
                if f is not None:
                    return f
        # TODO - REMOVE FOLLOWING CODE; USE root = doc.root INSTEAD
        query = db.Query("good_filters", "xml")
        query.where(query.Condition("id", doc.id))
        xml = query.execute(self.session.cursor).fetchone()[0]
        root = etree.fromstring(bytes(xml, "utf-8"))
        # TODO - END TEMPORARY CODE
        for name in ("import", "include"):
            for node in root.findall(Doc.qname(Filter.NS, name)):
                href = node.get("href")
                node.set("href", href.replace(" ", "%20"))
        xml = etree.tostring(root.getroottree(), encoding="utf-8")
        if doc.version:
            with self.session.cache.filter_lock:
                if key not in self.session.cache.filters:
                    self.session.cache.filters[key] = Filter(doc_id, xml)
                return self.session.cache.filters[key]
        else:
            return Filter(doc_id, xml)

    def __apply_filter(self, filter_xml, doc, parser=None, **parms):
        transform = etree.XSLT(etree.fromstring(filter_xml, parser))
        return self.FilterResult(transform(doc), error_log=transform.error_log)

    @staticmethod
    def get_text(node, default=None):
        """
        Assemble the concatenated text nodes for an element of the document.

        Note that the call to node.itertext() must include the wildcard
        string argument to specify that we want to avoid recursing into
        nodes which are not elements. Otherwise we will get the content
        of processing instructions, and how ugly would that be?!?

        Pass:
            node - element node from an XML document parsed by the lxml package
            default - what to return if the node is None

        Return:
            default if node is None; otherwise concatenated string node
            descendants
        """

        if node is None:
            return default
        return "".join(node.itertext("*"))

    @staticmethod
    def qname(ns, local):
        return "{{{}}}{}".format(ns, local)

    @staticmethod
    def id_from_title(title, cursor=None):
        title = title.replace("@@SLASH@@", "/").replace("+", " ")
        query = db.Query("document", "id")
        query.where(query.Condition("title", title))
        rows = query.execute(cursor).fetchall()
        if len(rows) > 1:
            raise Exception("Multiple documents with title %s" % title)
        return rows and rows[0][0] or None

    class FilterResult:
        """
        Results of passing an XML document through one or more XSL/T filters

        Attributes:
          doc - object containing the DOM tree for the filtered output;
                use `bytes(result.doc)` to get the serialized document
                encoded using the encoding specified in the last filter
                (default UTF-8); use `unicode(result.doc)` to get the
                serialized document as unicode (with `unicode` as an
                alias for `str` in Python 3)
          messages - optional sequence of string messages emitted by
                     the XSL/T engine (used for the final return value
                     of the `Doc.filter()` method); these are unicode
                     strings
          error_log - used for the individual calls to `__apply_filter()`
        """

        def __init__(self, doc, **opts):
            self.doc = doc
            self.error_log = opts.get("error_log")
            self.messages = opts.get("messages")

    class Parser(etree.XMLParser):
        def __init__(self):
            etree.XMLParser.__init__(self)
            self.resolvers.add(Resolver("cdrutil"))
            self.resolvers.add(Resolver("cdr"))
            self.resolvers.add(Resolver("cdrx"))


class Local(threading.local):
    def __init__(self, **kw):
        self.docs = []
        self.__dict__.update(kw)


class Resolver(etree.Resolver):
    UNSAFE = re.compile(r"insert\s|update\s|delete\s|create\s|alter\s"
                        r"exec[(\s]|execute[(\s]")
    ID_KEY_STRIP = re.compile("[^A-Z0-9]+")
    local = Local()

    def resolve(self, url, pubid, context):
        self.doc = self.local.docs[-1]
        self.session = self.doc.session
        self.cursor = self.session.cursor
        self.url = urllib.parse.unquote(url.replace("+", " "))
        self.url = self.url.replace("@@PLUS@@", "+")
        if url == "cdrx:/last":
            return self.resolve_string("<empty/>", context)
        scheme, parms = self.url.split(":", 1)
        parms = parms.strip("/")
        if scheme in ("cdr", "cdrx"):
            return self.get_doc(parms, context)
        elif scheme == "cdrutil":
            return self.run_function(parms, context)
        raise Exception("unsupported url {!r}".format(self.url))

    def run_function(self, parms, context):
        function, args = parms, None
        if "/" in parms:
            function, args = parms.split("/", 1)
        if function == "docid":
            return self.get_doc_id(context)
        elif function == "sql-query":
            return self.run_sql_query(args, context)
        elif function == "get-pv-num":
            return self.get_pv_num(args, context)
        elif function == "denormalizeTerm":
            return self.get_term(args, context)
        elif function == "dedup-ids":
            return self.dedup_ids(args, context)
        elif function == "valid-zip":
            return self.valid_zip(args, context)
        error = "unsupported function {!r} in {!r}".format(function, self.url)
        raise Exception(error)

    @classmethod
    def make_id_key(cls, id):
        return cls.ID_KEY_STRIP.sub("", id.upper())

    def valid_zip(self, args, context):
        """
        Look up a string in the `zipcode` table

        Return the base (first 5 digits) for the ZIP code, or an empty
        element if the zip code is not found
        """

        result = etree.Element("ValidZip")
        query = db.Query("zipcode", "zip")
        query.where(query.Condition("zip", args))
        row = query.execute(self.cursor).fetchone()
        if row and row[0]:
            result.text = str(row[0])[:5]
        return self.package_result(result, context)

    def dedup_ids(self, args, context):
        ids = []
        skip = set()
        if "~~" in args:
            primary, secondary = [i.split("~") for i in args.split("~~", 1)]
            for p in primary:
                skip.add(self.make_id_key(p))
            for s in secondary:
                key = self.make_id_key(s)
                if key and key not in skip:
                    ids.append(s)
                    skip.add(key)
        result = etree.Element("result")
        for i in ids:
            etree.SubElement(result, "id").text = i
        return self.package_result(result, context)

    def get_term(self, args, context):
        if "/" in args:
            doc_id = Doc.extract_id(args.split("/")[0])
            upcode = False
        else:
            doc_id = Doc.extract_id(args)
            upcode = True
        term = Term.get_term(self.session, doc_id)
        if term is None:
            term_xml = "<empty/>"
        else:
            term_xml = term.get_xml(upcode)
        return self.resolve_string(term_xml, context)

    def get_pv_num(self, args, context):
        doc = Doc(self.session, id=args)
        answer = etree.Element("PubVerNumber")
        answer.text = str(doc.last_publishable_version or 0)
        return self.package_result(answer, context)

    def run_sql_query(self, args, context):
        if "~" in args:
            query, values = args.split("~", 1)
            values = values.split("~")
        else:
            query, values = args, []
        if self.UNSAFE.search(query):
            raise Exception("query contains disallowed sql keywords")
        if query.count("?") != len(values):
            raise Exception("wrong number of sql query placeholder values")
        if db.Query.PLACEHOLDER != "?":
            query = query.replace("?", db.Query.PLACEHOLDER)
        self.cursor.execute(query, tuple(values))
        names = [col[0] for col in self.cursor.description]
        result = etree.Element("SqlResult")
        r = 1
        for values in self.cursor.fetchall():
            row = etree.SubElement(result, "row", id=str(r))
            for c, v in enumerate(values):
                col = etree.SubElement(row, "col", id=str(c), name=names[c])
                if v is None:
                    col.set("null", "Y")
                else:
                    col.text = str(v)
            r += 1
        return self.package_result(result, context)

    def get_doc_id(self, context):
        element = etree.Element("DocId")
        element.text = self.doc.cdr_id
        return self.package_result(element, context)

    def get_doc(self, parms, context):
        if parms.startswith("*"):
            if "/CdrCtl" in parms:
                element = self.doc.legacy_doc_control(filtering=True)
                return self.package_result(element, context)
            elif "/DocTitle" in parms:
                element = etree.Element("CdrDocTitle")
                element.text = self.doc.title
                return self.package_result(element, context)
            else:
                raise Exception("unsupported url {!r}".format(self.url))
        if parms.startswith("name:"):
            parms = parms[5:]
            if "/" in parms:
                title, version = parms.split("/", 1)
            else:
                title, version = parms, None
            doc_id = Doc.id_from_title(title, self.cursor)
            if not doc_id:
                return None
            doc = Doc(self.session, id=doc_id, version=version)
            if doc.doctype == "Filter":
                doc_xml = doc.get_filter(doc_id, version=version).xml
                return self.resolve_string(doc_xml, context)
            parms = str(doc.id)
            if version:
                parms = "%d/%s" % (doc_id, version)
        else:
            doc_id, version = parms, None
            if "/" in parms:
                doc_id, version = parms.split("/", 1)
            if not doc_id:
                raise Exception("no document specified")
            doc = Doc(self.session, id=doc_id, version=version)
        return self.resolve_string(doc.xml, context)

    def package_result(self, result, context):
        result = etree.tostring(result, encoding="utf-8")
        return self.resolve_string(result, context)

    @staticmethod
    def escape_uri(context, arg=""):
        if isinstance(arg, (list, tuple)):
            arg = "".join(arg)
        try:
            return urllib.parse.quote(arg.replace("+", "@@PLUS@@"))
        except:
            print("cdr:escape_uri(%r)" % arg)
            raise

etree.FunctionNamespace(Doc.NS).update({"escape-uri": Resolver.escape_uri})

class Term:
    def __init__(self, session, doc_id, depth=0):
        self.session = session
        self.doc_id = doc_id
        self.cdr_id = "CDR%010d" % doc_id
        self.include = True
        self.parents = {}
        self.name = self.pdq_key = self.xml = self.full_xml = None
        try:
            doc = Doc(session, id=doc_id, version="lastp")
            self.name = doc.get_text(doc.root.find("PreferredName"))
            self.pdq_key = doc.get_text(doc.root.find("PdqKey"))
            for node in doc.root.findall("TermType/TermTypeName"):
                if doc.get_text(node) in ("Header term", "Obsolete term"):
                    self.include = False
                    break
            for node in root.findall("TermRelationship/ParentTerm/TermId"):
                self.get_parent(node, depth)
            if self.name and not depth:
                self.serialize()
        except Exception as e:
            if Doc.NO_PUBLISHABLE_VERSIONS in str(e):
                return
            raise
    def get_xml(self, with_upcoding=True):
        with self.session.cache.term_lock:
            if self.xml and self.full_xml:
                return with_upcoding and self.full_xml or self.xml
        self.serialize(need_locking=True)
        return with_upcoding and self.full_xml or self.xml
    def serialize(self, need_locking=False):
        term = etree.Element("Term", nsmap=Doc.NSMAP)
        term.set(Doc.CDR_REF, self.cdr_id)
        if self.pdq_key:
            term.set("PdqKey", "Term:" + self.pdq_key)
        etree.SubElement(term, "PreferredName").text = self.name
        xml = etree.tostring(term, encoding="utf-8")
        for doc_id in sorted(self.parents):
            parent = self.parents[doc_id]
            if parent is not None and parent.include and parent.name:
                child = etree.SubElement(term, "Term")
                child.set(Doc.CDR_REF, parent.cdr_id)
                if parent.pdq_key:
                    child.set("PdqKey", "Term:" + parent.pdq_key)
                etree.SubElement(child, "PreferredName").text = parent.name
        full_xml = etree.tostring(term, encoding="utf-8")
        if need_locking:
            with self.session.cache.term_lock:
                if not(self.xml and self.full_xml):
                    self.xml, self.full_xml = xml, full_xml
        else:
            self.xml, self.full_xml = xml, full_xml
    def get_parent(self, node, depth):
        try:
            doc_id = Doc.extract_id(node.get(Doc.CDR_REF))
        except:
            error = "No cdr:ref for parent of Term {}".format(self.cdr_id)
            raise Exception(failure)
        if doc_id not in self.parents:
            parent = Term.get_term(self.session, doc_id, depth + 1)
            if parent:
                self.parents.update(parent.parents)
                self.parents[doc_id] = parent
    @classmethod
    def get_term(cls, session, doc_id, depth=0):
        if depth > cls.MAX_DEPTH:
            error = "term hierarchy depth exceeded at CDR()".format(doc_id)
            raise Exception(error)
        with session.cache.term_lock:
            if doc_id in session.terms:
                return session.cache.terms[doc_id]
        term = cls(session, doc_id, depth)
        if not term.name:
            term = None
        with session.cache.term_lock:
            if doc_id not in session.cache.terms:
                session.cache.terms[doc_id] = term
        return term


class Filter:
    MAX_FILTER_SET_DEPTH = 20
    NS = "http://www.w3.org/1999/XSL/Transform"
    def __init__(self, doc_id, xml):
        self.doc_id = doc_id
        self.xml = xml.encode("utf-8") if isinstance(xml, unicode) else xml
        self.now = time.time()


class DTD:
    NS = "http://www.w3.org/2001/XMLSchema"
    SCHEMA = Doc.qname("schema", NS)
    ELEMENT = Doc.qname("element", NS)
    COMPLEX_TYPE = Doc.qname("complexType", NS)
    SIMPLE_TYPE = Doc.qname("simpleType", NS)
    SIMPLE_CONTENT = Doc.qname("simpleContent", NS)
    GROUP = Doc.qname("group", NS)
    INCLUDE = Doc.qname("include", NS)
    ATTRIBUTE = Doc.qname("attribute", NS)
    SEQUENCE = Doc.qname("sequence", NS)
    CHOICE = Doc.qname("choice", NS)
    RESTRICTION = Doc.qname("restriction", NS)
    EXTENSION = Doc.qname("extension", NS)
    ENUMERATION = Doc.qname("enumeration", NS)
    NAME_START_CATEGORIES = { "Ll", "Lu", "Lo", "Lt", "Nl" }
    OTHER_NAME_CATEGORIES = { "Mc", "Me", "Mn", "Lm", "Nd" }
    NAME_CHAR_CATEGORIES = NAME_START_CATEGORIES | OTHER_NAME_CATEGORIES
    EMPTY = "empty"
    MIXED = "mixed"
    TEXT_ONLY = "text-only"
    ELEMENT_ONLY = "element-only"
    UNBOUNDED = "unbounded"

    def __init__(self, **args):
        self.types = {}
        self.groups = {}
        self.top = None
        self.name = args.get("name")
        self.parse_schema(self.name)

    def parse_schema(self, name):
        root = Validator.get_doc(name)
        if root.tag != self.SCHEMA:
            raise Exception("Top-level element must be schema")
        for node in root:
            if node.tag == self.ELEMENT:
                assert not self.top, "only one top-level element allowed"
                self.top = self.Element(self, node)
            elif node.tag == self.COMPLEX_TYPE:
                self.ComplexType(self, node)
            elif node.tag == self.SIMPLE_TYPE:
                self.SimpleType(self, node)
            elif node.tag == self.GROUP:
                self.Group(self, node)
            elif node.tag == self.INCLUDE:
                self.parse_schema(node.get("schemaLocation"))
                util.Session.logger.debug("resume parsing %s", name)
        util.Session.logger.debug("finished parsing %s", name)
    def __str__(self):
        lines = ["<!-- Generated from %s -->" % self.name, ""]
        self.defined = set()
        self.top.define(lines)
        return "\n".join(lines) + "\n"
    def add_type(self, t):
        assert t not in self.types, "duplicate type %s" % t.name
        self.types[t.name] = t

    class CountedNode:
        def __init__(self, dtd, node):
            self.dtd = dtd
            self.min_occurs = self.max_occurs = 1
            min_occurs = node.get("minOccurs")
            max_occurs = node.get("maxOccurs")
            if min_occurs:
                self.min_occurs = int(min_occurs)
            if max_occurs == DTD.UNBOUNDED:
                self.max_occurs = sys.maxsize
            elif max_occurs:
                self.max_occurs = int(max_occurs)
            if self.min_occurs < 1:
                self.count_char = self.max_occurs == 1 and "?" or "*"
            else:
                self.count_char = self.max_occurs > 1 and "+" or ""
    class Element(CountedNode):
        def __init__(self, dtd, node):
            DTD.CountedNode.__init__(self, dtd, node)
            self.name = node.get("name")
            self.type_name = node.get("type")
            assert self.name, "element name required"
            assert self.type_name, "element type required"
            debug = util.Session.logger.debug
            debug("Element %s of type %s", self.name, self.type_name)
        def lookup_type(self):
            element_type = self.dtd.types.get(self.type_name)
            if element_type:
                return element_type
            if "xsd:" not in self.type_name:
                vals = self.name, self.type_name
                raise Exception("element %s: type %s not found" % vals)
            return DTD.Type(self.dtd, self.type_name)
        def define(self, lines):
            self.children = []
            lines += self.lookup_type().define(self)
            if not self.dtd.defined:
                self.add_control_definitions(lines)
            for child in self.children:
                if child.name not in self.dtd.defined:
                    self.dtd.defined.add(child.name)
                    child.define(lines)
        def add_control_definitions(self, lines):
            lines.append("<!ELEMENT CdrDocCtl (DocId, DocTitle)>")
            lines.append("<!ATTLIST CdrDocCtl readyForReview CDATA #IMPLIED>")
            lines.append("<!ELEMENT DocId (#PCDATA)>")
            lines.append("<!ATTLIST DocId readonly (yes|no) #IMPLIED>")
            lines.append("<!ELEMENT DocTitle (#PCDATA)>")
            lines.append("<!ATTLIST DocTitle readonly (yes|no) #IMPLIED>")
            for name in (self.name, "CdrDocCtl", "DocId", "DocTitle"):
                self.dtd.defined.add(name)
        def get_node(self, elements, serialize=False):
            if self.name not in [element.name for element in elements]:
                elements.append(self)
            if serialize:
                return "%s%s" % (self.name, self.count_char)
    class Type:
        def __init__(self, dtd, name):
            self.dtd = dtd
            self.name = name
            assert self.name, "type must have a name"
        def define(self, element):
            definitions = [self.define_element(element)]
            attributes = self.define_attributes(element)
            if attributes:
                definitions.append(attributes)
            return definitions
        def define_element(self, element):
            return "<!ELEMENT %s (#PCDATA)>" % element.name
        def define_attributes(self, element):
            return None
        @staticmethod
        def map_type(schema_type):
            for n in ("ID", "IDREFS", "NMTOKEN", "NMTOKENS"):
                if schema_type == "xsd:%s" % n:
                    return n
            return "CDATA"

    class SimpleType(Type):
        def __init__(self, dtd, node):
            DTD.Type.__init__(self, dtd, node.get("name"))
            self.base = None
            self.nmtokens = []
            util.Session.logger.debug("SimpleType %s", self.name)
            for restriction in node.findall(DTD.RESTRICTION):
                self.base = restriction.get("base")
                for enum in restriction.findall(DTD.ENUMERATION):
                    value = enum.get("value")
                    if DTD.is_nmtoken(value):
                        self.nmtokens.append(value)
                    else:
                        self.nmtokens = None
                        break
            dtd.add_type(self)
        def dtd_type(self):
            if self.nmtokens:
                return "(%s)" % "|".join(sorted(self.nmtokens))
            if "xsd:" in self.base:
                return DTD.Type.map_type(self.base)
            base = self.dtd.types.get(self.base)
            assert base, "%s: base type %s not found" % (self.name, self.base)
            return base.dtd_type()

    class ComplexType(Type):
        CONTENT = "sequence", "choice", "simpleContent", "group"
        CNAMES = ", ".join(["xsd:%s" % c for c in CONTENT])
        ERROR = "complex type may only contain one of %s" % CNAMES
        def __init__(self, dtd, node):
            DTD.Type.__init__(self, dtd, node.get("name"))
            util.Session.logger.debug("ComplexType %s", self.name)
            self.attributes = {}
            self.content = None
            self.model = DTD.EMPTY
            if node.get("mixed") == "true":
                self.model = DTD.MIXED
            for child in node.findall("*"):
                if child.tag == DTD.ATTRIBUTE:
                    self.add_attribute(dtd, child)
                elif self.content:
                    raise Exception("%s: %s" % (self.name, self.ERROR))
                elif child.tag == DTD.SIMPLE_CONTENT:
                    self.model = DTD.TEXT_ONLY
                    extension = child.find(DTD.EXTENSION)
                    assert len(extension), "%s: missing extension" % self.name
                    for child in extension.findall(DTD.ATTRIBUTE):
                        self.add_attribute(dtd, child)
                    break
                else:
                    if self.model == DTD.EMPTY:
                        self.model = DTD.ELEMENT_ONLY
                    if child.tag == DTD.SEQUENCE:
                        self.content = DTD.Sequence(dtd, child)
                    elif child.tag == DTD.CHOICE:
                        self.content = DTD.Choice(dtd, child)
                    elif child.tag == DTD.GROUP:
                        self.content = DTD.Group(dtd, child)
                    else:
                        raise Exception("%s: %s" % (self.name, self.ERROR))
            dtd.add_type(self)
        def add_attribute(self, dtd, child):
            attribute = DTD.Attribute(dtd, child)
            if attribute.name in self.attributes:
                values =  (self.name, attribute.name)
                error = "Duplicate attribute %s/@%s" % values
                raise Exception(error)
            self.attributes[attribute.name] = attribute
        def define_element(self, element):
            if self.model == DTD.TEXT_ONLY:
                return "<!ELEMENT %s (#PCDATA)>" % element.name
            elif self.model == DTD.EMPTY:
                return "<!ELEMENT %s EMPTY>" % element.name
            elif self.model == DTD.MIXED:
                self.content.get_node(element.children)
                children = "|".join([c.name for c in element.children])
                names = (element.name, children)
                return "<!ELEMENT %s (#PCDATA|%s)*>" % names
            elif self.model == DTD.ELEMENT_ONLY:
                content = self.content.get_node(element.children, True)
                assert content, "Elements required for elementOnly content"
                if not self.dtd.defined:
                    content = "(CdrDocCtl,%s)" % content
                elif not content.startswith("("):
                    content = "(%s)" % content
                return "<!ELEMENT %s %s>" % (element.name, content)
            raise Exception("%s: unrecognized content model" % self.name)
        def define_attributes(self, element):
            attributes = [str(a) for a in self.attributes.values()]
            if not self.dtd.defined and "readonly" not in self.attributes:
                attributes.append("readonly CDATA #IMPLIED")
            if attributes or not self.dtd.defined:
                attributes = sorted(attributes)
                if not self.dtd.defined:
                    attributes = ["xmlns:cdr CDATA #IMPLIED"] + attributes
                attributes = " ".join(attributes)
                return "<!ATTLIST %s %s>" % (element.name, attributes)

    class ChoiceOrSequence:
        def __init__(self, dtd, node):
            DTD.CountedNode.__init__(self, dtd, node)
            self.nodes = []
            for child in node:
                if child.tag == DTD.ELEMENT:
                    self.nodes.append(DTD.Element(dtd, child))
                if child.tag == DTD.CHOICE:
                    self.nodes.append(DTD.Choice(dtd, child))
                elif child.tag == DTD.SEQUENCE:
                    self.nodes.append(DTD.Sequence(dtd, child))
                elif child.tag == DTD.GROUP:
                    self.nodes.append(DTD.Group(dtd, child))
            assert self.nodes, "choice or sequence cannot be empty"
        def get_node(self, elements, serialize=False):
            nodes = [node.get_node(elements, serialize) for node in self.nodes]
            if serialize:
                string = self.separator.join(nodes)
                if len(self.nodes) > 1:
                    string = "(%s)" % string
                return "%s%s" % (string, self.count_char)
    class Choice(ChoiceOrSequence):
        separator = "|"
        def __init__(self, dtd, node):
            DTD.ChoiceOrSequence.__init__(self, dtd, node)
            util.Session.logger.debug("Choice with %d nodes", len(self.nodes))
    class Sequence(ChoiceOrSequence):
        separator = ","
        def __init__(self, dtd, node):
            DTD.ChoiceOrSequence.__init__(self, dtd, node)
            util.Session.logger.debug("Sequence with %d nodes", len(self.nodes))
    class Group:
        def __init__(self, dtd, node):
            self.dtd = dtd
            self.ref = node.get("ref")
            if self.ref:
                util.Session.logger.debug("Reference to group %s", self.ref)
                return
            self.name = node.get("name")
            util.Session.logger.debug("Group %s", self.name)
            if self.name in dtd.groups:
                raise Exception("multiple definitions for group %s" % self.name)
            nodes = []
            for child in node:
                if child.tag == DTD.CHOICE:
                    nodes.append(DTD.Choice(dtd, node))
                elif child.tag == DTD.SEQUENCE:
                    nodes.append(DTD.Sequence(dtd, node))
            assert len(nodes) == 1, "%s: %d nodes" % (self.name, len(nodes))
            self.node = nodes[0]
            dtd.groups[self.name] = self
        def get_node(self, elements, serialize=False):
            if self.ref:
                return self.dtd.groups[self.ref].get_node(elements, serialize)
            else:
                return self.node.get_node(elements, serialize)
    class Attribute:
        def __init__(self, dtd, node):
            self.dtd = dtd
            self.name = node.get("name")
            self.type_name = node.get("type")
            debug = util.Session.logger.debug
            debug("Attribute %s of type %s", self.name, self.type_name)
            self.required = node.get("use") == "required"
            if self.name.startswith("cdr-"):
                self.name = self.name.replace("cdr-", "cdr:")
        def __str__(self):
            required = self.required and "REQUIRED" or "IMPLIED"
            return "%s %s #%s" % (self.name, self.dtd_type(), required)
        def dtd_type(self):
            if "xsd:" in self.type_name:
                return DTD.Type.map_type(self.type_name)
            simple_type = self.dtd.types.get(self.type_name)
            if not simple_type:
                vals = self.type_name, self.name
                error = "unrecognized type %s for @%s" % vals
                raise Exception(error)
            return simple_type.dtd_type()
    @classmethod
    def alternate_is_nmtoken(cls, string):
        for c in string:
            o = ord(c)
            if 0x20DD < o <= 0x20E0:
                return False
            if 0xF900 < o < 0xFFFF:
                return False
            if c not in u".-_:\u00B7\u0e87":
                if unicodedata.category(c) not in cls.NAME_CHAR_CATEGORIES:
                    return False
        return True

    NMTOKEN_PATTERN = re.compile(
        r"[-:_."
        r"\xB7"
        r"0-9A-Za-z"
        r"\U000000C0-\U000000D6"
        r"\U000000D8-\U000000F6"
        r"\U000000F8-\U000002FF"
        r"\U00000300-\U0000036F"
        r"\U00000370-\U0000037D"
        r"\U0000037F-\U00001FFF"
        r"\U0000200C-\U0000200D"
        r"\U0000203F-\U00002040"
        r"\U00002070-\U0000218F"
        r"\U00002C00-\U00002FEF"
        r"\U00003001-\U0000D7FF"
        r"\U0000F900-\U0000FDCF"
        r"\U0000FDF0-\U0000FFFD"
        r"\U00010000-\U000EFFFF]+"
    )
    @classmethod
    def is_nmtoken(cls, string):
        """
        See https://www.w3.org/TR/REC-xml/#sec-common-syn
        """
        if " " in string:
            return False
        return cls.NMTOKEN_PATTERN.match(string) and True or False

