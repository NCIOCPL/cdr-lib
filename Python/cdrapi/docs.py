"""
Manage CDR documents
"""

import base64
import copy
import datetime
import re
import sys
import threading
import time
from adodbapi import Binary
import dateutil.parser
from lxml import etree
#from cdrapi import db
from cdrapi.db import Query


# ----------------------------------------------------------------------
# Try to make the module compatible with Python 2 and 3.
# ----------------------------------------------------------------------
from six import itervalues
try:
    basestring
    base64encode = base64.encodestring
    base64decode = base64.decodestring
except:
    base64encode = base64.encodebytes
    base64decode = base64.decodebytes
    basestring = str, bytes
    unicode = str
try:
    from urllib.parse import quote as url_quote
    from urllib.parse import unquote as url_unquote
except ImportError:
    from urllib import quote as url_quote
    from urllib import unquote as url_unquote


class Doc(object):
    """
    Information about an XML document in the CDR repository

    All of the attributes for the object are implemented as properties,
    fetched as needed to optimize away potentially expensive unnecessary
    processing. We have to be careful not to leave cached values lying
    around after they are no longer correct. (TODO: check this)

    Read-only attributes:
      active_status - 'A' if the document is active; 'I' if inactive
      cdr_id - standard string representation for the document's id
      comment - description of this version of the document
      creation - `Doc.Action` object for document creation information
      denormalized_xml - xml with links resolved
      eids - copy of document error location IDs
      errors - sequence of `Error` objects
      errors_node - legacy DOM node for the document's errors
      first_pub - date/time the document was first published (if known)
      frag_ids - set of unique IDs for candidate link target in this doc
      hard_error_count - number of real errors (not warnings or info messages)
      highest_fragment_id - highest cdr:id attribute value in the form _\d+
      id - primary key in the `all_docs` database table for the document
      last_publishable_version - integer for most recent pub version
      last_saved - when the document was most recently saved
      last_version - integer for the most recently created version
      last_version_date - when the last version was created
      level - what to keep from revision markup filtering
      lock - `Doc.Lock` object (or None if the document isn't checked out)
      modification - `Doc.Action` object for last document modification info
      publishable - True iff the object's version is marked publishable
      ready_for_review - True if the document has be marked review ready
      resolved - document after going through revision markup filtering
      root - parsed tree for the document's XML
      session - login session for which the document information was collected
      title - string for the title of this version of the document
      val_date - when the version's validation status was last determined
      val_status - V[alid], I[nvalid], U[nvalidated], or M[alformed]
      valid - True iff the document has passed all validation tests
      version - optional integer represent version requested for the doc
      has_unversioned changes - True if the all_docs table was updated
                                more recently than the doc's latest version

    Read/write attributes:
      xml - unicode string for the serialized DOM for the document
      blob - bytes for a BLOB associated with the document (optional)
      doctype - `Doctype` object
    """

    NS = "cips.nci.nih.gov/cdr"
    NSMAP = {"cdr": NS}
    NOT_VERSIONED = "document not versioned"
    NO_PUBLISHABLE_VERSIONS = "no publishable version found"
    UNVALIDATED = "U"
    VALID = "V"
    INVALID = "I"
    MALFORMED = "M"
    ACTIVE = "A"
    BLOCKED = INACTIVE = "I"
    DELETED = "D"
    VALIDATION_TEMPLATE = None
    VALIDATION = "validation"
    LINK = "link"
    OTHER = "other"
    LEVEL_OTHER = "other"
    LEVEL_INFO = "info"
    LEVEL_WARNING = "warning"
    LEVEL_ERROR = "error"
    LEVEL_FATAL = "fatal"
    MAX_TITLE_LEN = 255
    MAX_COMMENT_LEN = 255
    MAX_SQLSERVER_INDEX_SIZE = 800
    MAX_INDEX_ELEMENT_DEPTH = 40
    INDEX_POSITION_WIDTH = 4
    MAX_LOCATION_LENGTH = INDEX_POSITION_WIDTH * MAX_INDEX_ELEMENT_DEPTH
    HEX_INDEX = "{{:0{}X}}".format(INDEX_POSITION_WIDTH)
    INTEGERS = re.compile(r"\d+")
    REVISION_LEVEL_PUBLISHED = 3
    REVISION_LEVEL_PUBLISHED_OR_APPROVED = 2
    REVISION_LEVEL_PUBLISHED_OR_APPROVED_OR_PROPOSED = 1
    DEFAULT_REVISION_LEVEL = REVISION_LEVEL_PUBLISHED
    LEGACY_MAILER_CUTOFF = 390000

    def __init__(self, session, **opts):
        """
        Capture the session and options passed by the caller

        Two typical scenarios for invoking this constructor would be
          * pass in the XML for a new document we will then save
          * pass is an ID (and possibly a version) to fetch information
            about an existing document

        There are many variations on these uses. For example, use the
        second option to fetch a document, then make some modifications
        to the XML and then save a new version. Or, assuming you already
        know what the new XML should be, pass in both the ID and the XML
        to the constructor, and then call `doc.save()`.

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
          level - what to retain when filtering revision markup
                  default is DEFAULT_REVISION_LEVEL
        """

        self.__session = session
        self.__opts = opts
        self._errors = []

    # ------------------------------------------------------------------
    # PROPERTIES START HERE.
    # ------------------------------------------------------------------

    @property
    def active_status(self):
        """
        'A' if the document is active; 'I' if inactive ("blocked")
        """

        if not self.id:
            return None
        query = Query("all_docs", "active_status")
        query.where(query.Condition("id", self.id))
        row = query.execute(self.cursor).fetchone()
        assert row.active_status in "AID", "Invalid active_status value"
        return row.active_status

    @property
    def blob(self):
        """
        Bytes for BLOB associated with this version of the document
        """

        if not hasattr(self, "_blob"):
            if "blob" in self.__opts:
                self._blob = self.__opts["blob"]
            elif not self.has_blob:
                self._blob = None
            else:
                query = Query("doc_blob", "data")
                query.where(query.Condition("id", self._blob_id))
                row = query.execute(self.cursor).fetchone()
                if not row:
                    raise Exception("no blob found")
                self._blob = row.data
        return self._blob

    @blob.setter
    def blob(self, value): self._blob = value

    @property
    def cdr_id(self):
        """
        Canonical string form for the CDR document ID (CDR9999999999)
        """

        return "CDR{:010d}".format(self.id) if self.id else None

    @property
    def comment(self):
        """
        String describing this version of the document
        """

        return self.__fetch_document_property("comment")

    @property
    def creation(self):
        """
        When and by whom the document was originally created

        Return:
          `Doc.Action` object (or None if the document has never been saved)
        """

        if not self.id:
            return None
        query = Query("audit_trail t", "t.dt", "u.id", "u.name", "u.fullname")
        query.join("action a", "a.id = t.action")
        query.join("usr u", "u.id = t.usr")
        query.where(query.Condition("t.document", self.id))
        query.where("a.name = 'ADD DOCUMENT'")
        row = query.execute(self.cursor).fetchone()
        if not row:
            raise Exception("No audit trail for document creation")
        return self.Action(row)

    @property
    def cursor(self):
        """
        Give the document object its own cursor
        """

        if not hasattr(self, "_cursor"):
            self._cursor = self.session.conn.cursor()
        return self._cursor

    @property
    def denormalized_xml(self):
        """
        Pass the document's XML through the Fast Denormalization Filter

        Don't denormalize filter, css, or schema docs.

        If filtering fails (as it will if the original XML is malformed)
        return the original XML string.
        """

        if not self.xml:
            return None
        if hasattr(self, "_denormalized_xml") and self._denormalized_xml:
            return self._denormalized_xml
        if self.is_control_type:
            self._denormalized_xml = self.xml
        else:
            try:
                result = self.filter("name:Fast Denormalization Filter")
                self._denormalized_xml = unicode(result.result_tree)
            except:
                self._denormalized_xml = self.xml
        return self._denormalized_xml

    @property
    def doctype(self):
        """
        `Doctype` object  representing the type of the document

        We have to be careful to look in the row for the version if
        the `Doc` object represents a specific version, because the
        document type can change from one version to the next.
        """

        if not hasattr(self, "_doctype"):
            if "doctype" in self.__opts:
                name = self.__opts["doctype"]
                self._doctype = Doctype(self.session, name=name)
            elif not self.id:
                self._doctype = None
            else:
                table = "doc_version" if self.version else "document"
                query = Query(table, "doc_type")
                query.where(query.Condition("id", self.id))
                if self.version:
                    query.where(query.Condition("num", self.version))
                row = query.execute(self.cursor).fetchone()
                if not row:
                    what = "version" if self.version else "document"
                    raise Exception(what + " not found")
                self._doctype = Doctype(self.session, id=row.doc_type)
        return self._doctype

    @doctype.setter
    def doctype(self, value):
        """
        Set the document type according to the caller's document type name
        """

        self._doctype = Doctype(name=value)

    @property
    def eids(self):
        """
        Return the version of the doc which has cdr-eid attributes (if any)
        """

        if hasattr(self, "_eids"):
            return self._eids
        return None

    @property
    def errors(self):
        """
        Sequence of `Error` objects recorded during processing of document
        """

        return self._errors if hasattr(self, "_errors") else []

    @property
    def errors_node(self):
        """
        DOM node representing all of the documents errors/warnings

        Used for reporting errors to clients from the API.
        """

        if not self.errors:
            return None
        node = etree.Element("Errors", count=str(len(self.errors)))
        for error in self.errors:
            node.append(error.to_node())
        return node

    @property
    def first_pub(self):
        """
        Date/time the document was first published if known
        """

        if not self.id:
            return None
        query = Query("document", "first_pub")
        query.where(query.Condition("id", self.id))
        row = query.execute(self.cursor).fetchone()
        date = row.first_pub
        if isinstance(date, datetime.datetime):
            return date.replace(microsecond=0)
        return date

    @property
    def frag_ids(self):
        """
        Return the set of unique IDs for candidate link targets in this doc
        """

        return self._frag_ids if hasattr(self, "_frag_ids") else None

    @property
    def hard_error_count(self):
        """
        Return the count of real errors (ignoring warnings and info)
        """

        count = 0
        for error in self._errors:
            if error.level in (self.LEVEL_ERROR, self.LEVEL_FATAL):
                count += 1
        return count

    @property
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
        row = query.execute(self.cursor).fetchone()
        self._blob_id = row.blob_id if row else None
        return True if self._blob_id else False

    @property
    def has_unversioned_changes(self):
        """
        Determine if the document has saved after the last version
        """

        last_saved = self.last_saved
        if last_saved is None:
            return False
        last_version_date = self.last_version_date
        if not last_version_date:
            return True
        return last_version_date < self.last_saved

    @property
    def highest_fragment_id(self):
        """
        Find the highest automatically assigned link target ID

        These are stored in `cdr:id` attributes using values starting
        with an underscore character followed by one or more decimal
        digits.

        Return:
          integer for the highest target ID assigned to the document
        """

        highest = 0
        if self.root is None:
            return 0
        for node in self.root.xpath("//*[@cdr:id]", namespaces=self.NSMAP):
            cdr_id = node.get(Link.CDR_ID)
            if cdr_id is not None and cdr_id.startswith("_"):
                digits = cdr_id[1:]
                if digits.isdigit():
                    highest = max(highest, int(digits))
        return highest

    @property
    def id(self):
        """
        Unique integer identifier for the CDR document
        """

        if not hasattr(self, "_id"):
            try:
                self._id = self.extract_id(self.__opts.get("id"))
            except:
                self._id = None
        return self._id

    @property
    def is_content_type(self):
        """
        Return True if the document is a non-control type
        """

        if not self.doctype:
            return False
        return not self.is_control_type

    @property
    def is_control_type(self):
        """
        Return True iff the document is a Filter, schema, or css document
        """

        if not self.doctype:
            return False
        return self.doctype.name in ("Filter", "css", "schema")

    @property
    def last_publishable_version(self):
        """
        Integer for the most recently created publishable version, if any
        """

        if not self.id:
            return None
        query = Query("doc_version", "MAX(num) AS n")
        query.where(query.Condition("id", self.id))
        query.where("publishable = 'Y'")
        row = query.execute(self.cursor).fetchone()
        return row.n if row else None

    @property
    def last_saved(self):
        """
        Return the last time the document was saved

        Includes document creation or modification, with or without
        versioning.
        """

        modification = self.modification
        if modification:
            return modification.when
        creation = self.creation
        if creation:
            return creation.when
        return None

    @property
    def last_version(self):
        """
        Integer for the most recently saved version, if any; else None
        """

        if not self.id:
            return None
        query = Query("doc_version", "MAX(num) AS n")
        query.where(query.Condition("id", self.id))
        row = query.execute(self.cursor).fetchone()
        return row.n if row else None

    @property
    def last_version_date(self):
        """
        Date/time when the last version was created, if any; else None
        """

        if not self.id:
            return None
        query = Query("doc_version", "MAX(updated_dt) as dt")
        query.where(query.Condition("id", self.id))
        row = query.execute(self.cursor).fetchone()
        date = row.dt if row else None
        if isinstance(date, datetime.datetime):
            return date.replace(microsecond=0)
        return date

    @property
    def lock(self):
        """
        `Doc.Lock` object if checked out; otherwise None

        Don't cache this value (in case some other process locks
        the document). This means that users of the property should
        assign it to a local variable for efficiency within a block
        of processing over a short period of time.
        """

        if not self.id:
            return None
        fields = "c.dt_out", "u.id", "u.name", "u.fullname"
        query = Query("checkout c", *fields)
        query.join("usr u", "u.id = c.usr")
        query.where(query.Condition("c.id", self.id))
        query.where("c.dt_in IS NULL")
        row = query.execute(self.cursor).fetchone()
        return self.Lock(row) if row else None

    @property
    def modification(self):
        """
        When and by whom the document was last modified

        Return:
          `Doc.Action` object if modification found; otherwise None
        """

        if not self.id:
            return None
        query = Query("audit_trail t", "t.dt", "u.id", "u.name", "u.fullname")
        query.join("action a", "a.id = t.action")
        query.join("usr u", "u.id = t.usr")
        query.where(query.Condition("t.document", self.id))
        query.where("a.name = 'MODIFY DOCUMENT'")
        query.order("t.dt DESC").limit(1)
        row = query.execute(self.cursor).fetchone()
        return self.Action(row) if row else None

    @property
    def publishable(self):
        """
        True if this is a numbered publishable version; else False
        """

        if not self.id or not self.version:
            return None
        query = Query("doc_version", "publishable")
        query.where(query.Condition("id", self.id))
        query.where(query.Condition("num", self.version))
        row = query.execute(self.cursor).fetchone()
        if not row:
            message = "Information for version {} missing".format(self.version)
            raise Exception(message)
        return row.publishable == "Y"

    @property
    def ready_for_review(self):
        """
        True if this is a new document which is ready for review
        """

        query = Query("ready_for_review", "doc_id")
        query.where(query.Condition("doc_id", self.id))
        row = query.execute(self.cursor).fetchone()
        return True if row else False

    @property
    def resolved(self):
        """
        Copy of `self.root` with revision markup applied.
        """

        if self.root is None:
            return None
        return self.__apply_revision_markup()

    @property
    def revision_level(self):
        """
        Integer showing what should be retained by revision markup filtering
        """

        return self.__opts.get("level") or self.DEFAULT_REVISION_LEVEL


    @property
    def root(self):
        """
        Parsed tree for the document's XML
        """

        if not hasattr(self, "_root") or self._root is None:
            try:
                self._root = etree.fromstring(self.xml.encode("utf-8"))
            except:
                self.session.logger.exception("can't parse %r", self.xml)
                self._root = None
        return self._root

    @property
    def session(self):
        """
        `Session` for which this `Doc` object was requested
        """

        return self.__session

    @property
    def title(self):
        """
        String for the title of this version of the document
        """

        return self.__fetch_document_property("title")

    @property
    def val_date(self):
        """
        Date/time this version of the document was last validated
        """

        val_date = self.__fetch_document_property("val_date")
        if isinstance(val_date, datetime.datetime):
            return val_date.replace(microsecond=0)
        return val_date

    @property
    def val_status(self):
        """
        'V' (valid), 'I' (invalid), 'Y' (unvalidated), or 'M' (malformed)
        """

        if hasattr(self, "_val_status"):
            return self._val_status
        return self.__fetch_document_property("val_status")

    @property
    def valid(self):
        """
        Return True iff the document passed all validation tests
        """

        return self.val_status == self.VALID

    @property
    def version(self):
        """
        Integer for specific version for all_doc_versions row (or None)
        """

        #self.session.logger.info("@version: __opts = %s", self.__opts)
        # Pull out the version-related options passed into the constructor.
        version = self.__opts.get("version")
        cutoff = self.__opts.get("before")

        # If we've done this before, the version integer has been cached
        if not hasattr(self, "_version") or self._version is None:

            # If the document hasn't been saved (no ID) it has no version.
            if not self.id:
                self._version = None

            # Look up any "before this date" versions.
            elif cutoff:
                lastp = str(version).startswith("lastp")
                self._version = self.__get_version_before(cutoff, lastp)

            # See if this is an object for the current working document.
            elif not version:
                self._version = None

            # If the constructor already got an integer, our work is done.
            elif not isinstance(version, basestring):
                version = int(version)
                self._version = version if version > 0 else None

            # At this point we assume version is a string; normalize it.
            else:
                try:
                    version = version.lower()
                except:
                    raise Exception("invalid version {!r}".format(version))

                # Current is an alias for non-versioned copy.
                if version in ("current", "none"):
                    self._version = None

                # We have properties for last (published) versions.
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

                # Version labels have never been used, but you never know!
                elif version.startswith("label "):
                    tokens = version.split(" ", 1)
                    if len(tokens) != 2:
                        error = "missing token for version specifier"
                        raise Exception(error)
                    prefix, label = tokens
                    self._version = self.__get_labeled_version(label)

                # Last chance: an integer string.
                else:
                    try:
                        self._version = int(version)
                    except:
                        error = "invalid version spec {}".format(version)
                        self.session.logger.exception(error)
                        raise Exception(error)

        # Return the cached version value.
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
            row = query.execute(self.cursor).fetchone()
            if not row:
                raise Exception("no xml found")
            self._xml = row.xml
        return self._xml

    @xml.setter
    def xml(self, value):
        """
        Assign a new value to the `xml` property, coercing to Unicode

        Invalidate any parse trees.

        Pass:
          value - new property value
        """

        self._xml = value
        if self._xml and not isinstance(self._xml, unicode):
            self._xml = self._xml.decode("utf-8")
        self._root = self._denormalized_xml = self._resolved = None

    # ------------------------------------------------------------------
    # PUBLIC METHODS START HERE.
    # ------------------------------------------------------------------

    def add_external_mapping(self, usage, value, **opts):
        """
        Insert a row into the external mapping table

        This is used by the XMetaL client when the user wants to
        register a variant phrase found in the document being edited
        for a glossary term.

        Required positional arguments:
          usage - string representing the context for the mapping
                  (for example, 'Spanish GlossaryTerm Phrases')
          value - string for the value to be mapped to this document

        Optional keyword arguments:
          bogus - if "Y" value does not really map to any document,
                  but is instead a known invalid value found in
                  (usually imported) data
          mappable - if "N" the value is not an actual field value;
                     often it's a comment explaining why no value
                     which could be mapped to a CDR doc is available

        Return:
          integer primary key for newly inserted mapping table row
        """

        # Make sure we have the required arguments.
        if not usage:
            raise Exception("Missing usage name")
        if not value:
            raise Exception("Missing mapping value")

        # Get values for the optional arguments.
        bogus = (opts.get("bogus") or "N").upper()
        mappable = (opts.get("mappable") or "Y").upper()
        assert bogus in "YN", "Bogus 'bogus' option"
        assert mappable in "YN", "Invalidate 'mappable' options"

        # Find the usage ID and action name.
        query = Query("external_map_usage u", "u.id", "a.name")
        query.join("action a", "a.id = u.auth_action")
        query.where(query.Condition("u.name", usage))
        row = query.execute(self.cursor).fetchone()
        if not row:
            raise Exception("Unknown usage {!r}".format(usage))
        usage_id, action = row

        # Make sure the user is allowed to add a row for this usage.
        if not self.session.can_do(action):
            message = "User not allowed to add {} mappings".format(usage)
            raise Exception(message)

        # Add the new mapping row.
        fields = dict(
            usage=usage_id,
            value=str(value),
            doc_id=self.id,
            usr=self.session.user_id,
            last_mod=datetime.datetime.now().replace(microsecond=0),
            bogus=bogus,
            mappable=mappable
        )
        names = sorted(fields)
        args = ", ".join(names), ", ".join(["?"] * len(names))
        values = tuple([fields[name] for name in names])
        insert = "INSERT INTO external_map ({}) VALUES ({})".format(*args)
        self.cursor.execute(insert, values)
        self.session.conn.commit()
        self.cursor.execute("SELECT @@IDENTITY AS id")
        return self.cursor.fetchone().id

    def add_error(self, message, location=None, **opts):
        """
        Add an `Error` object to our list

        This is public because `Link` objects call it.

        Required positional argument:
          message - description of the problem

        Optional keyword arguments:
          location - where the error was found (None if unavailable)
          type - string for type of error (default 'validation')
          level - how serious is the problem (default 'error')
        """

        # Make sure we've got something to append to.
        if not hasattr(self, "_errors"):
            self._errors = []

        # Make the error messages refer to the real attribute names.
        for ncname in ("id", "href", "ref", "xref"):
            message = message.replace("cdr-" + ncname, "cdr:" + ncname)

        # Create the `Error` object and put it in our list.
        self._errors.append(self.Error(message, location, **opts))

    def check_in(self, **opts):
        """
        Release the lock on the document so others can edit it

        Public wrapper for __check_in(), committing changes to the database

        Optional keyword arguments:
          force - if True, try to check in even if locked by another account
          comment - optional string to update comment (to NULL if empty)
          abandon - if True, don't save unversioned changes as a new version
          publishable - if True, mark version publishable if we create one
        """

        self.session.logger.info("checking in %s", self.cdr_id)
        self.__check_in(**opts)
        self.session.conn.commit()
        self.session.logger.info("checked in %s", self.cdr_id)

    def check_out(self, **opts):
        """
        Lock the document for editing

        Public wrapper for __check_out(), commiting changes to the database

        Optional keyword arguments:
          force - if True, steal the lock if necessary (and allowed)
          comment - optional string for the `checkout.comment` column
        """

        self.__check_out(**opts)
        self.session.conn.commit()

    def delete(self, **opts):
        """
        Mark the document as deleted

        We don't actually remove the document or any of its versions
        from the repository. We just set the `active_status` column
        to 'D' so it drops out of the `document` view.

        Optional keyword arguments:
          validate - if True, make sure nothing links to the document
          reason - string to be recorded in the audit trail
        """

        # Make sure the audit trail records don't step on each other.
        self.__audit_trail_delay()

        # Start with a clean slate.
        self._errors = []

        # Make sure the user can delete documents of this type.
        if not self.session.can_do("DELETE DOCUMENT", self.doctype.name):
            message = "User not authorized to delete {} documents"
            raise Exception(message.format(self.doctype.name))

        # Make sure the document isn't published.
        query = Query("pub_proc_cg", "id")
        query.where(query.Condition("id", self.id))
        row = query.execute(self.cursor).fetchone()
        if row:
            message = "Cannot delete published doc {}".format(self.cdr_id)
            raise Exception(message)

        # Make sure it's not in the external mapping table.
        query = Query("external_map", "COUNT(*) AS n")
        query.where(query.Condition("doc_id", self.id))
        if query.execute(self.cursor).fetchone().n > 0:
            message = "Cannot delete {} which is in the external mapping table"
            raise Exception(message.format(self.cdr_id))

        # Make sure someone else doesn't have it locked.
        reason = opts.get("reason")
        lock = self.lock
        if lock:
            if lock.locker.id != self.session.user_id:
                message = "Document {} is checked out by another user"
                raise Exception(message.format(self.cdr_id))
            self.__check_in(abandon=True, comment=reason)

        # Make sure we back out any pending transactions if we fail.
        try:

            # Take care of any links to this document.
            if self.__delete_incoming_links(**opts):

                # We got the green light to proceed with the deletion.
                update = "UPDATE document SET active_status = 'D' WHERE id = ?"
                for table in "query_term", "query_term_pub":
                    delete = "DELETE FROM {} WHERE doc_id = ?".format(table)
                    self.cursor.execute(delete, (self.id,))
                self.cursor.execute(update, (self.id,))
                self.__audit_action("Doc.delete", "DELETE DOCUMENT", reason)
                self.session.conn.commit()
        except:
            self.session.logger.exception("Deletion failed")
            self.cursor.execute("SELECT @@TRANCOUNT AS tc")
            if self.cursor.fetchone().tc:
                self.cursor.execute("ROLLBACK TRANSACTION")
            raise

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
          doc - parsed document to filter instead of self.root

        Return:
          `FilterResult` object if `output` option is not False;
          otherwise, return the sequence of message strings emitted
          by the XSL/T engine
        """

        message = "ctl option not supported by filter(); use doc instead"
        assert not opts.get("ctl"), message
        parms = opts.get("parms") or {}
        doc = opts.get("doc")
        if doc is None:
            doc = self.root
        assert doc is not None, "no document to filter"
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
                doc = result.result_tree
                for entry in result.error_log:
                    messages.append(entry.message)
            if opts.get("output", True):
                return self.FilterResult(doc, messages=messages)
            return messages
        except:
            self.session.logger.exception("filter() failure")
            raise
        finally:
            Resolver.local.docs.pop()

    def get_filter(self, doc_id, **opts):
        """
        Fetch a possibly cached filter document

        This is a public method, because the `Resolver` class below
        needs it.

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
        query = Query("good_filters", "xml")
        query.where(query.Condition("id", doc.id))
        row = query.execute(self.cursor).fetchone()
        if not row:
            raise Exception("filter {} not found".format(doc.cdr_id))
        root = etree.fromstring(row.xml.encode("utf-8"))
        # TODO - END TEMPORARY CODE
        for name in ("import", "include"):
            qname = Doc.qname(name, Filter.NS)
            for node in root.iter(qname):
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

    def get_tree(self, depth=1):
        """
        Fetch parents and children of this Term document

        Pass:
          depth - number of levels to descend for children (default=1)

        Return:
          object containing parent-child relationships and term names
        """

        if not self.session.can_do("GET TREE"):
            raise Exception("GET TREE action not authorized for this user")
        class Tree:
            def __init__(self):
                self.relationships, self.names = list(), dict()
            class Relationship:
                def __init__(self, parent, child):
                    self.parent, self.child = parent, child
        tree = Tree()
        self.cursor.callproc("cdr_get_term_tree", (self.id, depth))
        for child, parent in self.cursor.fetchall():
            tree.relationships.append(Tree.Relationship(parent, child))
        if not self.cursor.nextset():
            raise Exception("Failure retrieving Term data")
        for term_id, term_name in self.cursor.fetchall():
            tree.names[term_id] = term_name
        return tree

    def label(self, label):
        """
        Apply a label to a specific version of this document

        Pass:
          label - string for this label's name
        """

        assert self.version, "Missing version for label"
        query = Query("version_label", "id")
        query.where(query.Condition("name", label))
        row = query.execute(self.cursor).fetchone()
        if not row:
            raise Exception("Unable to find label {!r}".format(label))
        names = "label, document, num"
        values = row.id, self.id, self.version
        insert = "INSERT INTO doc_version_label ({}) VALUES (?, ?, ?)"
        self.cursor.execute(insert.format(names), values)
        self.session.conn.commit()

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
        cdr_doc.set("Type", self.doctype.name)
        if self.cdr_id:
            cdr_doc.set("Id", self.cdr_id)
        cdr_doc.append(self.legacy_doc_control(**opts))
        if opts.get("get_xml"):
            if opts.get("locators"):
                xml = etree.tostring(self.eids, encoding="utf-8")
                xml = xml.decode("utf-8")
            elif opts.get("denormalize"):
                xml = self.denormalized_xml
            else:
                xml = self.xml
            etree.SubElement(cdr_doc, "CdrDocXml").text = etree.CDATA(xml)
        if opts.get("get_blob") and self.has_blob:
            blob = etree.SubElement(cdr_doc, "CdrDocBlob", encoding="base64")
            blob.text = base64encode(self.blob).decode("ascii")
        return cdr_doc

    def legacy_doc_control(self, **opts):
        """
        Create a CdrDocCtl DOM tree

        Used for assembling part of a CdrDoc DOM tree for the CdrGetDoc
        command. Also used independently of a CdrDoc document, and with
        additional child elements, for filtering callbacks.

        Public method so `Resolver` class can use it.

        Pass:
          brief - if True, just include DocTitle and DocActiveStatus
          filtering - if True, include extra Create, Modify, and FirstPub
                      blocks; if False, mark all children as read-only

        Return:
          `etree.Element` object with nested children
        """

        filtering = opts.get("filtering", False)
        modified = modifier = None
        modification = self.modification
        if modification:
            modified = self.make_xml_date_string(self.modification.when)
            modifier = modification.user.name
        val_date = self.make_xml_date_string(self.val_date)
        doc_control = etree.Element("CdrDocCtl")
        control_info = [
            ("DocTitle", self.title),
            ("DocActiveStatus", self.active_status),
            ("DocValStatus", self.val_status),
            ("DocValDate", val_date),
            ("DocVersion", self.version),
            ("DocModified", modified),
            ("DocModifier", modifier),
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
            creation = self.creation
            if creation:
                created = self.make_xml_date_string(creation.when)
                creator = creation.user.name
                wrapper = etree.SubElement(doc_control, "Create")
                etree.SubElement(wrapper, "Date").text = created
                etree.SubElement(wrapper, "User").text = creator
                if modification: # set above
                    wrapper = etree.SubElement(doc_control, "Modify")
                    etree.SubElement(wrapper, "Date").text = modified
                    etree.SubElement(wrapper, "User").text = modifier
            first_pub = self.make_xml_date_string(self.first_pub)
            if first_pub:
                wrapper = etree.SubElement(doc_control, "FirstPub")
                etree.SubElement(wrapper, "Date").text = first_pub
        return doc_control

    def legacy_validation_response(self, with_locators=False):
        """
        Wrap the validation command's response in a DOM node

        We do this here because for some reason the cdr module's
        `valDoc()` function returns the serialized response from
        the server, so we need this functionality even if we are
        invoking `valDoc()` locally instead of across the network.

        Pass:
           with_locators - if True, include the serialized document
                           with attributes which the client can use
                           to find the reported errors
        """

        active_status = self.active_status or ""
        response = etree.Element("CdrValidateDocResp", nsmap=Doc.NSMAP)
        etree.SubElement(response, "DocId").text = self.cdr_id or ""
        etree.SubElement(response, "DocActiveStatus").text = active_status
        errors = self.errors_node
        if errors is not None:
            response.append(errors)
            if with_locators:
                response.append(self.legacy_doc(get_xml=True, locators=True))
        return response

    def link_report(self):
        query = Query("link_net n", "n.source_doc", "d.title", "n.target_frag")
        query.join("document d", "d.id = n.source_doc")
        query.where(query.Condition("n.target_doc", self.id))
        links = []
        pattern = "Document {:d}: ({}) links to this document"
        for row in query.execute(self.cursor).fetchall():
            link = pattern.format(row.source_doc, row.title)
            if row.target_frag:
                link += " Fragment({})".format(row.target_frag)
            links.append(link)
        return links

    def list_versions(self, limit=None):
        fields = "num AS number", "dt AS saved", "comment"
        query = Query("doc_version", *fields).order("num DESC")
        query.where(query.Condition("id", self.id))
        if limit is not None:
            query.limit(limit)
        return list(query.execute(self.cursor).fetchall())

    def reindex(self):
        """
        Repopulate the search support tables for this document
        """

        # Make sure the document is in the repository.
        if not self.id:
            raise Exception("reindex(): missing document id")

        # Make sure the object we have represents the latest XML.
        doc = self
        last_pub_ver = doc.last_publishable_version
        last_saved = doc.last_saved
        last_ver = doc.last_version
        last_ver_date = doc.last_version_date
        if doc.version:
            if doc.version < last_ver or last_ver_date < last_saved:
                doc = Doc(self.session, id=doc.id)

        # Find out which tables to populate with this XML
        tables = ["query_term"]
        if last_pub_ver:
            if last_pub_ver == last_ver and last_saved == last_ver_date:
                tables.append("query_term_pub")

        # Make sure we roll back everything if we fail anything.
        try:
            doc.update_query_terms(tables=tables)
            if last_pub_ver and "query_term_pub" not in tables:
                doc = Doc(self.session, id=doc.id, version=last_pub_ver)
                doc.update_query_terms(tables=["query_term_pub"])
                self.session.conn.commit()
        except Exception as e:
            self.session.logger.exception("Reindex failed")
            self.cursor.execute("SELECT @@TRANCOUNT AS tc")
            if self.cursor.fetchone().tc:
                self.cursor.execute("ROLLBACK TRANSACTION")
            raise

    def save(self, **opts):
        """
        Store the new or updated document

        Make sure the transaction is rolled back if anything goes wrong.

        Optional keyword arguments:
          version - if True, create a version
          publishable - if True, create a publishable version
          val_types - which validations to perform ("schema" and/or "links")
          set_links - if False, suppress population of link tables
          locators - if True, include cdr-eid attributes when echoing doc
          del_blobs - if True, delete all blobs associated with the document
          needs_review - if True, try to add row to ready_for_review table
          active_status - optional override of all_docs.active_status value
          comment - for comment column of all_docs or all_versions table
          reason - for comment column of audit_trail table
          unlock - if True, check in the document; else leave it locked
          title - fallback document title if no title filter exists for
                  this document type; if a title filter does exist, and
                  it produces a non-empty string, this option is ignored
        """

        try:
            self.__audit_trail_delay()
            self.__save(**opts)
            self.session.conn.commit()
            self.cursor.close()
            self._cursor = None
        except:
            self.session.logger.exception("Doc.save() failure")
            self.cursor.execute("SELECT @@TRANCOUNT AS tc")
            if self.cursor.fetchone().tc:
                self.cursor.execute("ROLLBACK TRANSACTION")
            raise

    def set_status(self, status, **opts):
        """
        Modify the `all_docs.active_status` value for the document

        Pass:
          status - "A" (active) or "I" (inactive); required
          comment - optional keyword argument for string describing the change
        """

        if not self.doctype:
            raise Exception("Document not found")
        if not self.session.can_do("PUBLISH DOCUMENT", self.doctype.name):
            message = "User not authorized to change status of {} documents"
            raise Exception(message.format(self.doctype.name))
        valid = self.ACTIVE, self.INACTIVE
        if status not in valid:
            raise Exception("Status must be {} or {}".format(*valid))
        args = self.active_status, status
        self.session.logger.info("Old status=%r new status=%r", *args)
        if status != self.active_status:
            try:
                self.__audit_trail_delay()
                self.__set_status(status, **opts)
                self.session.conn.commit()
                self._active_status = status
                self.session.logger.info("New status committed")
            except:
                self.session.logger.exception("Doc.set_status() failure")
                self.cursor.execute("SELECT @@TRANCOUNT AS tc")
                if self.cursor.fetchone().tc:
                    self.cursor.execute("ROLLBACK TRANSACTION")
                raise

    def unlabel(self, label):
        """
        Apply a label to a specific version of this document

        Pass:
          label - string for this label's name
        """

        query = Query("version_label", "id")
        query.where(query.Condition("name", label))
        row = query.execute(self.cursor).fetchone()
        if not row:
            raise Exception("Unable to find label {!r}".format(label))
        table = "doc_version_label"
        delete = "DELETE FROM {} WHERE document = ? AND label = ?"
        self.cursor.execute(delete.format(table), (self.id, row.id))
        self.session.conn.commit()

    def update_title(self):
        """
        Regenerate the document's title using the document type's title filter

        Return:
          True if the document's title was changed; otherwise False
        """

        if self.id and self.title is not None:
            title = self.__create_title()
            #print("created title is {} existing title is {}".format(title, self.title))
            if title is not None and self.title != title:
                update = "UPDATE all_docs SET title = ? WHERE id = ?"
                self.cursor.execute(update, (title, self.id))
                self.session.conn.commit()
                self._title = title
                return True
        return False

    def update_query_terms(self, **opts):
        """
        Populate the query support tables with values from the document

        Optional keyword argument:
          tables - set of strings identifying which index table(s) to
                   update (`query_term` and/or `query_term_def`); default
                   is both tables
        """

        # We don't index control documents or documents with malformed XML.
        if not self.is_content_type or self.root is None:
            return

        # Nor do we index unsaved documents or documents with no doctype.
        if not self.id or not self.doctype.name:
            return

        # Find out which table(s) we're updating.
        tables = opts.get("tables", ["query_term", "query_term_def"])
        if not tables:
            return

        # Find out which elements and attributes get indexed (`paths`).
        absolute_path = "path LIKE '/{}/%'".format(self.doctype.name)
        relative_path = "path LIKE '//%'"
        query = Query("query_term_def", "path")
        query.where(query.Or(absolute_path, relative_path))
        rows = query.execute(self.cursor).fetchall()
        paths = set([row.path for row in rows])

        # Collect the indexable values and store them.
        terms = set()
        self.__collect_query_terms(self.resolved, terms, paths)
        for table in tables:
            self.__store_query_terms(terms, table=table)

    def validate(self, **opts):
        """
        Determine whether the document conforms to the rules for its type

        External wrapper for __validate(), committing changes to database.

        Optional keyword arguments:
          types - sequence of strings indication which validations
                  to apply to the document ("schema" and/or "links")
          locators - Boolean indicating whether to include attributes
                     to be used for finding the parts of the document
                     with errors in the client; default is False
          store - "always": record validation results in the database (default)
                  "never": don't touch the database
                  "valid": only update val_status if full validation passes
                  (only applicable if the document exists in the repository);
          level - which level of revision markup to keep for validation
                  (default is revision marked as ready to be published)
        """

        # Hand off the work to the private validation method.
        self._errors = []
        try:
            self.__validate(**opts)

            # Find out if there are changes to the database; if so, commit them.
            self.cursor.execute("SELECT @@TRANCOUNT AS tc")
            if self.cursor.fetchone().tc:
                self.session.conn.commit()
        except:
            self.session.logger.exception("Validation failed")
            self.cursor.execute("SELECT @@TRANCOUNT AS tc")
            if self.cursor.fetchone().tc:
                self.cursor.execute("ROLLBACK TRANSACTION")
            raise

    # ------------------------------------------------------------------
    # PRIVATE METHODS START HERE.
    # ------------------------------------------------------------------

    def __apply_filter(self, filter_xml, doc, parser=None, **parms):
        """
        Transform the document using an XSL/T filter

        Pass:
          filter_xml - utf-8 bytes for serialized XSL/T document
          doc - parse tree object for document to be filtered
          parser - optional object capable of resolving URLs
          parms - dictionary of  parameters to be passed to the filtering
                  engine (parameter values indexed by parameter names)

        Return:
          `Doc.FilterResult` object
        """

        #print("filter_xml={}".format(filter_xml))
        transform = etree.XSLT(etree.fromstring(filter_xml, parser))
        doc = transform(doc, **parms)
        return self.FilterResult(doc, error_log=transform.error_log)

    def __apply_revision_markup(self, root=None, level=None):
        """
        Resolve revision markup to requested level

        Pass:
          root - parsed document to be filtered
          level - integer (1-3) for what markup to apply/discard
        """

        filter = "name:Revision Markup Filter"
        parms = dict(useLevel=str(level or self.revision_level))
        doc = root or self.root
        filter_opts = dict(parms=parms, doc=doc)
        result = self.filter(filter, **filter_opts)
        return result.result_tree.getroot()

    def __assemble_filters(self, *filter_specs, **opts):
        """
        Get the XSL/T filter needed for a document transformation job

        Pass:
          filter_spec[, filter_spec ...] - strings identifying filters:
                                           "set:..." for named filter set
                                           "name:..." for named filters
                                           any other string is a filter ID

        Optional keyword arguments:
          version - versions to fetch (e.g., 'lastp')
          before - restrict versions to those created before this date/time

        Return:
          sequence of `Filter` objects, to be applied in order
        """
        version = opts.get("version")
        before = opts.get("before")
        opts = dict(version=version, before=before)
        filters = []
        #print("filter_specs={}".format(filter_specs))
        for spec in filter_specs:
            spec = str(spec)
            if spec.startswith("set:"):
                name = spec.split(":", 1)[1]
                filters += self.__get_filter_set(name, **opts)
            else:
                if spec.startswith("name:"):
                    name = spec.split(":", 1)[1]
                    doc_id = Doc.id_from_title(name, self.cursor)
                else:
                    doc_id = spec
                #print("doc_id={} opts={}".format(doc_id, opts))
                filters.append(self.get_filter(doc_id, **opts))
        return filters

    def __audit_action(self, program, action, comment=None):
        """
        Keep a record of who did what with this document

        For example, creating or updating a document, blocking it.

        Discard sub-second precision in the date/time value, to avoid
        a bug in adodbapi.

        Pass:
          program - name of the calling software (arbitrary strings)
          action - string naming the action being recorded (matching
                   `name` column values in the `action` table)
          comment - optional value to be stored in the `comment` column
                    (otherwise NULL)

        Return:
          `datetime` value stored in the `audit_trail.dt` column
        """

        # Look up the primary key matching the action's name.
        query = Query("action", "id")
        query.where(query.Condition("name", action))
        row = query.execute(self.cursor).fetchone()
        if not row:
            raise Exception("Invalid action {!r}".format(action))

        # Prepare and execute the INSERT statement.
        when = datetime.datetime.now().replace(microsecond=0)
        values = self.id, self.session.user_id, row.id, program, comment, when
        fields = "document", "usr", "action", "program", "comment", "dt"
        args = ", ".join(fields), ", ".join(["?"] * len(fields))
        insert = "INSERT INTO audit_trail ({}) VALUES ({})".format(*args)
        self.cursor.execute(insert, values)
        return when

    def __audit_added_action(self, action, when):
        """
        Record a secondary action performed in connection with a primary action

        These actions go in a separate table, precariously linked to the
        `audit_trail` table with a datetime value (yes, I know).

        Pass:
          action - string naming the action being recorded (matching
                   `name` column values in the `action` table)
          when - `datetime` value for the `dt` column
        """

        # Look up the primary key matching the action's name.
        query = Query("action", "id")
        query.where(query.Condition("name", action))
        row = query.execute(self.cursor).fetchone()
        if not row:
            raise Exception("Invalid action {!r}".format(action))
        action_id = row.id

        # Prepare and execute the INSERT statement for the new row.
        values = self.id, when, action_id
        fields = "document", "dt", "action"
        insert = "INSERT INTO audit_trail_added_action ({}) VALUES (?, ?, ?)"
        insert = insert.format(", ".join(fields))
        self.cursor.execute(insert, values)

    def __audit_trail_delay(self):
        if self.id:
            query = Query("audit_trail", "MAX(dt) AS dt")
            query.where(query.Condition("document", self.id))
            last = query.execute(self.cursor).fetchone().dt
            now = datetime.datetime.now().replace(microsecond=0)
            logged = False
            while now == last:
                if not logged:
                    message = "{}: audit trail delay".format(self.cdr_id)
                    self.session.logger.warning(message)
                    logged = True
                time.sleep(.1)
                now = datetime.datetime.now().replace(microsecond=0)

    def __check_in(self, **opts):
        """
        Check in the locked document.

        Optional keyword arguments:
          force - if True, try to check in even if locked by another account
          comment - optional string to update comment (to NULL if empty)
          abandon - if True, don't save unversioned changes as a new version
          publishable - if True, mark version publishable if we create one
        """

        # Make sure there's a lock to release.
        lock = self.lock
        if lock is None:
            raise Exception("Document is not checked out")

        # See if the document is locked by another account.
        lock_broken = False
        if lock.locker.id != self.session.user_id:
            if opts.get("force"):
                if not self.session.can_do("FORCE CHECKOUT", doctype):
                    raise Exception(str(lock))
                lock_broken = True
            else:
                raise Exception(str(lock))

        # See if we're going to need to create a new version.
        if self.has_unversioned_changes and not opts.get("abandon"):
            version = self.last_version
            need_new_version = True
        else:
            need_new_version = False
            version = None

        # Clear out all the locks for the document.
        self.__audit_trail_delay()
        update = "UPDATE checkout SET dt_in = ?, version = ?"
        when = datetime.datetime.now().replace(microsecond=0)
        values = when, version, self.id
        comment = opts.get("comment")
        if comment is not None:
            update += ", comment = ?"
            values = when, version, comment or None, self.id
        update += " WHERE id = ? AND dt_in IS NULL"
        self.cursor.execute(update, values)

        # Save any unversioned changes unless instructed otherwise.
        if need_new_version:
            self.__create_version(publishable=opts.get("publishable"))

        # If we broke someone else's lock, audit that information.
        if lock_broken:
            self.__audit_action("Doc.check_in", "UNLOCK", comment)

    def __check_out(self, **opts):
        """
        Add a row to the `checkout` table for this document

        Optional keyword arguments:
          force - if True, steal the lock if necessary (and allowed)
          comment - optional string for the `checkout.comment` column
        """

        # Make sure the account has sufficient permissions.
        doctype = self.doctype.name
        if not self.session.can_do("MODIFY DOCUMENT", doctype):
            raise Exception("User not authorized to modify document")

        # See if the document is already locked.
        lock = self.lock
        if lock:
            if lock.locker.id == self.session.user_id:
                return
            if opts.get("force"):
                if not self.session.can_do("FORCE CHECKOUT", doctype):
                    raise Exception("User not authorized to force checkout")
                self.__check_in(abandon=True, force=True)
            else:
                raise Exception(str(lock))

        # Insert a row into the `checkout` table.
        when = datetime.datetime.now().replace(microsecond=0)
        values = self.id, when, self.session.user_id, opts.get("comment")
        fields = "id", "dt_out", "usr", "comment"
        template = "INSERT INTO checkout ({}) VALUES (?, ?, ?, ?)"
        insert = template.format(", ".join(fields))
        self.cursor.execute(insert, values)

    def __check_save_permissions(self, **opts):
        """
        Make sure the user can perform this save action

        As a side effect, make a note of any additional audit actions to
        be recorded (in self._extra_audit_actions).

        Optional keyword arguments:
          publishable - if True, create a publishable version
          val_types - which validations to perform ("schema" and/or "links")
          set_links - if True we update the link_net table
          title - fallback title in case doctype has no title filter
          active_status - optional override for `active_status` column value
        """

        # Make sure the document is locked if it already exists.
        if self.id:
            query = Query("usr u", "u.id", "u.name", "u.fullname", "c.dt_out")
            query.join("checkout c", "c.usr = u.id")
            query.where(query.Condition("c.id", self.id))
            query.where("c.dt_in IS NULL")
            row = query.execute(self.cursor).fetchone()
            if row:
                user_id, name, fullname, checked_out = row
                if user_id != self.session.user_id:
                    args = self.session.user_name, name, fullname, checked_out
                    message = "User {} cannot check-in document checked out "
                    message += "by user {} ({}) at {}"
                    message = message.format(*args)
                    raise Exception(message)
            else:
                raise Exception("Document is not locked")

        # Most fundamental requirement is that we have a document type.
        if not self.doctype:
            raise Exception("no document type specified")
        doctype = self.doctype.name

        # Check the basic add/modify permission for this document type.
        action = "MODIFY" if self.id else "ADD"
        if not self.session.can_do("{} DOCUMENT".format(action), doctype):
            args = action.lower(), doctype
            message = "user not authorized to {} {} documents".format(*args)
            raise Exception(message)

        # Make sure revision markup filtering is done correctly.
        if self.revision_level != self.DEFAULT_REVISION_LEVEL:
            message = "Save action cannot override default revision filtering"
            raise Exception(message)

        # Detect contradictory instructions about link table processing
        if opts.get("set_links") == False and "links" in val_types:
            raise Exception("Cannot validate links without setting them")

        # Make sure full validation performed if creating a publishable verion.
        if opts.get("publishable"):
            val_types = opts.get("val_types") or []
            if not self.last_publishable_version:
                if not self.session.can_do("PUBLISH DOCUMENT", doctype):
                    message = "user not authorized to create first pub version"
                    raise Exception(message)
            if "schema" not in val_types or "links" not in val_types:
                raise Exception("publishable version requires full validation")

        # If this is a control document, make sure it has a unique title.
        if self.is_control_type:
            title = self.__create_title() or opts.get("title")
            if not title:
                raise Exception("document has no title")
            query = Query("document", "COUNT(*) AS n")
            query.where(query.Condition("title", title))
            if self.id:
                query.where(query.Condition("id", self.id, "<>"))
            row = query.execute(self.cursor).fetchone()
            if row.n:
                raise Exception("title {!r} already exists".format(title))

        # If the active_status is being changed, make sure that's allowed.
        active_status_change = None
        if self.id:
            query = Query("all_docs", "active_status")
            query.where(query.Condition("id", self.id))
            row = query.execute(self.cursor).fetchone()
            if not row:
                raise Exception("document {} not found".format(self.cdr_id))
            active_status = row.active_status
        else:
            active_status = "A"
        new_active_status = opts.get("active_status", active_status)
        if "D" in (active_status, new_active_status):
            raise Exception("can't save deleted document")
        message = "Invalid active_status value {!r}".format(new_active_status)
        assert new_active_status in ("A", "I"), message
        if new_active_status != active_status:
            action = "block" if active_status == "A" else "unblock"
            message = "user not authorized to {} document".format(action)
            if not self.session.can_do("PUBLISH DOCUMENT", doctype):
                raise Exception(message)
            active_status_change = "{} DOCUMENT".format(action.upper())

        # Inform caller know about any additional audit actions to be recorded.
        return active_status_change

    def __collect_links(self, doc):
        """
        Find all of the internal linking elements in the document

        'Internal' in this context means 'linking to a CDR document'
        as opposed to 'linking to a URL outside the CDR' (can be
        a link within the same document, but that's not a requirement).

        As a side effect, also records (@property `frag_ids`) the unique
        identifiers for all of the document's elements which can be
        link targets themselves.

        Pass:
          doc - parsed (and possibly filtered) document from which to
                collect links

        Return:
          sequence of `Link` objects
        """

        # Start with a clean slate.
        self._frag_ids = set()
        links = []
        unique_links = set()

        # Walk through all of the element nodes in the document.
        for node in doc.iter("*"):
            link = Link(self, node)

            # We're only returning links to CDR documents.
            if link.internal:
                links.append(link)
                link.store = link.key not in unique_links
                if link.store:
                    unique_links.add(link.key)
                if link.nlink_attrs > 1:
                    message = "Can only have one link from a single element"
                    self.add_error(message, link.eid, type=self.LINK)

            # Also populate the `frag_ids` property.
            if link.id:
                if link.id in self._frag_ids:
                    message = "cdr:id {!r} used more than once".format(link.id)
                    self.add_error(message, link.eid, type=self.LINK)
                else:
                    self._frag_ids.add(link.id)

        # Return the sequence of `Link` objects we found for 'internal' links.
        return links

    def __collect_query_terms(self, node, terms, paths, parent="", loc=""):
        """
        Recursively collect all of the document's indexable values

        Pass:
          node - reference to element to consider for indexing
          terms - set of path/location/value tuples we're assembling
          paths - set of string controlling which values we index
          parent - xpath string for current node's parent
          loc - string containing concatenated 4-digit hex numbers
                showing the zero-based position of each node in
                this element's path relative to that node's siblings,
                with the position of the root node of the document
                represented by an empty string (since that node
                has no siblings, its position is known)
        """

        # Find out where we are and check for infinite recursion.
        if len(loc) > self.MAX_LOCATION_LENGTH:
            raise Exception("Indexing beyond max allowed depth")
        path = "{}/{}".format(parent, node.tag)
        wild = "//{}".format(node.tag)
        max_length = self.MAX_SQLSERVER_INDEX_SIZE
        error_opts = dict(type=self.OTHER, level=self.LEVEL_WARNING)
        template = "Only {} characters of {} will be indexed"

        # If the element's text value should be indexed, add it.
        if path in paths or wild in paths:
            value = Doc.get_text(node, "")
            if len(value) > max_length:
                value = value[:max_length]
                message = template.format(max_length, path)
                self.add_error(message, **error_opts)
            terms.add((path, loc, value))

        # Do the same thing for each of the element's attributes.
        for name in node.attrib:
            full_attr_path = "{}/@{}".format(path, name)
            wild_attr_path = "//@{}".format(name)
            if full_attr_path in paths or wild_attr_path in paths:
                value = node.attrib[name]
                if len(value) > max_length:
                    value = value[:max_length]
                    message = template.format(max_length, full_attr_path)
                    self.add_error(message, **error_opts)
                terms.add((full_attr_path, loc, value))

        # Recursively check each of this element's child elements.
        position = 0
        for child in node.findall("*"):
            child_loc = loc + self.HEX_INDEX.format(position)
            position += 1
            self.__collect_query_terms(child, terms, paths, path, child_loc)

    def __create_title(self):
        """
        Pass the document's XML through the title filter for its doctype

        Return:
          Unicode string from the filter's results or None
        """

        query = Query("doc_type", "title_filter")
        query.where(query.Condition("id", self.doctype.id))
        row = query.execute(self.cursor).fetchone()
        try:
            opts = dict(doc=self.resolved)
            return unicode(self.filter(row.title_filter, **opts).result_tree)
        except:
            self.session.logger.exception("__create_title() failure")
            return None

    def __create_version(self, **opts):
        """
        Add a row to the all_doc_versions table

        Note that we're storing two datetime values in the table row.
        One (`dt`) represents when the row was created. The other
        (`updated_dt`) matches the `dt` value in the audit table row
        which was created when we updated the values in the `all_docs`
        table (or created the row, for a new document). I realize
        that's a squirrelly way to link rows from two tables, but
        that's how Mike did it, and a bunch of software assumes that's
        how it works.

        Optional keyword argument:
          publishable - if True, mark the version publishable
        """

        # Get the document's BLOB ID (if any).
        query = Query("doc_blob_usage", "blob_id")
        query.where(query.Condition("doc_id", self.id))
        row = query.execute(self.cursor).fetchone()
        blob_id = row.blob_id if row else None

        # Figure out what the next version number is for the document.
        query = Query("doc_version", "MAX(num) AS n")
        query.where(query.Condition("id", self.id))
        row = query.execute(self.cursor).fetchone()
        version = (row.n if row and row.n else 0) + 1

        # Assemble the values for the insertion SQL.
        cols = "val_status", "val_date", "title", "xml", "comment", "doc_type"
        query = Query("document", *cols)
        query.where(query.Condition("id", self.id))
        row = query.execute(self.cursor).fetchone()
        if not row:
            raise Exception("Can't version a document which hasn't been saved")
        val_date = row.val_date
        if isinstance(val_date, datetime.datetime):
            val_date = val_date.replace(microsecond=0)
        fields = {
            "id": self.id,
            "usr": self.session.user_id,
            "updated_dt": self.last_saved,
            "num": version,
            "xml": row.xml,
            "title": row.title,
            "doc_type": row.doc_type,
            "comment": row.comment,
            "val_status": row.val_status,
            "val_date": val_date,
            "publishable": "Y" if opts.get("publishable") else "N"
        }
        if row.val_status != "V":
            fields["publishable"] = "N"
        names = sorted(fields)
        values = [fields[name] for name in names]
        names.append("dt")

        # Insert the new row and adjust the internal property values.
        args = ", ".join(names), ", ".join(["?"] * len(values) + ["GETDATE()"])
        insert = "INSERT INTO all_doc_versions ({}) VALUES ({})".format(*args)
        self.cursor.execute(insert, tuple(values))
        self._version = self._last_version = version
        if opts.get("publishable"):
            self._last_publishable_version = version

        # Version the BLOB, too, if there is one.
        if blob_id is not None:
            values = self.id, blob_id, version
            fields = "doc_id", "blob_id", "doc_version"
            template = "INSERT INTO version_blob_usage ({}) VALUES ({})"
            args = ", ".join(fields), ", ".join(["?"] * len(fields))
            insert = template.format(*args)
            self.cursor.execute(insert, values)

    def __delete_blobs(self):
        """
        Wipe out all of the rows for this document's BLOBs

        This is special-case code to allow the users to remove all
        copies of board meeting recordings after they have outlived
        their usefulness (to avoid retaining potentially sensitive
        information indefinitely).
        """

        # Prevent any disastrous mistakes.
        if self.doctype.name != "Media":
            message = "Attempt to remove all blob versions from non-Media doc"
            raise Exception(message)

        # Remove all the usage information linking the blobs to the document.
        blob_ids = set()
        for table in ("version_blob_usage", "doc_blob_usage"):
            query = Query(table, "blob_id")
            query.where(query.Condition("doc_id", self.id))
            rows = query.execute(self.cursor).fetchall()
            blob_ids += set([row.blog_id for row in rows])
            delete = "DELETE FROM {} WHERE doc_id = ?".format(table)
            self.cursor.execute(delete, (self.id,))

        # Remove the BLOBs themselves.
        for blob_id in blob_ids:
            delete = "DELETE FROM doc_blob WHERE id = ?"
            self.cursor.execute(delete, (blob_id,))

        # Wipe the cached information about the BLOB.
        self._blob = self._blob_id = None

    def __delete_incoming_links(self, **opts):
        """
        Remove links to this document in preparation for marking it deleted

        Optional keyword argument:
          validate - if True, just record the links as problems

        Return:
          True if no inbound links are found, or `validate` is `False`
          (in either of these cases, marking of the document as deleted
          can proceed)
        """

        # Find the links to this document; ignore links to self.
        query = Query("link_net", "source_doc", "target_frag")
        query.where(query.Condition("target_doc", self.id))
        query.where(query.Condition("source_doc", self.id, "<>"))
        rows = query.execute(self.cursor).fetchall()

        # If no inbound links, the document can be marked as deleted.
        if not rows:
            return True

        # Record the inbound links as errors.
        for doc_id, frag_id in rows:
            doc = Doc(id=doc_id)
            args = doc.cdr_id, doc.title
            message = "Document {} ({}) links to this document".format(*args)
            if frag_id:
                message += " Fragment({})".format(frag_id)
            self.add_error(message, type=self.LINK)

        # Tell the caller not to proceed with the deletion (links were found).
        if opts.get("validate"):
            return False

        # Delete the links and tell the caller to proceed with the 'deletion'.
        delete = "DELETE FROM {} WHERE source_doc = ?"
        for table in ("link_net", "link_fragment"):
            sql = delete.format(table)
            self.cursor.execute(sql, (self.id,))
        return True

    def __extract_rule_sets(self, schema, sets):
        """
        Recursively fetch the custom rules from a schema document

        Pass:
          schema - reference to the parsed XML schema document
          sets - dictionary of rules sets to be populated
        """

        for node in schema:
            if node.tag == Schema.ANNOTATION:
                for child in node.findall("{}/pattern".format(Schema.APPINFO)):
                    rule_set = self.RuleSet(child)
                    if rule_set.name not in sets:
                        sets[rule_set.name] = []
                    sets[rule_set.name].append(rule_set)
            elif node.tag == Schema.INCLUDE:
                name = node.get("schemaLocation")
                xml = self.get_schema_xml(name, self.cursor)
                included_schema = etree.fromstring(xml.encode("utf-8"))
                self.__extract_rule_sets(included_schema, sets)

    def __fetch_document_property(self, column):
        """
        Fetch a value from a column from `document` or `doc_version` view

        Pass:
          column - string for name of column

        Return:
          value from named column
        """

        if not self.id:
            return None
        table = "doc_version" if self.version else "document"
        query = Query(table, column)
        query.where(query.Condition("id", self.id))
        if self.version:
            query.where(query.Condition("num", self.version))
        row = query.execute(self.cursor).fetchone()
        return row[0]

    def __generate_fragment_ids(self):
        """
        Make sure all of the elements which can have a cdr:id attribute get one

        This time we assign directly to self._xml, because we know it will
        match the `root` property we just finished manipulating.
        """

        message = "Can't generate fragment IDs for malformed documents"
        assert self.root is not None, message
        allowed = self.doctype.elements_allowing_fragment_ids()
        highest_fragment_id = self.highest_fragment_id
        for node in self.root.iter("*"):
            if node.tag in allowed:
                if not node.get(Link.CDR_ID):
                    highes_fragment_id += 1
                    fragment_id = "_{:d}".format(highest_fragment_id)
                    node.set(Link.CDR_ID, fragment_id)
        self._xml = etree.tostring(self.root, encoding="utf-8").decode("utf-8")
        self._resolved = None

    def __get_filter_set(self, name, **opts):
        """
        Get the filters for the set with the specified name

        Requred positional argument:
          name - string for the filter set's name

        Optional keyword options:
          version - versions to fetch (e.g., 'lastp')
          before - restrict versions to those created before this date/time

        Return:
          sequence of `Filter` objects
        """

        query = Query("filter_set", "id")
        query.where(query.Condition("name", name))
        row = query.execute(self.cursor).fetchone()
        if not row:
            return []
        return self.__get_filter_set_by_id(row.id, 0, **opts)

    def __get_filter_set_by_id(self, set_id, depth, **opts):
        """
        Recursively fetch the filters contained in a named set

        Required positional arguments:
          set_id - primary key integer into the `filter_set` table
          depth - indication of how far we have recursed

        Optional keyword options:
          version - versions to fetch (e.g., 'lastp')
          before - restrict versions to those created before this date/time

        Return:
          sequence of `Filter` objects
        """

        # Make sure we aren't getting into an endless recursion.
        if depth > Filter.MAX_FILTER_SET_DEPTH:
            raise Exception("infinite filter set recursion")
        depth += 1

        # Fetch the set from the cache if we've already assembled it.
        with self.session.cache.filter_set_lock:
            if set_id in self.session.cache.filter_sets:
                return self.session.cache.filter_sets[set_id]

        # Find the set's members (can be filters or nested filter sets).
        fields = "filter", "subset"
        query = Query("filter_set_member", *fields).order("position")
        query.where(query.Condition("filter_set", set_id))
        filters = []
        rows = query.execute(self.cursor).fetchall()
        for filter_id, subset_id in rows:
            if filter_id:
                filters.append(self.get_filter(filter_id, **opts))
            elif subset_id:
                more = self.__get_filter_set_by_id(subset_id, depth, **opts)
                filters += more

        # Cache the set so we don't have to do this again.
        with self.session.cache.filter_set_lock:
            if set_id not in self.session.cache.filter_sets:
                self.session.cache.filter_sets[set_id] = filters

        # Return the sequence of `Filter` objects.
        return filters

    def __get_labeled_version(self, label):
        """
        Find the version for this document with the specified label

        This feature has never been used in all the years the CDR
        has been in existence, but CIAT has requested that we preserve
        the functionality.
        """

        query = Query("doc_version v", "MAX(v.num) AS n")
        query.join("doc_version_label d", "d.document = v.id")
        query.join("version_label l", "l.id = d.label")
        query.where(query.Condition("v.id", self.id))
        query.where(query.Condition("l.name", label))
        row = query.execute(self.cursor).fetchone()
        if not row.n:
            raise Exception("no version labeled {}".format(label))
        return row.n

    def __get_schema(self):
        """
        Return the schema for the document's document type

        Hook it up to a parser which knows how to find included schema
        documents.

        Return:
           parsed schema document
        """

        query = Query("document", "title")
        query.where(query.Condition("id", self.doctype.schema_id))
        row = query.execute(self.cursor).fetchone()
        assert row, "no schema for document type {}".format(self.doctype.name)
        parser = etree.XMLParser()
        parser.resolvers.add(self.SchemaResolver(self.cursor))
        xml = self.get_schema_xml(row.title, self.cursor)
        return etree.fromstring(xml.encode("utf-8"), parser)

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

        # Fix for bug in adodbapi.
        when = when.replace(microsecond=0)

        query = Query("doc_version", "MAX(num) AS n")
        query.where(query.Condition("id", self.id))
        query.where(query.Condition("dt", when, "<"))
        if publishable is True:
            query.where("publishable = 'Y'")
        elif publishable is False:
            query.where("publishable = 'N'")
        row = query.execute(self.cursor).fetchone()
        if not row:
            raise Exception("no version before {}".format(when))
        return row.n

    def __insert_eids(self, root=None):
        """
        Add cdr-eid attributes to all document elements

        Used for locating errors found in the document. Update
        is done in place, so if you want to preserve the original
        pass a deep copy of it.

        Pass:
          root - parsed document to update (default `self.root`)
        """

        if root is None:
            root = self.root
        if root is not None:
            eid = 1
            for node in root.iter("*"):
                node.set("cdr-eid", "_%d" % eid)
                eid += 1

    def __namespaces_off(self):
        """
        Rename attributes with namespaces to non-colonized names

        We do this because the XML Schema specification does not
        support validation of attributes with namespace-qualified
        attribute names across nested schema documents, when the
        attributes' constraints need to change from one complex
        type to another. Basic pattern is:
          1. {CDR-NS}xxx => cdr-xxx for all cdr: attributes
          2. perform schema validation on the document
          3. cdr-xxx => {CDR-NS}xxx
        """

        if self.resolved is not None:
            NS = "{{{}}}".format(self.NS)
            for node in self.resolved.iter("*"):
                for name in node.attrib:
                    if name.startswith(NS):
                        ncname = name.replace(NS, "cdr-")
                        node.set(ncname, node.get(name))
                        del node.attrib[name]

    def __namespaces_on(self):
        """
        Restore the namespace-qualified names for attributes

        See comment above in `Doc.__namespaces_off`.
        """

        if self.resolved is not None:
            NS = "{{{}}}".format(self.NS)
            for node in self.resolved.iter("*"):
                for name in node.attrib:
                    if name.startswith("cdr-") and name != "cdr-eid":
                        qname = name.replace("cdr-", NS)
                        node.set(qname, node.get(name))
                        del node.attrib[name]

    def __preprocess_save(self, **opts):
        """
        Make the final tweaks to the XML prior to saving the document

        Avoid doing this for control documents (schemas, filters, etc.)
        because they don't need it, and those documents are edited by
        hand (rather than in XMetaL), and that's easier to do if we
        don't mess up the careful document layout formatting.

        Optional keyword arguments:
          val_types - sequence of strings indicating what validation
                      is to be performed (used here to determine whether
                      it's time to get rid of the XMetaL PIs)
        """

        if self.is_content_type and self.root is not None:
            if opts.get("val_types"):
                self.__strip_xmetal_pis()
            self.__generate_fragment_ids()
            if "cdr-eid" in self.xml:
                self.__strip_eids()
                xml = etree.tostring(self.root, encoding="utf-8")
                self.xml = xml.decode("utf-8")

    def __save(self, **opts):
        """
        Store a new or updated document, but don't commit

        See `save()` which wraps this in a try block and rolls back
        any transactions if any exceptions are encountered, for details
        about the optional keyword arguments passed in.

        This might not fit in a single window, but it was a 500-line
        method in the C++ code!
        """

        """
        # Make sure we can detect unversioned changes.
        if not opts.get("version") and not opts.get("publishable"):
            last_versioned = self.last_version_date
            if last_versioned is not None:
                now = datetime.datetime.now().replace(microsecond=0)
                logged = False
                while now == last_versioned:
                    if not logged:
                        self.session.logger.info("delay for document save")
                        logged = True
                    time.sleep(.1)
                    now = datetime.datetime.now().replace(microsecond=0)
        """

        # Set the stage for validation.
        self._errors = []
        if self.root is None:
            self._val_status = self.MALFORMED
        else:
            self._val_status = self.UNVALIDATED

        # Make sure the conditions for performing the save operation are met.
        status_action = self.__check_save_permissions(**opts)

        # Prepare the XML for saving.
        self.__preprocess_save(**opts)

        # Save the document if it's new so we'll have the document ID now.
        new = self.id is None
        if new:
            self.__store(**opts)
            if not opts.get("unlock"):
                self.__check_out(comment="New document checkout")

        # Validate the document if requested, and update the link tables.
        if self.is_content_type:
            val_types = opts.get("val_types") or []
            if val_types:
                val_opts = {
                    "types": val_types,
                    "locators": opts.get("locators"),
                    "store": "always"
                }
                self.__validate(**val_opts)
                if opts.get("publishable"):
                    opts["version"] = True
                    if self.val_status != self.VALID:
                        message = "Non-publishable version will be created."
                        error_opts = dict(
                            type=self.OTHER,
                            level=self.LEVEL_WARNING
                        )
                        self.add_error(message, **error_opts)
                        opts["publishable"] = False
            if opts.get("set_links") != False and "links" not in val_types:
                if self.id and self.resolved is not None:
                    self.__store_links(self.__collect_links(self.resolved))

        # If the document already existed, we still need to store it.
        if not new:
            self.__store(**opts)

        # Index the document for searching.
        if self.is_content_type:
            index_tables = ["query_term"]
            if opts.get("publishable"):
                index_tables.append("query_term_pub")
            self.update_query_terms(tables=index_tables)

        # Remember who performed this save action.
        action = "ADD DOCUMENT" if new else "MODIFY DOCUMENT"
        reason = opts.get("reason") or opts.get("comment")
        when = self.__audit_action("Doc.save", action, reason)
        if status_action:
            self.__audit_added_action(status_action, when)

        # Special case processing to eliminate sensitive meeting recordings.
        if opts.get("del_blobs"):
            self.__delete_blobs()

        # Create a permanent frozen version of the document if requested.
        if opts.get("version") or opts.get("publishable"):
            self.__create_version(**opts)

        # Check the document back in unless caller wants to keep it locked.
        if not new and opts.get("unlock"):

            # Most of self.check_in() is unnecessary, so UPDATE directly.
            update = "UPDATE checkout SET dt_in = GETDATE(), comment = ? "
            update += "WHERE id = ? AND dt_in IS NULL"
            values = reason, self.id
            self.cursor.execute(update, values)


    def __set_status(self, status, **opts):
        """
        Do the database writes for setting the document status

        This is separated out into a helper method so that we can
        roll back interim writes if we fail along the way.

        Pass:
          status - "A" (active) or "I" (inactive); required
          comment - optional string describing the change
        """

        action = "Block" if status == self.INACTIVE else "Unblock"
        comment = opts.get("comment", "{}ing document".format(action))
        args = "set_status()", "MODIFY DOCUMENT"
        when = self.__audit_action(*args, comment=opts.get("comment"))
        self.__audit_added_action("{} DOCUMENT".format(action.upper()), when)
        update = "UPDATE all_docs SET active_status = ? WHERE id = ?"
        self.cursor.execute(update, (status, self.id))

    def __store(self, **opts):
        """
        Write to the `all_docs` table (through the `document` view)

        Also stores the document's BLOB (if any) and marks the document
        as "ready for review" if appropriate.

        Creating a version is handled separately, in `__create_version()`.

        Optional keyword arguments:
          title - fallback title in case doctype has no title filter
          comment - for comment column of `all_docs` table
          needs_review - if True, try to add row to ready_for_review table
        """

        # Make sure the values will fit.
        title = self.__create_title() or opts.get("title") or "[NO TITLE]"
        if len(title) > self.MAX_TITLE_LEN:
            self.session.logger.warning("truncating title %r", title)
            title = title[:self.MAX_TITLE_LEN-4] + " ..."
        comment = opts.get("comment")
        if comment and len(comment) > self.MAX_COMMENT_LEN:
            self.session.logger.warning("truncating comment %r", comment)
            comment = comment[:self.MAX_COMMENT_LEN-4] + " ..."

        # Assemble the values for the `all_docs` table row.
        fields = {
            "val_status": self.val_status or self.UNVALIDATED,
            "active_status": self.active_status or self.ACTIVE,
            "doc_type": self.doctype.id,
            "title": title,
            "xml": self.xml,
            "comment": comment,
            "last_frag_id": self.highest_fragment_id
        }
        names = sorted(fields)
        values = [fields[name] for name in names]
        assert fields["title"], "Missing document title"

        # Add or update the documents table row.
        if not self.id:
            sql = "INSERT INTO document ({}) VALUES ({})"
            sql = sql.format(", ".join(names), ", ".join(["?"] * len(names)))
        else:
            values.append(self.id)
            sql = "UPDATE document SET {} WHERE id = ?"
            sql = sql.format(", ".join([name + " = ?" for name in names]))
        self.cursor.execute(sql, tuple(values))

        # If the document is new, get its ID and update the title (in
        # case the title filter uses the document ID).
        if not self.id:
            self.cursor.execute("SELECT @@IDENTITY AS id")
            self._id = int(self.cursor.fetchone().id)
            title = self.__create_title()
            if title and title != fields["title"]:
                update = "UPDATE document SET title = ? WHERE id = ?"
                self.cursor.execute(update, (title, self.id))

            # XXX TODO GET RID OF THIS CODE WHEN WE GO TO PRODUCTION!!!!
            if self.doctype.name == "schema":
                insert = "INSERT INTO good_schemas (id, xml) VALUES (?, ?)"
                self.cursor.execute(insert, (self.id, self.xml))
            elif self.doctype.name == "Filter":
                insert = "INSERT INTO good_filters (id, xml) VALUES (?, ?)"
                self.cursor.execute(insert, (self.id, self.xml))
        elif self.doctype.name == "schema":
            update = "UPDATE good_schemas SET xml = ? WHERE id = ?"
            self.cursor.execute(update, (self.xml, self.id))
        elif self.doctype.name == "Filter":
            update = "UPDATE good_filters SET xml = ? WHERE id = ?"
            self.cursor.execute(update, (self.xml, self.id))
            # XXX TODO END OF CODE BLOCK THAT NEEDS TO GO AWAY

        # If the document has a binary large object (BLOB), save it.
        if self.blob is not None:
            self._blob_id = self.__store_blob()

        # Mark document as ready for review (ignoring failures, which just
        # mean the document is already marked).
        if opts.get("needs_review"):
            insert = "INSERT INTO ready_for_review (doc_id) VALUES (?)"
            try:
                self.cursor.execute(insert, (self.id,))
            except:
                pass

    def __store_blob(self):
        """
        Store the document's BLOB and link to it

        Optimize away any unnecessary work if the BLOB is already
        linked to the document in the database.

        Treat a BLOB with zero bytes as a special case, indicating
        that no blob should be associated with the document.

        These two considerations make this method somewhat more
        complicated than it would otherwise be (but they're worth it).

        Return:
          primary key for the `doc_blob` table row or `None`
        """

        # Find out if a BLOB is already connected to the document.
        query = Query("doc_blob_usage", "blob_id")
        query.where(query.Condition("doc_id", self.id))
        row = query.execute(self.cursor).fetchone()
        blob_id = row.blob_id if row else None
        if blob_id:

            # Find out if the BLOB is connected with any of the doc's versions.
            query = Query("version_blob_usage", "COUNT(*) AS n")
            query.where(query.Condition("blob_id", blob_id))
            row = query.execute(self.cursor).fetchone()
            blob_is_versioned = row.n > 0
            if not self.blob:

                # self.blob is an empty sequence of bytes; break the link.
                delete = "DELETE FROM doc_blob_usage WHERE doc_id = ?"
                self.cursor.execute(delete, (self.id,))
                if not blob_is_versioned:

                    # The blob is completely orphaned; drop the row.
                    delete = "DELETE FROM doc_blob WHERE id = ?"
                    self.cursor.execute(delete, (blob_id,))

                # The document no longer has a BLOB; we're done here.
                return None

            # We have a real BLOB--see if it has changed.
            query = Query("doc_blob", "data")
            query.where(query.Condition("id", blob_id))
            blob = query.execute(self.cursor.execute).fetchone().data

            # If the BLOB is unchanged we're done.
            if blob == self.blob:
                return blob_id

            # If there are no versions connected with the BLOB, all we
            # have to do is replace the old bytes with the new bytes.
            if not blob_is_versioned:
                update = "UPDATE doc_blob SET data = ? WHERE id = ?"
                blob = Binary(self.blob)
                self.cursor.execute(update, (blob, blob_id))
                return blob_id

        # Did the caller ask us to remove a BLOB which doesn't exist?
        if not self.blob:
            return None

        # Figure out whether we're adding or updating the link to the BLOB.
        if blob_id:
            dbu = "UPDATE doc_blob_usage SET  blob_id = ? WHERE doc_id = ?"
        else:
            dbu = "INSERT INTO doc_blob_usage (blob_id, doc_id) VALUES (?, ?)"

        # Store the bytes for the BLOB.
        insert = "INSERT INTO doc_blob (data) VALUES (?)"
        self.cursor.execute(insert, (Binary(self.blob),))

        # Connect the document to the BLOB.
        self.cursor.execute("SELECT @@IDENTITY AS blob_id")
        blob_id = self.cursor.fetchone().blob_id
        self.cursor.execute(dbu, (blob_id, self.id))
        return blob_id

    def __store_links(self, links):
        """
        Populate the `link_net` and `link_fragment` tables

        The `link_fragment` table is of questionable value, as I
        can't find any code anywhere which actually uses it. But
        it's easy to populate, so I haven't dropped it.

        Pass:
          links - sequence of `Link` objects representing links
                  to CDR documents found in this document
        """

        # Fetch the existing link_net rows and determine the deltas
        # between what we found last time and what we found now.
        query = Query("link_net", "source_elem", "url")
        query.where(query.Condition("source_doc", self.id))
        rows = query.execute(self.cursor).fetchall()
        old_links = set([tuple(row) for row in rows])
        new_links = set([link.key for link in links if link.linktype])
        wanted = new_links - old_links
        unwanted = old_links - new_links

        # If optimizing the update is inefficient, start with a clean slate.
        delete = "DELETE FROM link_net WHERE source_doc = ?"
        if len(unwanted) > 500 and len(unwanted) > len(new_links) / 2:
            wanted = new_links
            self.cursor.execute(delete, (self.id,))

        # Otherwise just delete the rows which are not longer correct.
        else:
            delete += " AND source_elem = ? AND url = ?"
            for source_element, url in unwanted:
                args = self.id, source_element, url
                self.cursor.execute(delete, args)

        # Insert the rows that aren't already in place.
        for link in links:
            if link.key in wanted:
                link.save(self.cursor)

        # Apply the same technique to the `link_fragment` table.
        query = Query("link_fragment", "fragment")
        query.where(query.Condition("doc_id", self.id))
        rows = query.execute(self.cursor).fetchall()
        old = set([row.fragment for row in rows])
        new = self.frag_ids or set()
        wanted = new - old
        unwanted = old - new
        delete = "DELETE FROM link_fragment WHERE doc_id = ?"
        if len(unwanted) > 500 and len(unwanted) > len(new) / 2:
            wanted = new
            self.cursor.execute(delete, (self.id))
        else:
            delete += " AND fragment = ?"
            for fragment_id in unwanted:
                self.cursor.execute(delete, (self.id, fragment_id))
        insert = "INSERT INTO link_fragment (doc_id, fragment) VALUES (?, ?)"
        for fragment_id in wanted:
            self.cursor.execute(insert, (self.id, fragment_id))

    def __store_query_terms(self, terms, **opts):
        """
        Save the indexable search values for the document in a query term table

        We optimize this operation for the most common case, in which most
        of the terms are unchanged, by surgically determining which rows
        need to be deleted, and which need to be inserted. However, if we
        see that too many of the terms have changed, rendering this approach
        inefficient, we wipe out the document's rows and start with a fresh
        slate, inserting all of the rows for the values we've collected.

        Required positional argument:
          terms - set of path/location/value tuples for the documents
                  queryable values

        Optional keyword argument:
          table - name of the table we're writing to (default is `query_term`)
        """

        # Collect path/location/value tuples for the old index rows.
        table = opts.get("table", "query_term")
        query = Query(table, "path", "node_loc", "value")
        query.where(query.Condition("doc_id", self.id))
        rows = query.execute(self.cursor).fetchall()
        old_terms = set([tuple(row) for row in rows])

        # Figure out what needs to be eliminated, what needs to be added.
        unwanted = old_terms - terms
        wanted = terms - old_terms

        # If the "optimized" approach will be inefficient, start fresh.
        delete = "DELETE FROM {} WHERE doc_id = ?".format(table)
        if len(unwanted) > 1000 and len(unwanted) > len(terms) / 2:
            wanted = terms
            self.cursor.execute(delete, (self.id,))

        # Otherwise, refine the DELETE query and do some surgical pruning.
        else:
            delete += " AND path = ? AND node_loc = ? AND value = ?"
            for path, location, value in unwanted:
                args = self.id, path, location, value
                self.cursor.execute(delete, args)

        # Insert the rows which still need to be added (possibly all of them).
        fields = "doc_id", "path", "value", "int_val", "node_loc"
        args = table, ", ".join(fields), ", ".join(["?"] * len(fields))
        insert = "INSERT INTO {} ({}) VALUES ({})".format(*args)
        for path, location, value in wanted:
            integers = self.INTEGERS.findall(value)
            int_val = int(integers[0]) if integers else None
            args = self.id, path, value, int_val, location
            self.cursor.execute(insert, args)

    def __strip_eids(self, root=None):
        """
        Remove all cdr-eid attributes from the document

        Update is done in place, so if you want to preserve the
        original pass a deep copy of it.

        Pass:
          root - reference to top element of document (default `self.root`)
        """

        if root is None:
            root = self.root
        if root is not None:
            for node in root.xpath("//*[@cdr-eid]"):
                del node.attrib["cdr-eid"]

    def __strip_xmetal_pis(self):
        """
        Get rid of the XMetaL processing instructions

        The XMetaL PIs impede validation, and they've outlived most of
        their usefulness once the document has reached the point when
        it's ready to be validated. Assigning to the `xml` property
        invalidates the `root` property, forcing it to be reparsed.
        """

        self.xml = unicode(self.filter("name:Strip XMetaL PIs").result_tree)

    def __update_val_status(self, store):
        """
        Store the current validation status in the database

        Writes a single-character status (V, I, U, or M) to the val_status
        column of the `all_docs` table (through the `document` view). Does
        nothing if the document is not stored in the database. We leave
        the commit of the transaction to the caller.

        Pass:
          store - indicates conditions under which to update the database:
                  "never": don't touch the database
                  "valid": store "V" if the document is valid
                  "always": store the status no matter what it is

        Return:
          True if we wrote to the database; otherwise False
        """

        if not self.id or store == "never":
            return False
        if self.val_status != self.VALID and store != "always":
            return False
        update = "UPDATE document SET val_status = ?, val_date = GETDATE() "
        update += "WHERE id = ?"
        args = self.val_status, self.id
        self.cursor.execute(update, args)
        return True

    def __validate(self, **opts):
        """
        Determine whether the document conforms to the rules for its type

        Optional keyword arguments:
          types - sequence of strings indication which validations
                  to apply to the document ("schema" and/or "links")
          locators - Boolean indicating whether to include attributes
                     to be used for finding the parts of the document
                     with errors in the client; default is False
          store - "always": record validation results in the database (default)
                  "never": don't touch the database
                  "valid": only update val_status if full validation passes
                  (only applicable if the document exists in the repository);
        """

        # Make sure the user is authorized to validate this document.
        if not self.session.can_do("VALIDATE DOCUMENT", self.doctype.name):
            message = "Validation of {} documents not authorized for this user"
            raise Exception(message.format(self.doctype.name))

        # Start with a clean slate.
        self._val_status = self.UNVALIDATED
        self._eids = None
        self.__strip_eids()

        # Defaults to be possibly overridden for well-formed content documents.
        validation_xml = self.xml
        complete = True

        # Most validation is only done for well-formed non-control documents.
        if self.root is not None and self.is_content_type:

            # Use the filtered document for private use characters.
            utf8 = etree.tostring(self.resolved, encoding="utf-8")
            validation_xml = utf8.decode("utf-8")

            # Find out if we've been asked to do schema and/or link validation.
            validation_types = opts.get("types", ["schema", "links"])
            if validation_types:

                # Create a copy with error location breadcrumbs if requested.
                if opts.get("locators"):
                    self._eids = copy.deepcopy(self.root)
                    self.__insert_eids(self.eids)

                # Apply schema validation if requested.
                if "schema" in validation_types:
                    self.__validate_against_schema()
                else:
                    complete = False

                # Apply link validation if requested.
                if "links" in validation_types:
                    self.__validate_links(store=opts.get("store", "always"))
                else:
                    complete = False

        # We can't parse the document.
        else:
            self.add_error("Document malformed")
            self._val_status = self.MALFORMED

        # Check for an error found in Microsoft documents which schema
        # validation can't detect. Do this for all documents.
        if re.search(u"[\uE000-\uF8FF]+", validation_xml):
            message = "Document contains private use character(s)"
            self.add_error(message)

        # Set the validation status if not done already.
        if self.val_status == self.UNVALIDATED:

            # We can mark the document as invalid if we find any errors ...
            if self.hard_error_count:
                self._val_status = self.INVALID

            # But we won't mark it as valid unless all val types were checked.
            elif complete:
                self._val_status = self.VALID

        # Optionally record the results of the validation.
        self.__update_val_status(opts.get("store", "always"))

    def __validate_against_schema(self):
        """
        Check the XML document against the requirements for its doctype
        """

        # Don't bother to validate control documents. Shouldn't happen.
        if not self.is_content_type:
            raise Exception("can't validate control document against schema")

        # Fetch the schema template for custom validation rules.
        # This would fail if we asked for a specific version, because that
        # would cause get_filter() to try and acquire the same lock.
        with self.session.cache.filter_lock:
            if not Doc.VALIDATION_TEMPLATE:
                title = "Validation Template"
                doc_id = Doc.id_from_title(title, self.cursor)
                Doc.VALIDATION_TEMPLATE = self.get_filter(doc_id).xml

        # Get the document node to validate and schema for the document's type.
        doc = self.resolved
        schema_doc = self.__get_schema()
        schema = etree.XMLSchema(schema_doc)

        # Put a reference to our `Doc` object somewhere where callbacks
        # can find it.
        Resolver.local.docs.append(self)
        try:

            # Validate against the schema.
            self.__namespaces_off()
            if not schema.validate(doc):
                location = None
                if self.eids is not None:
                    line_map = self.LineMap(self.eids)
                for error in schema.error_log:
                    if self.eids is not None:
                        location = line_map.get_error_location(error)
                    self.add_error(error.message, location)

            # If there are any custom rules, apply them, too.
            sets_dictionary = dict()
            self.__extract_rule_sets(schema_doc, sets_dictionary)
            for name in sets_dictionary:
                rule_sets = sets_dictionary[name]
                if not name:
                    name = "anonymous"
                for rule_set in rule_sets:
                    filter_xml = etree.tostring(rule_set.get_xslt())
                    parser = Doc.Parser()
                    result = self.__apply_filter(filter_xml, doc, parser)
                    for node in result.result_tree.findall("Err"):
                        self.add_error(node.text, node.get("cdr-eid"))
                    for entry in result.error_log:
                        self.add_error(entry.message)
        finally:

            # We have to get our reference to ourselves off the stack,
            # even if something goes horribly wrong during the validation.
            Resolver.local.docs.pop()
            self.__namespaces_on()

    def __validate_links(self, store="always"):
        """
        Collect and check all of the document's links for validity

        Pass:
          store - control's whether we also populate the linking tables
                  (assuming the document is in the database)
                  "never": don't touch the database
                  "valid": store link info if the document is valid
                  "always": store the link info unconditionally (the default)
        """

        if not self.doctype or not self.doctype.id:
            problem = "invalid" if self.doctype else "missing"
            raise Exception("__validate_links(): {} doctype".format(problem))
        links = self.__collect_links(self.resolved)
        for link in links:
            if link.internal:
                link.validate()
        if self.id:
            if store == "always" or store == "valid" and not self.errors:
                self.__store_links(links)

    # ------------------------------------------------------------------
    # STATIC AND CLASS METHODS START HERE.
    # ------------------------------------------------------------------

    @staticmethod
    def create_label(session, label, comment=None):
        """
        Create a name which can be used for tagging one or more doc versions

        Pass:
          session - reference to object representing user's login
          label - string used to tag document versions
          comment - optional string describing the label's usage
        """

        assert label, "Missing label name"
        cursor = session.conn.cursor()
        query = Query("version_label", "COUNT(*) AS n")
        query.where(query.Condition("name", label))
        if query.execute(cursor).fetchone().n > 0:
            raise Exception("Label {!r} already exists".format(label))
        insert = "INSERT INTO version_label (name, comment) VALUES (?, ?)"
        cursor.execute(insert, (label, comment))
        session.conn.commit()
        cursor.close()

    @staticmethod
    def delete_label(session, label):
        assert label, "Missing label name"
        cursor = session.conn.cursor()
        query = Query("version_label", "id")
        query.where(query.Condition("name", label))
        row = query.execute(cursor).fetchone()
        if not row:
            raise Exception("Can't find label {!r}".format(label))
        label_id = row.id
        delete = "DELETE FROM doc_version_label WHERE label = ?"
        cursor.execute(delete, (label_id,))
        delete = "DELETE FROM version_label WHERE id = ?"
        try:
            cursor.execute(delete, (label_id,))
            session.conn.commit()
        except:
            cursor.execute("ROLLBACK TRANSACTION")
            session.logger.exception("delete_label() failure")
            raise Exception("Failure deleting label {!r}".format(label))

    @staticmethod
    def extract_id(arg):
        """
        Return the CDR document ID as an integer (ignoring fragment suffixes)
        """

        if isinstance(arg, basestring):
            return int(re.sub(r"[^\d]", "", str(arg).split("#")[0]))
        return int(arg)

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

    @classmethod
    def get_schema_xml(cls, name, cursor):
        """
        TODO: fix schemas in all_docs table and use them
        """

        assert name, "can't get a schema without a name"
        names = name, name.replace(".xsd", ".xml")
        assert name, "get_schema_xml(): no name for schema"
        query = Query("good_schemas s", "s.xml")
        query.join("document d", "d.id = s.id")
        query.join("doc_type t", "t.id = d.doc_type")
        query.where("t.name = 'schema'")
        query.where(query.Condition("d.title", names, "IN"))
        try:
            return query.execute(cursor).fetchone()[0]
        except:
            raise Exception("schema {!r} not found".format(name))

    @staticmethod
    def id_from_title(title, cursor=None):
        """
        Look up the CDR document ID for the specified document title

        Some characters (spaces and slashes) may be escaped in order
        for the title to conform to URL syntax requirements.

        Pass:
          title - required string for document title
          cursor - optional reference to database cursor object

        Return:
          integer for unique document ID or None if title not found

        Raise:
          `Exception` if more than one document matches the title
        """

        title = title.replace("@@SLASH@@", "/").replace("+", " ")
        query = Query("document", "id")
        query.where(query.Condition("title", title))
        rows = query.execute(cursor).fetchall()
        if len(rows) > 1:
            raise Exception("Multiple documents with title %s" % title)
        for row in rows:
            return row.id
        return None

    @classmethod
    def delete_failed_mailers(cls, session):
        """
        Mark tracking documents for failed mailer jobs as deleted

        Invoked by the CdrMailerCleanup command. We skip past mailers
        converted from the legacy Oracle PDQ system as an optimization.

        Pass:
          session - reference to object representing user's login

        Return:
          object carrying IDs for deleted documents and error strings
        """

        class CleanupReport:
            def __init__(self): self.deleted, self.errors = [],[]
        reason = "Deleting tracking document for failed mailer job"
        report = CleanupReport()
        cursor = session.conn.cursor()
        query = Query("query_term q", "q.doc_id").unique()
        query.join("pub_proc p", "p.id = q.int_val", "p.status = 'Failure'")
        query.where("q.path = '/Mailer/JobId'")
        query.where("q.doc_id > {}".format(cls.LEGACY_MAILER_CUTOFF))
        for row in query.execute(cursor).fetchall():
            try:
                Doc(session, id=row.doc_id).delete(reason=reason)
                report.deleted.append(row.doc_id)
            except Exception as e:
                report.errors.append(str(e))
        cursor.close()
        return report

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

    @staticmethod
    def normalize_id(doc_id):
        if doc_id is None:
            return None
        if isinstance(doc_id, basestring):
            if not isinstance(doc_id, unicode):
                doc_id = doc_id.decode("ascii")
            doc_id = int(re.sub("[^0-9]", "", doc_id))
        return "CDR{:010d}".format(doc_id)

    @classmethod
    def qname(cls, local, ns=None):
        """
        Create the string for a namespace-qualified XML name

        Pass:
          local - string for the unqualified portion of the name
          ns - optional namespace string (default is the CDR namespace)
        """

        if ns is None:
            ns = cls.NS
        return "{{{}}}{}".format(ns, local)


    # ------------------------------------------------------------------
    # NESTED CLASSES START HERE.
    # ------------------------------------------------------------------


    class Action:
        """
        Information about an ADD or UPDATE action

        Attributes:
          when - datetime object for the save
          user - `Doc.User` object for the account performing the action
        """

        def __init__(self, row):
            """
            Capture information about save action

            Pass:
              row from database query result
            """

            self.when = row.dt.replace(microsecond=0)
            self.user = Doc.User(row.id, row.name, row.fullname)


    class Assertion:
        """
        Test to be applied for a custom validation rule found in a schema

        Attributes:
          test - string for the test to be applied
          message - string for the error to be logged if the test doesn't pass
        """

        def __init__(self, node):
            """
            Extract the attributes and verify that they were found
            """

            self.test = Doc.get_text(node.find("test"))
            self.message = Doc.get_text(node.find("message"))
            assert self.test and self.message, "assert requires test, message"

        def make_test(self):
            """
            Create an xsl:if node for applying the test

            Return:
              node to be inserted into XSL/T document for custom rules
            """

            IF = Doc.qname("if", Filter.NS)
            CALL = Doc.qname("call-template", Filter.NS)
            WITH = Doc.qname("with-param", Filter.NS)
            test = etree.Element(IF, test="not({})".format(self.test))
            call = etree.SubElement(test, CALL, name="pack-error")
            message = '"{}"'.format(self.message)
            etree.SubElement(call, WITH, name="msg", select=message)
            return test

    class Error:
        """
        Problem found when processing (unsually validating) a CDR document

        Attributes:
          message - description of the problem
          location - unique ID identifying the node where the error is found
          type - string for the type of error (e.g., 'validation', the default)
          level - 'info' | 'warning' | 'error' | 'fatal'
        """

        def __init__(self, message, location, **opts):
            """
            Store the attributes

            Pass:
              message - string describing the problem
              location - where the error was found (None if unavailable)
              type - string for type of error (default 'validation')
              level - how serious is the problem (default 'error')
            """

            self.message = re.sub(r"\s+", " ", message.strip())
            self.location = location
            self.type = opts.get("type") or Doc.VALIDATION
            self.level = opts.get("level") or Doc.LEVEL_ERROR

        def to_node(self):
            """
            Create a legacy `Err` node to wrap the error

            Return:
              `Err` element node
            """

            node = etree.Element("Err", etype=self.type, elevel=self.level)
            node.text = self.message
            if self.location:
                node.set("eref", self.location)
            return node

        def __str__(self):
            """
            Return a human-readable string for the `Error` object
            """

            args = self.location, self.type, self.level, self.message
            return "{} [{} {}] {}".format(*args)

    class FilterResult:
        """
        Results of passing an XML document through one or more XSL/T filters

        Attributes:
          result_tree - _XSLTResultTree object
          messages - optional sequence of string messages emitted by
                     the XSL/T engine (used for the final return value
                     of the `Doc.filter()` method); these are unicode
                     strings
          error_log - used for the individual calls to `__apply_filter()`
        """

        def __init__(self, result_tree, **opts):
            self.result_tree = result_tree
            self.error_log = opts.get("error_log")
            self.messages = opts.get("messages")

    class LineMap:
        class Line:
            def __init__(self):
                self.tags = dict()
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
            self.lines = dict()
            for node in root.iter("*"):
                line = self.lines.get(node.sourceline)
                if not line:
                    line = self.lines[node.sourceline] = self.Line()
                line.add_node(node)
        def get_error_location(self, error):
            line = self.lines.get(error.line)
            return line and line.get_error_location(error) or None

    class Lock:
        """
        Who has the document checked out, beginning when
        """

        def __init__(self, row):
            """
            Invoked by the `Doc.lock` property

            Pass:
              row from database query result
            """

            self.locker = Doc.User(row.id, row.name, row.fullname)
            self.locked = row.dt_out
            if isinstance(self.locked, datetime.datetime):
                self.locked = self.locked.replace(microsecond=0)

        def __str__(self):
            """
            Create a user-readable description of the lock

            Suitable for raising Exception objects to explain why
            the current user is unable to do something with a document.
            """

            args = self.locker.name, self.locker.fullname, self.locked
            return "Document checked out to {} ({}) {}".format(*args)

    class Parser(etree.XMLParser):
        def __init__(self):
            etree.XMLParser.__init__(self)
            self.resolvers.add(Resolver("cdrutil"))
            self.resolvers.add(Resolver("cdr"))
            self.resolvers.add(Resolver("cdrx"))

    class Rule:
        def __init__(self, node):
            self.assertions = []
            self.context = node.get("context")
            for child in node.findall("assert"):
                self.assertions.append(Doc.Assertion(child))
        def template(self):
            TEMPLATE = Doc.qname("template", Filter.NS)
            template = etree.Element(TEMPLATE, match=self.context)
            for assertion in self.assertions:
                template.append(assertion.make_test())
            return template

    class RuleSet:
        def __init__(self, node):
            self.name = node.get("name")
            self.value = node.get("value") # never been used, AFAIK
            self.rules = [Doc.Rule(r) for r in node.findall("rule")]
        def get_xslt(self):
            root = self.make_base()
            for rule in self.rules:
                root.append(rule.template())
            return root
        @classmethod
        def make_base(cls):
            return etree.fromstring(Doc.VALIDATION_TEMPLATE)


    class SchemaResolver(etree.Resolver):
        def __init__(self, cursor):
            etree.Resolver.__init__(self)
            self.__cdr_cursor = cursor
        def resolve(self, url, id, context):
            #sys.stderr.write("SchemaResolver url={}\n".format(url))
            xml = Doc.get_schema_xml(url, self.__cdr_cursor)
            return self.resolve_string(xml, context)


    class User:
        """
        Information about a user who did something with the document

        Attributes:
          id - primary key integer for the `usr` table
          name - string for the user account name
          fullname - string for the user's complete name
        """

        def __init__(self, id, name, fullname):
            """
            Capture the attributes (no validation)
            """

            self.id = id
            self.name = name
            self.fullname = fullname


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
        #sys.stderr.write("url={}\n".format(url))
        self.doc = self.local.docs[-1]
        self.session = self.doc.session
        self.cursor = self.session.conn.cursor()
        self.url = url_unquote(url.replace("+", " "))
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
        query = Query("zipcode", "zip")
        query.where(query.Condition("zip", args))
        row = query.execute(self.cursor).fetchone()
        if row and row.zip:
            result.text = str(row.zip)[:5]
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
        if Query.PLACEHOLDER != "?":
            query = query.replace("?", Query.PLACEHOLDER)
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
        uri = parms
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
            if doc.doctype.name == "Filter":
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
        try:
            xml = doc.xml
        except:
            raise Exception("Unable to resolve uri {}".format(uri))
        return self.resolve_string(doc.xml, context)

    def package_result(self, result, context):
        result = etree.tostring(result, encoding="utf-8")
        return self.resolve_string(result, context)

    @staticmethod
    def escape_uri(context, arg=""):
        if isinstance(arg, (list, tuple)):
            arg = "".join(arg)
        try:
            return url_quote(arg.replace("+", "@@PLUS@@"))
        except:
            raise

etree.FunctionNamespace(Doc.NS).update({"escape-uri": Resolver.escape_uri})

class Term:
    """
    Term document with parents

    This class is used for XSL/T filtering callbacks.
    """

    def __init__(self, session, doc_id, depth=0):
        self.session = session
        self.doc_id = doc_id
        self.cdr_id = "CDR%010d" % doc_id
        self.include = True
        self.parents = dict()
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
        term.set(Link.CDR_REF, self.cdr_id)
        if self.pdq_key:
            term.set("PdqKey", "Term:" + self.pdq_key)
        etree.SubElement(term, "PreferredName").text = self.name
        xml = etree.tostring(term, encoding="utf-8")
        for doc_id in sorted(self.parents):
            parent = self.parents[doc_id]
            if parent is not None and parent.include and parent.name:
                child = etree.SubElement(term, "Term")
                child.set(Link.CDR_REF, parent.cdr_id)
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
            doc_id = Doc.extract_id(node.get(Link.CDR_REF))
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


class Schema:
    NS = "http://www.w3.org/2001/XMLSchema"
    ANNOTATION = Doc.qname("annotation", NS)
    APPINFO = Doc.qname("appinfo", NS)
    SCHEMA = Doc.qname("schema", NS)
    ELEMENT = Doc.qname("element", NS)
    COMPLEX_TYPE = Doc.qname("complexType", NS)
    SIMPLE_TYPE = Doc.qname("simpleType", NS)
    SIMPLE_CONTENT = Doc.qname("simpleContent", NS)
    GROUP = Doc.qname("group", NS)
    INCLUDE = "{%s}include" % NS
    ATTRIBUTE = Doc.qname("attribute", NS)
    SEQUENCE = Doc.qname("sequence", NS)
    CHOICE = Doc.qname("choice", NS)
    RESTRICTION = Doc.qname("restriction", NS)
    EXTENSION = Doc.qname("extension", NS)
    ENUMERATION = Doc.qname("enumeration", NS)
    NESTED_ATTRIBUTE = "/".join([SIMPLE_CONTENT, EXTENSION, ATTRIBUTE])


class Doctype:
    """
    Class of CDR documents controlled by a schema
    """

    def __init__(self, session, **opts):
        self.__session = session
        self.__opts = opts

    @property
    def active(self):
        if not hasattr(self, "_active"):
            self._active = self.__opts.get("active")
            if not self._active:
                if self.id:
                    query = Query("doc_type", "active")
                    query.where(query.Condition("id", self.id))
                    row = query.execute(self.cursor).fetchone()
                    self._active = row.active if row else "Y"
                else:
                    self._active = "Y"
        assert self._active in "YN", "invalid doctype active value"
        return self._active

    @active.setter
    def active(self, value):
        assert value in "YN", "invalid doctype active value"
        self._active = value

    @property
    def comment(self):
        if not hasattr(self, "_comment"):
            if "comment" in self.__opts:
                self._comment = self.__opts["comment"]
            elif self.id:
                query = Query("doc_type", "comment")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.cursor).fetchone()
                self._comment = row.comment if row else None
            else:
                self._comment = None
            if self._comment:
                self._comment = self._comment.strip()
            else:
                self._comment = None
        return self._comment

    @comment.setter
    def comment(self, value):
        self._comment = value

    @property
    def created(self):
        if not hasattr(self, "_created"):
            self.__fetch_dates()
        return self._created

    @property
    def cursor(self):
        if not hasattr(self, "_cursor"):
            self._cursor = self.session.conn.cursor()
        return self._cursor

    @property
    def dtd(self):
        if not hasattr(self, "_dtd_string"):
            self._dtd = DTD(self.session, name=self.schema)
            self._dtd_string = str(self._dtd)
        return self._dtd_string

    @property
    def format(self):
        if not hasattr(self, "_format"):
            self._format = self.__opts.get("format")
            if not self._format and self.id:
                query = Query("format f", "f.name", "f.id")
                query.join("doc_type t", "t.format = f.id")
                query.where(query.Condition("t.id", self.id))
                row = query.execute(self.cursor).fetchone()
                if row:
                    self._format, self._format_id = row
                else:
                    self._format = "xml"
            else:
                self._format = "xml"
        return self._format

    @format.setter
    def format(self, value):
        self._format = value
        format_id = self.__format_id_from_name(value)
        if format_id is None:
            raise Exception("Unrecognized doctype format {!r}".format(value))
        self._format_id = format_id

    @property
    def format_id(self):
        if not hasattr(self, "_format_id"):
            if self.format:
                if not hasattr(self, "_format_id"):
                    self._format_id = self.__format_id_from_name(self.format)
        return self._format_id

    @property
    def id(self):
        if not hasattr(self, "_id"):
            self._id = self.__opts.get("id")
            if not self._id:
                if hasattr(self, "_name"):
                    name = self._name
                else:
                    name = self.__opts.get("name")
                if name:
                    query = Query("doc_type", "id")
                    query.where(query.Condition("name", name))
                    row = query.execute(self.cursor).fetchone()
                    self._id = row.id if row else None
                else:
                    self.session.logger.warning("Doctype.id: NO NAME!!!")
        return self._id

    @property
    def name(self):
        if not hasattr(self, "_name"):
            self._name = self.__opts.get("name")
            if not self._name and self.id:
                query = Query("doc_type", "name")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.cursor).fetchone()
                self._name = row.name if row else None
        return self._name

    @name.setter
    def name(self, value):
        """
        Modify the name of the document type

        First make sure the `id` property has been set from the old name.

        Pass:
          value - new name for the document type
        """

        if self.id != "~~~some bogus value":
            self._name = value

    @property
    def schema(self):
        if not hasattr(self, "_schema"):
            if "schema" in self.__opts:
                self._schema = self.__opts["schema"]
            elif self.id:
                self._schema_id = self._schema = None
                query = Query("document d", "d.id", "d.title")
                query.join("doc_type t", "t.xml_schema = d.id")
                query.where(query.Condition("t.id", self.id))
                row = query.execute(self.cursor).fetchone()
                if row:
                    self._schema_id, self._schema = row
            else:
                self.session.logger.warning("@schema: no doctype id")
                self._schema = None
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value
        if value is None:
            self._schema_id = None
        else:
            schema_id = Doc.id_from_title(value, self.cursor)
            if not schema_id:
                raise Exception("Schema {!r} not found".format(value))
            self._schema_id = schema_id

    @property
    def schema_id(self):
        if not hasattr(self, "_schema_id"):
            if self.schema and not hasattr(self, "_schema_id"):
                schema_id = Doc.id_from_title(self.schema, self.cursor)
                if schema_id:
                    self._schema_id = schema_id
                else:
                    message = "Schema {!r} not found".format(self.schema)
                    raise Exception(message)
        return self._schema_id

    @property
    def schema_mod(self):
        if not hasattr(self, "_schema_date"):
            self.__fetch_dates()
        return self._schema_date

    @property
    def session(self):
        return self.__session

    @property
    def versioning(self):
        if not hasattr(self, "_versioning"):
            self._versioning = self.__opts.get("versioning")
            if not self._versioning:
                if self.id:
                    query = Query("doc_type", "versioning")
                    query.where(query.Condition("id", self.id))
                    row = query.execute(self.cursor).fetchone()
                    self._versioning = row.versioning if row else "Y"
                else:
                    self._versioning = "Y"
        assert self._versioning in "YN", "invalid doctype versioning value"
        return self._versioning

    @versioning.setter
    def versioning(self, value):
        assert value in "YN", "invalid doctype versioning value"
        self._versioning = value

    @property
    def vv_lists(self):
        if not hasattr(self, "_vv_lists"):
            self._vv_lists = self._dtd.values if self.dtd else None
        return self._vv_lists

    @property
    def linking_elements(self):
        if not hasattr(self, "_linking_elements"):
            if self.dtd:
                self._linking_elements = self._dtd.linking_elements
            else:
                self._linking_elements = None
        return self._linking_elements

    def delete(self):
        if not self.session.can_do("DELETE DOCTYPE"):
            raise Exception("User not authorized to delete document types")
        if not self.id:
            raise Exception("Document type {!r} not found".format(self.name))
        query = Query("all_docs", "COUNT(*) AS n")
        query.where(query.Condition("doc_type", self.id))
        if query.execute(self.cursor).fetchone().n:
            message = "Cannot delete document type for which documents exist"
            raise Exception(message)
        tables = [
            ("grp_action", "doc_type"),
            ("link_xml", "doc_type"),
            ("link_target", "target_doc_type"),
            ("doc_type", "id")
        ]
        for table, column in tables:
            sql = "DELETE FROM {} WHERE {} = ?".format(table, column)
            self.cursor.execute(sql, (self.id,))
        self.session.conn.commit()

    def elements_allowing_fragment_ids(self):
        """
        Find out which elements accept cdr:id attributes

        Recursively parse the schema documents for this document type
        to learn which elements are defined and what their type names
        are, as well as which complex types allow cdr:id attributes.
        The test `if types.get(elements[name])` could fail for one of
        two reasons:
          (1) the element's type is not a complex type; or
          (2) the element's complex type does not allow cdr:id attributes
        In either case we avoid add the element's name to the return set.

        Return:
          set of element names for which cdr:id attributes are allowed
        """

        if not self.schema:
            return set()
        schemas = set() # prevent infinite recursion
        elements = dict()
        types = dict()
        self.__parse_schema(self.schema, elements, types, schemas)
        names = set()
        for name in elements:
            if types.get(elements[name]):
                names.add(name)
        return names

    def save(self, **opts):
        self.session.logger.info("Doctype.save(%s)", opts)
        now = datetime.datetime.now().replace(microsecond=0)
        try:
            fields = {
                "name": opts.get("name", self.name),
                "format": opts.get("format", self.format_id),
                "versioning": opts.get("versioning", self.versioning),
                "xml_schema": self.schema_id,
                "comment": opts.get("comment", self.comment) or None,
                "active": opts.get("active", self.active),
                "schema_date": now
            }
            if fields["comment"]:
                fields["comment"] = fields["comment"].strip()
        except:
            self.session.logger.exception("can't set fields")
            raise
        self.session.logger.info("fields=%s", fields)
        if not self.id:
            fields["created"] = now
            names = sorted(fields)
            values = tuple([fields[name] for name in names])
            pattern = "INSERT INTO doc_type ({}) VALUES ({})"
            placeholders = ["?"] * len(names)
            sql = pattern.format(", ".join(names), ", ".join(placeholders))
        else:
            names = sorted(fields)
            values = tuple([fields[name] for name in names] + [self.id])
            assignments = ["{} = ?".format(name) for name in names]
            pattern = "UPDATE doc_type SET {} WHERE id = ?"
            sql = pattern.format(", ".join(assignments))
        self.session.logger.info("sql=%s", sql)
        self.session.logger.info("values=%s", values)
        self.cursor.execute(sql, values)
        self._schema_date = now
        if not self.id:
            self._created = now
            self.cursor.execute("SELECT @@IDENTITY AS id")
            self._id = self.cursor.fetchone().id
        self.session.conn.commit()
        self.session.logger.info("committed doctype %s", self.id)
        return self.id

    def __format_id_from_name(self, name):
        query = Query("format", "id")
        query.where(query.Condition("name", name))
        row = query.execute(self.cursor).fetchone()
        return row.id if row else None

    def __fetch_dates(self):
        self._created = self._schema_date = None
        if self.id:
            query = Query("doc_type", "created", "schema_date")
            query.where(query.Condition("id", self.id))
            row = query.execute(self.cursor).fetchone()
            if row:
                values = []
                for value in row:
                    if isinstance(value, datetime.datetime):
                        value = value.replace(microsecond=0)
                    values.append(value)
                self._created, self._schema_date = values

    def __parse_schema(self, title, elements, types, schemas):
        """
        Recursively parse schema to see which elements allow cdr:id attributes

        Populate two dictionaries, the first mapping element names to
        type names and the second mapping complex types to a Boolean
        flag indicating whether the type allows cdr:id attributes.
        Because of a flaw in the XSD standard, we map attributes names
        with namespaces to local names (e.g., cdr:id becomes cdr-id)
        temporarily while we are validating.

        Pass:
          title - name under which the schema document is stored
          elements - dictionary mapping element names to type names
          types - dictionary mapping type names to Boolean flags
          schemas - set of schema names we've already seen, preventing
                    recursion
        """

        assert title, "can't parse a schema that has no title"
        if title in schemas:
            raise Exception("infinite schema inclusion")
        schemas.add(title)
        xml = Doc.get_schema_xml(title, self.cursor)
        root = etree.fromstring(xml.encode("utf-8"))
        for node in root.iter("*"):
            if node.tag == Schema.COMPLEX_TYPE:
                name = node.get("name")
                if name not in types:
                    allowed = False
                    for path in Schema.ATTRIBUTE, Schema.NESTED_ATTRIBUTE:
                        for child in node.findall(path):
                            if allowed:
                                break
                            if child.get("name") == "cdr-id":
                                allowed = True
                    types[name] = allowed
            elif node.tag == Schema.ELEMENT:
                name = node.get("name")
                if name not in elements:
                    elements[name] = node.get("type")
            elif node.tag == Schema.INCLUDE:
                location = node.get("schemaLocation")
                self.__parse_schema(location, elements, types, schemas)

    @staticmethod
    def get_css_files(session):
        """
        Fetch the CSS from the repository

        The current CSS is stored in version control, not the CDR, but
        there's still a path in the DLL code which invokes this as a
        fallback, so I'm not going to get rid of this (yet). Leaving
        the CSS as binary, as it seems to be a proprietary format
        previously used by XMetaL (we use standard CSS text files now).

        Pass:
          session - reference to object representing user's login

        Return:
          dictionary of CSS files, indexed by their document names
        """

        query = Query("doc_blob b", "d.title", "b.data")
        query.join("document d", "d.id = b.id")
        query.join("doc_type t", "t.id = d.doc_type")
        query.where("t.name = 'css'")
        rows = query.execute(session.cursor).fetchall()
        return dict([tuple(row) for row in rows])

    @staticmethod
    def list_doc_types(session):
        """
        Assemble the list of active document types names

        Pass:
          session - reference to object representing user's login

        Return:
          sequence of document type names, sorted alphabetically
        """

        if not session.can_do("LIST DOCTYPES"):
            raise Exception("User not authorized to list document types")
        query = Query("doc_type", "name").order("name").where("active = 'Y'")
        query.where("name <> ''")
        return [row.name for row in query.execute(session.cursor).fetchall()]

    @staticmethod
    def list_schema_docs(session):
        """
        Assemble the list of schema documents currently stored in the CDR

        Pass:
          session - reference to object representing user's login

        Return:
          sequence of document type names, sorted alphabetically
        """

        query = Query("document d", "d.title").order("d.title")
        query.join("doc_type t", "t.id = d.doc_type")
        query.where("t.name = 'schema'")
        return [row.title for row in query.execute(session.cursor).fetchall()]


class DTD:
    NAME_START_CATEGORIES = { "Ll", "Lu", "Lo", "Lt", "Nl" }
    OTHER_NAME_CATEGORIES = { "Mc", "Me", "Mn", "Lm", "Nd" }
    NAME_CHAR_CATEGORIES = NAME_START_CATEGORIES | OTHER_NAME_CATEGORIES
    EMPTY = "empty"
    MIXED = "mixed"
    TEXT_ONLY = "text-only"
    ELEMENT_ONLY = "element-only"
    UNBOUNDED = "unbounded"

    def __init__(self, session, **opts):
        self.session = session
        self.cursor = session.conn.cursor()
        self.types = dict()
        self.groups = dict()
        self.top = None
        self.name = opts.get("name")
        self.parse_schema(self.name)

    def parse_schema(self, name):
        assert name, "how can we parse something we can't find?"
        schema_xml = Doc.get_schema_xml(name, self.cursor)
        root = etree.fromstring(schema_xml.encode("utf-8"))
        if root.tag != Schema.SCHEMA:
            raise Exception("Top-level element must be schema")
        for node in root:
            if node.tag == Schema.ELEMENT:
                assert not self.top, "only one top-level element allowed"
                self.top = self.Element(self, node)
            elif node.tag == Schema.COMPLEX_TYPE:
                self.ComplexType(self, node)
            elif node.tag == Schema.SIMPLE_TYPE:
                self.SimpleType(self, node)
            elif node.tag == Schema.GROUP:
                self.Group(self, node)
            elif node.tag == Schema.INCLUDE:
                self.parse_schema(node.get("schemaLocation"))
                self.session.logger.debug("resume parsing %s", name)
        self.session.logger.debug("finished parsing %s", name)
    def __str__(self):
        self.values = dict()
        self.linking_elements = set()
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
            debug = dtd.session.logger.debug
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
            self.values = []
            assert self.name, "type must have a name"
        def define(self, element):
            if self.values:
                self.dtd.values[element.name] = self.values
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
            dtd.session.logger.debug("SimpleType %s", self.name)
            for restriction in node.findall(Schema.RESTRICTION):
                self.base = restriction.get("base")
                for enum in restriction.findall(Schema.ENUMERATION):
                    value = enum.get("value")
                    self.values.append(value)
                    if self.nmtokens is not None:
                        if DTD.is_nmtoken(value):
                            self.nmtokens.append(value)
                        else:
                            self.nmtokens = None
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
            dtd.session.logger.debug("ComplexType %s", self.name)
            self.attributes = dict()
            self.content = None
            self.model = DTD.EMPTY
            if node.get("mixed") == "true":
                self.model = DTD.MIXED
            for child in node.findall("*"):
                if child.tag == Schema.ATTRIBUTE:
                    self.add_attribute(dtd, child)
                elif self.content:
                    raise Exception("%s: %s" % (self.name, self.ERROR))
                elif child.tag == Schema.SIMPLE_CONTENT:
                    self.model = DTD.TEXT_ONLY
                    extension = child.find(Schema.EXTENSION)
                    assert len(extension), "%s: missing extension" % self.name
                    for child in extension.findall(Schema.ATTRIBUTE):
                        self.add_attribute(dtd, child)
                    break
                else:
                    if self.model == DTD.EMPTY:
                        self.model = DTD.ELEMENT_ONLY
                    if child.tag == Schema.SEQUENCE:
                        self.content = DTD.Sequence(dtd, child)
                    elif child.tag == Schema.CHOICE:
                        self.content = DTD.Choice(dtd, child)
                    elif child.tag == Schema.GROUP:
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
            attributes = []
            for attribute in self.attributes.values():
                if attribute.name == "cdr:ref":
                    self.dtd.linking_elements.add(element.name)
                attributes.append(str(attribute))
                values = attribute.values()
                if values:
                    key = "{}@{}".format(element.name, attribute.name)
                    self.dtd.values[key] = values
            if not self.dtd.defined and "readonly" not in self.attributes:
                attributes.append("readonly CDATA #IMPLIED")
            if attributes or not self.dtd.defined:
                attributes = sorted(attributes)
                if not self.dtd.defined:
                    attributes = ["xmlns:cdr CDATA #IMPLIED"] + attributes
                attributes = " ".join(attributes)
                return "<!ATTLIST %s %s>" % (element.name, attributes)

    class ChoiceOrSequence(CountedNode):
        def __init__(self, dtd, node):
            DTD.CountedNode.__init__(self, dtd, node)
            self.dtd = dtd
            self.nodes = []
            for child in node:
                if child.tag == Schema.ELEMENT:
                    self.nodes.append(DTD.Element(dtd, child))
                if child.tag == Schema.CHOICE:
                    self.nodes.append(DTD.Choice(dtd, child))
                elif child.tag == Schema.SEQUENCE:
                    self.nodes.append(DTD.Sequence(dtd, child))
                elif child.tag == Schema.GROUP:
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
            dtd.session.logger.debug("Choice with %d nodes", len(self.nodes))
    class Sequence(ChoiceOrSequence):
        separator = ","
        def __init__(self, dtd, node):
            DTD.ChoiceOrSequence.__init__(self, dtd, node)
            dtd.session.logger.debug("Sequence with %d nodes", len(self.nodes))
    class Group:
        def __init__(self, dtd, node):
            self.dtd = dtd
            self.ref = node.get("ref")
            if self.ref:
                dtd.session.logger.debug("Reference to group %s", self.ref)
                return
            self.name = node.get("name")
            dtd.session.logger.debug("Group %s", self.name)
            if self.name in dtd.groups:
                message = "multiple definitions for group {}".format(self.name)
                raise Exception(message)
            nodes = []
            for child in node:
                if child.tag == Schema.CHOICE:
                    nodes.append(DTD.Choice(dtd, node))
                elif child.tag == Schema.SEQUENCE:
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
            debug = dtd.session.logger.debug
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
        def values(self):
            if "xsd:" in self.type_name:
                return None
            simple_type = self.dtd.types.get(self.type_name)
            return simple_type.values
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

class LinkType:
    TYPES = dict()
    TYPE_IDS = dict()
    LOCK = threading.Lock()
    CHECK_TYPES = {
        "C": "current document",
        "P": "publishable document",
        "V": "document version"
    }

    def __init__(self, session, **opts):
        self.__session = session
        self.__opts = opts

    @property
    def cursor(self):
        if not hasattr(self, "_cursor"):
            self._cursor = self.session.conn.cursor()
        return self._cursor

    @property
    def session(self):
        return self.__session

    @property
    def properties(self):
        if not hasattr(self, "_properties"):
            if "properties" in self.__opts:
                self._properties = self.__opts["properties"]
            elif self.id:
                fields = "t.name", "p.value", "p.comment"
                query = Query("link_properties p", *fields)
                query.join("link_prop_type t", "t.id = p.property_id")
                query.where(query.Condition("p.link_id", self.id))
                rows = query.execute(self.cursor).fetchall()
                self._properties = []
                message = "Property type {!r} not supported"
                for row in rows:
                    try:
                        cls = getattr(LinkType, row.name)
                        property = cls(self.session, *row)
                        if not isinstance(property, LinkType.Property):
                            raise Exception(message.format(row.name))
                        self._properties.append(property)
                    except:
                        raise Exception(message.format(row.name))
            else:
                self._properties = []
        return self._properties

    class LinkSource:
        def __init__(self, doctype, element):
            self.doctype = doctype
            self.element = element

    @property
    def sources(self):
        if not hasattr(self, "_sources"):
            if "sources" in self.__opts:
                self._sources = self.__opts["sources"]
            elif self.id:
                query = Query("doc_type t", "t.id", "t.name", "x.element")
                query.join("link_xml x", "x.doc_type = t.id")
                query.where(query.Condition("x.link_id", self.id))
                rows = query.execute(self.cursor).fetchall()
                sources = []
                for row in rows:
                    doctype = Doctype(self.session, id=row.id, name=row.name)
                    sources.append(self.LinkSource(doctype, row.element))
                self._sources = sources
            else:
                self._sources = []
        return self._sources

    @property
    def comment(self):
        if not hasattr(self, "_comment"):
            if "comment" in self.__opts:
                self._comment = self.__opts["comment"]
            elif self.id:
                query = Query("link_type", "comment")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.cursor).fetchone()
                self._comment = row.comment if row else None

                # Clean up from an old CGI bug.
                if self._comment == "None":
                    self._comment = None
            else:
                self._comment = None
        return self._comment

    @property
    def targets(self):
        if not hasattr(self, "_targets"):
            if "targets" in self.__opts:
                self._targets = self.__opts["targets"]
            elif self.id:
                query = Query("doc_type t", "t.id", "t.name")
                query.join("link_target l", "l.target_doc_type = t.id")
                query.where(query.Condition("l.source_link_type", self.id))
                rows = query.execute(self.cursor).fetchall()
                targets = dict()
                for row in rows:
                    doctype = Doctype(self.session, id=row.id, name=row.name)
                    targets[row.id] = doctype
                self._targets = targets
            else:
                self._targets = dict()
        return self._targets

    @property
    def chk_type(self):
        if not hasattr(self, "_chk_type"):
            if "chk_type" in self.__opts:
                self._chk_type = self.__opts["chk_type"]
            elif self.id:
                query = Query("link_type", "chk_type")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.cursor).fetchone()
                self._chk_type = row.chk_type if row else None
            else:
                self._chk_type = None
        if self._chk_type is not None:
            message = "Invalid check type {!r}".format(self._chk_type)
            assert self._chk_type in self.CHECK_TYPES, message
        return self._chk_type

    @property
    def id(self):
        if not hasattr(self, "_id"):
            if "id" in self.__opts:
                self._id = self.__opts["id"]
            elif self.name:
                query = Query("link_type", "id")
                query.where(query.Condition("name", self.name))
                row = query.execute(self.cursor).fetchone()
                self._id = row.id if row else None
            else:
                self._id = None
        return self._id

    @property
    def name(self):
        if not hasattr(self, "_name"):
            if "name" in self.__opts:
                self._name = self.__opts["name"]
            else:
                if not hasattr(self, "_id"):
                    if "id" in self.__opts:
                        self._id = self.__opts["id"]
                    else:
                        self._id = None
                if self.id:
                    query = Query("link_type", "name")
                    query.where(query.Condition("id", self.id))
                    row = query.execute(self.cursor).fetchone()
                    self._name = row.name if row else None
                else:
                    self._name = None
        return self._name

    def search(self, **opts):
        """
        Collect documents eligible to be linked with this link type

        Keyword arguments:
          pattern - titles of candidate target docs must match this pattern
          limit - optional integer restricting the size of the result set

        Return:
          possibly empty sequence of `Doc` objects
        """

        pattern = opts.get("pattern")
        limit = opts.get("limit")
        query = Query("document d", "d.id", "d.title").order("d.title")
        query.join("doc_type t", "t.id = d.doc_type")
        targets = list(self.targets)
        if len(targets) == 1:
            query.where(query.Condition("t.id", targets[0]))
        else:
            query.where(query.Condition("t.id", targets, "IN"))
        if pattern:
            query.where(query.Condition("d.title", pattern, "LIKE"))
        if limit:
            query.limit(int(limit))
        for property in self.properties:
            for condition in property.conditions:
                query.where(condition)
        rows = query.execute(self.cursor).fetchall()
        return [Doc(self.session, id=row.id, title=row.title) for row in rows]

    def save(self):
        action = "MODIFY LINKTYPE" if self.id else "ADD LINKTYPE"
        if not self.session.can_do(action):
            raise Exception("User not authorized to perform {}".format(action))
        if not self.targets:
            raise Exception("Link type {} allows no targets".format(self.name))
        if self.chk_type not in self.CHECK_TYPES:
            raise Exception("Invalid check type {!r}".format(self.chk_type))
        try:
            self.__save()
            self.session.conn.commit()
        except:
            self.session.logger.exception("LinkType.save() failure")
            self.cursor.execute("SELECT @@TRANCOUNT AS tc")
            if self.cursor.fetchone().tc:
                self.cursor.execute("ROLLBACK TRANSACTION")
            raise

    def __drop_related_rows(self):
        for table in ("link_xml", "link_target", "link_properties"):
            column = "link_id"
            if table == "link_target":
                column = "source_link_type"
            delete = "DELETE FROM {} WHERE {} = ?".format(table, column)
            self.cursor.execute(delete, (self.id,))

    def delete(self):
        if not self.session.can_do("DELETE LINKTYPE"):
            raise Exception("User not authorized to delete link types")
        if not self.id:
            raise Exception("Link type {} not found".format(self.name))
        try:
            self.__delete()
            self.session.conn.commit()
        except:
            self.session.logger.exception("LinkType.delete() failure")
            self.cursor.execute("SELECT @@TRANCOUNT AS tc")
            if self.cursor.fetchone().tc:
                self.cursor.execute("ROLLBACK TRANSACTION")
            raise

    def __delete(self):
        self.__drop_related_rows()
        delete = "DELETE FROM link_type WHERE id = ?"
        self.cursor.execute(delete, (self.id,))

    def __save(self):
        fields = dict(
            name=self.name,
            chk_type=self.chk_type,
            comment=self.comment
        )
        names = sorted(fields)
        values = [fields[name] for name in names]
        if self.id:
            values.append(self.id)
            assignments = ", ".join(["{} = ?".format(name) for name in names])
            sql = "UPDATE link_type SET {} WHERE id = ?".format(assignments)
        else:
            names = ", ".join(names)
            sql = "INSERT INTO link_type ({}) VALUES (?, ?, ?)".format(names)
        self.cursor.execute(sql, tuple(values))
        if self.id:
            self.__drop_related_rows()
        else:
            self.cursor.execute("SELECT @@IDENTITY as id")
            self._id = self.cursor.fetchone().id
        for source in self.sources:
            args = self.session, source.doctype, source.element
            linktype = self.lookup(*args)
            if linktype:
                args = linktype.name, source.doctype.name, source.element
                message = "Link type {} already defined for {}/{}"
                raise Exception(message.format(*args))
            names = "link_id, doc_type, element"
            values = self.id, source.doctype.id, source.element
            insert = "INSERT INTO link_xml ({}) VALUES (?, ?, ?)".format(names)
            self.cursor.execute(insert, values)
        for target in itervalues(self.targets):
            assert target.id, "doc type {} not found".format(target.name)
            names = "source_link_type, target_doc_type"
            values = self.id, target.id
            insert = "INSERT INTO link_target ({}) VALUES (?, ?)".format(names)
            try:
                self.cursor.execute(insert, values)
            except:
                self.session.logger.info("targets=%s", self.targets)
                raise
        for prop in (self.properties or []):
            names = "link_id, property_id, value, comment"
            values = self.id, prop.id, prop.value, prop.comment
            insert = "INSERT INTO link_properties ({}) VALUES (?, ?, ?, ?)"
            insert = insert.format(names)
            self.cursor.execute(insert, values)
    @classmethod
    def get_linktype_names(cls, session):
        """
        Fetch the list of all link type names

        Bypass the cache for this.
        """

        query = Query("link_type", "name").order("name")
        return [row.name for row in query.execute(session.cursor).fetchall()]

    @classmethod
    def get(cls, session, id):
        if id not in cls.TYPES:
            cls.TYPES[id] = cls(session, id)
        return cls.TYPES[id]

    @classmethod
    def lookup(cls, session, doctype, element_tag):
        """
        Find the `LinkType` object for this linking source

        Pass:
          session - logged-in sesion of CDR account
          doctype - `Doctype` for the linking document
          element_tag - string for the linking element's name

        Return:
          `LinkType` object or None
        """

        cursor = session.conn.cursor()
        query = Query("link_xml", "link_id")
        query.where(query.Condition("doc_type", doctype.id))
        query.where(query.Condition("element", element_tag))
        row = query.execute(cursor).fetchone()
        cursor.close()
        return cls(session, id=row.link_id) if row else None

    @classmethod
    def get_property_types(cls, session):
        """
        Fetch information from the link_prop_type table
        """

        cursor = session.conn.cursor()
        query = Query("link_prop_type", "name", "comment")
        rows = query.execute(cursor).fetchall()
        class PropertyType:
            def __init__(self, row):
                self.name = row.name
                self.comment = row.comment
        types = [PropertyType(row) for row in rows]
        cursor.close()
        return types

    class TargetType:
        def __init__(self, id, name):
            self.id = id
            self.name = name

    class Property:
        """
        Base class for link type properties

        These are used to refine the logic for determine which elements
        can link to which documents. At present we only have one such
        flavor of `Property`, for determining whether the target document
        contains specific values.
        """

        def __init__(self, session, name, value, comment):
            self.session = session
            self.cursor = session.conn.cursor()
            self.name = name
            self.value = value
            self.comment = comment

        @property
        def id(self):
            if hasattr(self, "_id"):
                return self._id
            query = Query("link_prop_type", "id")
            query.where(query.Condition("name", self.name))
            row = query.execute(self.cursor).fetchone()
            if row:
                self._id = row.id
                return self._id
            return None

    class LinkTargetContains(Property):
        """
        Restriction of allowable link targets by testing for specific values

        The value string for properties of this class identify one or
        more testable assertions, connected by "AND" or "OR" (case
        insensitive). Sub-sequences of assertions can be enclosed in
        parentheses in order to override the default precedence ("AND"
        binds more tightly than "OR"). An individual assertion consists
        of three parts: a path, matching the `path` column in the
        `query_term` table, an operator ("==" or "!=") used to test the
        assertion, and a double-quoted value (the value itself cannot
        contain the double-quote mark). There are alternate operators
        ("+=" for "==" and "-=" for "!=") which can be used for assertions
        which are only to be used when selecting documents for a picklist
        of candidates for targets of a link, but which are ignored when
        when validating an existing link (in that context the assertion
        always passes). Assertions (or parenthesized sequences of assertions)
        can be optionally prefixed with "NOT" to negate the result of the
        test.

        Some examples:

            /GlossaryTermName/TermNameStatus != "Rejected"

            /Term/TermType/TermTypeName == "Semantic type" &
            /Term/TermType/TermTypeName -= "Obsolete term"


            /Term/TermType/TermTypeName -= "Obsolete term" AND
            (/Term/TermType/TermTypeName=="Index term" OR
             /Term/TermType/TermTypeName=="Header term" OR
             /Term/TermType/TermTypeName=="Semantic type")

        Note that the order of the token tests in `PATTERN` is significant.
        In particular, the match for double-quoted values has to come first,
        because there no limit (other than disallowing the double-quote
        mark itself) on when can show up between the double-quote marks.
        """

        PATTERN = re.compile(r"""
            "[^"]*"          # double-quoted string for value to test for
          | /[^\s()|&=!+*-]+ # path value for query_term[_pub] table tests
          | [()|&*]          # single-character tokens
          | [=!+-]=          # double-character operator tokens
          | \bAND\b          # alias for & operator
          | \bOR\b           # alias for | operator
          | \bNOT\b          # negation of assertion
        """, re.VERBOSE | re.IGNORECASE)
        CONNECTORS = {"|": "OR", "&": "AND"}
        OPERATORS = {"==", "!=", "+=", "-="}

        def __init__(self, session, name, value, comment):
            LinkType.Property.__init__(self, session, name, value, comment)
            self.assertions = self.parse(value)

        @classmethod
        def parse(cls, property_string):
            """
            Extract the tokens from the property string (see class docs)
            """

            top = []
            stack = [top]
            path = operator = None
            for token in cls.PATTERN.findall(property_string):
                if token.startswith("/"):
                    path = token
                elif token in cls.OPERATORS:
                    operator = token
                elif token == "*" or token.startswith('"'):
                    negative = False
                    if top:
                        previous = top[-1]
                        if isinstance(previous, cls.Testable):
                            raise Exception("missing Boolean connector")
                        elif previous == "NOT":
                            negative = True
                            top.pop()
                    value = None if token == "*" else token.strip('"')
                    top.append(cls.Assertion(path, operator, value, negative))
                    path = operator = None
                elif token == "(":
                    top = []
                    stack.append(top)
                elif token == ")":
                    if len(stack) < 2:
                        raise Exception("unbalanced parentheses")
                    negative = False
                    nodes = stack.pop()
                    top = stack[-1]
                    if top:
                        previous = top[-1]
                        if isinstance(previous, cls.Testable):
                            raise Exception("missing Boolean connector")
                        elif previous == "NOT":
                            negative = True
                            top.pop()
                    top.append(cls.Assertions(nodes, negative))
                elif token in cls.CONNECTORS:
                    if not top or not isinstance(top[-1], cls.Testable):
                        raise Exception("misplaced Boolean connector")
                    top.append(cls.CONNECTORS[token])
                else:
                    token = token.upper()
                    if token == "NOT":
                        if top and top[-1] not in ("AND", "OR"):
                            raise Exception("misplaced 'NOT'")
                        top.append(token)
                    else:
                        if not top or not isinstance(top[-1], cls.Testable):
                            raise Exception("misplaced Boolean connector")
                        top.append(token)
            if len(stack) != 1:
                raise Exception("unbalanced parentheses")
            if not top:
                raise Exception("link property without assertions")
            if not isinstance(top[-1], cls.Testable):
                raise Exception("malformed link property specification")
            return cls.Assertions(top)

        def validate(self, link):
            if not self.assertions.test(link):
                error = "Failed link target rule: {}".format(self.value)
                link.add_error(error)

        #def refine_query(self, query):
        @property
        def conditions(self):
            """
            Add clauses needed to find link targets satisfying this property

            Passes on the work to the `Assertions` object.

            Pass:
              query - `db.Query` object to be refined with new conditions
            """

            return self.assertions.conditions
            for condition in self.assertions.conditions:
                query.where(condition)


        class Testable:
            """
            Base class for assertions which can be tested
            """


        class Assertions(Testable):
            """
            Sequence of zero or more assertions connected by Boolean logic
            """

            def __init__(self, nodes, negative=False):
                """
                Pass:
                  nodes - sequence of `Testable` objects, separated by
                          string nodes containing "AND" or "OR"
                  negative - return the opposite of the test results if True
                """

                self.nodes = nodes
                self.negative = negative

            def test(self, link):
                """
                Determine whether the link is valid against these assertions

                Standard precedence applies (AND binds more tightly than OR).
                Process the nodes by collapsing the results of each test
                into a single Boolean value as we go. With the AND connector,
                the cumulative result is True only if both sides of the
                connector (so far) resolve to True. For the OR connector,
                if what we've seen so far is True, we can ignore the rest
                of the tests in the sequence (standard short-circuit Boolean
                logic). In all other cases, we can ignore everything which
                came before the current test (either because this is the
                first test in the sequence, or what preceded was False
                followed by an OR connector).

                Pass:
                  link = `Link` object whose validity is being tested

                Return:
                  Boolean indicating whether the link is valid against
                  these test assertions
                """

                valid = True
                connector = None
                for node in self.nodes:
                    if isinstance(node, LinkType.LinkTargetContains.Testable):
                        result = node.test(link)
                        if connector == "AND":
                            valid = valid and result
                        elif connector == "OR" and valid:
                            break
                        else:
                            valid = result
                    else:
                        connector = node
                if self.negative:
                    valid = not valid
                return valid

            @property
            def conditions(self):
                """
                Assemble the list of conditions needed to satisfy this property

                Complicated! Be sure to include this in the code walkthrough!

                Because this is complicated enough, it's not wrapped in
                caching code. That's OK, because the way the code for the
                class is currently written, any given object's `conditions`
                property is only hit once. Be aware of this, though, and
                try to preserve that approach. If in the future you need
                to evaluate the value of this property more than one place
                in the code, be sure to store that value in your own local
                variable.

                Return:
                  sequence of `Query.Condition` objects
                """

                # At least one of these will be empty each time we hit the
                # top of the loop.
                ands = []
                ors = []

                # We haven't seen a connector yet.
                connector = None

                # Each node is a connector, assertion set, or assertion.
                for node in self.nodes:

                    # If the node is a string, it's a Boolean connector.
                    if isinstance(node, basestring):
                        connector = node

                        # If this is an "AND" and there's a sequence of "OR"
                        # conditions assembled on the left, fold them into
                        # the ANDed sequence.
                        if ors and connector == "AND":
                            ands = [Query.Or(*ors)]
                            ors = []

                        # Similarly, if this is the beginning of a new "OR"
                        # chain, start the chain by making the ANDed nodes
                        # the first node in the chain.
                        elif not ors and connector == "OR":

                            # If there's just one node on the left, enclosing
                            # it in parentheses as a group is unnecessary.
                            ors = [ands[0]] if len(ands) == 1 else [ands]

                            # These have been folded into the ORs; don't
                            # need them here any more.
                            ands = []

                    # Is this a nested set of assertions?
                    elif isinstance(node, self.__class__):

                        # If this set goes in the ORs pile, it goes as
                        # a single group.
                        if connector == "OR":
                            ors.append(node.conditions)

                        # Otherwise, each condition goes into the
                        # sequence of ANDed conditions
                        else:
                            ands += node.conditions

                    # The remaining possibility is a single assertion.
                    else:

                        # Find out which sequence it goes in.
                        sequence = ors if connector == "OR" else ands
                        sequence.append(node.condition)

                # If the last thing we saw was a condition preceded by "OR"
                # then the whole sequence of conditions is bundled as a single
                # set of (possibly nested) conditions joined by OR.
                if ors:
                    ands = [Query.Or(*ors)]

                # Return the results, but don't cache them.
                #print(ands)
                return ands





                # At least one of these will be empty each time we hit the
                # top of the loop.
                ands = []
                ors = []

                # We haven't seen a connector yet.
                connector = None

                # Each node is a connector, assertion, or assertion set.
                for node in self.nodes:

                    # The connector nodes are easy.
                    if isinstance(node, basestring):
                        connector = node

                    # The current node will be folded into a Query.Or object.
                    elif connector == "OR":

                        # Treat sequences of conditions as a unit for an OR.
                        if isinstance(node, self.__class__):
                            more = node.conditions
                        else:
                            more = node.condition

                        # If we've already started a chain of ORs, add to it.
                        if ors:
                            ors.append(more)

                        # Otherwise, start a new chain, folding in ANDed nodes.
                        else:

                            # If there's just one node on the left, enclosing
                            # it in parentheses as a group is unnecessary.
                            if len(ands) == 1:
                                ors = [ands[0], more]
                            else:
                                ors = [ands, more]

                            # These have been folded into the ORs; don't
                            # need them here any more.
                            ands = []

                    # If we got here, we're either on the right side of
                    # an AND connector, or we're at the beginning of the
                    # sequence of nodes. Is the current node a nested
                    # set of assertions?
                    elif isinstance(node, self.__class__):

                        # If we have a chain of nodes connected by OR,
                        # make that a unit as the first in a sequence
                        # of ANDed conditions.
                        if ors:
                            ands = [Query.Or(ors)] + node.conditions

                        # Otherwise, add the conditions to the sequence
                        # of nodes which must all be true
                        else:
                            ands += node.conditions

                    # This must be a node for a single assertion.
                    elif ors:

                        # We have a chain of nodes connected by OR,
                        # so make that a unit as the left side of an
                        # ANDed pair of conditions.
                        ands = [Query.Or(ors), node.condition]
                        ors = []

                    # Otherwise (no ORs hanging around), just pop the
                    # new condition on the end of the chain of conditions
                    # which must all be true.
                    else:
                        ands.append(node.condition)

                    """
                    if isinstance(node, LinkType.LinkTargetContains.Assertion):
                        if connector == "OR":
                            if ands:
                                if len(ands) == 1:
                                    ors = ands[0], node.condition
                                else:
                                    ors = ands, node.condition
                            else:
                                ors.append(node.condition)
                        else:
                            if ors:
                                ands = Query.Or(ors), node.condition
                                ors = []
                            else:
                                ands.append(node.condition)
                    elif isinstance(node, self.__class__):
                        if connector == "OR":
                            if ands:
                                if len(ands) == 1:
                                    ors = ands[0], node.conditions
                                else:
                                    ors = ands, node.conditions
                                ands = []
                            else:
                                ors.append(node.conditions)
                        else:
                            if ors:
                                ands = [Query.Or(ors)] + node.conditions
                            else:
                                ands += node.conditions
                    else:
                        connector = node
                    """

                # If the last thing we saw was a condition preceded by "OR"
                # then the whole sequence of conditions is bundled as a single
                # set of (possibly nested) conditions joined by OR.
                if ors:
                    ands = [Query.Or(ors)]

                # Return the results, but don't cache them.
                return ands

        class Assertion(Testable):
            def __init__(self, path, operator, value, negative=False):
                if not path or not operator:
                    raise Exception("malformed link property assertion")
                self.path = path
                self.operator = operator
                self.value = value
                self.negative = negative
                self.picklist_only = operator[0] in "+-"
            def test(self, link):
                if self.picklist_only:
                    result = True
                else:
                    doc_id = link.target_doc.id
                    cursor = link.doc.session.conn.cursor()
                    query = Query("query_term", "COUNT(*)")
                    query.where(query.Condition("doc_id", doc_id))
                    query.where(query.Condition("path", self.path))
                    if self.value:
                        query.where(query.Condition("value", self.value))
                    count = query.execute(cursor).fetchone()[0]
                    if self.operator == "==":
                        result = count > 0
                    else:
                        result = count == 0
                    if self.negative:
                        result = not result
                return result

            @property
            def condition(self):
                query = Query("query_term", "doc_id")
                query.where(query.Condition("path", self.path))
                if self.value:
                    query.where(query.Condition("value", self.value))
                negative = self.negative
                if self.operator[0] in "!-":
                    negative = not negative
                operator = "NOT IN" if negative else "IN"
                return query.Condition("d.id", query, operator)

            """
            def __repr__(self):
                return str(self)
            def __str__(self):
                query = [
                    "SELECT doc_id",
                    "FROM query_term",
                    "WHERE path = '{}'".format(self.path.replace("'", "''"))
                ]
                if self.value:
                    value = self.value.replace("'", "''")
                    query.append("AND value = '{}'".format(value))
                return " ".join(query)
            """

class Link:
    """
    An attribute in a CDR document representing a link to something

    The 'something' can be a link to another location within the same
    document, a link to another CDR document, or an external URL.

    Class attributes:
      CDR_ID - name of attribute uniquely identifying a document's node
               within that document
      CDR_REF - name of attribute identying a link to a CDR document
      CDR_HREF - name of attribute for an inline element linking to
                 a CDR document; unlike CDR_REF links, which are
                 denormalized by having their text content replaced
                 by the title of the linked document, the content of
                 CDR_HREF linking elements is preserved
      CDR_XREF - name of attribute containing an external link
      LINK_ATTRS - sequence of names for all possible possible linking
                   attributes
      INTERNAL_LINK_ATTRS - LINK_ATTRS without CDR_XREF
      VERSIONS - map of `LinkType.CHECK_TYPE` codes to strings used
                 for generically specifying which version of a document
                 to fetch

    Instance attributes:
      doc - reference to `Doc` object representing CDR document in which
            this link was found
      node - element node in which the link was found
      element - name (tag) of that element node
      link_name - name of the attribute containing the link
      url - linking value stored in the attribute
      internal - True iff the link connects to a CDR document
      key - tuple of linking element name and linking attribute value
      store - set to False by the `Doc` object if we already have seen
              another link in the document with the same `key` in which
              case we don't need to add a row to the `link_net` table
              for this link
      target_doc - reference to `Doc` object to which this link connects
                   (if any)
      fragment_id - string identifying specific node to which we are
                    linking if appropriate
      linktype - reference to `LinkType` object used to validate this
                 link
      eid - unique identifier of the link's element node, used for
            helping the client find the location of the linking element
            if any errors are found (this is an ephemeral identifier)
      id - stable unique identifier for this element node if it can
           be the explicit target of link
      nlink_attrs - count of the linking attributes found for the element
                    node (used for detecting the invalid condition of
                    and element with more than one linking attribute)
    """

    CDR_ID = Doc.qname("id")
    CDR_REF = Doc.qname("ref")
    CDR_HREF = Doc.qname("href")
    CDR_XREF = Doc.qname("xref")
    LINK_ATTRS = CDR_REF, CDR_HREF, CDR_XREF
    INTERNAL_LINK_ATTRS = {CDR_REF, CDR_HREF}
    VERSIONS = dict(C="Current", V="last", P="lastp")

    def __init__(self, doc, node):
        """
        Collect the linking information for this element node (if any)
        """

        # Start with a clean slate
        self.link_name = self.url = self.internal = self.store = None
        self.target_doc = self.fragment_id = self.linktype = None
        self.nlink_attrs = 0

        # Capture the values we were given.
        self.doc = doc
        self.node = node
        self.element = node.tag
        self.eid = node.get("cdr-eid")
        self.id = node.get(self.CDR_ID)

        # Check to see if we one (or more) linking attributes
        for name in self.LINK_ATTRS:
            value = node.get(name)
            if value:
                self.nlink_attrs += 1

                # We only save the value for the first linking attribute.
                if not self.link_name:
                    self.link_name = name
                    self.url = value
                    self.internal = name in self.INTERNAL_LINK_ATTRS
                    self.key = self.element, value

        # Collect the information that's only relevant to internal links.
        if self.internal:
            if "#" in self.url:
                doc_id, self.fragment_id = self.url.split("#", 1)
            else:
                doc_id = self.url
            args = doc.session, doc.doctype, node.tag
            self.linktype = LinkType.lookup(*args)
            self.chk_type = self.linktype.chk_type if self.linktype else "C"
            version = self.VERSIONS[self.chk_type]
            try:
                target_doc = Doc(doc.session, id=doc_id, version=version)
                assert target_doc.doctype, "version not found"
                self.target_doc = target_doc
            except:
                self.store = False

    def save(self, cursor):
        """
        Remember the link in the `link_net` table

        Pass:
          cursor - session's object for executing the INSERT statement
        """

        target_doc = self.target_doc
        target_doc_id = target_doc.id if target_doc else None
        fields = dict(
            source_doc=self.doc.id,
            link_type=self.linktype.id,
            source_elem=self.element,
            target_doc=target_doc_id,
            target_frag=self.fragment_id,
            url=self.url
        )
        names = sorted(fields)
        placeholders = ["?"] * len(names)
        args = ", ".join(names), ", ".join(placeholders)
        insert = "INSERT INTO link_net ({}) VALUES ({})".format(*args)
        cursor.execute(insert, tuple([fields[name] for name in names]))

    def add_error(self, message):
        """
        Record a linking validation failure in the document's error log.

        Pass:
          message - string describing the error (the `Link` object knows
                    everything else that's needed)
        """

        self.doc.add_error(message, self.eid, type=self.doc.LINK)

    def validate(self, **opts):
        """
        Find out whether this link meets its requirements
        """

        # Make sure the document we're linking to actually exists.
        if not self.target_doc:
            what = LinkType.CHECK_TYPES[self.chk_type]
            message = "No {} found for link target".format(what)
            self.add_error(message)
            return

        # Check links to a specific location in the target document.
        if self.fragment_id:
            table = "query_term_pub" if self.chk_type == "P" else "query_term"
            if self.target_doc.id == self.doc.id:
                found = self.fragment_id in self.doc.frag_ids
            else:
                found = False
                query = Query(table, "value")
                query.where(query.Condition("doc_id", self.target_doc.id))
                query.where(query.Condition("value", self.fragment_id))
                query.where("path LIKE '%@cdr:id'")
                for row in query.execute(self.doc.cursor).fetchall():
                    if self.fragment_id == row.value:
                        found = True
                        break
            if not found:
                template = "Fragment {} not found in target document"
                message = template.format(self.fragment_id)
                self.add_error(message)

        # Make sure we're allowed to link from this element.
        if not self.linktype:
            message = "linking not allowed from {}".format(self.element)
            self.add_error(message)
            return

        # Make sure we're linking to an allowed document type.
        if self.target_doc.doctype.id not in self.linktype.targets:
            pattern = "linking from {} to {} documents not permitted"
            doctype = self.target_doc.doctype.name
            message = pattern.format(self.element, doctype)
            self.add_error(message)
            return

        # Check any custom rules for the link type.
        for property in self.linktype.properties:
            property.validate(self)

class FilterSet:
    def __init__(self, session, **opts):
        """
        """
        self.__session = session
        self.__opts = opts
        self.session.logger.info("FilterSet(opts=%s)", opts)
    @property
    def cursor(self):
        if not hasattr(self, "_cursor"):
            self._cursor = self.session.conn.cursor()
        return self._cursor

    @property
    def id(self):
        if not hasattr(self, "_id"):
            self._id = int(self.__opts.get("id", 0)) or None
            if not self._id and self.name:
                query = Query("filter_set", "id")
                query.where(query.Condition("name", self.name))
                row = query.execute(self.cursor).fetchone()
                self._id = row.id if row else None
        return self._id
    @property
    def name(self):
        if not hasattr(self, "_name"):
            self._name = self.__opts.get("name")
            if not self._name:
                if not hasattr(self, "_id"):
                    self._id = int(self.__opts.get("id", 0)) or None
                if self.id:
                    query = Query("filter_set", "name")
                    query.where(query.Condition("id", self.id))
                    row = query.execute(self.cursor).fetchone()
                    self._name = row.name if row else None
        return self._name
    @property
    def description(self):
        if not hasattr(self, "_description"):
            if "description" in self.__opts:
                self._description = self.__opts["description"]
            elif self.id:
                query = Query("filter_set", "description")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.cursor).fetchone()
                self._description = row.description if row else None
            else:
                self._description = None
        return self._description

    @property
    def members(self):
        if not hasattr(self, "_members"):
            if "members" in self.__opts:
                self._members = self.__opts["members"]
            elif self.id:
                self._members = []
                query = Query("filter_set_member", "filter", "subset")
                query.where(query.Condition("filter_set", self.id))
                query.order("position")
                rows = query.execute(self.cursor).fetchall()
                for filter_id, set_id in rows:
                    if filter_id:
                        self._members.append(Doc(self.session, id=filter_id))
                    else:
                        filter_set = FilterSet(self.session, id=set_id)
                        self._members.append(filter_set)
            else:
                self._members = []
        return self._members

    @property
    def notes(self):
        if not hasattr(self, "_notes"):
            if "notes" in self.__opts:
                self._notes = self.__opts["notes"]
            elif self.id:
                query = Query("filter_set", "notes")
                query.where(query.Condition("id", self.id))
                row = query.execute(self.cursor).fetchone()
                self._notes = row.notes if row else None
            else:
                self._notes = None
        return self._notes

    @property
    def session(self):
        return self.__session

    def delete(self):
        if not self.session.can_do("DELETE FILTER SET"):
            raise Exception("User not authorized to delete filter sets.")
        if not self.id:
            if self.name:
                raise Exception("Can't find filter set {}".format(self.name))
            else:
                raise Exception("No filter set identified for deletion")
        query = Query("filter_set_member", "COUNT(*) AS n")
        query.where(query.Condition("subset", self.id))
        if query.execute(self.cursor).fetchone().n > 0:
            raise Exception("Can't delete set which is itself a set member")
        try:
            return self.__delete()
        except:
            self.session.logger.exception("Validation failed")
            self.cursor.execute("SELECT @@TRANCOUNT AS tc")
            if self.cursor.fetchone().tc:
                self.cursor.execute("ROLLBACK TRANSACTION")
            raise

    def __delete(self):
        tables = [
            ("filter_set_member", "filter_set"),
            ("filter_set", "id")
        ]
        for table, column in tables:
            sql = "DELETE FROM {} WHERE {} = ?".format(table, column)
            self.cursor.execute(sql, (self.id,))
        self.session.conn.commit()

    def save(self):
        action = "MODIFY FILTER SET" if self.id else "ADD FILTER SET"
        if not self.session.can_do(action):
            what = "modify" if self.id else "add"
            message = "User not authorized to {} filter sets.".format(what)
            raise Exception(message)
        try:
            return self.__save()
        except:
            self.session.logger.exception("Validation failed")
            self.cursor.execute("SELECT @@TRANCOUNT AS tc")
            if self.cursor.fetchone().tc:
                self.cursor.execute("ROLLBACK TRANSACTION")
            raise

    def __save(self):
        fields = dict(
            name=self.name,
            description=self.description,
            notes=self.notes
        )
        names = sorted(fields)
        values = [] #fields[name] for name in names]
        # BUG IN ADODBAPI WHEN TRYING TO INSERT NULL INTO NTEXT WITH NONE
        if self.id:
            assignments = []
            for name in names:
                value = fields[name]
                if value is None:
                    assignments.append("{} = NULL".format(name))
                else:
                    assignments.append("{} = ?".format(name))
                    values.append(value)
            assignments = ", ".join(assignments)
            values.append(self.id)
            sql = "UPDATE filter_set SET {} WHERE id = ?".format(assignments)
        else:
            placeholders = []
            for name in names:
                value = fields[name]
                if value is None:
                    placeholders.append("NULL")
                else:
                    placeholders.append("?")
                    values.append(value)
            names = ", ".join(names)
            ph = ", ".join(placeholders)
            sql = "INSERT INTO filter_set ({}) VALUES ({})".format(names, ph)
        self.session.logger.info("sql=%s values=%s", sql, tuple(values))
        self.cursor.execute(sql, values)
        if not self.id:
            self.cursor.execute("SELECT @@IDENTITY AS id")
            self._id = self.cursor.fetchone().id
        else:
            delete = "DELETE FROM filter_set_member WHERE filter_set = ?"
            self.cursor.execute(delete, (self.id,))
        names = "filter_set", "position", "filter", "subset"
        args = ", ".join(names), ", ".join(["?"] * len(names))
        insert = "INSERT INTO filter_set_member ({}) VALUES ({})".format(*args)
        position = 1
        for member in self.members:
            if isinstance(member, Doc):
                values = self.id, position, member.id, None
            else:
                values = self.id, position, None, member.id
            self.cursor.execute(insert, values)
            position += 1
        self.session.conn.commit()
        return len(self.members)

    @classmethod
    def get_filter_sets(cls, session):
        query = Query("filter_set", "id", "name").order("name")
        return [tuple(row) for row in query.execute(session.cursor).fetchall()]
    @classmethod
    def get_filters(cls, session):
        query = Query("document d", "d.id", "d.title").order("d.title")
        query.join("doc_type t", "t.id = d.doc_type")
        query.where("t.name = 'Filter'")
        rows = query.execute(session.cursor).fetchall()
        return [Doc(session, id=row.id, title=row.title) for row in rows]


class GlossaryTermName:

    UNWANTED = re.compile(u"""['".,?!:;()[\]{}<>\u201C\u201D\u00A1\u00BF]+""")
    TOKEN_SEP = re.compile(r"[\n\r\t -]+")

    def __init__(self, id, name):
        self.id = id
        self.name = name or None
        self.phrases = set()
    @classmethod
    def get_mappings(cls, session, language="en"):
        names = dict()
        phrases = set()
        name_tag = "TermName" if language == "en" else "TranslatedName"
        n_path = "/GlossaryTermName/{}/TermNameString".format(name_tag)
        s_path = "/GlossaryTermName/TermNameStatus"
        e_path = "/GlossaryTermName/{}/@ExcludeFromGlossifier".format(name_tag)
        e_cond = ["e.doc_id = n.doc_id", "e.path = '{}'".format(e_path)]
        if language == "es":
            e_cond.append("LEFT(n.node_loc, 4) = LEFT(e.node_loc, 4)")
        query = Query("query_term n", "n.doc_id", "n.value")
        query.join("query_term s", "s.doc_id = n.doc_id")
        query.outer("query_term e", *e_cond)
        query.where(query.Condition("n.path", n_path))
        query.where(query.Condition("s.path", s_path))
        query.where("s.value <> 'Rejected'")
        query.where("(e.value IS NULL OR e.value <> 'Yes')")
        for doc_id, name in query.execute(session.cursor).fetchall():
            term_name = names[doc_id] = GlossaryTermName(doc_id, name)
            phrase = cls.normalize(name)
            if phrase and phrase not in phrases:
                phrases.add(phrase)
                term_name.phrases.add(phrase)
        query = Query("external_map m", "m.doc_id", "m.value")
        query.join("external_map_usage u", "u.id = m.usage")
        prefix = "" if language == "en" else "Spanish "
        usage = prefix + "GlossaryTerm Phrases"
        query.where(query.Condition("u.name", usage))
        for doc_id, name in query.execute(session.cursor).fetchall():
            term_name = names.get(doc_id)
            if term_name is not None:
                phrase = cls.normalize(name)
                if phrase and phrase not in phrases:
                    phrases.add(phrase)
                    term_name.phrases.add(phrase)
        return list(names.values())

    @classmethod
    def normalize(cls, phrase):
        phrase = cls.UNWANTED.sub(u"", cls.TOKEN_SEP.sub(u" ", phrase)).upper()
        return phrase.strip()
