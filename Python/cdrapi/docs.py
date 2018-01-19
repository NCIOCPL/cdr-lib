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
from cdrapi.db import Query


# ----------------------------------------------------------------------
# Try to make the module compatible with both Python 2 and 3.
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

    # The XML namespace used by CDR documents for links and fragment IDs
    NS = "cips.nci.nih.gov/cdr"
    NSMAP = {"cdr": NS}

    # Validation status codes (stored in `val_status` columns)
    UNVALIDATED = "U"
    VALID = "V"
    INVALID = "I"
    MALFORMED = "M"

    # Status codes indicating whether a document is blocked or deleted
    ACTIVE = "A"
    BLOCKED = INACTIVE = "I"
    DELETED = "D"
    VALIDATION_TEMPLATE = None
    VALIDATION = "validation"

    # Type and level values for error messages
    LINK = "link"
    OTHER = "other"
    LEVEL_OTHER = "other"
    LEVEL_INFO = "info"
    LEVEL_WARNING = "warning"
    LEVEL_ERROR = "error"
    LEVEL_FATAL = "fatal"

    # Value size constraints
    MAX_TITLE_LEN = 255
    MAX_COMMENT_LEN = 255
    MAX_SQLSERVER_INDEX_SIZE = 800
    MAX_INDEX_ELEMENT_DEPTH = 40
    INDEX_POSITION_WIDTH = 4
    MAX_LOCATION_LENGTH = INDEX_POSITION_WIDTH * MAX_INDEX_ELEMENT_DEPTH

    # Patterns for generating the values for columns in the query term tables
    HEX_INDEX = "{{:0{}X}}".format(INDEX_POSITION_WIDTH)
    INTEGERS = re.compile(r"\d+")

    # Codes indicating which markup revision should be applied
    REVISION_LEVEL_PUBLISHED = 3
    REVISION_LEVEL_PUBLISHED_OR_APPROVED = 2
    REVISION_LEVEL_PUBLISHED_OR_APPROVED_OR_PROPOSED = 1
    DEFAULT_REVISION_LEVEL = REVISION_LEVEL_PUBLISHED

    # Optimization for mailer cleanup, avoiding mailers from the Oracle system
    LEGACY_MAILER_CUTOFF = 390000

    # Error messages for exceptions raised when a version can't be found
    NOT_VERSIONED = "document not versioned"
    NO_PUBLISHABLE_VERSIONS = "no publishable version found"

    def __init__(self, session, **opts):
        """
        Capture the session and options passed by the caller

        Called by:
          cdr.getDoc()
          client XML wrapper command CdrGetDoc

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
        if hasattr(self, "_creation"):
            return self._creation
        query = Query("audit_trail t", "t.dt", "u.id", "u.name", "u.fullname")
        query.join("action a", "a.id = t.action")
        query.join("usr u", "u.id = t.usr")
        query.where(query.Condition("t.document", self.id))
        query.where("a.name = 'ADD DOCUMENT'")
        row = query.execute(self.cursor).fetchone()
        if not row:

            # A small handful of documents bootstrapped the system without
            # the audit trail on June 22, 2002.
            if self.id > 374:
                raise Exception("No audit trail for document creation")
            class Action:
                def __init__(self, when, user):
                    self.when = when
                    self.user = user
            when = datetime.datetime(2002, 6, 22, 7)
            user = Doc.User(2, "bkline", "Bob Kline")
            self._creation = Action(when, user)
        else:
            self._creation = self.Action(row)
        return self._creation

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

        # Pull out the version-related options passed into the constructor.
        self.session.logger.debug("@version: __opts = %s", self.__opts)
        version = self.__opts.get("version")
        cutoff = self.__opts.get("before")

        # If we've done this before, the version integer has been cached
        if not hasattr(self, "_version") or self._version is None:

            # Handle the obvious case first.
            if str(version).isdigit():
                self._version = int(version) or None

            # If the document hasn't been saved (no ID) it has no version.
            elif not self.id:
                self._version = None

            # Look up any "before this date" versions.
            elif cutoff:
                lastp = str(version).startswith("lastp")
                self._version = self.__get_version_before(cutoff, lastp)

            # See if this is an object for the current working document.
            elif not version:
                self._version = None

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

                # We've run out of valid options.
                else:
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

        Called by:
          cdr.addExternalMapping()
          client XML wrapper command CdrAddExternalMapping

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
        self.session.log("add_external_usage({!r}, {!r})".format(usage, value))
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

        Called by:
          cdr.unlock()
          client XML wrapper command CdrCheckIn

        Optional keyword arguments:
          force - if True, try to check in even if locked by another account
          comment - optional string to update comment (to NULL if empty)
          abandon - if True, don't save unversioned changes as a new version
          publishable - if True, mark version publishable if we create one
        """

        self.session.log("Doc.check_in({!r}, {!r})".format(self.cdr_id, opts))
        self.__check_in(audit=True, **opts)
        try:
            self.session.conn.commit()
        except:
            # Might not be anything to commit.
            pass

    def check_out(self, **opts):
        """
        Lock the document for editing

        Public wrapper for __check_out(), commiting changes to the database

        Called by:
          cdr.checkOutDoc()
          client XML wrapper command CdrCheckOut

        Optional keyword arguments:
          force - if True, steal the lock if necessary (and allowed)
          comment - optional string for the `checkout.comment` column
        """

        self.session.log("Doc.check_out({!r}, {!r})".format(self.cdr_id, opts))
        self.__check_out(**opts)
        self.session.conn.commit()

    def delete(self, **opts):
        """
        Mark the document as deleted

        We don't actually remove the document or any of its versions
        from the repository. We just set the `active_status` column
        to 'D' so it drops out of the `document` view.

        Called by:
          cdr.delDoc()
          cdr.lastVersions()
          client XML wrapper command CdrDelDoc
          client XML wrapper command CdrLastVersions

        Optional keyword arguments:
          validate - if True, make sure nothing links to the document
          reason - string to be recorded in the audit trail
        """

        # Make sure the audit trail records don't step on each other.
        self.session.log("Doc.delete({!r}, {!r})".format(self.cdr_id, opts))
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

        Called by:
          cdr.filterDoc()
          client XML wrapper command CdrFilter
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

        args = self.id, filters, opts
        self.session.log("Doc.filter({!r}, {!r}, {!r})".format(*args))
        for spec in filters:
            if not spec:
                raise Exception("missing filter spec")
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
                if f.doc_id:
                    self.session.logger.debug("applying filter %d", f.doc_id)
                else:
                    self.session.logger.debug("applying in-memory filter")
                result = self.__apply_filter(f.xml, doc, parser, **parms)
                doc = result.result_tree
                self.session.logger.debug("filter result: %r", str(doc))
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
            self.session.logger.debug("filter() finished")

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
        self.session.logger.debug("get_filter(%s, %r)", doc.id, opts)
        if doc.version:
            with self.session.cache.filter_lock:
                f = self.session.filters.get(key)
                if f is not None:
                    return f
        root = doc.root

        # TODO - REMOVE FOLLOWING CODE WHEN GAUSS LANDS ON DEV
        if self.session.tier.name == "DEV":
            query = Query("good_filters", "xml")
            query.where(query.Condition("id", doc.id))
            row = query.execute(self.cursor).fetchone()
            if not row:
                raise Exception("filter {} not found".format(doc.cdr_id))
            root = etree.fromstring(row.xml.encode("utf-8"))
        else:
        # TODO - END TEMPORARY CODE -- OUTDENT NEXT LINE!!! XXX

            # OUTDENT THIS LINE ONE LEVEL WHEN TEMPORARY CODE ABOVE GOES AWAY!
            root = doc.root
        for name in ("import", "include"):
            qname = Doc.qname(name, Filter.NS)
            for node in root.iter(qname):
                href = node.get("href")
                node.set("href", href.replace(" ", "%20"))
        xml = etree.tostring(root.getroottree(), encoding="utf-8")
        if doc.version:
            with self.session.cache.filter_lock:
                if key not in self.session.cache.filters:
                    self.session.cache.filters[key] = Filter(doc.id, xml)
                return self.session.cache.filters[key]
        else:
            return Filter(doc.id, xml)

    def get_tree(self, depth=1):
        """
        Fetch parents and children of this Term document

        Called by:
          cdr.getTree()
          client XML wrapper command CdrGetTree

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
        args = self.id, depth
        self.session.log("Doc.get_tree({!r}, depth={!r})".format(*args))
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

        Called by:
          cdr.label_doc()
          client XML wrapper command CdrLabelDocument

        Pass:
          label - string for this label's name
        """

        self.session.log("Doc.label({!r}, {!r})".format(self.id, label))
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
          denormalize - if True and including XML, pass through fast
                        denormalization filter
          locators - if True and including XML, use the version that has
                     error location IDs
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
        ready = "Y" if self.ready_for_review else "N"
        blocked = "Y" if self.active_status == self.BLOCKED else "N"
        doc_control.set("readyForReview", ready)
        doc_control.set("blocked", blocked)
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
                try:
                    child.text = unicode(value)
                except:
                    #print(repr((tag, value)))
                    raise
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
        """
        Find out which documents link to this one

        Called by:
          cdr.get_links()
          client XML wrapper command CdrGetLinks

        Return:
          sequence of strings describing each inbound link
        """

        self.session.log("Doc.link_report({})".format(self.id))
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
        """
        Find information about the latest versions of this document

        Called by:
          cdr.listVersions()
          client XML wrapper command CdrListVersions

        Return:
          sequence of tuples, latest versions first, each tuple containing:
            * integer for the version number
            * date/time the version was saved
            * comment for the version, if any (otherwise None)
        """

        self.session.log("Doc.list_versions({}, {!r})".format(self.id, limit))
        fields = "num AS number", "dt AS saved", "comment"
        query = Query("doc_version", *fields).order("num DESC")
        query.where(query.Condition("id", self.id))
        if limit is not None:
            query.limit(limit)
        return list(query.execute(self.cursor).fetchall())

    def reindex(self):
        """
        Repopulate the search support tables for this document

        Called by:
          cdr.reindex()
          client XML wrapper command CdrReindexDoc

        Return:
          None
        """

        # Make sure the document is in the repository.
        if not self.id:
            raise Exception("reindex(): missing document id")

        # Make sure the object we have represents the latest XML.
        self.session.log("Doc.reindex({})".format(self.id))
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

        Called by:
          cdr.addDoc()
          cdr.repDoc()
          client XML wrapper command CdrAddDoc
          client XML wrapper command CdrRepDoc

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

        Return:
          None
        """

        self.session.log("Doc.save({}, {!r})".format(self.id, opts))
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

        Called by:
          cdr.setDocStatus()
          client XML wrapper command CdrSetDocStatus

        Pass:
          status - "A" (active) or "I" (inactive); required
          comment - optional keyword argument for string describing the change

        Return:
          None
        """

        args = self.id, status, opts
        self.session.log("Doc.set_status({}, {!r}, {!r})".format(*args))
        if not self.doctype:
            raise Exception("Document not found")
        if not self.session.can_do("PUBLISH DOCUMENT", self.doctype.name):
            message = "User not authorized to change status of {} documents"
            raise Exception(message.format(self.doctype.name))
        valid = self.ACTIVE, self.INACTIVE
        if status not in valid:
            raise Exception("Status must be {} or {}".format(*valid))
        args = self.active_status, status
        self.session.logger.debug("Old status=%r new status=%r", *args)
        if status != self.active_status:
            try:
                self.__audit_trail_delay()
                self.__set_status(status, **opts)
                self.session.conn.commit()
                self._active_status = status
                self.session.logger.debug("New status committed")
            except:
                self.session.logger.exception("Doc.set_status() failure")
                self.cursor.execute("SELECT @@TRANCOUNT AS tc")
                if self.cursor.fetchone().tc:
                    self.cursor.execute("ROLLBACK TRANSACTION")
                raise

    def unlabel(self, label):
        """
        Apply a label to a specific version of this document

        Called by:
          cdr.unlabel_doc()
          client XML wrapper command CdrUnlabelDocument

        Pass:
          label - string for this label's name
        """

        self.session.log("Doc.unlabel({}, {!r})".format(self.id, label))
        query = Query("version_label", "id")
        query.where(query.Condition("name", label))
        row = query.execute(self.cursor).fetchone()
        if not row:
            raise Exception("Unable to find label {!r}".format(label))
        table = "doc_version_label"
        delete = "DELETE FROM {} WHERE document = ? AND label = ?"
        self.cursor.execute(delete.format(table), (self.id, row.id))
        self.session.conn.commit()

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

    def update_title(self):
        """
        Regenerate the document's title using the document type's title filter

        Called by:
          cdr.updateTitle()
          client XML wrapper command CdrUpdateTitle

        Return:
          True if the document's title was changed; otherwise False
        """

        self.session.log("Doc.update_title({})".format(self.id))
        if self.id and self.title is not None:
            title = self.__create_title()
            if title is not None and self.title != title:
                update = "UPDATE all_docs SET title = ? WHERE id = ?"
                self.cursor.execute(update, (title, self.id))
                self.session.conn.commit()
                self._title = title
                return True
        return False

    def validate(self, **opts):
        """
        Determine whether the document conforms to the rules for its type

        External wrapper for __validate(), committing changes to database.

        Called by:
          cdr.valDoc()
          client XML wrapper command CdrValidateDoc

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
        start = datetime.datetime.now()
        self.session.log("Doc.validate({}, {!r})".format(self.id, opts))
        self._errors = []
        try:
            self.__validate(**opts)

            # Find out if there are changes to the database; if so, commit them.
            self.cursor.execute("SELECT @@TRANCOUNT AS tc")
            if self.cursor.fetchone().tc:
                self.session.conn.commit()
            elapsed = (datetime.datetime.now() - start).total_seconds()
            self.session.logger.info("validated doc in %f seconds", elapsed)
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

        for name in parms:
            if isinstance(parms[name], basestring):
                parms[name] = etree.XSLT.strparam(parms[name])
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
        doc = root if root is not None else self.root
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
        """
        Make sure we don't hit a primary key constraint for the audit table

        Mike made the audit trail's primary key a composite including
        a date/time field, and he used that field to connect rows in this
        table with rows in the doc_version table (it's a view now, but
        it was a table back then). That worked almost all of the time
        because SQL Server stores times down to a few milliseconds of
        granularity. However, there's a bug in the adodbapi package we're
        using now, which causes INSERTs to fail if a DATETIME value has
        too much precision. I may try and tackle that bug and possibly
        come up with a patch at some point in the future, but for now
        I'm working around the bug by stripping down the granularity
        for the `audit_trail.dt` column values to whole seconds. As a
        result, we sometimes (generally when we're testing the API,
        and not so much in real world usage) need to introduce a delay
        to make sure we don't try to insert a row into the table
        for the same document ID/date-time combination.
        """

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
          audit - if True, audit the unlock action
        """

        # Make sure there's a lock to release.
        lock = self.lock
        if lock is None:
            self.session.logger.warning("Document is not checked out")
            return

        # See if the document is locked by another account.
        if lock.locker.id != self.session.user_id:
            if opts.get("force"):
                if not self.session.can_do("FORCE CHECKOUT", doctype):
                    raise Exception(str(lock))
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

        # If the unlock is a standalone action, log it.
        if opts.get("audit"):
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
        val_types = opts.get("val_types") or []
        if opts.get("set_links") == False and "links" in val_types:
            raise Exception("Cannot validate links without setting them")

        # Make sure full validation performed if creating a publishable verion.
        if opts.get("publishable"):
            if not self.last_publishable_version:
                if not self.session.can_do("PUBLISH DOCUMENT", doctype):
                    message = "user not authorized to create first pub version"
                    raise Exception(message)
            if "schema" not in val_types or "links" not in val_types:
                if doctype.lower() != "filter":
                    message = "publishable version requires full validation"
                    raise Exception(message)

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
        start = datetime.datetime.now()
        self.session.logger.debug("top of __collect_links()")
        self._frag_ids = set()
        links = []
        unique_links = set()

        # Find all the links to CDR documents.
        namespaces = dict(cdr="cips.nci.nih.gov/cdr")
        for local_name in ("ref", "href"):
            xpath = "//*[@cdr:{}]".format(local_name)
            name = Doc.qname(local_name)
            for node in doc.xpath(xpath, namespaces=namespaces):
                link = Link(self, node, name)
                links.append(link)
                link.store = link.key not in unique_links
                if link.store:
                    unique_links.add(link.key)

        # Find all of the target fragment IDs in this document.
        for node in doc.xpath("//*[@cdr:id]", namespaces=namespaces):
            cdr_id = node.get(Link.CDR_ID)
            if cdr_id in self._frag_ids:
                message = "cdr:id {!r} used more than once".format(cdr_id)
                self.add_error(message, node.get("cdr-eid"), type=self.LINK)
            else:
                self._frag_ids.add(cdr_id)

        # Return the sequence of `Link` objects we found for 'internal' links.
        elapsed = (datetime.datetime.now() - start).total_seconds()
        args = len(links), elapsed
        self.session.logger.info("collected %d links in %f seconds", *args)
        self.session.logger.debug("checked %d frag ids", len(self._frag_ids))
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
        namespace = "{{{}}}".format(Doc.NS)
        for name in node.attrib:
            prefixed_name = name.replace(namespace, "cdr:")
            full_attr_path = "{}/@{}".format(path, prefixed_name)
            wild_attr_path = "//@{}".format(prefixed_name)
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
        if not row or not row.title_filter:
            message = "doctype {} has no title filter"
            self.session.logger.warning(message.format(self.doctype.name))
            return None
        try:
            opts = dict(doc=self.resolved)
            return unicode(self.filter(row.title_filter, **opts).result_tree)
        except:
            self.session.logger.exception("__create_title() failure")
            return None

    def __create_version(self, **opts):
        """
        Add a row to the `all_doc_versions` table

        Note that we're storing two datetime values in the table row.
        One (`dt`) represents when the row was created. The other
        (`updated_dt`) matches the `dt` value in the audit table row
        which was created when we updated the values in the `all_docs`
        table (or created the row, for a new document). I realize
        that's a squirrelly way to link rows from two tables, but
        that's how Mike did it, and a bunch of software assumes that's
        how it works. We grind the value down to whole-second granularity
        to work around a bug in the adodbapi package.

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
        information indefinitely), an exception to the general
        principle that we don't discard versioned document information
        from the repository once we've accepted and stored it.
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
            blob_ids |= set([row.blob_id for row in rows])
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

        Return:
          nothing (side effect is population of `sets`)
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
                    highest_fragment_id += 1
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

        Pass:
          label - string for the name of the labeled version to find

        Return:
          integer version number matching the label

        Raise:
          Exception if the version doesn't exist
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
                message = "unrecognized date/time format: {!r}".format(before)
                raise Exception(message)

        # Workaround for bug in adodbapi.
        when = when.replace(microsecond=0)

        query = Query("doc_version", "MAX(num) AS n")
        query.where(query.Condition("id", self.id))
        query.where(query.Condition("dt", when, "<"))
        if publishable is True:
            query.where("publishable = 'Y'")
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
                node.set("cdr-eid", "_{:d}".format(eid))
                eid += 1

    def __namespaces_off(self, root):
        """
        Rename attributes with namespaces to non-colonized names

        We do this because the XML Schema specification does not
        support validation of attributes with namespace-qualified
        attribute names across nested schema documents, when the
        attributes' constraints need to change from one complex
        type to another. Basic pattern is:
          1. {CDR-NS}xxx => cdr-xxx for all cdr: attributes
          2. perform schema validation on the document
        """

        if root is not None:
            NS = "{{{}}}".format(self.NS)
            for node in root.iter("*"):
                for name in node.attrib:
                    if name.startswith(NS):
                        ncname = name.replace(NS, "cdr-")
                        node.set(ncname, node.get(name))
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
        self.session.logger.debug("__preprocess_save() finished")

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
                self.session.logger.debug("__validate() finished")
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
                if self.id:
                    resolved = self.resolved
                    if  resolved is not None:
                        self.__store_links(self.__collect_links(resolved))
                        self.session.logger.debug("__store_links() finished")

        # If the document already existed, we still need to store it.
        if not new:
            self.__store(**opts)
            self.session.logger.debug("__store() finished")

        # Index the document for searching.
        if self.is_content_type:
            index_tables = ["query_term"]
            if opts.get("publishable"):
                index_tables.append("query_term_pub")
            self.update_query_terms(tables=index_tables)
            self.session.logger.debug("update_query_terms() finished")

        # Remember who performed this save action.
        action = "ADD DOCUMENT" if new else "MODIFY DOCUMENT"
        reason = opts.get("reason") or opts.get("comment")
        when = self.__audit_action("Doc.save", action, reason)
        self.session.logger.debug("__audit_action() finished")
        if status_action:
            self.__audit_added_action(status_action, when)
            self.session.logger.debug("__audit_added_action finished")

        # Special case processing to eliminate sensitive meeting recordings.
        if opts.get("del_blobs"):
            self.__delete_blobs()
            self.session.logger.debug("__delete_blobs() finished")

        # Create a permanent frozen version of the document if requested.
        if opts.get("version") or opts.get("publishable"):
            self.__create_version(**opts)
            self.session.logger.debug("__create_version() finished")

        # Check the document back in unless caller wants to keep it locked.
        if not new and opts.get("unlock"):

            # Most of self.check_in() is unnecessary, so UPDATE directly.
            update = "UPDATE checkout SET dt_in = GETDATE(), comment = ? "
            update += "WHERE id = ? AND dt_in IS NULL"
            values = reason, self.id
            self.cursor.execute(update, values)
            self.session.logger.debug("checkout table updated")

        self.session.logger.debug("__save() finished")


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
        active_status = opts.get("active_status", self.active_status)
        fields = {
            "val_status": self.val_status or self.UNVALIDATED,
            "active_status": active_status or self.ACTIVE,
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

            # TODO: ELIMINATE THIS BLOCK OF CODE WHEN GAUSS LANDS ON DEV
            if self.session.tier.name == "DEV":
                if self.doctype.name == "schema":
                    insert = "INSERT INTO good_schemas (id, xml) VALUES (?, ?)"
                    self.cursor.execute(insert, (self.id, self.xml))
                elif self.doctype.name == "Filter":
                    insert = "INSERT INTO good_filters (id, xml) VALUES (?, ?)"
                    self.cursor.execute(insert, (self.id, self.xml))
        elif self.session.tier.name == "DEV":
            if self.doctype.name == "schema":
                update = "UPDATE good_schemas SET xml = ? WHERE id = ?"
                self.cursor.execute(update, (self.xml, self.id))
            elif self.doctype.name == "Filter":
                update = "UPDATE good_filters SET xml = ? WHERE id = ?"
                self.cursor.execute(update, (self.xml, self.id))
                # TODO: END OF TEMPORARY CODE BLOCK

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
            blob = query.execute(self.cursor).fetchone().data

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
        self.session.logger.debug("top of __store_links()")
        query = Query("link_net", "source_elem", "url")
        query.where(query.Condition("source_doc", self.id))
        rows = query.execute(self.cursor).fetchall()
        old_links = set([tuple(row) for row in rows])
        new_links = set([link.key for link in links if link.linktype])
        wanted = new_links - old_links
        unwanted = old_links - new_links
        args = len(wanted), len(unwanted)
        self.session.logger.debug("%d wanted links, %d unwanted links", *args)

        # If optimizing the update is inefficient, start with a clean slate.
        delete = "DELETE FROM link_net WHERE source_doc = ?"
        if len(unwanted) > 500 and len(unwanted) > len(new_links) / 2:
            wanted = new_links
            self.cursor.execute(delete, (self.id,))
            self.session.logger.debug("storing all links from scratch")

        # Otherwise just delete the rows which are not longer correct.
        else:
            delete += " AND source_elem = ? AND url = ?"
            for source_element, url in unwanted:
                args = self.id, source_element, url
                self.cursor.execute(delete, args)
            self.session.logger.debug("cleared out unwanted links")

        # Insert the rows that aren't already in place.
        for link in links:
            if link.key in wanted:
                link.save(self.cursor)
        self.session.logger.debug("stored %d links", len(links))

        # Apply the same technique to the `link_fragment` table.
        query = Query("link_fragment", "fragment")
        query.where(query.Condition("doc_id", self.id))
        rows = query.execute(self.cursor).fetchall()
        old = set([row.fragment for row in rows])
        new = self.frag_ids or set()
        wanted = new - old
        unwanted = old - new
        args = len(wanted), len(unwanted)
        self.session.logger.debug("%d wanted fragments, %d unwanted", *args)
        delete = "DELETE FROM link_fragment WHERE doc_id = ?"
        if len(unwanted) > 500 and len(unwanted) > len(new) / 2:
            wanted = new
            self.cursor.execute(delete, (self.id))
            self.session.logger.debug("storing all fragments from scratch")
        else:
            delete += " AND fragment = ?"
            for fragment_id in unwanted:
                self.cursor.execute(delete, (self.id, fragment_id))
            self.session.logger.debug("cleared out unwanted fragments")
        insert = "INSERT INTO link_fragment (doc_id, fragment) VALUES (?, ?)"
        for fragment_id in wanted:
            self.cursor.execute(insert, (self.id, fragment_id))
        self.session.logger.debug("stored %d fragments", len(links))

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
          level - which level of revision markup to keep for validation
                  (default is revision marked as ready to be published)

        Return:
          None

        Side effects:
          set `val_status` property to one of the valid codes
          populate the `errors` property
          set `eids` property to a parsed document with cdr-eid attributes
              for finding errors
          optionally update the `all_docs.val_status` column for the document
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

            # Create a copy with error location breadcrumbs if requested.
            if opts.get("locators"):
                root = self._eids = copy.deepcopy(self.root)
                self.__insert_eids(self.eids)
            else:
                root = self.root

            # Use the filtered document for validation.
            resolution_opts = dict(root=root, level=opts.get("level"))
            resolved = self.__apply_revision_markup(**resolution_opts)
            utf8 = etree.tostring(resolved, encoding="utf-8")
            validation_xml = utf8.decode("utf-8")

            # Find out if we've been asked to do schema and/or link validation.
            validation_types = opts.get("types", ["schema", "links"])
            if validation_types:

                # Apply schema validation if requested.
                if "schema" in validation_types:
                    self.__validate_against_schema(resolved)
                else:
                    complete = False

                # Apply link validation if requested.
                if "links" in validation_types:
                    store = opts.get("store", "always")
                    self.__validate_links(resolved, store=store)
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

    def __validate_against_schema(self, resolved):
        """
        Check the XML document against the requirements for its doctype

        Pass:
          resolved - document with revision markup resolved

        Populates the `errors` property by calling `add_error()`.
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
        doc = copy.deepcopy(resolved)
        self.__namespaces_off(doc)
        schema_doc = self.__get_schema()
        schema = etree.XMLSchema(schema_doc)

        # Put a reference to our `Doc` object somewhere where callbacks
        # can find it.
        Resolver.local.docs.append(self)
        try:

            # Validate against the schema.
            doc_without_eids = copy.deepcopy(doc)
            self.__strip_eids(doc_without_eids)
            if not schema.validate(doc_without_eids):
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
                    filter_xml = etree.tostring(rule_set.xslt)
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

    def __validate_links(self, resolved, store="always"):
        """
        Collect and check all of the document's links for validity

        Populates the `errors` property by calling `add_error()`.

        Pass:
          resolved - document with revision markup resolved
          store - control's whether we also populate the linking tables
                  (assuming the document is in the database)
                  "never": don't touch the database
                  "valid": store link info if the document is valid
                  "always": store the link info unconditionally (the default)
        """

        if not self.doctype or not self.doctype.id:
            problem = "invalid" if self.doctype else "missing"
            raise Exception("__validate_links(): {} doctype".format(problem))
        links = self.__collect_links(resolved)
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

        Called by:
          cdr.create_label()
          client XML wrapper command CdrCreateLabel

        Pass:
          session - reference to object representing user's login
          label - string used to tag document versions
          comment - optional string describing the label's usage
        """

        args = label, comment
        session.log("Doc.create_label({!r}, comment={!r})".format(*args))
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
        """
        Remove a name previously created for tagging document versions

        Called by:
          cdr.delete_label()
          client XML wrapper command CdrDeleteLabel

        Also removes any uses of the label.

        Pass:
          session - reference to object representing user's login
          label - string name for label to be removed
        """

        session.log("Doc.delete_label({!r})".format(label))
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
        Fetch the XML for a schema document by nane

        TODO: remove special code for DEV tier

        Pass:
          name - title of the schema document
          cursor - database access for fetching the xml

        Return:
          Unicode string for schema XML
        """

        assert name, "get_schema_xml(): no name for schema"

        # TODO: REMOVE THIS BLOCK WHEN GAUSS LANDS ON DEV
        if not hasattr(cls, "TIER"):
            from cdrapi.settings import Tier
            cls.TIER = Tier().name
        if cls.TIER == "DEV":
            names = name, name.replace(".xsd", ".xml")
            query = Query("good_schemas s", "s.xml")
            query.join("document d", "d.id = s.id")
            query.join("doc_type t", "t.id = d.doc_type")
            query.where("t.name = 'schema'")
            query.where(query.Condition("d.title", names, "IN"))
            try:
                return query.execute(cursor).fetchone().xml
            except:
                raise Exception("schema {!r} not found".format(name))
        # TODO: END TEMPORARY CODE

        schema_id = cls.id_from_title(name, cursor)
        query = Query("document", "xml")
        query.where(query.Condition("id", schema_id))
        try:
            return query.execute(cursor).fetchone().xml
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
            raise Exception("Multiple documents with title {}".format(title))
        for row in rows:
            return row.id
        return None

    @classmethod
    def delete_failed_mailers(cls, session):
        """
        Mark tracking documents for failed mailer jobs as deleted

        Invoked when a new mailer is created. We skip past mailers
        converted from the legacy Oracle PDQ system as an optimization.

        Called by:
          cdr.mailerCleanup()
          client XML wrapper command CdrMailerCleanup

        Pass:
          session - reference to object representing user's login

        Return:
          object carrying IDs for deleted documents and error strings
        """

        session.log("Doc.delete_failed_mailers()")
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
        """
        Create a string version of the document's ID

        Pass:
          integer ID; or
          string for integer ID; or
          string for normalized ID

        Return:
          string version of ID in the form "CDR9999999999"
        """

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

        def protect(self, match):
            return re.sub(r"\s+", "+", match.group(1).replace("+", "@@PLUS@@"))


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
        """
        Map of lines in the source XML to elements starting on those lines

        Takes advantage of the fact that we have added unique `cdr-eid`
        attribute values to every element in the document being validated.

        Attribute:
          lines - dictionary of `Doc.LineMap.Line` objects indexed by line #
        """

        def __init__(self, root):
            """
            Walk through the parsed document and build the line map

            Pass:
              root - parsed XML document to be mapped
            """

            self.lines = dict()
            for node in root.iter("*"):
                line = self.lines.get(node.sourceline)
                if not line:
                    line = self.lines[node.sourceline] = self.Line()
                line.add_node(node)

        def get_error_location(self, error):
            """
            Find the cdr-eid attribute for an element containing an error

            Pass:
              error - string in which error was described

            Return:
              cdr-eid attribute value of the element in which the error
              occurred (or our best guess)
            """

            line = self.lines.get(error.line)
            return line and line.get_error_location(error) or None


        class Line:
            """
            Map of `cdr-eid` attribute values for nodes in an XML source line

            Attributes:
              tags - dictionary of `cdr-eid` attribute values indexed by
                     element names; if multiple occurrences of the same
                     element appear on the line, only the `cdr-eid` value
                     for the first occurrence is recorded
              first - `cdr-eid` attribute value for the first node found
                      for this line in the XML source
            """

            def __init__(self):
                """
                Create an object with attribute initialized as having no values

                Values will be filled in later
                """

                self.tags = dict()
                self.first = None

            def add_node(self, node):
                """
                Record a node's `cdr-eid` attribute value

                If this is the line's first node, remember the location ID
                in the `first` attribute. If this is the first occurrence
                of elements with this name for this line, add the location
                ID to the `tags` dictionary.

                Pass:
                  node - reference to element in the XML document
                """

                if not self.first:
                    self.first = node.get("cdr-eid")
                if not self.tags.get(node.tag):
                    self.tags[node.tag] = node.get("cdr-eid")

            def get_error_location(self, error):
                """
                Find the location ID of a validation error found in the doc

                This code relies on the format of the error string, which
                we have determined to be consistently:
                   "Element 'ELEMENT-TAG' ...."

                Pass:
                  error - string in which error was described

                Return:
                  best guess (if any) as to the location ID for the error
                """

                match = re.match("Element '([^']+)'", error.message)
                if not match:
                    return None
                tag = match.group(1) or None
                location = self.tags.get(tag)
                return location or self.first


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
        """
        Create a custom parser for filtering which can resolve our URIs
        """

        def __init__(self):
            """
            Register resolvers for the URI schemes we support
            """

            etree.XMLParser.__init__(self)
            self.resolvers.add(Resolver("cdrutil"))
            self.resolvers.add(Resolver("cdr"))
            self.resolvers.add(Resolver("cdrx"))


    class Rule:
        """
        Custom validation rule embedded in our XML schema documents

        Attributes:
          assertions - sequence of `Doc.Assertion` objects
          context - path identifying which elements in the document to test

        Property:
          template - XSL/T template element used for applying the rule's test
        """

        def __init__(self, node):
            """
            Remember the parts of the document we test and what the tests are

            Pass:
              `appinfo/pattern/rule` node found in the schema document
            """

            self.assertions = []
            self.context = node.get("context")
            for child in node.findall("assert"):
                self.assertions.append(Doc.Assertion(child))

        @property
        def template(self):
            """
            Assemble the XSL/T node for applying the test for this rule
            """

            TEMPLATE = Doc.qname("template", Filter.NS)
            template = etree.Element(TEMPLATE, match=self.context)
            for assertion in self.assertions:
                template.append(assertion.make_test())
            return template


    class RuleSet:
        """
        Set of custom validation rules embedded in a XML schema document

        Attributes:
          name - name of the rule set
          value - never been used, AFAIK
          rules - sequence of `Doc.Rule` objects

        Property:
          xslt - filtering document used to generate error messages for
                 custom document validation rules
        """

        def __init__(self, node):
            """
            Collect the `Rule` objects for the set
            """

            self.name = node.get("name")
            self.value = node.get("value")
            self.rules = [Doc.Rule(r) for r in node.findall("rule")]

        @property
        def xslt(self):
            """
            Build the `xsl:transform` filter document for custom validation

            Return:
              parsed `xsl:transform` node
            """

            root = self.make_base()
            for rule in self.rules:
                root.append(rule.template)
            return root

        @classmethod
        def make_base(cls):
            """
            Create a parsed tree for the XSL/T validation template

            The XML utf-8 bytes for the template have already been
            retrieved into `Doc.VALIDATION_TEMPLATE` by the Doc
            class's `__validate_against_schema()` method, which is
            higher up in the call stack.

            Return:
              parsed `xsl:transform` node
            """

            return etree.fromstring(Doc.VALIDATION_TEMPLATE)


    class SchemaResolver(etree.Resolver):
        """
        Glue for fetching nested schema documents
        """

        def __init__(self, cursor):
            """
            Stash away a database cursor so we can retrieve schema documents

            Pass:
              reference to database cursor object
            """

            etree.Resolver.__init__(self)
            self.__cdr_cursor = cursor

        def resolve(self, url, id, context):
            """
            Fetch the serialized XML for a nested schema document

            Pass:
              url - string for title of the schema document (e.g.,
                    "CdrCommonSchema.xml")
              id - unused positional argument
              context - opaque information, passed through to the
                        `resolve_string` method of the base class

            Return:
              return value from `resolve_string` method of the base class
            """

            xml = Doc.get_schema_xml(url, self.__cdr_cursor)
            return self.resolve_string(xml, context)


    class User:
        """
        Information about a user who did something with the document

        This class is used to carry information about users who have
        added, modified, or locked a CDR document. This is a lighter-
        weight class than the one in the `cdrapi.users` module.

        Attributes:
          id - primary key integer for the `usr` table
          name - string for the user account name
          fullname - string for the user's complete name
        """

        def __init__(self, id, name, fullname):
            """
            Capture the attributes (no validation)

            Pass:
              id - unique integer for the row in the `usr` table
              name - short account name (e.g., "klem")
              fullname - optional real name of the user (for example,
                         "Klem Kadiddlehopper")
            """

            self.id = id
            self.name = name
            self.fullname = fullname


class Local(threading.local):
    """
    Thread-specific storage for XSL/T filtering

    Attribute:
      docs - stack of documents, the one currently being processed on top
             (this is the only way we can get to information we need from
             the documents within the XSL/T callbacks, as the maintainer
             of the lxml package didn't seem to understand why anyone would
             need to have user-specific storage passed on the stack, and
             I couldn't convince him otherwise; as tempting as the name of
             the `context` argument to `Resolver.resolve()` might look, we
             don't own it, and we don't have access to its internals).
    """

    def __init__(self, **kw):
        self.docs = []
        self.__dict__.update(kw)


class Resolver(etree.Resolver):
    """
    Callback support for XSL/T filtering
    """

    # Strings which might represent SQL injection attacks. We reject these.
    UNSAFE = re.compile(r"insert\s|update\s|delete\s|create\s|alter\s"
                        r"exec[(\s]|execute[(\s]")

    # Expression for normalizing protocol IDs for eliminating duplicates.
    ID_KEY_STRIP = re.compile("[^A-Z0-9]+")

    # Thread-specific storage.
    local = Local()

    def resolve(self, url, pubid, context):
        """
        Handle a callback from an XSL/T filter

        Callback requests are received in the form of a URL in the syntax
        SCHEME:/PARAMETERS, where SCHEME is one of:
           'cdr' or 'cdrx'
              request for a document; these two scheme prefixes are
              synonymous, except that the 'cdrx' scheme just returns
              an empty result when the URL can't be resolved, whereas
              the 'cdr' scheme raises an exception (yes, it would have
              made more sense for the 'x' version to be the one which
              raises the eXception, but that's not how the original
              programmer implemented the callback; cdr:/1234 retrieves
              the current working document for CDR0000001234; see the
              `__get_doc()` method below for specifics on the syntax
           'cdrutil'
              invokes a named custom function; e.g., cdrutil:/date
              to get the current date and time in XML format; see the
              `__run_function()` method below for a list of supported
              callback functions

        Pass:
          url - string for the callback request
          pubid - ignored
          context - opaque information, passed through to the method of the
                    base class used to package the return value

        Return:
          result packaged by the base class
        """

        # Get the document being filtered from the top of the stack
        self.doc = self.local.docs[-1]
        self.session = self.doc.session
        self.cursor = self.session.conn.cursor()
        self.session.logger.debug("resolve(%r)", url)

        # Remove the escaping imposed on the URL to elude syntax constraints.
        self.url = url_unquote(url.replace("+", " "))
        self.url = self.url.replace("@@PLUS@@", "+")

        # If a document request slipped through without an ID, we know
        # we're not going to find the document.
        if url == "cdrx:/last":
            return self.resolve_string("<empty/>", context)

        # Parse the request and route it to the appropriate handler.
        scheme, parms = self.url.split(":", 1)
        parms = parms.strip("/")
        if scheme in ("cdr", "cdrx"):
            return self.__get_doc(scheme, parms, context)
        elif scheme == "cdrutil":
            return self.__run_function(parms, context)
        raise Exception("unsupported url {!r}".format(self.url))

    def __get_doc(self, scheme, parms, context):
        """
        Fetch information from another CDR document

        Pass:
          scheme - 'cdr' or 'cdrx'
          parms - string specifying what to retrieve for which document
          context - opaque information echoed back to the caller

        Return:
          varies; see logic below
        """

        # Prepare for failure.
        message = "Unable to resolve uri {!r}".format(parms)

        # Documents (usually, but not always, Filters) can be fetched by name.
        if parms.startswith("name:"):
            parms = parms[5:]
            if "/" in parms:
                title, version = parms.split("/", 1)
            else:
                title, version = parms, None
            doc_id = Doc.id_from_title(title, self.cursor)
            if not doc_id:
                if scheme == "cdrx":
                    return self.resolve_string("<empty/>", context)
                raise Exception("Filter {!r} not found".format(title))
            doc = Doc(self.session, id=doc_id, version=version)
            if doc.doctype.name == "Filter":
                doc_xml = doc.get_filter(doc_id, version=version).xml
                return self.resolve_string(doc_xml, context)
            spec = None

        # Find the requested document by ID
        elif "/" in parms:
            doc_id, spec = parms.split("/", 1)
            if doc_id == "*":
                doc = self.doc
            else:
                if "/" in spec:
                    version, spec = spec.split("/", 1)
                elif spec.isdigit() or spec in ("last", "lastp"):
                    version, spec = spec, None
                else:
                    version, spec = None, spec
                try:
                    doc = Doc(self.session, id=doc_id, version=version)
                except:
                    message = "Doc({}) failure".format(parms)
                    self.doc.session.logger.exception(message)
                    if scheme == "cdrx":
                        return self.resolve_string("<empty/>", context)
                    raise Exception(message)
        else:
            spec = None
            try:
                doc = Doc(self.session, id=parms)
            except:
                message = "Doc({}) failure".format(parms)
                self.doc.session.logger.exception(message)
                if scheme == "cdrx":
                    return self.resolve_string("<empty/>", context)
                raise Exception(message)

        # Return control information about the doc if requested.
        if spec == "CdrCtl":
            element = doc.legacy_doc_control(filtering=True)
            return self.__package_result(element, context)

        # Return the document's title if requested.
        elif spec == "DocTitle":
            element = etree.Element("CdrDocTitle")
            element.text = doc.title
            return self.__package_result(element, context)

        # Guard against unsupported requests.
        elif spec:
            raise Exception(message)

        # Wrap up the document XML and return it.
        try:
            return self.resolve_string(doc.xml, context)
        except:
            self.doc.session.logger.exception("resolve_string() failure")
            if scheme == "cdrx":
                return self.resolve_string("<empty/>", context)
            raise Exception(message)

    def __run_function(self, parms, context):
        """
        Handle a custom callback function

        Functions currently implemented are:
          'date' - get current date/time in XML standard format
          'dedup-ids' - collapse protocol IDs to a unique set
          'denormalizeTerm' - get a term document, possibly with upcoding
          'docid' - get the ID of the current document
          'get-pv-num' - fetch the number of the doc's last publishable version
          'sql-query' - run a SQL query and return XML-wrapped results
          'valid-zip' - look up a ZIP code and return its first 5 digits

        Pass:
          parms - right side of the URL, following scheme plus ":/"
          context - opaque information, passed through to the method of the
                    base class used to package the return value

        Return:
          wrapped result of the function
        """

        function, args = parms, None
        if "/" in parms:
            function, args = parms.split("/", 1)
        method_name = "_{}".format(function.lower().replace("-", "_"))
        handler = getattr(self, method_name)
        if handler is not None:
            return handler(args, context)
        error = "unsupported function {!r} in {!r}".format(function, self.url)
        raise Exception(error)

    def _date(self, args, context):
        """
        Get the current date/time

        Pass:
          args - ignored for this function
          context - opaque information echoed back to the caller

        Return:
          string for the current date/time to the nearest microsecond,
          in the form YYYY-MM-DDTHH:MM:SS.MMM
        """

        result = etree.Element("Date")
        now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
        if "." in now:
            front, back = now.split(".", 1)
        else:
            front, back = now, ""
        back = "{}000".format(back)[:3]
        result.text = "{}.{}".format(front, back)
        return self.__package_result(result, context)

    def _dedup_ids(self, args, context):
        """
        Squash a list of protocol IDs into a unique list

        Pass:
          args - string containing primary and secondary protocol IDs;
                 the set of primary IDs is separated from the set of
                 secondary IDs with the delimiter string "~~" and within
                 each set individual protocol IDs are separated from each
                 other by a single "~" character
          context - opaque information echoed back to the caller

        Return:
          `result` element wrapping a sequence of `id` child elements
        """

        ids = []
        skip = set()
        if "~~" in args:
            primary, secondary = [i.split("~") for i in args.split("~~", 1)]
            for p in primary:
                skip.add(self.__make_id_key(p))
            for s in secondary:
                key = self.__make_id_key(s)
                if key and key not in skip:
                    ids.append(s)
                    skip.add(key)
        result = etree.Element("result")
        for i in ids:
            etree.SubElement(result, "id").text = i
        return self.__package_result(result, context)

    def _docid(self, args, context):
        """
        Get the normalized CDR ID for the current document

        Pass:
          args - ignored for this function
          context - opaque information echoed back to the caller

        Return:
          a `DocId` element node containing the doc's normalized CDR ID
        """

        element = etree.Element("DocId")
        element.text = self.doc.cdr_id
        return self.__package_result(element, context)

    def _denormalizeterm(self, args, context):
        """
        Build a custom XML document for the specified term

        It's unclear whether we still need this function, as it's
        only used by the protocol denormalization filters, and protocols
        are so last year!

        Pass:
          args - term document ID string, optionally followed by a slash
                 character to suppress upcoding
          context - opaque information echoed back to the caller

        Return:
          a `Term` block element node, possibly containing upcoding;
          information (or an `empty` element node, if the term document
          is not found)
        """

        doc_id = Doc.extract_id(args.split("/")[0])
        upcode = False if "/" in args else True
        term = Term.get_term(self.session, doc_id)
        if term is None:
            term_xml = "<empty/>"
        else:
            term_xml = term.get_xml(upcode)
        return self.resolve_string(term_xml, context)

    def _get_pv_num(self, doc_id, context):
        """
        Find the version number for a document's latest publishable version

        Pass:
          doc_id - unique identifier for the document
          context - opaque information echoed back to the caller

        Return:
          a `PubVerNumber` element with text content of the version number
          (or "0" if no version if found)
        """

        doc = Doc(self.session, id=doc_id)
        answer = etree.Element("PubVerNumber")
        answer.text = str(doc.last_publishable_version or 0)
        return self.__package_result(answer, context)

    def _sql_query(self, args, context):
        """
        Run a database query and return the results as XML

        We screen the query for possibly dangerous keywords, and refuse
        to run the query if we find any.

        Pass:
          args - SQL query string, possibly with parameters delimited by "~"
          context - opaque information echoed back to the caller

        Return:
          a `SqlResult` block element node
        """

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
        self.session.logger.debug("query: %r", query)
        self.session.logger.debug("values: %r", values)
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
                    try:
                        col.text = unicode(v)
                    except:
                        col.text = v.decode("utf-8")
            r += 1
        return self.__package_result(result, context)

    def _valid_zip(self, args, context):
        """
        Look up a ZIP code string and return its first five digits

        Pass:
          args - string for zip code to be verified
          context - opaque information echoed back to the caller

        Return:
          `ValidZip` element containing the first five digits of the
                     ZIP code if it is found in the database (otherwise
                     empty)
        """

        result = etree.Element("ValidZip")
        query = Query("zipcode", "zip")
        query.where(query.Condition("zip", args))
        row = query.execute(self.cursor).fetchone()
        if row and row.zip:
            result.text = str(row.zip)[:5]
        return self.__package_result(result, context)

    def _extern_map(self, args, context):
        """
        Look up a value in the external_map table

        Creates a row in the table with a NULL documnet ID if the value
        isn't found.

        Probably won't implement this, as it is only invoked by the
        Import CTGovProtocol filter, and we don't do those imports
        any more.

        Pass:
          args - extra information passed to the function, if any
          context - opaque information echoed back to the caller

        Return:
          `DocId` element Node (empty if value not found)
        """

        raise Exception("obsolete command")

    def _pretty_url(self, args, context):
        """
        Swap a GUID for a user-friendly URL

        Probably won't implement this, as it uses a defunct web service

        Pass:
          args - string for URL's GUID
          context - opaque information echoed back to the caller

        Return:
          `PrettyUrl` element node
        """

        raise Exception("obsolete command")

    def _verification_date(self, args, context):
        """
        Get the date a protocol was last verified by a mailer response

        Probably won't implement this, as it is only used by the
        InScopeProtocol denormalization filter, and we don't do
        InScopeProtocol documents any more

        Pass:
          args - extra information passed to the function, if any
          context - opaque information echoed back to the caller

        Return:
          `VerificationDate` element node
        """

        raise Exception("obsolete command")

    def __package_result(self, result, context):
        """
        Use `resolve_string()` method of the base class to wrap return string

        Pass:
          result - parsed XML document node
          context - opaque information echoed back to the caller

        Return:
          wrapped string result
        """

        result = etree.tostring(result, encoding="utf-8")
        return self.resolve_string(result, context)

    @classmethod
    def __make_id_key(cls, id):
        """
        Normalize a protocol ID for de-duplication of ID lists

        Pass:
          id - string for protocol ID to be normalized

        Return:
          uppercase string with non-alphanumerics stripped
        """

        return cls.ID_KEY_STRIP.sub("", id.upper())

    @staticmethod
    def escape_uri(context, arg=""):
        """
        Prepare a URI string so that it makes it through the lxml gauntlet

        This is a custom function to be made available to XSL/T code so
        that URI values containing non-conforming characters won't cause
        filter processing to fail.

        Pass:
          context - ignored
          arg - string or sequence of strings to be escaped

        Return:
          single (possibly concatenated) escaped string
        """

        if isinstance(arg, (list, tuple)):
            arg = "".join(arg)
        try:
            return url_quote(arg.replace("+", "@@PLUS@@"))
        except:
            raise

# Register our custom extension function for XSL/T filters to use.
etree.FunctionNamespace(Doc.NS).update({"escape-uri": Resolver.escape_uri})

class Term:
    """
    Term document with parents

    This class is used for XSL/T filtering callbacks.
    """

    def __init__(self, session, doc_id, depth=0):
        """
        Recursively fetch a term's names and parents

        Pass:
          session - reference to object representing current login
          doc_id - unique ID of the CDR `Term` document
          depth - integer representing level of recursion
        """

        self.session = session
        self.doc_id = doc_id
        self.cdr_id = Doc.normalize_id(doc_id)
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
        """
        Return one of the serialized versions of the Term document

        Pass:
          with_upcoding - return the version which includes parent terms
                          if `True`

        Return:
          utf-8 bytes for serialized document
        """

        with self.session.cache.term_lock:
            if self.xml and self.full_xml:
                return with_upcoding and self.full_xml or self.xml
        self.serialize(need_locking=True)
        return with_upcoding and self.full_xml or self.xml

    def serialize(self, need_locking=False):
        """
        Generate two string representations of the term

        One serialization is for the bare Term information, and the
        other includes upcoding.

        Pass:
          need_locking - if True, take precautions to avoid cross-thread
                         collisions with use of the cache

        Return:
          None (side effect is population of `self.xml` and `self.full_xml`)
        """

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
        """
        Fetch the parent document from the cache or the database

        Pass:
          node - document node containing the link to the parent term document
          depth - integer for how far we've crawled in the stack (preventing
                  out-of-control recursion)

        Return:
          None (side effect is population of cache and `self.parents`)
        """

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
        """
        Pull a Term document from the cache or the database

        Pass:
          session - reference to object representing the current login
          doc_id - unique ID of the CDR Term document
          depth - keeps track of level of recursion
        """

        # Stop if recursion has gotten out of hand.
        if depth > cls.MAX_DEPTH:
            error = "term hierarchy depth exceeded at CDR()".format(doc_id)
            raise Exception(error)

        # Pull the Term object from the cache if we already have it.
        with session.cache.term_lock:
            if doc_id in session.terms:
                return session.cache.terms[doc_id]

        # Fetch the term information from the database, cache and return it.
        term = cls(session, doc_id, depth)
        if not term.name:
            term = None
        with session.cache.term_lock:
            if doc_id not in session.cache.terms:
                session.cache.terms[doc_id] = term
        return term


class Filter:
    """
    Lightweight object for a cacheable XSL/T filter document

    Attributes:
      doc_id - unique ID of the CDR Filter document
      xml - utf-8 bytes for the serialized filter document
      now - was originally used to track how long the filter has sat in
            the cache unused; drop?
    """

    # Keep recursion from getting out of hand.
    MAX_FILTER_SET_DEPTH = 20

    # Namespace for XSL/T documents
    NS = "http://www.w3.org/1999/XSL/Transform"

    def __init__(self, doc_id, xml):
        """
        Capture the filter ID and xml, forcing the xml to bytes

        Pass:
          doc_id - unique ID for the CDR Filter document
          xml - utf-8 bytes or Unicode serialization of the Filter document
        """

        self.doc_id = doc_id
        self.xml = xml.encode("utf-8") if isinstance(xml, unicode) else xml
        self.now = time.time()


class Schema:
    """
    Values used for parsing schema validation documents
    """

    # Standard namespace for schema documents
    NS = "http://www.w3.org/2001/XMLSchema"

    # Fully qualified names for elements in schema documents
    ANNOTATION = Doc.qname("annotation", NS)
    APPINFO = Doc.qname("appinfo", NS)
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

    # Path to attribute definitions enclosed in `simpleContent` blocks.
    NESTED_ATTRIBUTE = "/".join([SIMPLE_CONTENT, EXTENSION, ATTRIBUTE])


class Doctype:
    """
    Class of CDR documents controlled by a schema
    """

    def __init__(self, session, **opts):
        """
        Construct a new `Doctype` argument

        Called by:
          cdr.getDoctype()
          client XML wrapper command CdrGetDocType

        Required positional argument:
          session - object representing login session

        Optional keyword arguments:
          id - primary key for the `doc_type` row
          name - string by which the document type is known
          active - "Y" or "N"
          comment - string describing the document type's use
          format - e.g., "xml" (the default) or "css"
          schema - string for the title of the schema document used to
                   validate documents of this type
          versioning - "Y" (the default, meaning documents of this type
                       have separate versions created) or "N"
        """

        self.__session = session
        self.__opts = opts

    @property
    def active(self):
        """
        Flag indicating whether the document type is available for use

        Valid values are "Y" (the default) or "N"
        """

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
        """
        Description of the documents created for this type
        """

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
        """
        When the document type was first defined
        """

        if not hasattr(self, "_created"):
            self.__fetch_dates()
        return self._created

    @property
    def cursor(self):
        """
        Give the document type object its own database cursor
        """

        if not hasattr(self, "_cursor"):
            self._cursor = self.session.conn.cursor()
        return self._cursor

    @property
    def dtd(self):
        """
        DTD string generated from the schema (including its included schemas)
        """

        if not hasattr(self, "_dtd_string"):
            self._dtd = DTD(self.session, name=self.schema)
            self._dtd_string = str(self._dtd)
        return self._dtd_string

    @property
    def format(self):
        """
        String for the name of the file format for documents of this type

        Valid values are controlled by the `format` lookup table. Most are
        "xml" (the default).
        """

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
        """
        Primary key for the link to this type's row in the `format` table
        """

        if not hasattr(self, "_format_id"):
            if self.format:
                if not hasattr(self, "_format_id"):
                    self._format_id = self.__format_id_from_name(self.format)
        return self._format_id

    @property
    def id(self):
        """
        Primary key for the `doc_type` table
        """

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
        """
        String for the name by which this document type is know
        """

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
        """
        String for the title of this document type's schema document
        """

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
        """
        Integer for the schema's row in the `all_docs` table
        """

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
        """
        Date/time the document type was last modified

        This is an odd name for the column (and property) of a value
        which is updated every time the row in the `doc_type` table
        is modified, regardless of whether the schema itself is changed
        (or even exists), but that's the way the original developer
        set it up.
        """

        if not hasattr(self, "_schema_date"):
            self.__fetch_dates()
        return self._schema_date

    @property
    def session(self):
        """
        Reference to the object representing the current login
        """

        return self.__session

    @property
    def versioning(self):
        """
        Flag (Y/N) indicating whether documents of this type are versioned
        """

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
        """
        Dictionary of valid values for this document type

        Each item in the dictionary is a sequence of strings, indexed
        by the element-name or element-name/@attribute-name string.
        """

        if not hasattr(self, "_vv_lists"):
            self._vv_lists = self._dtd.values if self.dtd else None
        return self._vv_lists

    @property
    def linking_elements(self):
        """
        Set of string for elements which can have a `cdr:ref` attribute
        """

        if not hasattr(self, "_linking_elements"):
            if self.dtd:
                self._linking_elements = self._dtd.linking_elements
            else:
                self._linking_elements = None
        return self._linking_elements

    def delete(self):
        """
        Drop the document type's row from the `doc_type` table

        Called by:
          cdr.delDoctype()
          client XML wrapper command CdrDelDocType
        """

        self.session.log("Doctype.delete({!r})".format(self.name))
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
        """
        Write the document type information to the `doc_type` row

        Called by:
          cdr.addDoctype()
          cdr.modDoctype()
          client XML wrapper command CdrAddDocType
          client XML wrapper command CdrModDocType

        Optional keyword arguments:
          name - string by which the document type is known
          active - "Y" or "N"
          comment - string describing the document type's use
          format - e.g., "xml" (the default) or "css"
          versioning - "Y" (the default, meaning documents of this type
                       have separate versions created) or "N"
        """

        self.session.log("Doctype.save({!r}, {!r})".format(self.name, opts))
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
        self.session.logger.debug("fields=%s", fields)
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
        self.session.logger.debug("sql=%s", sql)
        self.session.logger.debug("values=%s", values)
        self.cursor.execute(sql, values)
        self._schema_date = now
        if not self.id:
            self._created = now
            self.cursor.execute("SELECT @@IDENTITY AS id")
            self._id = self.cursor.fetchone().id
        self.session.conn.commit()
        self.session.logger.debug("committed doctype %s", self.id)
        return self.id

    def __format_id_from_name(self, name):
        """
        Find the row in the `format` table for a format name

        Pass:
          name - string naming the format (e.g., "xml")

        Return:
          integer for the row's primary key
        """

        query = Query("format", "id")
        query.where(query.Condition("name", name))
        row = query.execute(self.cursor).fetchone()
        return row.id if row else None

    def __fetch_dates(self):
        """
        Fetch the datetime values from the `doc_type` table

        The column "schema_date" is misnamed; it actually represents
        the date/time the row in the `doc_type` table was modified.

        Return:
          None (side effect is population of `self._schema_date` and
          `self._created` attributes)
        """

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

        Called by:
          cdr.get_css_files()
          client XML wrapper command CdrGetCssFiles

        Pass:
          session - reference to object representing user's login

        Return:
          dictionary of CSS files, indexed by their document names
        """

        session.log("Doctype.get_css_files()")
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

        Called by:
          cdr.getDoctypes()
          client XML wrapper command CdrListDocTypes

        Pass:
          session - reference to object representing user's login

        Return:
          sequence of document type names, sorted alphabetically
        """

        session.log("Doctype.list_doc_types()")
        if not session.can_do("LIST DOCTYPES"):
            raise Exception("User not authorized to list document types")
        query = Query("doc_type", "name").order("name").where("active = 'Y'")
        query.where("name <> ''")
        return [row.name for row in query.execute(session.cursor).fetchall()]

    @staticmethod
    def list_schema_docs(session):
        """
        Assemble the list of schema documents currently stored in the CDR

        Called by:
          cdr.getSchemaDocs()
          client XML wrapper command CdrListSchemaDocs

        Pass:
          session - reference to object representing user's login

        Return:
          sequence of document type names, sorted alphabetically
        """

        session.log("Doctype.list_schema_docs()")
        query = Query("document d", "d.title").order("d.title")
        query.join("doc_type t", "t.id = d.doc_type")
        query.where("t.name = 'schema'")
        return [row.title for row in query.execute(session.cursor).fetchall()]


class DTD:
    """
    Document Type Definition (DTD) for validating XML documents

    Within the CDR we use Schema validation, but XMetaL did not
    originally support Schema validation, so we generate DTDs
    from our schemas and ship them to the client for use by XMetaL
    to control document validation. The more recent versions of
    XMetaL have added support for Schema validation, but switching
    will take some non-trivial work which has not been done yet.

    Attributes:
      session - reference to object representing current CDR login
      cursor - object for database queries
      types - dictionary of simple and complex types found in the schema
      groups - dictionary of named sequences of elements
      top - reference to the top node of the schema document
      name - string for the schema title
      defined - set of names for types which have already been serialized
      linking_elements - elements which can have `cdr:ref` attributes
      values - dictionary of valid values sequences indexed by element
               or element/@attribute name strings
    """

    # Unicode categories used for recognizing NMTOKEN strings.
    NAME_START_CATEGORIES = { "Ll", "Lu", "Lo", "Lt", "Nl" }
    OTHER_NAME_CATEGORIES = { "Mc", "Me", "Mn", "Lm", "Nd" }
    NAME_CHAR_CATEGORIES = NAME_START_CATEGORIES | OTHER_NAME_CATEGORIES

    # Names of element types
    EMPTY = "empty"
    MIXED = "mixed"
    TEXT_ONLY = "text-only"
    ELEMENT_ONLY = "element-only"

    # Special value for maxOccurs attribute to mean 'unlimited'.
    UNBOUNDED = "unbounded"

    def __init__(self, session, **opts):
        """
        Initialize the attributes and parse the document type's schema

        Pass:
          session - reference to object representing the current CDR login
          name - required keyword argument for name of the schema document
        """

        self.session = session
        self.cursor = session.conn.cursor()
        self.types = dict()
        self.groups = dict()
        self.top = None
        self.name = opts.get("name")
        self.parse_schema(self.name)

    def parse_schema(self, name):
        """
        Pull out the schema information we need for generating the DTD
        """

        # Fetch the CDR schema document.
        assert name, "how can we parse something we can't find?"
        schema_xml = Doc.get_schema_xml(name, self.cursor)
        root = etree.fromstring(schema_xml.encode("utf-8"))
        if root.tag != Schema.SCHEMA:
            raise Exception("Top-level element must be schema")

        # Walk through the top-level children of the root element
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
        """
        Generate the DTD document string
        """

        self.values = dict()
        self.linking_elements = set()
        lines = ["<!-- Generated from {} -->".format(self.name), ""]
        self.defined = set()
        self.top.define(lines)
        return "\n".join(lines) + "\n"

    def add_type(self, t):
        """
        Insert the type object into the `types` dictionary attribute
        """

        assert t not in self.types, "duplicate type {}".format(t.name)
        self.types[t.name] = t


    class CountedNode:
        """
        Base class for `Element` and `ChoiceOrSequence` classes

        Attributes:
          dtd - reference to the object for the DTD we're building
          min_occurs - integer specifying lowest allowed number of occurrences
          max_occurs - integer for highest number of occurrences allowed
          count_char - "?", "*", "+" or an empty string
        """

        def __init__(self, dtd, node):
            """
            Pull out the common information about allowed occurrences
            """

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
        """
        One of the elements allowed for documents of this type

        Attributes:
          name - string for the element's XML tag
          type_name - string for key to dtd's `types` attribute
        """

        def __init__(self, dtd, node):
            """
            Fetch the attributes and confirm that we got them
            """

            DTD.CountedNode.__init__(self, dtd, node)
            self.name = node.get("name")
            self.type_name = node.get("type")
            assert self.name, "element name required"
            assert self.type_name, "element type required"
            debug = dtd.session.logger.debug
            debug("Element %s of type %s", self.name, self.type_name)

        def lookup_type(self):
            """
            Find the custom or builtin type object for this element

            This only works after the entire schema has been parsed.
            It is invoked during the serialization of the DTD.
            """

            # If the element uses one of our own types, return it.
            element_type = self.dtd.types.get(self.type_name)
            if element_type:
                return element_type

            # Otherwise, should be a built-in type; make/return the object.
            if "xsd:" not in self.type_name:
                args = self.name, self.type_name
                raise Exception("element {}: type {} not found".format(*args))
            return DTD.Type(self.dtd, self.type_name)

        def define(self, lines):
            """
            Add the lines for this element to the DTD

            Recursively handles children of this element. The actual
            generation of the string for the element's lines is handed
            off to the object for the element's type.

            Pass:
              lines - object for sequence of strings for the DTD

            Return:
              None (side effect is recursive population of `lines`)
            """

            # Start with an empty `children` attribute.
            self.children = []

            # Add our own lines; populates the `children` attribute.
            lines += self.lookup_type().define(self)

            # If this is the top-level element, add definitions for
            # elements which are injected into the copy of the document
            # given to XMetaL.
            if not self.dtd.defined:
                self.add_control_definitions(lines)

            # Recursively add definitions for children which haven't
            # already been handled.
            for child in self.children:
                if child.name not in self.dtd.defined:
                    self.dtd.defined.add(child.name)
                    child.define(lines)

        def add_control_definitions(self, lines):
            """
            Append lines for elements carrying metadata about the documents

            Pass:
              lines - object for sequence of strings for the DTD

            Return:
              None (side effect is recursive population of `lines`)
            """

            lines.append("<!ELEMENT CdrDocCtl (DocId, DocTitle)>")
            lines.append("<!ATTLIST CdrDocCtl readyForReview CDATA #IMPLIED>")
            lines.append("<!ELEMENT DocId (#PCDATA)>")
            lines.append("<!ATTLIST DocId readonly (yes|no) #IMPLIED>")
            lines.append("<!ELEMENT DocTitle (#PCDATA)>")
            lines.append("<!ATTLIST DocTitle readonly (yes|no) #IMPLIED>")
            for name in (self.name, "CdrDocCtl", "DocId", "DocTitle"):
                self.dtd.defined.add(name)

        def get_node(self, elements, serialize=False):
            """
            Insert the element's name into the sequence passed in

            This queues up the element to be processed recursively, and is
            invoked when we're serializing the parent. Optionally we
            also return the string used to show this element's appearance
            in the sequence of a compound element's content.

            Pass:
              elements - sequence to which we append this element if it's
                         not already there
              serialize - flag indicating whether we should return a
                          serialized version of this element as content
                          of a wrapper element

            Return:
              serialized string if requested, else None
            """

            if self.name not in [element.name for element in elements]:
                elements.append(self)
            if serialize:
                return "{}{}".format(self.name, self.count_char)


    class Type:
        """
        Base class for element and attribute types

        Attributes:
          dtd - reference to the object for the DTD we're building
          name string by which the element or attribute type is identified
          values - sequence of string values allowed for the type
        """

        def __init__(self, dtd, name):
            """
            Capture and verify the object's attributes

            Pass:
              dtd - reference to the object for the DTD we're building
              name - string by which the element or attribute type is known
            """

            self.dtd = dtd
            self.name = name
            self._values = []
            self.base = None
            assert self.name, "type must have a name"

        @property
        def values(self):
            if self._values:
                return self._values
            if self.base:
                base = self.dtd.types.get(self.base)
                if base:
                    values = base.values
                    if values:
                        return values
            return None

        def define(self, element):
            """
            Default method for assembling definitions for an element

            Pass:
              element - object for element to be serialized to the DTD

            Return:
              sequence of definition strings to be included in the DTD
            """

            values = self.values
            if values:
                self.dtd.values[element.name] = values
            definitions = [self.define_element(element)]
            attributes = self.define_attributes(element)
            if attributes:
                definitions.append(attributes)
            return definitions

        def define_element(self, element):
            """
            Create string for DTD definition of simple text-only element

            Pass:
              element - object for element to be serialized to the DTD

            Return:
              DTD definition string for element
            """

            return "<!ELEMENT {} (#PCDATA)>".format(element.name)

        def define_attributes(self, element):
            """
            Default type has no attributes
            Pass:
              element - object for element to be serialized to the DTD

            Return:
              None
            """
            return None

        @staticmethod
        def map_type(schema_type):
            """
            Convert schema name for attribute type to DTD name

            Pass:
              schema_type - string for schema type (e.g. "xsd:id")

            Return:
              DTD equivalent for schema type if found; otherwise "CDATA"
            """

            for n in ("ID", "IDREFS", "NMTOKEN", "NMTOKENS"):
                if schema_type == "xsd:{}".format(n):
                    return n
            return "CDATA"


    class SimpleType(Type):
        """
        Schema type for attributes or plain elements

        `SimpleType` elements can have no attributes or child elements

        Attributes not inherited from `Type` class:
          base - base type if this type is derived
          nmtokens - valid values for an attribute of this type
        """

        def __init__(self, dtd, node):
            """
            Extract the base type and valid values (if any) and register it

            Pass:
              dtd - reference to the object for the DTD we're building
              node - definition for this type in the schema
            """

            DTD.Type.__init__(self, dtd, node.get("name"))
            self.base = None
            self.nmtokens = []
            dtd.session.logger.debug("SimpleType %s", self.name)
            for restriction in node.findall(Schema.RESTRICTION):
                self.base = restriction.get("base")
                for enum in restriction.findall(Schema.ENUMERATION):
                    value = enum.get("value")
                    self._values.append(value)
                    if self.nmtokens is not None:
                        if DTD.is_nmtoken(value):
                            self.nmtokens.append(value)
                        else:
                            self.nmtokens = None
            dtd.add_type(self)

        def dtd_type(self):
            """
            Get the string used to define an attribute in the DTD
            """

            if self.nmtokens:
                return "({})".format("|".join(sorted(self.nmtokens)))
            if "xsd:" in self.base:
                return DTD.Type.map_type(self.base)
            base = self.dtd.types.get(self.base)
            args = self.name, self.base
            assert base, "{}: base type {} not found".format(*args)
            return base.dtd_type()


    class ComplexType(Type):
        """
        Schema type for elements which can have attributes and/or child nodes

        Attributes not inherited from `Type` class:
          attributes - dictionary of attributes allowed or required
          content - `DTD.Sequence`, `DTD.Choice`, `DTD.Group`, or None
          model - DTD.EMPTY, DTD.MIXED, DTD.TEXT_ONLY, DTD.ELEMENT_ONLY
        """

        # Prepared error message for multiple content definitions
        CONTENT = "sequence", "choice", "simpleContent", "group"
        CNAMES = ", ".join(["xsd:{}".format(c) for c in CONTENT])
        ERROR = "complex type may only contain one of {}".format(CNAMES)

        def __init__(self, dtd, node):
            """
            Detmine the content and model for this type

            Pass:
              dtd - reference to the object for the DTD we're building
              node - definition for this type in the schema
            """

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
                    raise Exception("{}: {}".format(self.name, self.ERROR))
                elif child.tag == Schema.SIMPLE_CONTENT:
                    self.model = DTD.TEXT_ONLY
                    extension = child.find(Schema.EXTENSION)
                    message = "{}: missing extension".format(self.name)
                    assert len(extension), message
                    self.base = extension.get("base")
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
                        raise Exception("{}: {}".format(self.name, self.ERROR))
            dtd.add_type(self)

        def add_attribute(self, dtd, child):
            """
            Insert an attribute into the `attributes` dictionary

            Pass:
              dtd - reference to the object for the DTD we're building
              node - definition for this attribute in the schema
            """

            attribute = DTD.Attribute(dtd, child)
            if attribute.name in self.attributes:
                values =  self.name, attribute.name
                error = "Duplicate attribute {}/@{}".format(*values)
                raise Exception(error)
            self.attributes[attribute.name] = attribute

        def define_element(self, element):
            """
            Create the string defining an element of this type for the DTD

            Pass:
              element - reference to a `DTD.Element` object

            Return:
              string beginning "<!ELEMENT {element.name} ..."
            """

            if self.model == DTD.TEXT_ONLY:
                return "<!ELEMENT {} (#PCDATA)>".format(element.name)
            elif self.model == DTD.EMPTY:
                return "<!ELEMENT {} EMPTY>".format(element.name)
            elif self.model == DTD.MIXED:
                self.content.get_node(element.children)
                children = "|".join([c.name for c in element.children])
                names = element.name, children
                return "<!ELEMENT {} (#PCDATA|{})*>".format(*names)
            elif self.model == DTD.ELEMENT_ONLY:
                content = self.content.get_node(element.children, True)
                assert content, "Elements required for elementOnly content"
                if not self.dtd.defined:
                    content = "(CdrDocCtl,{})".format(content)
                elif not content.startswith("("):
                    content = "({})".format(content)
                return "<!ELEMENT {} {}>".format(element.name, content)
            raise Exception("{}: unrecognized content model".format(self.name))

        def define_attributes(self, element):
            """
            Make definition for attributes of an element of this type

            Pass:
              element - reference to a `DTD.Element` object

            Return:
              string beginning "<!ATTLIST {element.name} ..."
            """

            attributes = []
            for attribute in self.attributes.values():
                if attribute.name == "cdr:ref":
                    self.dtd.linking_elements.add(element.name)
                attributes.append(str(attribute))
                values = attribute.values
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
                return "<!ATTLIST {} {}>".format(element.name, attributes)


    class ChoiceOrSequence(CountedNode):
        """
        Base class for `Choice` and `Sequence` classes

        Attributes:
          nodes - sequence of content allowed for elements of this type
        """

        def __init__(self, dtd, node):
            """
            Populate and validate the `nodes` attribute

            Pass:
              dtd - reference to the object for the DTD we're building
              node - definition for this choice or sequence in the schema
            """

            DTD.CountedNode.__init__(self, dtd, node)
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
            """
            Invoke `get_node()` for each of the objects nodes

            Also, if requested, we return the serialized DTD definition
            string for the choice or sequence. The side effect of this
            method is to queue up the elements represented by `nodes`
            to have their own definitions build and added to the DTD.

            Pass:
              elements - sequence to which we append elements
              serialize - flag indicating whether we should return a
                          serialized version of this object as content
                          of a wrapper element

            Return:
              serialized string if requested, else None
            """

            nodes = [node.get_node(elements, serialize) for node in self.nodes]
            if serialize:
                string = self.separator.join(nodes)
                if len(self.nodes) > 1:
                    string = "({})".format(string)
                return "{}{}".format(string, self.count_char)


    class Choice(ChoiceOrSequence):
        """
        Set of elements of which one is allowed or required
        """

        # This is the only difference between a `Choice` and a `Sequence`
        separator = "|"

        def __init__(self, dtd, node):
            """
            Hand off construction work to the base class

            Pass:
              dtd - reference to the object for the DTD we're building
              node - definition for this choice in the schema
            """

            DTD.ChoiceOrSequence.__init__(self, dtd, node)
            dtd.session.logger.debug("Choice with %d nodes", len(self.nodes))


    class Sequence(ChoiceOrSequence):
        """
        Ordered sequence of allowed/required elements
        """

        # This is the only difference between a `Choice` and a `Sequence`
        separator = ","

        def __init__(self, dtd, node):
            """
            Hand off construction work to the base class

            Pass:
              dtd - reference to the object for the DTD we're building
              node - definition for this sequence in the schema
            """

            DTD.ChoiceOrSequence.__init__(self, dtd, node)
            dtd.session.logger.debug("Sequence with %d nodes", len(self.nodes))


    class Group:
        """
        Named choice or sequence

        This construct is used to avoid code duplication within a schema,
        so the same list of choices and/or sequence can be used in more
        than one place with copying and pasting the entire definitions.

        Attributes:
          dtd - reference to the object for the DTD we're building
          ref - if set, this object represents a pointer to another
                `Group` object holding the `name` and `node` attributes
          name - string for name by which this choice or sequence is known
          node - reference to `DTD.Choice` or `DTD.Sequence` object
        """

        def __init__(self, dtd, node):
            """
            Extract values from the schema node and register the name

            Pass:
              dtd - reference to the object for the DTD we're building
              node - definition for this group in the schema
            """

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
                    nodes.append(DTD.Choice(dtd, child))
                elif child.tag == Schema.SEQUENCE:
                    nodes.append(DTD.Sequence(dtd, child))
            args = self.name, len(nodes)
            assert len(nodes) == 1, "group {} has {:d} nodes".format(*args)
            self.node = nodes[0]
            dtd.groups[self.name] = self

        def get_node(self, elements, serialize=False):
            """
            Pass through the `get_node()` call to the named choice or sequence

            Pass:
              elements - sequence to which we append elements
              serialize - flag indicating whether we should return a
                          serialized version of this object as content
                          of a wrapper element

            Return:
              serialized string if requested, else None
            """

            if self.ref:
                return self.dtd.groups[self.ref].get_node(elements, serialize)
            else:
                return self.node.get_node(elements, serialize)


    class Attribute:
        """
        Definition for an XML attribute which can attached to an XML element

        Attributes:
          dtd - reference to the object for the DTD we're building
          name - string naming the attribute
          type_name - string identifying the type of the attribute
          required - True if the attribute must be present for its element
                     to be valid

        Property:
          values - sequence of valid value strings for this attribute, if any
        """

        def __init__(self, dtd, node):
            """
            Extract the name and type from the schema node

            Because of a quirk in the way the XSD standard handles namespaces
            in included schema documents, our schemas are unable to use
            namespaces for the attribute names. So we're using attribute
            names without namespaces in the schemas, replacing the colon
            which would have separated the namespace prefix from the local
            name ("non-colonized name" in XML jargon) with a hyphen. Then
            when we pass a document to the schema validation engine, we
            munge the attribute names of the documents to match what we've
            done to the schemas. DTDs don't have this problem, so the DTDs
            we give to XMetaL for validation of the CDR documents have the
            attribute names which actually correspond to what's stored in
            the repository. If we had it to do all over again, knowing what
            we know about the final XSD standard (which hadn't been finished
            at the point when the CDR project was launched), we'd probably
            avoid using namespaces in the CDR documents altogether.

            Pass:
              dtd - reference to the object for the DTD we're building
              node - definition for this attribute in the schema
            """

            self.dtd = dtd
            self.name = node.get("name")
            self.type_name = node.get("type")
            debug = dtd.session.logger.debug
            debug("Attribute %s of type %s", self.name, self.type_name)
            self.required = node.get("use") == "required"
            if self.name.startswith("cdr-"):
                self.name = self.name.replace("cdr-", "cdr:")

        def __str__(self):
            """
            Serialize the attribute definition for the DTD

            Return:
              string for this attribute's DTD definition
            """

            required = self.required and "REQUIRED" or "IMPLIED"
            return "{} {} #{}".format(self.name, self.dtd_type(), required)

        def dtd_type(self):
            """
            Get the string used to define an attribute in the DTD

            Return:
              DTD version of the string used by the schema to represent
              the attribute's base type (e.g., "NMTOKEN", "CDATA", etc.)
            """

            if "xsd:" in self.type_name:
                return DTD.Type.map_type(self.type_name)
            simple_type = self.dtd.types.get(self.type_name)
            if not simple_type:
                vals = self.type_name, self.name
                error = "unrecognized type {} for @{}".format(*vals)
                raise Exception(error)
            return simple_type.dtd_type()

        @property
        def values(self):
            """
            Sequence of valid value strings for this attribute, if any

            Return:
              sequence of string values, or None
            """

            if "xsd:" in self.type_name:
                return None
            simple_type = self.dtd.types.get(self.type_name)
            return simple_type.values

    @classmethod
    def alternate_is_nmtoken(cls, string):
        """
        Original method for determining if a string is an NMTOKEN

        Drop this after the code review.
        """

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

    # Regular-expression pattern used to determine whether a string
    # matches the specification for an NMTOKEN value.
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
    def is_nmtoken(cls, value):
        """
        Test a string to see if it is an NMTOKEN

        Optimize by checking for a space in the string first.

        See https://www.w3.org/TR/REC-xml/#sec-common-syn

        Pass:
          value - string that we're testing

        Return:
          True if the value is an NMTOKEN string; otherwise False
        """
        if " " in value:
            return False
        return cls.NMTOKEN_PATTERN.match(value) and True or False


class LinkType:
    """
    Named specification of which links are allowed from a given element
    """

    # Caching of the link types.
    TYPES = dict()
    TYPE_IDS = dict()
    LOCK = threading.Lock()

    # Codes for limitations on the link target's version.
    CHECK_TYPES = {
        "C": "current document",
        "P": "publishable document",
        "V": "document version"
    }

    def __init__(self, session, **opts):
        """
        Instantiate `LinkType` object

        Called by:
          cdr.getLinkType()
          client XML wrapper command CdrGetLinkType

        Required positional argument:
          session - reference to object representing the current login

        Optional keyword arguments:
          id - primary key into the `link_type` table
          name - string by which the link type is known
          sources - sequence of `LinkSource` objects representing
                    combinations of document type/element name
                    allowed to link to other CDR documents for this
                    link type
          targets - dictionary of `Doctype` objects indexed by
                    doctype ID, representing the types of documents
                    to which links of this type can be made
          comment - string describing the usage of the link type
          properties - sequence of `Property` objects representing custom
                       selection logic for eligible link targets
          chk_type - "C", "P", or "V" (see `LinkType.CHECK_TYPES` above);
                     oddly, the custom logic in the properties does not
                     use this flag; I have not changed this behavior,
                     not knowing what such a change might break, but
                     I susspect such a change would be a good idea
        """

        self.__session = session
        self.__opts = opts

    @property
    def cursor(self):
        """
        Give the link type object its own database cursor
        """

        if not hasattr(self, "_cursor"):
            self._cursor = self.session.conn.cursor()
        return self._cursor

    @property
    def session(self):
        """
        Reference to `users.Session` object representing the current login
        """

        return self.__session

    @property
    def properties(self):
        """
        Sequence of `Property` object for custom validation rules
        """

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
        """
        Combination of document type and element for linking documents
        """

        def __init__(self, doctype, element):
            self.doctype = doctype
            self.element = element

    @property
    def sources(self):
        """
        Sequence of `LinkSource` object for allowable linker for this type
        """

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
        """
        Description of this link types usage
        """

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
        """
        Dictionary of `Doctype` object allowed as target for link type

        The dictionary is indexed by the `doc_type` table primary keys.
        """

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
        """
        Code for the version requirements imposed on valid link targets

        One of `LinkType.CHECK_TYPES`.
        """

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
        """
        Primary key into the `link_type` table for this object
        """

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
        """
        Human-readable name for this link type
        """

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

        Useful for populating picklists in the user interface when
        the user is trying to create a valid link from the document
        being edited to another CDR document.

        Called by:
          cdr.search_links()
          client XML wrapper command CdrSearchLinks

        Keyword arguments:
          pattern - titles of candidate target docs must match this pattern
          limit - optional integer restricting the size of the result set

        Return:
          possibly empty sequence of `Doc` objects
        """

        self.session.log("LinkType.search({!r}, {!r})".format(self.name, opts))
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
        """
        Store a new or modified link type's information

        Called by:
          cdr.pubLinkType()
          client XML wrapper command CdrAddLinkType
          client XML wrapper command CdrModLinkType
        """

        self.session.log("LinkType.save({!r})".format(self.name))
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

    def delete(self):
        """
        Remove the type's row from the `link_type` table

        Also drops related rows.

        Called by:
          cdr.delLinkType()
          client XML wrapper command CdrDelLinkType
        """

        self.session.log("LinkType.delete({!r})".format(self.name))
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
        """
        Delete database rows for the link type

        Drop related rows first so database relational integrity doesn't
        block dropping the primary row.

        Wrapped in a helper function to facilitate rollback in the event
        of failure.
        """

        self.__drop_related_rows()
        delete = "DELETE FROM link_type WHERE id = ?"
        self.cursor.execute(delete, (self.id,))

    def __drop_related_rows(self):
        """
        Delete rows linking to this type's `link_type` row

        This done in preparation for deleting the link type altogether.
        Called (indirectly) by `LinkType.delete()`.
        """

        for table in ("link_xml", "link_target", "link_properties"):
            column = "link_id"
            if table == "link_target":
                column = "source_link_type"
            delete = "DELETE FROM {} WHERE {} = ?".format(table, column)
            self.cursor.execute(delete, (self.id,))

    def __save(self):
        """
        Separate out the database writes so rollback from failures is easier
        """

        # Create or update the row in the `link_type` table.
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

        # Create the rows identifying which elements in which document
        # types can contain links of this type.
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

        # Create the rows identifying which document types can be targets
        # for this link type.
        for target in itervalues(self.targets):
            assert target.id, "doc type {} not found".format(target.name)
            names = "source_link_type, target_doc_type"
            values = self.id, target.id
            insert = "INSERT INTO link_target ({}) VALUES (?, ?)".format(names)
            try:
                self.cursor.execute(insert, values)
            except:
                self.session.logger.exception("targets=%s", self.targets)
                raise

        # Save the custom validation/filtering logic for the link type.
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

        Called by:
          cdr.getLinkTypes()
          client XML wrapper command CdrListLinkTypes
        """

        session.log("LinkType.get_linktype_names()")
        query = Query("link_type", "name").order("name")
        return [row.name for row in query.execute(session.cursor).fetchall()]

    @classmethod
    def get(cls, session, id):
        """
        Get or create the `LinkType` object for this linking type

        Cache the type so we don't have to create it multiple times.

        Pass:
          session - reference to object representing the current login
          id - primary key for the `link_type` table

        Return:
          reference to a `LinkType` object
        """

        if id not in cls.TYPES:
            cls.TYPES[id] = cls(session, id)
        return cls.TYPES[id]

    @classmethod
    def lookup(cls, session, doctype, element_tag):
        """
        Find the `LinkType` object for this linking source

        Called by:
          cdr.check_proposed_link()
          client XML wrapper command CdrPasteLink
          internal link validation code

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

        These are all of the flavors of custom link properties which
        can be assigned to a link type. Currently there is only one,
        but more could be added in the future.

        Called by:
          cdr.getLinkProps()
          client XML wrapper command CdrListLinkProps

        Required positional argument:
          session - reference to object representing the current login
        """

        session.log("LinkType.get_property_types()")
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

    class Property:
        """
        Base class for link type properties

        These are used to refine the logic for determine which elements
        can link to which documents. At present we only have one such
        flavor of `Property`, for determining whether the target document
        contains specific values.

        Attributes:
          session - reference to object representing current login
          cursor - the property object gets its own database cursor
          name - string telling us what category of property we have
          value - string holding logic for custom filtering/validation
          comment - optional string describing the custom property
        """

        def __init__(self, session, name, value, comment):
            """
            Wrap the custom rule attributes in an object

            Pass:
              session - reference to object representing current login
              name - string telling us what category of property we have
              value - string holding logic for custom filtering/validation
              comment - optional string describing the custom property
            """

            self.session = session
            self.cursor = session.conn.cursor()
            self.name = name
            self.value = value
            self.comment = comment

        @property
        def id(self):
            """
            Primary key into the `link_prop_type` table
            """

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
        because there's no limit (other than disallowing the double-quote
        mark itself) on when can show up between the double-quote marks.
        """

        # Regular expression used to pull out tokens from the property string.
        PATTERN = re.compile(r"""
            "[^"]*"          # double-quoted string for value to test for
          | /[^\s()|&=!+*-]+ # path value for query_term[_pub] table tests
          | [()|&*]          # single-character tokens
          | [=!+-]=          # double-character operator tokens
          | \bAND\b          # alias for & operator
          | \bOR\b           # alias for | operator
          | \bNOT\b          # negation of assertion
        """, re.VERBOSE | re.IGNORECASE)

        # Aliases for Boolean keywords.
        CONNECTORS = {"|": "OR", "&": "AND"}

        # See notes above for description of 'picklist-only' alternates.
        OPERATORS = {"==", "!=", "+=", "-="}

        def __init__(self, session, name, value, comment):
            """
            Extract the logic for the custom validation/filtering

            The base class constructor captures most of the object's
            attributes.

            Pass:
              session - reference to object representing current login
              name - string telling us what category of property we have
              value - string holding logic for custom filtering/validation
              comment - optional string describing the custom property
            """

            LinkType.Property.__init__(self, session, name, value, comment)
            self.assertions = self.parse(value)

        @classmethod
        def parse(cls, property_string):
            """
            Extract the tokens from the property string

            Refer to the class documentation above for specifics on the
            logic.

            Pass:
              property_string - string containing the custom test(s)
                                for filtering/validating links
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
            """
            Determine whether a specific link is allowed

            Pass:
              link - reference to `Link` object being evaluated

            Return:
              None

            Side effects:
              population of the link's document's `errors` property
              if the custom validation rules are not satisfied
            """

            if not self.assertions.test(link):
                error = "Failed link target rule: {}".format(self.value)
                link.add_error(error)

        @property
        def conditions(self):
            """
            Add clauses needed to find link targets satisfying this property

            Passes on the work to the `Assertions` object. This is used
            when we are constructing SQL queries for picklists containing
            link target candidates. It is not used for validation of an
            existing link, which is somewhat more straightforward.

            Pass:
              query - `db.Query` object to be refined with new conditions
            """

            return self.assertions.conditions


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

                Used for building picklists of candidate link targets (not
                for validating an existing link, which is more
                straightforward).

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
                return ands


        class Assertion(Testable):
            """
            Single test for values found in link target candidates

            Attributes:
              path - location of value in the link target candidate document
              operator - describes the relationship the document's value
                         should have to the value specified for the assertion;
                         (equal to or not equal to); see class documentation
                         above for the `LinkTargetContains` class
              value - string being compared to what is found in the link
                      target candidate document
              negative - True if assertion was preceded by "NOT"
              picklist_only - True if the assertion should be ignored
                              when we are validating an existing link
                              (rather than collecting candidate link targets
                              for a picklist)
            """

            def __init__(self, path, operator, value, negative=False):
                """
                Package the attributes into in `Assertion` instance

                Pass:
                  path - e.g., "/Term/TermType/TermTypeName"
                  operator - e.g., "!="
                  value - e.g., "Obsolete term"
                  negative - True if "NOT" preceded the assertion in the
                             property's string value; not that this is
                             different from the negation contained in
                             the "!= (not equal to) operator
                """

                if not path or not operator:
                    raise Exception("malformed link property assertion")
                self.path = path
                self.operator = operator
                self.value = value
                self.negative = negative
                self.picklist_only = operator[0] in "+-"

            def test(self, link):
                """
                Apply the assertion to an existing link to see if it is valid

                Pass:
                  link - reference to `Link` object

                Return:
                  `True` if the assertion is satisfied, or is not used for
                  validation (`picklist_only`); otherwise `False`
                """

                # If this assertion's operator indicates that it should
                # only be used for constructing picklists of link target
                # candidates, don't use it to declare an existing link
                # invalid.
                if self.picklist_only:
                    result = True

                else:
                    # See if the document has the value in the specified
                    # location at least once. Note that (unlike the search
                    # module) we don't support relationships other than
                    # equality (no CONTAINS or similar operators).
                    doc_id = link.target_doc.id
                    cursor = link.doc.session.conn.cursor()
                    query = Query("query_term", "COUNT(*)")
                    query.where(query.Condition("doc_id", doc_id))
                    query.where(query.Condition("path", self.path))

                    # The assertion can test for the presence of an
                    # element within the document without regard to
                    # what the value is.
                    if self.value:
                        query.where(query.Condition("value", self.value))

                    # If the document should have the value, but doesn't,
                    # the link is invalid; same if it shouldn't but does.
                    count = query.execute(cursor).fetchone()[0]
                    if self.operator == "==":
                        result = count > 0
                    else:
                        result = count == 0

                    # If the assertion was preceded by "NOT" flip the result.
                    if self.negative:
                        result = not result

                return result

            @property
            def condition(self):
                """
                Database `Query.Condition` object for picklist queries
                """
                query = Query("query_term", "doc_id")
                query.where(query.Condition("path", self.path))
                if self.value:
                    query.where(query.Condition("value", self.value))
                negative = self.negative
                if self.operator[0] in "!-":
                    negative = not negative
                operator = "NOT IN" if negative else "IN"
                return query.Condition("d.id", query, operator)


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
    """

    CDR_ID = Doc.qname("id")
    CDR_REF = Doc.qname("ref")
    CDR_HREF = Doc.qname("href")
    CDR_XREF = Doc.qname("xref")
    LINK_ATTRS = CDR_REF, CDR_HREF, CDR_XREF
    INTERNAL_LINK_ATTRS = {CDR_REF, CDR_HREF}
    VERSIONS = dict(C="Current", V="last", P="lastp")

    def __init__(self, doc, node, name):
        """
        Collect the linking information for this element node (if any)

        Pass:
          doc - reference to `Doc` object for linking document
          node - reference to `etree._Element` object containing the link
          name - attribute name for link
        """

        # Start with a clean slate
        doc.session.logger.debug("top of Link() constructor")
        self.link_name = self.url = self.internal = self.store = None
        self.target_doc = self.fragment_id = self.linktype = None

        # Capture the values we were given.
        self.doc = doc
        self.node = node
        self.element = node.tag
        self.link_name = name
        self.internal = name in self.INTERNAL_LINK_ATTRS
        self.eid = node.get("cdr-eid")
        self.id = node.get(self.CDR_ID)
        self.url = node.get(name)
        self.key = self.element, self.url
        doc.session.logger.debug("%r@%r=%r", self.element, name, self.url)

        # Collect the information that's only relevant to internal links.
        if self.internal:
            if "#" in self.url:
                doc_id, self.fragment_id = self.url.split("#", 1)
            else:
                doc_id = self.url
            args = doc.session, doc.doctype, node.tag
            self.linktype = LinkType.lookup(*args)
            if self.linktype:
                self.chk_type = self.linktype.chk_type
                doc.session.logger.debug("link type is %s", self.linktype.name)
            else:
                self.chk_type = "C"
                doc.session.logger.debug("link type not found")
            self.chk_type = self.linktype.chk_type if self.linktype else "C"
            version = self.VERSIONS[self.chk_type]
            try:
                target_doc = Doc(doc.session, id=doc_id, version=version)
                assert target_doc.doctype, "version not found"
                doc.session.logger.debug("target doc is %s", target_doc.cdr_id)
                self.target_doc = target_doc
            except Exception as e:
                doc.session.logger.debug("link type not found: %s", e)
                self.store = False
        doc.session.logger.debug("bottom of Link() constructor")

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

    def validate(self):
        """
        Find out whether this link meets its requirements

        Return:
          None

        Side effects:
          population of the linking document's error log
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
    """
    Named set of CDR filters

    Properties:
      id - primary key into the `filter_set` table
      name - unique string by which the set is known
      description - brief description of the set, used for UI
      notes - more extensive optional notes on the use of the filter set
      members - sequence of `Doc` and/or nested `FilterSet` objects
    """

    def __init__(self, session, **opts):
        """
        Construct an object for a named set of CDR filters

        Required positional argument:
          session - reference to object representing the current login

        Optional positional arguments:
          id - primary key into the `filter_set` table
          name - string by which the set is known
          desc - brief description of the set, used for UI
          notes - more extensive optional notes on the use of the filter set
          members - sequence of `Doc` and/or nested `FilterSet` objects

        Called by:
          cdr.getFilterSet()
          client XML wrapper command CdrGetFilterSet
        """
        self.__session = session
        self.__opts = opts
        self.session.logger.debug("FilterSet(opts=%s)", opts)

    @property
    def cursor(self):
        """
        Give the set object its own database cursor
        """

        if not hasattr(self, "_cursor"):
            self._cursor = self.session.conn.cursor()
        return self._cursor

    @property
    def id(self):
        """
        Primary key integer into the `filter_set` table
        """

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
        """
        Unique string by which the filter set is known
        """

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
        """
        Brief description of the set, used for UI
        """

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
        """
        Sequence of `Doc` and/or nested `FilterSet` objects
        """

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
        """
        More extensive optional notes on the use of the filter set
        """

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
        """
        Reference to `users.Session` object representing current login
        """

        return self.__session

    def delete(self):
        """
        Remove an existing named set of CDR filter

        Does not remove the filters themselves, just the set and its
        membership.

        Called by:
          cdr.delFilterSet()
          client XML wrapper command CdrDelFilterSet
        """

        self.session.log("FilterSet.delete({!r})".format(self.name))
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
        """
        Database DELETE requests, separated out for easier failure recovery
        """

        tables = [
            ("filter_set_member", "filter_set"),
            ("filter_set", "id")
        ]
        for table, column in tables:
            sql = "DELETE FROM {} WHERE {} = ?".format(table, column)
            self.cursor.execute(sql, (self.id,))
        self.session.conn.commit()

    def save(self):
        """
        Store the new or updated information for a named set of CDR filters

        Called by:
          cdr.addFilterSet()
          cdr.repFilterSet()
          client XML wrapper command CdrAddFilterSet
          client XML wrapper command CdrRepFilterSet

        Return:
          None
        """

        self.session.log("FilterSet.save({!r})".format(self.name))
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
        """
        Database writes, separated out for easier failure recovery

        There's a bug in the `adodbapi` package, which blows up when
        `None` is passed for a placeholder for a nullable `NTEXT` column.
        So we have to use "= NULL" in the code for NULL values.

        Return:
          integer for the number of members in the set (this is the
          number of direct members, not the number of (possibly nested)
          filters in the set)
        """

        # Assemble the values for the database INSERT or UPDATE.
        fields = dict(
            name=self.name,
            description=self.description,
            notes=self.notes
        )
        names = sorted(fields)
        values = [] #fields[name] for name in names]
        # BUG IN ADODBAPI WHEN TRYING TO INSERT NULL INTO NTEXT WITH NONE

        # Update the existing row if this is not a new filter set.
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

        # Otherwise, create a new row in the `filter_set` table.
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
        self.session.logger.debug("sql=%s values=%s", sql, tuple(values))
        self.cursor.execute(sql, values)

        # Clear out the `filter_set_member` for an existing set.
        if self.id:
            delete = "DELETE FROM filter_set_member WHERE filter_set = ?"
            self.cursor.execute(delete, (self.id,))
        else:
            self.cursor.execute("SELECT @@IDENTITY AS id")
            self._id = self.cursor.fetchone().id

        # Add the related rows for the set's members.
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

        # Tell how many direct children the set has (not total filters).
        return len(self.members)

    @classmethod
    def get_filter_sets(cls, session):
        """
        Fetch the list of filter sets in the CDR

        Called by:
          cdr.getFilterSets()
          client XML wrapper command CdrGetFilterSets

        Pass:
          session - reference to object for current login

        Return:
          sequence of id, name tuples for filter sets
        """

        session.log("FilterSet.get_filter_sets()")
        query = Query("filter_set", "id", "name").order("name")
        return [tuple(row) for row in query.execute(session.cursor).fetchall()]

    @classmethod
    def get_filters(cls, session):
        """
        Fetch the list of filter documents in the CDR

        Called by:
          cdr.getFilters()
          client XML wrapper command CdrGetFilters

        Pass:
          session - reference to object for current login

        Return:
          sequence of `Doc` objects for Filter documents
        """

        session.log("FilterSet.get_filters()")
        query = Query("document d", "d.id", "d.title").order("d.title")
        query.join("doc_type t", "t.id = d.doc_type")
        query.where("t.name = 'Filter'")
        rows = query.execute(session.cursor).fetchall()
        return [Doc(session, id=row.id, title=row.title) for row in rows]


class GlossaryTermName:
    """
    Dictionary term with its aliases

    We collect the aliases from within the documents (the other names fields)
    as well as from the external mapping table, where variants of the name
    for the term have been recorded manually. Used for glossification of
    running text in CDR documents.

    Attributes:
      id - primary key for the CDR `GlossaryTermName` document
      name - primary name for the string in the language being mapped
      phrases - set of alternate strings representing the same glossary
                concept
    """

    # Regular expressions uses to normalize the phrases for matching.
    UNWANTED = re.compile(u"""['".,?!:;()[\]{}<>\u201C\u201D\u00A1\u00BF]+""")
    TOKEN_SEP = re.compile(r"[\n\r\t -]+")

    def __init__(self, id, name):
        """
        Capture name and ID for document and start with an empty phrase set

        Pass:
          id - integer for unique CDR document indentifier
          name - primary name for string in the language being mapped
        """

        self.id = id
        self.name = name or None
        self.phrases = set()

    @classmethod
    def get_mappings(cls, session, language="en"):
        """
        Fetch the mappings of phrases to English or Spanish glossary term names

        Called by:
          cdr.get_glossary_map()
          client XML wrapper command CdrGetGlossaryMap
          client XML wrapper command CdrGetSpanishGlossaryMap

        Pass:
          session - reference to object for current login
          language - "en" (the default) or "es"

        Return:
          sequence of `GlossaryTermName` objects
        """

        session.log("GlossaryTermName.get_mappings({})".format(language))
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
        """
        Make it easier to match phrases in running text

        Strip unwanted characters and replace punctuation and other
        inter-word separators with a single space, collapsing multiple
        spaces into one. Trim leading and trailing whitespace and fold
        case together.

        Pass:
          phrase - string for phrase to be matched

        Return:
          normalized version of string
        """

        phrase = cls.UNWANTED.sub(u"", cls.TOKEN_SEP.sub(u" ", phrase)).upper()
        return phrase.strip()
