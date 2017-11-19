"""
CDR Python client wrapper

This module provides the legacy CDR functions, reimplemented to run
the new Python implementation of the functionality originally provided
by the C++ CDR Server. When that is not possible (that is, the client
is running on a different host, without direct access to the CDR
database) the commands are tunneled through an HTTPS web service, which
in turn invokes the new Python implementation of the functionality.

This module is now compatible with Python 3 and Python 2.
"""

import base64
import datetime
import logging
import random
import re
import dateutil.parser
import requests
from lxml import etree
import cdrapi.db
from cdrapi.settings import Tier
from cdrapi.users import Session
from cdrapi import docs

# ======================================================================
# Make sure we can run on both Python 2 and Python 3
# ======================================================================
try:
    basestring
    is_python3 = False
    base64encode = base64.encodestring
    base64decode = base64.decodestring
except:
    base64encode = base64.encodebytes
    base64decode = base64.decodebytes
    basestring = (str, bytes)
    unicode = str
    is_python3 = True


# ======================================================================
# Manage CDR login sessions
# ======================================================================
def login(username, password="", **opts):

    """
    Create a CDR login session

    Pass:
      username - name of CDR use account
      password - password for the account (can be empty when logging in
                 from localhost)
      comment - optional comment to be stored with the session info
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      string identifier for new session if tunneling; otherwise Session object
    """
    tier = opts.get("tier") or opts.get("host") or None
    credentials = username, password
    comment = opts.get("comment")
    return _Control.get_session(credentials, tier=tier, comment=comment)


def dupSession(session, **opts):
    """
    Duplicate a CDR login session

    Useful when a task which needs to be queued for later processing
    will possibly be run after the requesting user has closed the
    original session from which the request was made.

    Pass:
      session - string identifier for the existing session
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      string identifier for the new session
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(session, tier)
    if isinstance(session, Session):
        return session.duplicate()
    else:
        command = etree.Element("CdrDupSession")
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == "CdrDupSessionResp":
                return get_text(response.node.find("NewSessionId"))
            raise Exception(";".join(response.errors) or "missing response")
        raise Exception("missing response")


def logout(session, **opts):
    """
    Close a CDR login session

    Pass:
      session - string identifier for session
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(session, tier)
    if isinstance(session, Session):
        session.logout()
    else:
        _Control.send_command(session, etree.Element("CdrLogoff"), tier)


# ======================================================================
# Manage CDR groups/actions/permissions
# ======================================================================

def canDo(session, action, doctype="", **opts):
    """
    Determine whether a session is authorized to do something

    Pass:
      session - name of session whose permissions are being checked
      action - what the session wants to do
      doctype - optional name of doctype involved

    Return:
      True if allowed, otherwise False
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(session, tier)
    if isinstance(session, Session):
        return session.can_do(action, doctype)
    else:
        command = etree.Element("CdrCanDo")
        etree.SubElement(command, "Action").text = action
        if doctype:
            etree.SubElement(command, "DocType").text = doctype
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == "CdrCanDoResp":
                return response.node.text == "Y"
            raise Exception(";".join(response.errors) or "missing response")
        raise Exception("missing response")


def getActions(credentials, **opts):
    """
    Get the list of CDR actions which can be authorized

    Pass:
      credentials - name of existing session or login credentials
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      dictionary of authorizable actions, indexed by action name, with
      the value of a flag ("Y" or "N") indicating whether the action
      must be authorized on a per-doctype basis
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    actions = {}
    if isinstance(session, Session):
        for action in session.list_actions():
            actions[action.name] = action.doctype_specific
        return actions
    command = etree.Element("CdrListActions")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrListActionsResp":
            for action in response.node.findall("Action"):
                name = get_text(action.find("Name"))
                flag = get_text(action.find("NeedDoctype"))
                actions[name] = flag
            return actions
        raise Exception(";".join(response.errors) or "missing response")
    raise Exception("missing response")


def getAction(credentials, name, **opts):
    """
    Retrieve information about a CDR action

    Pass:
      credentials - name of existing session or login credentials
      name - name of the action
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      `Action` object, with `name`, `doctype_specific`, and `comment`
      attributes
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return session.get_action(name)
    command = etree.Element("CdrGetAction")
    etree.SubElement(command, "Name").text = name
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrGetActionResp":
            name = get_text(response.node.find("Name"))
            flag = get_text(response.node.find("DoctypeSpecific"))
            comment = get_text(response.node.find("Comment"))
            return Action(name, flag, comment)
        raise Exception(";".join(response.errors) or "missing response")
    raise Exception("missing response")


def putAction(credentials, name, action, **opts):
    """
    Store information for a CDR action

    Pass:
      credentials - name of existing session or login credentials
      name - name of action to update, or None if creating new action
      action - Session.Action object containing information to store
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        if name:
            if not action.id:
                action.id = getAction(session, name).id
            action.modify(session)
        else:
            action.add(session)
    else:
        if name:
            command_name = "CdrRepAction"
            new_name = action.name
        else:
            command_name = "CdrAddAction"
            name = action.name
            new_name = None
        command = etree.Element(command_name)
        fp = open("d:/tmp/putAction.log", "a")
        fp.write("command_name is {}\n".format(command_name))
        etree.SubElement(command, "Name").text = name
        if new_name and new_name != name:
            etree.SubElement(command, "NewName").text = new_name
        flag = action.doctype_specific
        etree.SubElement(command, "DoctypeSpecific").text = flag
        if action.comment is not None:
            etree.SubElement(command, "Comment").text = action.comment
        for response in _Control.send_command(session, command, tier):
            fp.write("tag nams is {}\n".format(response.node.tag))
            if response.node.tag == command_name + "Resp":
                fp.close()
                return
            fp.close()
            raise Exception(";".join(response.errors) or "missing response")
        raise Exception("missing response")


def delAction(credentials, name, **opts):
    """
    Delete a CDR action

    Pass:
      credentials - name of existing session or login credentials
      name - name of action to delete
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        session.get_action(name).delete(session)
    else:
        command = etree.Element("CdrDelAction")
        etree.SubElement(command, "Name").text = name
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == "CdrDelActionResp":
                return
            raise Exception(";".join(response.errors) or "missing response")
        raise Exception("missing response")


def getGroups(credentials, **opts):
    """
    Get the list of CDR authorization groups

    Pass:
      credentials - name of existing session or login credentials
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      sorted sequence of CDR group name strings
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return session.list_groups()
    command = etree.Element("CdrListGrps")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrListGrpsResp":
            groups = [g.text for g in response.node.findall("GrpName")]
            return sorted(groups)
        raise Exception(";".join(response.errors) or "missing response")
    raise Exception("missing response")


def getGroup(credentials, name, **opts):
    """
    Retrieve information about a CDR authorization group

    Pass:
      credentials - name of existing session or login credentials
      name - required string for name of group to retrieve
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      Session.Group object
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return session.get_group(name)
    command = etree.Element("CdrGetGrp")
    etree.SubElement(command, "GrpName").text = name
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrGetGrpResp":
            group = Session.Group()
            for child in response.node.findall("*"):
                if child.tag == "GrpName":
                    group.name = child.text
                elif child.tag == "GrpId":
                    group.id = int(child.text)
                elif child.tag == "Comment":
                    group.comment = child.text
                elif child.tag == "UserName":
                    group.users.append(child.text)
                elif child.tag == "Auth":
                    action = get_text(child.find("Action"))
                    doctype = get_text(child.find("DocType"))
                    if action not in group.actions:
                        group.actions[action] = []
                    group.actions[action].append(doctype or "")
            return group
        raise Exception(";".join(response.errors) or "missing response")
    raise Exception("missing response")


def putGroup(credentials, name, group, **opts):
    """
    Store information about a CDR permissions group

    Pass:
      credentials - name of existing session or login credentials
      name - name of group to update, or None if creating new group
      group - Session.Group object containing information to store
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        if name:
            if not group.id:
                group.id = getGroup(session, name).id
            group.modify(session)
        else:
            group.add(session)
    else:
        if name:
            command_name = "CdrModGrp"
            new_name = group.name
        else:
            command_name = "CdrAddGrp"
            name = group.name
            new_name = None
        command = etree.Element(command_name)
        etree.SubElement(command, "GrpName").text = name
        if new_name and new_name != name:
            etree.SubElement(command, "NewGrpName").text = new_name
        if group.comment:
            etree.SubElement(command, "Comment").text = group.comment
        for user in group.users:
            etree.SubElement(command, "UserName").text = user
        for action in sorted(group.actions):
            doctypes = group.actions[action] or [""]
            for doctype in doctypes:
                auth = etree.SubElement(command, "Auth")
                etree.SubElement(auth, "Action").text = action
                etree.SubElement(auth, "DocType").text = doctype or ""
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command_name + "Resp":
                return
            raise Exception(";".join(response.errors) or "missing response")
        raise Exception("missing response")


def delGroup(credentials, name, **opts):
    """
    Delete a CDR permission group

    Pass:
      credentials - name of existing session or login credentials
      name - name of group to delete
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        session.get_group(name).delete(session)
    else:
        command = etree.Element("CdrDelGrp")
        etree.SubElement(command, "GrpName").text = name
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == "CdrDelGrpResp":
                return
            raise Exception(";".join(response.errors) or "missing response")
        raise Exception("missing response")


# ======================================================================
# Manage CDR document types
# ======================================================================

#----------------------------------------------------------------------
# Class to contain CDR document type information.
#----------------------------------------------------------------------
class dtinfo:
    def __init__(self, **opts):
        names = ("type", "format", "versioning", "created", "schema_mod",
                 "dtd", "schema", "vvLists", "comment", "error", "active")
        for name in names:
            setattr(self, name, opts.get(name))
        if "type" not in opts:
            self.type = opts.get("name")

    @property
    def name(self):
        return self.type

    def getChildren(self, parent=None):
        """
        Get a list of top level children for a document type, or children
        of a specific element.

        Pass:
            parent - Name of element for which children are desired.
                     None = same as passing Document Type node.
                     Multi-element paths (e.g. "X/Y/Z") not allowed (:<)

        Return:
            Sequence of element names, in the order that the schema allows
            them to appear in the block.  The same name can appear more
            than once if that is allowed in the schema.
        """
        if not self.dtd:
            raise Exception("document type %s has no DTD" % self.type)
        parent = parent or self.name
        pattern = r"<!ELEMENT\s+{}\s+([^>]+)>".format(parent)
        match = re.search(pattern, self.dtd)
        if not match:
            raise Exception("definition of element %s not found" % parent)
        return [c for c in re.split(r"\W+", match.group(1))
                if c and c != "CdrDocCtl"]

    def __repr__(self):
        if self.error: return self.error
        return """\
[CDR Document Type]
            Name: {}
          Format: {}
      Versioning: {}
         Created: {}
          Active: {}
 Schema Modified: {}
          Schema:
{}
             DTD:
{}
         Comment:
{}
""".format(self.type or "",
           self.format or "",
           self.versioning or "",
           self.created or "",
           self.active or "",
           self.schema_mod or "",
           self.schema or "",
           self.dtd or "",
           self.comment or "")

def getDoctype(credentials, name, **opts):
    """
    Retrieve document type information from the CDR

    Add active flag (OCECDR-4091).

    Pass:
      credentials - name of existing session or login credentials
      name - string for document type's name
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return
      `dtinfo` object
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        doctype = docs.Doctype(session, name=name)
        values = doctype.vv_lists
        vv_lists = [(name, values[name]) for name in sorted(values)]
        args = dict(
            name=doctype.name,
            format=doctype.format,
            versioning=doctype.versioning,
            created=doctype.created,
            schema_mod=doctype.schema_mod,
            dtd=doctype.dtd,
            schema=doctype.schema,
            vvLists=vv_lists,
            comment=doctype.comment,
            active=doctype.active
        )
        return dtinfo(**args)
    command = etree.Element("CdrGetDocType", Type=name, GetEnumValues="Y")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrGetDocTypeResp":
            args = dict(
                name=response.node.get("Type"),
                format=response.node.get("Format"),
                versioning=response.node.get("Versioning"),
                created=response.node.get("Created"),
                schema_mode=response.node.get("SchemaMod"),
                active=response.node.get("Active"),
                vvLists=[]
            )
            for child in response.node:
                if child.tag == "Comment":
                    args["comment"] = get_text(child)
                elif child.tag == "DocDtd":
                    args["dtd"] = get_text(child)
                elif child.tag == "DocSchema":
                    args["schema"] = get_text(child)
                elif child.tag == "EnumSet":
                    values = [v.text for v in child.findall("ValidValue")]
                    args["vvLists"].append((child.get("Node"), values))
            return dtinfo(**args)
        else:
            raise Exception(";".join(response.errors) or "missing response")
    raise Exception("missing response")

#----------------------------------------------------------------------
# Create a new document type for the CDR.
#----------------------------------------------------------------------
def addDoctype(credentials, info, **opts):
    """
    Create a new document type for the CDR

    Required positional arguments:
      credentials - name of existing session or login credentials
      info - reference to `dtinfo` object for new document type

    Optional keyword arguments
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return
      `dtinfo` object
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        opts = dict(
            name=info.name,
            schema=info.schema,
            format=info.format,
            versioning=info.versioning,
            comment=info.comment
        )
        doctype = docs.Doctype(session, **opts)
        doctype.save()
        return getDoctype(credentials, info.name, tier=tier)

    # Create the command
    command = etree.Element("CdrAddDocType")
    command.set("Type", info.type)
    if info.format:
        command.set("Format", info.format)
    if info.versioning:
        command.set("Versioning", info.versioning)
    if info.active:
        command.set("Active", info.active)
    etree.SubElement(command, "DocSchema").text = info.schema
    if info.comment is not None:
        etree.SubElement(command, "Comment").text = info.comment

    # Submit the request.
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrAddDocTypeResp":
            return getDoctype(credentials, info.name, tier=tier)
        else:
            print(response.node.tag)
            raise Exception(";".join(response.errors) or "missing response")
    raise Exception("missing response")

def modDoctype(credentials, info, **opts):
    """
    Modify existing document type information in the CDR

    Required positional arguments:
      credentials - name of existing session or login credentials
      info - reference to `dtinfo` object for new document type

    Optional keyword arguments
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return
      `dtinfo` object
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        opts = dict(
            name=info.name,
            schema=info.schema,
            format=info.format,
            versioning=info.versioning,
            comment=info.comment,
        )
        if info.active:
            opts["active"] = info.active
        doctype = docs.Doctype(session, **opts)
        doctype.save()
        return getDoctype(credentials, info.name, tier=tier)

    # Create the command
    command = etree.Element("CdrModDocType")
    command.set("Type", info.type)
    command.set("Format", info.format)
    command.set("Versioning", info.versioning)
    if info.active:
        command.set("Active", info.active)
    etree.SubElement(command, "DocSchema").text = info.schema
    etree.SubElement(command, "Comment").text = info.comment or ""

    # Submit the request.
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrModDocTypeResp":
            return getDoctype(credentials, info.name, tier=tier)
        else:
            raise Exception(";".join(response.errors) or "missing response")
    raise Exception("missing response")

def delDoctype(credentials, name, **opts):
    """
    Delete document type from the CDR

    Of marginal use, since deleting a document type for which documents
    have been created is prevented. Adding this wrapper function so
    we can include the command in the unit test suite.

    Required positional arguments:
      credentials - name of existing session or login credentials
      name - string for document type's name

    Optional keyword arguments
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        doctype = docs.Doctype(session, name=name)
        doctype.delete()
    else:
        command = etree.Element("CdrDelDocType", Type=name)
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == "CdrDelDocTypeResp":
                return
            raise Exception(";".join(response.errors) or "missing response")
        raise Exception("missing response")

def getVVList(credentials, doctype, element, **opts):
    """
    Retrieve a list of valid values defined in a schema for a doctype

    Required positional arguments:
      credentials - name of existing session or login credentials
      doctype - string name of the document type
      element - string name of the element for which values are requested

    Optional keyword arguments:
      sorted - if True, sort list alphabetically
      first - value(s) to move to the top of the sequence
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      sequence of string values
    """

    # Get a `dtinfo` object for the document type
    doctype = getDoctype(credentials, doctype, **opts)

    # Find the value list for the specified element.
    values = None
    for name, vals in doctype.vvLists:
        if name == element:
            values = vals
            break
    if values is None:
        message = "No valid values for {!r} in doctype {!r}"
        raise Exception(message.format(doctype.name, element))

    # Put the values in alphabetical order if requested.
    if opts.get("sorted"):
        values.sort()

    # Customize the list order by moving some values to the front if requested.
    first = opts.get("first")
    if first:
        if not isinstance(first, (tuple, list, set)):
            first = [first]
        custom = list(first)
        first = set(first)
        for value in values:
            if value not in first:
                custom.append(value)

    # Give the caller the sequence of values
    return values

def getDoctypes(credentials, **opts):
    """
    Get the list of active CDR document types

    Pass:
      credentials - name of existing session or login credentials
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return
      sequence of active document type names, sorted alphabetically
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return docs.Doctype.list_doc_types(session)
    command = etree.Element("CdrListDocTypes")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrListDocTypesResp":
            return [get_text(t) for t in response.node.findall("DocType")]
        else:
            raise Exception(";".join(response.errors) or "missing response")
    raise Exception("missing response")

def getSchemaDocs(credentials, **opts):
    """
    Get the list of CDR schema documents.

    Pass:
      credentials - name of existing session or login credentials
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return
      sequence of schema document titles, sorted alphabetically
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return docs.Doctype.list_schema_docs(session)
    command = etree.Element("CdrListSchemaDocs")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrListSchemaDocsResp":
            return [get_text(t) for t in response.node.findall("DocTitle")]
        else:
            raise Exception(";".join(response.errors) or "missing response")
    raise Exception("missing response")


# ======================================================================
# Manage CDR documents
# ======================================================================


class Doc:
    """
    Object containing components of a CdrDoc element.
    """

    def __init__(self, x, type = None, ctrl = None, blob = None, id = None,
                 encoding = 'latin-1'):
        """
        An object encapsulating all the elements of a CDR document.

        NOTE: If the strings passed in for the constructor are encoded as
              anything other than latin-1, you MUST provide the name of
              the encoding used as the value of the `encoding' parameter!

        Parameters:
            x           XML as utf-8 or Unicode string.
            type        Document type.
                         If passed, all other components of the Doc must
                          also be passed.
                         If none, then a CdrDoc must be passed with all
                          other components derived from the document string.
            ctrl        If type passed, dictionary of CdrDocCtl elements:
                         key = element name/tag
                         value = element text content
            blob        If type passed, blob, as a string of bytes will be
                          encoded as base64.  Should not be encoded already.
                         Else if CdrDocBlob is in the document string as a
                          base64 encoded string, it will be extracted as
                          the blob.
                         Else no blob.
            id          If type passed, document id, else derived from CdrDoc.
            encoding    Character encoding.  Must be accurate.  All
                         XML strings will be internally converted to utf-8.
        """
        # Two flavors for the constructor: one for passing in all the pieces:
        if type:
            self.id       = id
            self.ctrl     = ctrl or {}
            self.type     = type
            self.xml      = x
            self.blob     = blob
            self.encoding = encoding
        # ... and the other for passing in a CdrDoc element to be parsed.
        else:
            if encoding.lower() != 'utf-8':
                if isinstance(x, unicode):
                    x = x.encode('utf-8')
                else:
                    x = unicode(x, encoding).encode('utf-8')
            root          = etree.fromstring(x)
            self.encoding = encoding
            self.ctrl     = {}
            self.xml      = ''
            self.blob     = None
            self.id       = root.get("Id")
            self.type     = root.get("Type")
            for node in root:
                if node.tag == "CdrDocCtl":
                    self.parseCtl(node)
                elif node.tag == "CdrDocXml":
                    self.xml = get_text(node, "").encode(encoding)
                elif node.tag == "CdrDocBlob":
                    self.extractBlob(node)

    def parseCtl(self, node):
        """
        Parse a CdrDocCtl node to extract all its elements into the ctrl
        dictionary.

        Pass:
            ElementTree node for CdrDocCtl.
        """

        for child in node:
            self.ctrl[child.tag] = get_text(child, "").encode(self.encoding)

    def extractBlob(self, node):
        """
        Extract a base64 encoded blob from the XML string.

        Pass:
            DOM node for CdrDocBlob.
        """

        self.blob = base64decode(get_text(node).encode("ascii"))

    def __str__(self):
        """
        Serialize the object into a single CdrDoc XML string.

        Return:
            utf-8 encoded XML string.
        """

        doc = etree.Element("CdrDoc", Type=self.type)
        if self.id:
            doc.set("Id", normalize(self.id))
        control_wrapper = etree.SubElement(doc, "CdrDocCtl")
        if self.ctrl:
            for key in self.ctrl:
                value = self.ctrl[key].decode(self.encoding)
                etree.SubElement(control_wrapper, key).text = value
        xml = self.xml.decode("utf-8")
        etree.SubElement(doc, "CdrDocXml").text = etree.CDATA(xml)
        if self.blob is not None:
            blob = base64encode(self.blob).decode("ascii")
            etree.SubElement(doc, "CdrDocBlob", encoding="base64").text = blob
        cdr_doc_xml = etree.tostring(doc, encoding="utf-8")
        #return cdr_doc_xml
        if is_python3:
            #print("converting")
            return cdr_doc_xml.decode("utf-8")
        #print("Not converting")
        return cdr_doc_xml

    # Construct name for publishing the document.  Zero padding is
    # different for media documents, based on Alan's Multimedia Publishing
    # Analysis document.
    def getPublicationFilename(self):
        if not self.id:
            raise Exception('missing document ID')
        if not self.type:
            raise Exception('missing document type')
        docId = exNormalize(self.id)[1]
        if self.type != 'Media':
            return "CDR%d.xml" % docId
        for node in etree.fromstring(self.xml).findall("PhysicalMedia"):
            for child in node.findall("ImageData/ImageEncoding"):
                encoding = get_text(child)
                if encoding == 'JPEG':
                    return "CDR%010d.jpg" % docId
                elif encoding == 'GIF':
                    return "CDR%010d.gif" % docId
            for child in node.findall("SoundData/SoundEncoding"):
                encoding = get_text(child)
                if encoding == 'MP3':
                    return "CDR%010d.mp3" % docId
        raise Exception("Media type not yet supported")


def makeCdrDoc(xml, docType, docId=None, ctrl=None):
    """
    Make XML suitable for sending to server functions expecting
    a CdrDoc wrapper.

    Pass:
        xml     - Serialized XML for document - unicode or utf-8.
        docType - Document type string.
        docId   - CDR doc ID, or None.
        ctrl    - optional dictionary of control elements

    Return:
        New XML string with passed xml as CDATA section, coded in utf-8.
    """

    doc = etree.Element("CdrDoc", Type=docType)
    if docId:
        doc.set("Id", normalize(docId))
    control_wrapper = etree.SubElement(doc, "CdrDocCtl")
    for name in (ctrl or {}):
        value = ctrl[name] or ""
        if isinstance(value, basestring) and not isinstance(value, unicode):
            value = value.decode("utf-8")
        etree.SubElement(control_wrapper, name).text = unicode(value)
    if isinstance(xml, basestring) and not isinstance(xml, unicode):
        xml = xml.decode("utf-8")
    etree.SubElement(doc, "CdrDocXml").text = etree.CDATA(unicode(xml))
    return etree.tostring(doc, encoding="utf-8")

def _put_doc(session, command_name, **opts):
    """
    Create and submit the XML command node for adding or replacing a CDR doc

    Factored tunneling code used by `addDoc()` and `modDoc()`.

    Pass:
      session - string for the user's current login session
      command_name - string for the name of the top node for the command
      opts - see documentation for `addDoc()` and `repDoc()`

    Return:
      see documentation for `addDoc()`
    """

    # Start the command for the tunneling API.
    command = etree.Element(command_name)

    # Indicate whether we should keep the document locked or check it in.
    check_in = opts.get("checkin") == "Y" or opts.get("checkIn") == "Y"
    etree.SubElement(command, "CheckIn").text = "Y" if check_in else "N"

    # Should a new version be created?
    version = etree.SubElement(command, "Version")
    version.text = opts.get("ver", "N")
    publishable = opts.get("publishable", opts.get("verPublishable", "N"))
    version.set("Publishable", publishable)

    # Indicate whether the document should be validated before saving it.
    locators = opts.get("locators") == "Y" or opts.get("errorLocators") == "Y"
    val = etree.SubElement(command, "Validate")
    val.text = opts.get("val", "N")
    val.set("ErrorLocators", "Y" if locators else "N")

    # Should we set modify the link_net table even if not validating?
    set_links = opts.get("set_links") != "N" and opts.get("setLinks") != "N"
    etree.SubElement(command, "SetLinks").text = "Y" if set_links else "N"

    # Specify the value to be saved in audit_trail.comment.
    reason = opts.get("reason") or opts.get("comment") or ""
    if isinstance(reason, basestring) and not isinstance(reason, unicode):
        reason = reason.decode("utf-8")
    etree.SubElement(command, "Reason").text = unicode(reason)

    # Get or create the CdrDoc node.
    filename = opts.get("doc_filename") or opts.get("file")
    try:
        if filename:
            cdr_doc = etree.parse(filename).getroot()
        else:
            doc = opts.get("doc")
            if isinstance(doc, unicode):
                doc = doc.encode("utf-8")
            cdr_doc = etree.fromstring(doc)
    except:
        error = cls.wrap_error("Unable to parse document")
        if show_warnings:
            return None, error
        return error

    # Plug in the BLOB if appropriate, replacing what's there.
    filename = opts.get("blob_filename") or opts.get("blobFile")
    if filename:
        try:
            with open(filename, "rb") as fp:
                blob = fp.read()
        except:
            error = cls.wrap_error("unable to read BLOB file")
            if show_warnings:
                return None, error
            return error
    else:
        blob = opts.get("blob")
        if blob is None:
            if opts.get("del_blob") or opts.get("delBlob"):
                blob = b""
    if opts.get("del_blobs") or opts.get("delAllBlobVersions"):
        blob = None
        etree.SubElement(command, "DelAllBlobVersions").text = "Y"
    if blob is not None:
        encoded_blob = base64encode(blob).decode("ascii")
        node = cdr_doc.find("CdrDocBlob")
        if node is not None:
            node.text = encoded_blob
        else:
            etree.SubElement(cdr_doc, "CdrDocBlob").text = encoded_blob

    # Plug in a new comment if appropriate.
    comment = opts.get("comment")
    if comment:
        if not isinstance(comment, unicode):
            comment = comment.decode("utf-8")
        doc_control = cdr_doc.find("CdrDocCtl")
        node = doc_control.find("DocComment")
        if node is not None:
            node.text = comment
        else:
            etree.SubElement(doc_control, "DocComment").text = comment

    # Block (or unblock) the document if requested.
    status = opts.get("active_status") or opts.get("activeStatus")
    if status:
        doc_control = cdr_doc.find("CdrDocCtl")
        node = doc_control.find("DocActiveStatus")
        if node is not None:
            node.text = status
        else:
            etree.SubElement(doc_control, "DocActiveStatus").text = status

    #Plug the CdrDoc node into the command.
    command.append(cdr_doc)

    # Submit the command and extract the return values from the response.
    tier = opts.get("tier") or opts.get("host") or None
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command_name + "Resp":
            doc_id = get_text(response.node.find("DocId"))
            errors = response.node.find("Errors")
            if errors is not None:
                errors = etree.tostring(errors, encoding="utf-8")
            if opts.get("show_warnings") or opts.get("showWarnings"):
                return doc_id, errors
            return doc_id or errors
        else:
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
    raise Exception("missing response")
    return command

def addDoc(credentials, **opts):
    """
    Add a document to the repository

    Required positional argument:
      credentials - name of existing session or login credentials

    Keyword arguments of which exactly one must be specified:
      doc - CdrDoc XML (unicode or utf-8)
      doc_filename - optional name of file containing CdrDoc XML (utf-8)

    Optional keyword arguments:
      active_status - 'I' to block or 'A' to unblock doc; otherwise no change
      blob - bytes string for blob, if any; otherwise None
      blob_filename - alternative way to get blob - from file of bytes
      check_in - if 'Y' unlock the document after saving it
      comment - for document.command and doc_version.comment columns
      locators - if 'Y' add eref attributes to Err elements returned
      publishable - if 'Y' make the version publishable
      reason - for audit_trail.comment (if no reason given, use comment)
      set_links - if 'N' suppress updating of the linking tables
      show_warnings  - if True return tuple of cdr_id, warnings
      tier - optional; one of DEV, QA, STAGE, PROD
      val - if 'Y' validate document
      ver - if 'Y' create a new version

    Deprecated aliases for keyword arguments:
      activeStatus - deprecated alias for active_status
      blobFile - deprecated alias for blob_filename
      checkIn - deprecated alias for check_in
      errorLocators - deprecated alias for locators
      file - deprecated alias for doc_filename
      host - deprecated alias for tier
      setLinks - deprecated alias for set_links
      showWarnings - deprecated alias for show_warnings
      verPublishable - deprecated alias for publishable

    Return:
      If show_warnings is True, the caller will be given a tuple
      containing the document ID string as the first value, and a
      possibly empty string containing an "Errors" element (for
      example, for validation errors).  Otherwise (the default
      behavior), a single value is returned containing a document ID
      in the form "CDRNNNNNNNNNN" or the string for an Errors element.
      We're using this parameter (and its default) in order to
      preserve compatibility with code which expects a simple return
      value.
    """

    # Handle the command locally if possible.
    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return _Control.save_doc(session, **opts)

    # Create and submit the command for the tunneling API.
    try:
        return _put_doc(session, "CdrAddDoc", **opts)
    except:
        LOGGER.exception("CdrAddDoc")
        raise

def repDoc(credentials, **opts):
    """
    Replace an existing document.

    See addDoc() above for argument and return information.

    Additional optional keyword arguments:
        del_blob - if True, remove the association with the blob for this
                   document; if it has not been version, it will be removed
                   from the database; passing a zero-byte blob has the same
                   effect
        del_blobs - if True delete all blobs associated with this document
                    or any of its versions; only for Media PDQ Board meeting
                    recordings

    Deprecated legacy aliases for these arguments are delBlob and
    delAllBlobVersions.
    """

    # Handle the command locally if possible.
    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return _Control.save_doc(session, **opts)

    # Create and submit the command for the tunneling API.
    try:
        return _put_doc(session, "CdrRepDoc", **opts)
    except:
        LOGGER.exception("CdrAddDoc")
        raise


def getDoc(credentials, docId, *args, **opts):
    """
    Retrieve the requested version of a document from the CDR server

    Required positional arguments:
      credentials - name of existing session or login credentials
      docId - CDR ID string or integer

    Optional positional arguments (also available as keyword options)
      checkout - lock the document if "Y" - default "N"
      version - default "Current"

    Keyword options
      xml - retrieve the XML for the document if "Y" (the default)
      blob - retrieve the blob for the document if "Y" (default "N")
      getObject - return a `cdr.Doc` object if True (default is to
                  retrieve a serialized string version of the doc object)
                  note that this legacy object is not the same as the
                  new `docs.Doc` class objects
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier
    """

    checkout = args[0] if len(args) > 0 else opts.get("checkout", "N")
    version = args[1] if len(args) > 1 else opts.get("version", "Current")
    include_xml = opts.get("xml", "Y") == "Y"
    include_blob = opts.get("blob", "N") == "Y"
    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        doc = docs.Doc(session, id=docId, version=version)
        if checkout == "Y":
            doc.check_out()
        doc = doc.legacy_doc(get_xml=include_xml, get_blob=include_blob)
    else:
        doc = None
        command = etree.Element("CdrGetDoc")
        command.set("includeXml", "Y" if include_xml else "N")
        command.set("includeBlob", "Y" if include_blob else "N")
        etree.SubElement(command, "DocId").text = normalize(docId)
        etree.SubElement(command, "Lock").text = checkout
        etree.SubElement(command, "DocVersion").text = str(version)
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == "CdrGetDocResp":
                doc = response.node.find("CdrDoc")
            else:
                error = ";".join(response.errors) or "missing response"
                raise Exception(error)
        if doc is None:
            raise Exception("missing response")
    doc_string = etree.tostring(doc, encoding="utf-8")
    if opts.get("getObject"):
        return Doc(doc_string, encoding="utf-8")
    return doc_string

def delDoc(credentials, docId, **opts):
    """
    Mark a CDR document as deleted

    Required positional arguments:
      credentials - name of existing session or login credentials
      docId - CDR ID string or integer

    Keyword options
      validate - if True, don't delete if any docs link to this one
      reason - string explaining why document was deleted
      val - legacy alternative to `validate` ("N" -> False "Y" -> True)
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier
    """

    validate = opts.get("validate") or opts.get("val") == "Y"
    reason = opts.get("reason")
    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        doc = docs.Doc(session, id=docId)
        opts = dict(reason=reason, validate=validate)
        doc.delete(**opts)
        if doc.errors:
            return etree.tostring(doc.errors_node, encoding="utf-8")
        return doc.cdr_id
    cdr_id = normalize(docId)
    command = etree.Element("CdrDelDoc")
    etree.SubElement(command, "DocId").text = cdr_id
    etree.SubElement(command, "Validate").text = "Y" if validate else "N"
    if reason:
        etree.SubElement(command, "Reason").text = reason
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrDelDocResp":
            if response.errors:
                return response.errors
            return cdr_id
        else:
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
    raise Exception("missing response")

def filterDoc(credentials, filter, docId=None, **opts):
    """
    Apply one or more filters to an XML document

    Positional arguments:
      credentials - result of login
      filter - one of:
               * filter XML string (if 'inline' option is True)
               * CDR ID for filter document
               * sequence, mixing any of the following
                     * filter title string prefixed by 'name:'
                     * filter set name string prefixed by 'set:'
                     * CDR ID for filter documenet
      docId - optional CDR ID for document to be filtered

    Optional keyword arguments:
      doc - XML string for document to be filter (if docId is not supplied)
      inline - if True, filter argument is XML string for XSL/T filter document
      parms - optional sequence of name/value pairs to be used as filter
              parameters (used by all of the filters; there is no way to
              pass different parameters for individual filters in the job)
              (deprecated legacy alias `parm` is also supported for this
              option)
      no_output - if "Y, retrieve messages but no filtered document
      ver - version number of document to be filtered, or 'last' or 'lastp'
            (docVer is accepted as a legacy alias for this option)
      date - used in combination with `docVer` to specify last version (or
             last publishable version) created before this date/time);
             docDate is supported as a deprecated legacy alias for this option
      filter_ver - which versions of the filters to use ('last' or 'last'
                   or a version number); a specific version number only
                   makes sense in the case of a request involving only one
                   filter, and is probably a mistake in most cases
                   (filterVer is supported as a deprecated legacy alias
                   for this option)
      filter_date - similar to date; if `date` is specified and `filter_date`
                    is not specified, `date` is used to restrict filter
                    version selection; filterDate is legacy alias
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
        tuple of document, messages if successful, else error string
    """

    xml = opts.get("doc")
    ver = opts.get("ver") or opts.get("docVer")
    date = opts.get("date") or opts.get("docDate")
    tier = opts.get("tier") or opts.get("host") or None
    filter_ver = opts.get("filter_ver") or opts.get("filterVer")
    filter_date = opts.get("filter_date") or opts.get("filterDate") or date
    no_output = opts.get("no_output", "N") == "Y"
    output = not no_output
    parms = opts.get("parms") or opts.get("parm")
    parms = dict(parms) if parms else {}
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        doc = docs.Doc(session, id=docId, version=ver, before=date, xml=xml)
        options = dict(
            parms=parms,
            output=output,
            version=filter_ver,
            date=filter_date
        )
        if opts.get("inline"):
            opts["filter"] = filter
            filters = []
        else:
            filters = filter
            if not isinstance(filters, (tuple, list)):
                filters = [filters]
        result = doc.filter(*filters, **options)
        if result.messages:
            messages = etree.Element("Messages")
            for message in result.messages:
                etree.SubElement(messages, "message").text = message
            messages = etree.tostring(messages, encoding="utf-8")
        else:
            messages = ""
        if output:
            return unicode(result.result_tree).encode("utf-8"), messages
        else:
            return messages
    command = etree.Element("CdrFilter")
    if no_output:
        command.set("Output", "N")
    if filter_ver:
        command.set("FilterVersion", str(filter_ver))
    if filter_date:
        command.set("FilterCutoff", str(filter_date))
    if opts.get("inline"):
        if not isinstance(unicode, filter):
            filter = filter.decode("utf-8")
        etree.SubElement(command, "Filter").text = etree.CDATA(filter)
    else:
        if not isinstance(filter, (tuple, list)):
            filter = [filter]
        for f in filter:
            f = str(f)
            if f.startswith("set:"):
                name = f.split(":", 1)[1]
                node = etree.SubElement(command, "FilterSet", Name=name)
            elif f.startswith("name:"):
                name = f.split(":", 1)[1]
                node = etree.SubElement(command, "Filter", Name=name)
            else:
                href = normalize(f)
                node = etree.SubElement(command, "Filter", href=href)
    node = etree.SubElement(command, "Document")
    if docId:
        node.set("href", normalize(docId))
        node.set("version", str(ver))
        if date:
            node.set("maxDate", str(date))
    elif xml:
        if not isinstance(unicode, xml):
            xml = xml.decode("utf-8")
        node.text = etree.CDATA(xml)
    else:
        raise Exception("nothing to filter")

    for name in parms:
        parm = etree.SubElement(command, "Parm")
        etree.SubElement(parm, "Name").text = str(name)
        etree.SubElement(parm, "Value").text = str(parms[name] or "")

    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrFilterResp":
            document = get_text(response.node.find("Document"))
            messages = response.node.find("Messages")
            if messages is not None:
                messages = etree.tostring(messages, encoding="utf-8")
            else:
                messages = ""
            if output:
                return document.encode("utf-8"), messages
            else:
                return messages
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def valDoc(credentials, doctype, **opts):
    """
    Validate a document, either in the database or passed to here.

    Positional arguments:
      credentials - result of login
      doctype - string for type of document to be validated

    Keyword arguments
      doc_id - unique ID of document to be fetched from database and validated
      docId - legacy alias for `doc_id` (deprecated)
      doc - string for document XML, possibly with CdrDoc wrapper
      link_validation - Boolean; set to False to suppress link validation
      valLinks - legacy equivalent of `validate_links` (character flag Y/N)
                 if "N" link validation is suppressed (deprecated)
      schema_validation - Boolean; set to False to suppress schema validation
      valSchema - legacy equivalent of `schema_validation`; if set to "N"
                  schema validation is suppressed (deprecated)
      validate_only - set to False to also update the database with the
                      newly determined validation status; ignored if
                      document ID is not specified
      validateOnly - deprecated legacy equivalent for `validate_only` (Y|N);
                     if set to "N" the database will be updated with the
                     new status; ignored if `doc` is passed instead of
                     the document's ID
      locators - Boolean, defaulting to False; set to True in order to have
                 the document echoed back with error location information
                 if errors are found
      errorLocators - deprecated legacy equivalent for `locators`; if set
                      to "Y" behaves the same as locators=True
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      the serialized CdrValidateDocResp element; this is an aberration
      from the usual pattern for the command-wrapper functions in this
      module, which usually extract the useful information from the
      response document; apparently the caller is expected to do that
      herself (for example, using cdr.getErrors() or cdr.deDupErrs());
      if we're invoking the command across the network instead of
      locally we actually return the entire serialized CdrResponseSet
      from the server (it's ok that the behavior is different because
      all of the callers dig down looking for the Err elements regardless
      of how deeply they're buried)
    """

    # Make sure we have a CdrDoc node or a document ID
    doc_id = opts.get("doc_id") or opts.get("docId")
    doc = opts.get("doc")
    if not doc_id and not doc:
        raise Exception("valDoc(): no doc or doc_id specified")
    if doc_id and doc:
        raise Exception("valDoc(): both doc and doc_id specified")
    if doc:
        if isinstance(doc, unicode):
            doc = doc.encode("utf-8")
        xml = None
        try:
            root = etree.fromstring(doc)
            if root.tag == "CdrDoc":
                doc = root
            else:
                xml = doc.decode("utf-8")
        except:
            xml = doc.decode("utf-8")
        if xml is not None:
            doc = etree.Element("CdrDoc")
            etree.SubElement(doc, "CdrDocXml").text = etree.CDATA(xml)

    # Extract what we need from the options.
    val_types = ["links", "schema"]
    if opts.get("schema_validation") is False or opts.get("valSchema") == "N":
        val_types.remove("schema")
    if opts.get("link_validation") is False or opts.get("valLinks") == "N":
        val_types.remove("links")
    if not val_types:
        raise Exception("valDoc(): no validation types specified")
    locators = opts.get("locators") or opts.get("errorLocators") == "Y"
    store = False
    if opts.get("validate_only") is False or opts.get("validateOnly") == "N":
        store = True
    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)

    # Handle the command locally if appropriate.
    if isinstance(session, Session):
        validation_opts = dict(type=val_types, locators=locators, store=store)
        if doc_id:
            doc = docs.Doc(session, id=doc_id)
        else:
            xml = get_text(doc.find("CdrDocXml"))
            level = doc.get("RevisionFilterLevel")
            if level:
                validation_opts["revision_filter_level"] = level
            doc = docs.Doc(session, xml=xml, doctype=doctype)
        doc.validate(**validation_opts)
        response = doc.legacy_validation_response(locators)
        return etree.tostring(response, encoding="utf-8")

    # Otherwise tunnel through the network.
    val_types = " ".join([t.capitalize() for t in val_types])
    command = etree.Element("CdrValidateDoc")
    command.set("DocType", doctype)
    command.set("ValidationTypes", val_types)
    command.set("ErrorLocators", "Y" if locators else "N")
    if doc_id:
        child = etree.SubElement(command, "DocId")
        child.text = normalize(doc_id)
        child.set("ValidateOnly", "N" if store else "Y")
    else:
        command.append(doc)
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrValidateDocResp":
            return etree.tostring(response.node, encoding="utf-8")
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def getCssFiles(credentials, **opts):
    """
    Get the CSS files used by the client

    This command is obsolete, but it is still present as a fallback
    in the DLL code, so I'm not removing it yet. The active CSS files
    are now maintained in version control, not in the CDR repository.
    The `data` member of the `CssFile` object is binary (reflecting
    an obsolete proprietary XMetaL format?).

    Optional keyword argument:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      sequence of `CssFile` objects
    """

    # Type for members of return sequence.
    class CssFile:
        def __init__(self, name, data):
            self.name = name
            self.data = data

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        files = docs.Doctype.get_css_files(session)
        return [CssFile(name, files[name]) for name in sorted(files)]
    command = etree.Element("CdrGetCssFiles")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrGetCssFilesResp":
            files = []
            for node in response.node.findall("File"):
                name = get_text(node.find("Name"))
                data = get_text(node.find("Data"))
                data = base64decode(data.encode("ascii"))
                files.append(CssFile(name, data))
            return files
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def addExternalMapping(credentials, usage, value, **opts):
    """
    Add a row to the `external_map' table

    Required positional arguments:
      credentials - result of login
      usage - string representing the context for the mapping
              (for example, 'Spanish GlossaryTerm Phrases')
      value - string for the value to be mapped to this document

    Optional keyword arguments:
      doc_id - None if we don't currently have a mapping for the value
      bogus - if "Y" value does not really map to any document,
              but is instead a known invalid value found in
              (usually imported) data
      mappable - if "N" the value is not an actual field value;
                 often it's a comment explaining why no value
                 which could be mapped to a CDR doc is available
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      integer primary key for newly inserted mapping table row
    """

    if isinstance(usage, basestring) and not isinstance(usage, unicode):
        usage = usage.decode("utf-8")
    if isinstance(value, basestring) and not isinstance(value, unicode):
        value = value.decode("utf-8")
    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        doc = docs.Doc(session, id=opts.get("doc_id"))
        return doc.add_external_mapping(usage, value, **opts)
    command = etree.Element("CdrAddExternalMapping")
    etree.SubElement(command, "Usage").text = usage
    etree.SubElement(command, "Value").text = value
    etree.SubElement(command, "Bogus").text = opts.get("bogus", "N")
    etree.SubElement(command, "Mappable").text = opts.get("mappable", "Y")
    doc_id = opts.get("doc_id")
    if doc_id:
        etree.SubElement(command, "CdrId").text = normalize(doc_id)
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrAddExternalMappingResp":
            return int(response.node.get("MappingId"))
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")


# ======================================================================
# Manage CDR filters
# ======================================================================

def getFilters(credentials, **opts):
    """
    Fetch the list of filter documents in the CDR

    Required positional argument:
      credentials - result of login

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      sequence of `IdAndName` objects with the `id` attribute containing
      the string for the normalized CDR ID for the filter document
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        filters = docs.FilterSet.get_filters(session)
        return [IdAndName(doc.cdr_id, doc.title) for doc in filters]
    command = etree.Element("CdrGetFilters")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrGetFiltersResp":
            filters = []
            for node in response.node.findall("Filter"):
                doc_id = node.get("DocId")
                name = get_text(node)
                filters.append(IdAndName(doc_id, name))
            return filters
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def getFilterSets(credentials, **opts):
    """
    Fetch the list of filter sets in the CDR

    Required positional argument:
      credentials - result of login

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      sequence of `IdAndName` objects
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return [IdAndName(*s) for s in docs.FilterSet.get_filter_sets(session)]
    command = etree.Element("CdrGetFilterSets")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrGetFilterSetsResp":
            sets = []
            for node in response.node.findall("FilterSet"):
                set_id = int(node.get("SetId"))
                name = get_text(node)
                sets.append(IdAndName(set_id, name))
            return sets
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")


class FilterSet:
    """
    Named set of CDR filter documents

    The members attribute in the object will contain a list of `IdAndName`
    objects with id and name attributes.  For a nested filter set, the
    id will be the integer representing the primary key of the set; for
    a filter, the id will be a string containing the CDR document ID in
    the form 'CDR0000099999'.

    Attributes:
      name - unique string used to identify the set
      desc - brief description of the set, used for UI
      notes - more extensive optional notes on the use of the filter set
      members - sequence of IdAndName objects representing filters and
                nested filter sets
      expanded - if True, nested filter sets have been recursively
                 replaced by their own members, so the set members
                 are all filter documents
    """

    def __init__(self, name, desc, notes=None, members=None, expanded=False):
        self.name = toUnicode(name)
        self.desc = toUnicode(desc)
        self.notes = toUnicode(notes) or None
        self.members = members or []
        self.expanded = expanded

    def save(self, credentials, new, **opts):
        """
        Store a CDR filter set

        Required positional arguments:
          credentials - result of login
          new - if False, update an existing set; otherwise create a new one

        Optional keyword arguments:
          tier - optional; one of DEV, QA, STAGE, PROD
          host - deprecated alias for tier

        Return:
          integer representing number of members of the set (filters or
          nested filter sets)
        """

        # Perform the operation locally if we can.
        tier = opts.get("tier") or opts.get("host") or None
        session = _Control.get_session(credentials, tier)
        if isinstance(session, Session):
            members = []
            for m in self.members:
                if isinstance(m.id, basestring):
                    member = docs.Doc(session, id=m.id, title=self.name)
                else:
                    member = docs.FilterSet(session, id=m.id, name=self.name)
                members.append(member)
            set_opts = dict(
                name=self.name,
                description=self.desc,
                notes=self.notes,
                members=members
            )
            filter_set = docs.FilterSet(session, **set_opts)
            if new and filter_set.id:
                message = "Filter set {!r} already exists".format(self.name)
                raise Exception(message)
            if not new and not filter_set.id:
                raise Exception("Filter set {!r} not found".format(self.name))
            return filter_set.save()

        # Handle the request through the HTTPS tunnel.
        name = "CdrAddFilterSet" if new else "CdrRepFilterSet"
        command = etree.Element(name)
        etree.SubElement(command, "FilterSetName").text = self.name
        description = etree.SubElement(command, "FilterSetDescription")
        description.text = self.desc
        if self.notes is not None:
            etree.SubElement(command, "FilterSetNotes").text = self.notes
        for member in self.members:
            if isinstance(member.id, basestring):
                etree.SubElement(command, "Filter", DocId=member.id)
            else:
                etree.SubElement(command, "FilterSet", SetId=str(member.id))
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == name + "Resp":
                return int(response.node.get("TotalFilters"))
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
        raise Exception("missing response")

    def __repr__(self):
        """
        Quick look at the filter set

        Return:
          string describing the set
        """

        lines = [u"name={}".format(self.name), u"desc={}".format(self.desc)]
        if self.notes:
            lines.append(u"notes={}".format(self.notes))
        if self.expanded:
            lines.append(u"Expanded list of filters:")
        for member in self.members:
            args = member.id, member.name
            if self.expanded or isinstance(member.id, basestring):
                args = u"filter", member.id, member.name
            else:
                args = u"filter set", member.id, member.name
            lines.append(u"{} {} ({})".format(*args))
        return u"\n".join(lines) + u"\n"

def addFilterSet(credentials, filter_set, **opts):
    """
    Create a new CDR filter set

    See documentation for `FilterSet.save()`
    """

    return filter_set.save(credentials, new=True, **opts)

def repFilterSet(credentials, filter_set, **opts):
    """
    Replace an existing CDR filter set

    See documentation for `FilterSet.save()`
    """

    return filter_set.save(credentials, new=False, **opts)

def getFilterSet(credentials, name, **opts):
    """
    Get the attributes and members of a CDR filter set

    Required positional arguments:
      credentials - result of login
      name - string for unique filter set name

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      `FilterSet` object with the `expanded` attribute set to `False`
    """

    members = []
    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        filter_set = docs.FilterSet(session, name=name)
        for member in filter_set.members:
            if isinstance(member, docs.Doc):
                members.append(IdAndName(member.cdr_id, member.title))
            else:
                members.append(IdAndName(member.id, member.name))
        return FilterSet(filter_set.name, filter_set.description,
                         filter_set.notes, members)
    command = etree.Element("CdrGetFilterSet")
    etree.SubElement(command, "FilterSetName").text = name
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrGetFilterSetResp":
            name = description = notes = None
            for node in response.node:
                if node.tag == "FilterSetName":
                    name = get_text(node)
                elif node.tag == "FilterSetDescription":
                    description = get_text(node)
                elif node.tag == "FilterSetNotes":
                    notes = get_text(node)
                elif node.tag == "Filter":
                    member = IdAndName(node.get("DocId"), get_text(node))
                    members.append(member)
                elif node.tag == "FilterSet":
                    member = IdAndName(int(node.get("SetId")), get_text(node))
                    members.append(member)
            return FilterSet(name, description, notes, members)
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

#----------------------------------------------------------------------
# Recursively rolls out the list of filters invoked by a named filter
# set.  In contrast with getFilterSet, which returns a list of nested
# filter sets and filters intermixed, all of the members of the list
# returned by this function represent filters.  Since there is no need
# to distinguish filters from nested sets by the artifice of
# representing filter IDs as strings, the id member of each object
# in this list is an integer.
#
# Takes the name of the filter set as input.  Returns a FilterSet
# object, with the members attribute as described above.
#
# Note: since it is possible for bad data to trigger infinite
# recursion, we throw an exception if the depth of nesting exceeds
# a reasonable level.
#
# WARNING: treat the returned objects as read-only, otherwise you'll
# corrupt the cache used for future calls.
#----------------------------------------------------------------------
_expandedFilterSetCache = {}
def expandFilterSet(session, name, level=0, **opts):
    global _expandedFilterSetCache
    if level > 100:
        raise Exception('expandFilterSet', 'infinite nesting of sets')
    if _expandedFilterSetCache.has_key(name):
        return _expandedFilterSetCache[name]
    filterSet = getFilterSet(session, name, host, port)
    newSetMembers = []
    for member in filterSet.members:
        if type(member.id) == type(9):
            nestedSet = expandFilterSet(session, member.name, level + 1)
            newSetMembers += nestedSet.members
        else:
            newSetMembers.append(member)
    filterSet.members = newSetMembers
    filterSet.expanded = 1
    _expandedFilterSetCache[name] = filterSet
    return filterSet

#----------------------------------------------------------------------
# Returns a dictionary containing all of the CDR filter sets, rolled
# out by the expandFilterSet() function above, indexed by the filter
# set names.
#----------------------------------------------------------------------
def expandFilterSets(session, **opts):
    sets = {}
    for fSet in getFilterSets(session):
        sets[fSet.name] = expandFilterSet(session, fSet.name, host = host,
                                          port = port)
    return sets

def delFilterSet(credentials, name, **opts):
    """
    Delete an existing CDR filter set

    Required positional arguments:
      credentials - result of login
      name - string for unique filter set name

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      None
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        filter_set = docs.FilterSet(session, name=name)
        filter_set.delete()
        return
    command = etree.Element("CdrDelFilterSet")
    etree.SubElement(command, "FilterSetName").text = name
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == "CdrDelFilterSetResp":
            return
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

class Logging:
    """
    The CDR has too many ways to do logging already. In spite of this,
    I'm adding yet another logging class, this one based on the standard
    library's logging module. The immediate impetus for doing this is the
    fact that we have to use that module in the new CDR Scheduler. The
    Logger class below is only used as a namespace wrapper, with a factory
    method for instantiating logger objects.
    """

    FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
    LEVELS = {
        "info": logging.INFO,
        "debug": logging.DEBUG,
        "warn": logging.WARN,
        "warning": logging.WARNING,
        "critical": logging.CRITICAL,
        "error": logging.ERROR
    }

    class Formatter(logging.Formatter):
        """Make our own logging formatter to get the time stamps right."""

        converter = datetime.datetime.fromtimestamp

        def formatTime(self, record, datefmt=None):
            ct = self.converter(record.created)
            if datefmt:
                s = ct.strftime(datefmt)
            else:
                t = ct.strftime("%Y-%m-%d %H:%M:%S")
                s = "%s.%03d" % (t, record.msecs)
            return s

    @classmethod
    def get_logger(cls, name, **opts):
        """
        Factory method for instantiating a logging object.

        name       required name for the logger
        path       optional path for the log file
        format     optional override for the default log format pattern
        level      optional verbosity for logging, defaults to info
        propagate  if True, the base handler also writes our entries
        multiplex  if True, add new handler even there already is one
        console    if True, add stream handler to write to stderr
        """

        logger = logging.getLogger(name)
        level = opts.get("level", "info")
        logger.setLevel(cls.LEVELS.get(level, logging.INFO))
        logger.propagate = opts.get("propagate", False)
        if not logger.handlers or opts.get("multiplex"):
            path = opts.get("path", "%s/%s.log" % (DEFAULT_LOGDIR, name))
            handler = logging.FileHandler(path)
            formatter = cls.Formatter(opts.get("format", cls.FORMAT))
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            if opts.get("console"):
                stream_handler = logging.StreamHandler()
                stream_handler.setFormatter(formatter)
                logger.addHandler(stream_handler)
        return logger


class IdAndName:
    """
    Object for ID and name

    Example usages include documents (in which case the name is the title
    of the document), or a filter set, or a user, or a group, etc.

    Attributes:
      id - integer or string uniquely identifying the object
      name - string by which the object is known
    """

    def __init__(self, id, name):
        self.id = id
        self.name = name

class _Control:
    """
    Wrap internals supporting the legacy CDR Python client interface.
    """

    PARSER = etree.XMLParser(strip_cdata=False)
    MIN_ID = 0x10000000000000000000000000000000
    MAX_ID = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
    TIER = Tier()

    try:
        CONN = cdrapi.db.connect(timeout=2)
    except Exception as e:
        print(e)
        CONN = None

    @classmethod
    def tunneling(cls, tier=None):
        return tier or not cls.CONN

    @classmethod
    def get_session(cls, credentials, tier=None, comment=None):
        """
        Return string or object representing CDR login session

        Pass:
          credentials - session name string or tuple of username, password
          tier - optional name of hosting tier (e.g., 'DEV')
          comment - optional comment to be stored with the session info

        Return:
          `Session` object if we're invoking the cdrapi methods directly
          otherwise (we're tunneling our CDR commands through an HTTPS
          proxy) the session name string
        """

        if isinstance(credentials, Session):
            return credentials
        if cls.tunneling(tier):
            if isinstance(credentials, basestring):
                return credentials
            else:
                username, password = credentials
                return cls.windows_login(username, password, tier)
        if not isinstance(credentials, basestring):
            user, password = credentials
            opts = dict(comment=comment, tier=tier)
            return Session.create_session(user, **opts)
        return Session(credentials, tier)

    @classmethod
    def windows_login(cls, username, password, tier=None):
        """
        Use the Windows authentication service to check login credentials

        Pass:
          username - string name for the CDR user account
          password - password string for the user's NIH domain account
          tier - optional string identifying which CDR server we want
                 to log into (e.g., 'QA')

        Return:
          unique (for this tier) session name string
        """

        tier = cdrapi.settings.Tier(tier) if tier else cls.TIER
        url = "https://{}/cgi-bin/secure/login.py".format(tier.hosts["APPC"])
        auth = requests.auth.HTTPDigestAuth(username, password)
        response = requests.get(url, auth=auth)
        if not response.ok:
            raise Exception(response.reason)
        return response.text.strip()

    class ResponseSet:
        def __init__(self, node):
            self.node = node
            self.time = dateutil.parser.parse(node.get("Time"))
            self.responses = [self.Response(r) for r in node.findall("*")]

        class Response:
            def __init__(self, node):
                if node.tag != "CdrResponse":
                    raise Exception("expected CdrResponse got " + node.tag)
                self.command_id = node.get("CmdId")
                self.node = node.find("*")
                self.errors = [err.text for err in node.findall("Errors/Err")]

    @classmethod
    def wrap_command(cls, node, command_id=None):
        if not command_id:
            command_id = "{:X}".format(random.randint(cls.MIN_ID, cls.MAX_ID))
        wrapper = etree.Element("CdrCommand", CmdId=command_id)
        wrapper.append(node)
        return wrapper

    @classmethod
    def wrap_commands(cls, session, *commands):
        wrapper = etree.Element("CdrCommandSet")
        session_node = etree.SubElement(wrapper, "SessionId")
        session_node.text = session
        for command_node in commands:
            wrapper.append(command_node)
        return wrapper

    @classmethod
    def send_commands(cls, commands, tier=None):
        tier = cdrapi.settings.Tier(tier) if tier else cls.TIER
        url = "https://" + tier.hosts["API"]
        request = etree.tostring(commands, encoding="utf-8")
        response = requests.post(url, request)
        if not response.ok:
            raise Exception(response.reason)
        root = etree.fromstring(response.content, parser=cls.PARSER)
        for error in root.findall("Errors/Err"):
            raise Exception(error.text)
        return cls.ResponseSet(root)

    @classmethod
    def send_command(cls, session, command, tier=None):
        commands = cls.wrap_commands(session, cls.wrap_command(command))
        return cls.send_commands(commands, tier).responses

    @classmethod
    def wrap_error(cls, message, serialize=True):
        wrapper = etree.Element("Errors")
        etree.SubElement(wrapper, "Err").text = str(message)
        if serialize:
            return etree.tostring(wrapper, encoding="utf-8")
        return wrapper

    @classmethod
    def save_doc(cls, session, **opts):
        show_warnings = False
        for name in ("show_warnings", "showWarnings"):
            if opts.get(name):
                show_warnings = True
        filename = opts.get("doc_filename") or opts.get("file")
        try:
            if filename:
                root = etree.parse(filename).getroot()
            else:
                doc = opts.get("doc")
                if isinstance(doc, unicode):
                    doc = doc.encode("utf-8")
                root = etree.fromstring(doc)
        except:
            error = cls.wrap_error("Unable to parse document")
            if show_warnings:
                return None, error
            return error
        filename = opts.get("blob_filename") or opts.get("blobFile")
        if filename:
            try:
                with open(filename, "rb") as fp:
                    blob = fp.read()
            except:
                error = cls.wrap_error("unable to read BLOB file")
                if show_warnings:
                    return None, error
                return error
        else:
            blob = opts.get("blob")
            if blob is None:
                if opts.get("del_blob") or opts.get("delBlob"):
                    blob = b""
        doc_opts = {
            "id": root.get("Id"),
            "doctype": root.get("Type"),
            "xml": get_text(root.find("CdrDocXml")),
            "blob": blob
        }
        doc = docs.Doc(session, **doc_opts)
        comment = opts.get("comment")
        reason = opts.get("reason")
        if comment and not isinstance(comment, unicode):
            comment = comment.decode("utf-8")
        if reason and not isinstance(reason, unicode):
            reason = reason.decode("utf-8")
        publishable = opts.get("publishable", opts.get("verPublishable", "N"))
        save_opts = {
            "version": opts.get("ver") == "Y",
            "publishable": publishable == "Y",
            "val_types": ("schema", "links") if opts.get("val") else (),
            "set_links": True,
            "locators": False,
            "del_blobs": False,
            "unlock": False,
            "comment": opts.get("comment"),
            "reason": opts.get("reason"),
            "title": get_text(root.find("CdrDocCtl/DocTitle"))
        }
        if opts.get("set_links") == "N" or opts.get("setLinks") == "N":
            save_opts["set_links"] = False
        if opts.get("locators") == "Y" or opts.get("errorLocators") == "Y":
            save_opts["locators"] = True
        if opts.get("check_in") == "Y" or opts.get("checkIn") == "Y":
            save_opts["unlock"] = True
        for name in ("active_status", "activeStatus"):
            if name in opts:
                save_opts["active_status"] = opts[name]
                break
        for name in ("del_blobs", "delAllBlobVersions"):
            if opts.get(name):
                save_opts["del_blobs"] = True
        try:
            doc.save(**save_opts)
        except Exception as e:
            if show_warnings:
                return (None, cls.wrap_error(e))
            return cls.wrap_error(e)
        if show_warnings:
            if doc.errors_node is not None:
                return doc.cdr_id, etree.tostring(doc.errors_node)
            else:
                return doc.cdr_id, ""
        else:
            return doc.cdr_id


def isDevHost():
    """
    Tell the caller if we are on the development host
    """
    return _Control.TIER.name == "DEV"


def isProdHost():
    """
    Tell the caller if we are on the production host
    """

    return _Control.TIER.name == "PROD"


def getHostName():
    """
    Give caller variant forms of the host name (cached tuple)

    Return the server host name as a tuple of:
        naked host name
        fully qualified host name
        fully qualified name, prefixed by "https://"
    """
    return HOST_NAMES


def ordinal(n):
    """
    Convert number to ordinal string.
    """

    n = int(n)
    endings = {1: "st", 2: "nd", 3: "rd"}
    ending = "th" if 4 < n % 100 <= 20 else endings.get(n % 10, "th")
    return "{0}{1}".format(n, ending)


def make_timestamp():
    """
    Create a string which will make a name unique (enough)
    """

    return datetime.datetime.now().strftime("%Y%m%d%H%M%S")


def getpw(name):
    """
    Get the password for a local CDR machine account
    """

    try:
        name = name.lower()
        with open(WORK_DRIVE + ":/etc/cdrpw") as fp:
            for line in fp:
                n, p = line.strip().split(":", 1)
                if n == name:
                    return p
    except:
        pass
    return None

def toUnicode(value, default=u""):
    """
    Convert value to Unicode

    Try different character encodings until one works.

    Pass:
      value - any value to be converted to Unicode
      default - what to pass if value is None

    Return:
      Unicode for the value (`default` if None)
    """

    if value is None:
        return default
    if not isinstance(value, basestring):
        return unicode(value)
    if isinstance(value, unicode):
        return value
    for encoding in ("ascii", "utf-8", "iso-8859-1"):
        try:
            return value.decode(encoding)
        except:
            pass
    raise Exception("unknown encoding for {!r}".format(value))

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
        default if node is None; otherwise concatenated string node descendants
    """

    if node is None:
        return default
    return u"".join(node.itertext("*"))

#----------------------------------------------------------------------
# Normalize a document id to form 'CDRnnnnnnnnnn'.
#----------------------------------------------------------------------
def normalize(id):
    if id is None: return None
    if type(id) == type(9):
        idNum = id
    else:
        digits = re.sub('[^\d]', '', id)
        idNum  = int(digits)
    return "CDR%010d" % idNum

#----------------------------------------------------------------------
# Extended normalization of ids
#----------------------------------------------------------------------
def exNormalize(id):
    """
    An extended form of normalize.
    Pass:
        An id in any of the following forms:
            12345
            'CDR0000012345'
            'CDR12345'
            'CDR0000012345#F1'
            '12345#F1'
            etc.
    Return:
        Tuple of:
            Full id string
            id as an integer
            Fragment as a string, or None if no fragment
        e.g.,
            'CDR0000012345#F1'
            12345
            'F1'
    Raise:
        Exception if not a CDR ID.
    """

    if type(id) == type(9):
        # Passed a number
        idNum = id
        frag  = None

    else:
        # Parse the string
        pat = re.compile (
            r"(^\s*?([Cc][Dd][Rr]0*)?)(?P<num>(\d+))\s*(\#(?P<frag>(.*)))?$")
        result = pat.search (id)

        if not result:
            raise Exception("Invalid CDR ID string: " + id)

        idNum = int (result.group ('num'))
        frag  = result.group ('frag')

    # Sanity check on number
    if idNum < 1 or idNum > 9999999999:
        raise Exception("Invalid CDR ID number: " + str(idNum))

    # Construct full id
    fullId = "CDR%010d" % idNum
    if frag:
        fullId += '#' + frag

    return (fullId, idNum, frag)


# ======================================================================
# Legacy global names
# ======================================================================
CBIIT_HOSTING = True
WORK_DRIVE = _Control.TIER.drive
GPMAILER = _Control.TIER.hosts["EMAILERS"]
GPMAILERDB = _Control.TIER.hosts["DBNIX"]
APPC = _Control.TIER.hosts["APPC"]
FQDN = open(WORK_DRIVE + ":/cdr/etc/hostname").read().strip()
HOST_NAMES = FQDN.split(".")[0], FQDN, "https://" + FQDN
CBIIT_NAMES = APPC.split(".")[0], APPC, "https://" + APPC
OPERATOR = "NCIPDQoperator@mail.nih.gov"
DOMAIN_NAME = FQDN.split(".", 1)[1]
PUB_NAME = HOST_NAMES[0]
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 2019
BATCHPUB_PORT = 2020
URDATE = "2002-06-22"
PYTHON = WORK_DRIVE + ":\\python\\python.exe"
BASEDIR = WORK_DRIVE + ":/cdr"
SMTP_RELAY = "MAILFWD.NIH.GOV"
DEFAULT_LOGDIR = BASEDIR + "/Log"
DEFAULT_LOGLVL = 5
DEFAULT_LOGFILE = DEFAULT_LOGDIR + "/debug.log"
PUBLOG = DEFAULT_LOGDIR + "/publish.log"
MANIFEST_NAME = "CdrManifest.xml"
CLIENT_FILES_DIR = BASEDIR + "/ClientFiles"
MANIFEST_PATH = "{}/{}".format(CLIENT_FILES_DIR, MANIFEST_NAME)
SENDCMDS_TIMEOUT = 300
SENDCMDS_SLEEP = 3
PDQDTDPATH = WORK_DRIVE + ":\\cdr\\licensee"
DEFAULT_DTD = PDQDTDPATH + "\\pdqCG.dtd"
NAMESPACE = "cips.nci.nih.gov/cdr"
LOGGER = Logging.get_logger("cdr-client", level="debug", console=True)
Group = Session.Group
Action = Session.Action

# ======================================================================
# Module data used by publishing.py and cdrpub.py.
# ======================================================================
PUBTYPES = {
    'Full Load': 'Send all documents to Cancer.gov',
    'Export': 'Send specified documents to Cancer.gov',
    'Reload': 'Re-send specified documents that failed loading',
    'Remove': 'Delete documents from Cancer.gov',
    'Hotfix (Remove)': 'Delete individual documents from Cancer.gov',
    'Hotfix (Export)': 'Send individual documents to Cancer.gov'
}


# ======================================================================
# Map for finding the filters for a given document type to run
# QC reports.
# ======================================================================
FILTERS = {
    'Citation':
        ["set:QC Citation Set"],
    'CTGovProtocol':
        ["set:QC CTGovProtocol Set"],
    'DrugInformationSummary':
        ["set:QC DrugInfoSummary Set"],
    'GlossaryTerm':
        ["set:QC GlossaryTerm Set"],
    'GlossaryTerm:rs':            # Redline/Strikeout
        ["set:QC GlossaryTerm Set (Redline/Strikeout)"],
    'GlossaryTermConcept':
        ["name:Glossary Term Concept QC Report Filter"],
    'GlossaryTermName':
        ["set:QC GlossaryTermName"],
    'GlossaryTermName:gtnwc':
        ["set:QC GlossaryTermName with Concept Set"],
    'InScopeProtocol':
        ["set:QC InScopeProtocol Set"],
    'Media:img':
        ["set:QC Media Set"],
    'MiscellaneousDocument':
        ["set:QC MiscellaneousDocument Set"],
    'MiscellaneousDocument:rs':
        ["set:QC MiscellaneousDocument Set (Redline/Strikeout)"],
    'Organization':
        ["set:QC Organization Set"],
    'Person':
        ["set:QC Person Set"],
    'PDQBoardMemberInfo':
        ["set:QC PDQBoardMemberInfo Set"],
    'Summary':
        ["set:QC Summary Set"],
    'Summary:bu':                 # Bold/Underline
        ["set:QC Summary Set (Bold/Underline)"],
    'Summary:buqd':               # Bold/Underline - Quick and Dirty
        ["set:QC QD Summary Set (Bold/Underline)"],
    'Summary:rs':                 # Redline/Strikeout
        ["set:QC Summary Set"],
    'Summary:rsqd':               # Redline/Strikeout - Quick and Dirty
        ["set:QC QD Summary Set"],
    # 'Summary:but':               # Bold/Underline
    #    ["set:QC Summary Set (Bold/Underline) Test"],
    # 'Summary:rst':               # Redline/Strikeout
    #    ["set:QC Summary Set Test"],
    'Summary:nm':                 # No markup
        ["set:QC Summary Set"],
    'Summary:pat':                # Patient
        ["set:QC Summary Patient Set"],
    'Summary:patrs':              # Patient R/S
        ["set:QC Summary Patient Set"],
    'Summary:patqd':              # Patient - Quick and Dirty
        ["set:QC QD Summary Patient Set"],
    'Summary:patrsqd':            # Patient R/S - Quick and Dirty
        ["set:QC QD Summary Patient Set"],
    'Summary:patbu':              # Patient BU
        ["set:QC Summary Patient Set (Bold/Underline)"],
    'Summary:patbuqd':            # Patient BU - Quick and Dirty
        ["set:QC QD Summary Patient Set (Bold/Underline)"],
    'Term':
        ["set:QC Term Set"]
}


'''
REPLACE/REWRITE ALL OF THIS
#----------------------------------------------------------------------
# Import required packages.
#----------------------------------------------------------------------
import socket, string, struct, sys, re, cgi, base64, xml.dom.minidom
import os, smtplib, time, atexit, cdrdb, tempfile, traceback, difflib
import xml.sax.saxutils, datetime, subprocess, cdrutil
import math
import lxml.etree as etree
import logging
import cdrdb2
import requests

# ---------------------------------------------------------------------
# The file cdrapphosts knows about the different server names in the
# CBIIT and OCE environments based on the tier
# ---------------------------------------------------------------------
h = cdrutil.AppHost(cdrutil.getEnvironment(),
                    cdrutil.getTier(WORK_DRIVE + ":"),
                    filename=WORK_DRIVE + ':/etc/cdrapphosts.rc')


#----------------------------------------------------------------------
# Set some package constants
#----------------------------------------------------------------------

#----------------------------------------------------------------------
# If we're not actually running on the CDR server machine, then
# "localhost" won't work.
#----------------------------------------------------------------------
try:
    os.stat("d:/cdr/bin/CdrService.exe")
except:
    DEFAULT_HOST = CBIIT_NAMES[1]

#----------------------------------------------------------------------
# Use this class (or a derived class) when raising an exception in
# all new Python code in the CDR, unless there is good justification
# for using another approach.  Avoid raising string objects, which
# is now deprecated.
#
# This class behaves as the standard Python Exception class (from
# which this class is derived), except that string representation
# for objects created with a single argument uses unicode instead
# of ascii, which makes it safe to interpolate such objects into
# unicode strings, even when the Exception object was created with
# a unicode string containing non-ascii characters.
#----------------------------------------------------------------------
_baseException = Exception
class Exception(_baseException):
    __baseException = _baseException
    def __str__(self):
        if len(self.args) == 1:
            return unicode(self.args[0])
        else:
            return Exception.__baseException.__str__(self)
del _baseException

#----------------------------------------------------------------------
# Find a port to the CdrServer, searching port numbers in the following
#
# Set TCP/IP port for publishing to value of CDRPUBPORT, if present,
# else DEFAULT_PORT, else BATCHPUB_PORT
#----------------------------------------------------------------------
def getPubPort():
    """
    Find a TCP/IP port to the CdrServer, searching port numbers in
    the following order:
        Value of environment variable "CDRPUBPORT".
            Typically used for testing/debugging software.
        DEFAULT_PORT (2019 at this time).
            The CDR is normally running on this port.
        BATCHPUB_PORT (2020 at this time).
            Typically used when 2019 is turned off to prevent users
            from running interactively during a publication job.
    Raises an error if there is no CdrServer listening on that port.
    """
    ports2check = (os.getenv("CDRPUBPORT"), DEFAULT_PORT, BATCHPUB_PORT)
    for port in ports2check:
        if port:
            try:
                # See if there's a CdrServer listening on this port
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((DEFAULT_HOST, port))
                sock.close()
                return port
            except:
                # No listener, keep trying
                pass

    # If we got here, we've tried all possibilities
    raise Exception("No CdrServer found for publishing")

#----------------------------------------------------------------------
# Log an error from sendCommands
#----------------------------------------------------------------------
def logSendFailure(failingPart, connAttempts, sendRecvAttempts,
                   startTime, timeout):
    """
    Write a message to the debug log describing an exception generated
    within sendCommands.

    Pass:
        failingPart      - Where we failed, connecting or send/recv
        connAttempts     - How many times we tried to connect
        sendRecvAttempts - How many times we tried to send/recv
        startTime        - Start time, time.time() at start of call
        timeout          - Number of seconds requested for timeout
    """
    # Human readable datetimes
    now      = datetime.datetime.now()
    start    = datetime.datetime.fromtimestamp(startTime)
    dtFormat = "%Y-%m-%d %H:%M:%S.%f"
    nowPrt   = now.strftime(dtFormat)
    startPrt = start.strftime(dtFormat)

    # Log to default debugging log
    logwrite("""\
%s - sendCommands.%s  Connection attempts=%d  Send/Recv attempts=%d
First started at %s  timeout=%d  Exception message follows:
%s""" % (nowPrt, failingPart, connAttempts, sendRecvAttempts,
         startPrt, timeout, exceptionInfo()))

#----------------------------------------------------------------------
# Change the default timeout for sendCommands
# Call this after importing CDR if it is desirable to change the default
#   for every call until the current module calls it again or exits.
#----------------------------------------------------------------------
def setGlobalSendCommandsTimeout(newTimeout):
    global SENDCMDS_TIMEOUT
    SENDCMDS_TIMEOUT = newTimeout

#----------------------------------------------------------------------
# CBIIT now requires that connections to the CDR Server from other
# hosts be encrypted. See https://tracker.nci.nih.gov/browse/OCECDR-3845.
#----------------------------------------------------------------------
def tunnelCommands(cmds, timeout, tier=None):
    prefix = CBIIT_NAMES[2]
    if tier is not None:
        prefix = "https://%s" % h.getTierHostNames(tier.upper(), "APPC").qname
    url = "%s/cgi-bin/cdr/https-tunnel.ashx" % prefix
    limit = time.time() + timeout
    attempts = 0
    while time.time() < limit:
        try:
            attempts += 1
            response = requests.post(url, cmds)
            return response.content
        except Exception, e:
            logwrite("tunnelCommands (attempt %d): %s" % (attempts, e))
            now = time.time()
            if now >= limit:
                logwrite("tunnelCommands: Giving up on connect failure")
                break
            sleep = SENDCMDS_SLEEP
            if now + sleep > limit:
                sleep = math.ceil(limit - now)
            time.sleep(sleep)
    raise Exception("tunnelCommands could not connect.  "
                    "See info in %s" % DEFAULT_LOGFILE)

#----------------------------------------------------------------------
# Send a set of commands to the CDR Server and return its response.
# The `host` parameter is actually the name of a tier. We can't change
# its name in case a caller used the old parameter name.
#----------------------------------------------------------------------
def sendCommands(cmds, host=DEFAULT_HOST, port=DEFAULT_PORT, timeout=None):

    # Set the timeout to the global default value if not set by the caller
    # Note: setting timeout=SENDCMDS_TIMEOUT would be bound at compile time,
    #       so we set the compile time timeout to None.
    # See setGlobalSendCommandsTimeout() above to change for entire process.
    if timeout is None:
        timeout = SENDCMDS_TIMEOUT

    # Approach mandated by CBIIT for outside connections to the CDR server.
    if DEFAULT_HOST != "localhost" or host != DEFAULT_HOST:
        tier = host if host != DEFAULT_HOST else None
        return tunnelCommands(cmds, timeout, tier)

    # Connect to the CDR Server.
    connAttempts     = 0
    sendRecvAttempts = 0
    startTime        = time.time()
    endTime          = startTime + timeout

    # Run until logic raises exception or returns data
    while True:
        try:
            connAttempts += 1
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("localhost", port))
        except:
            # Find out what network connections are active
            # This produces a huge output, so only do it if there is a failure
            #   and only do it once
            if connAttempts == 1:
                netStat = os.popen('netstat -a 2>&1')
                netStatus = netStat.read()
                netStat.close()

                # Log details to default log (not application log, don't know
                #  what the application is)
                logwrite("""\
sendCommands: netstat output from connection failure in sendCommands
host=localhost port=%d current netstat=
%s
""" % (port, netStatus))
            logSendFailure("Connecting", connAttempts, sendRecvAttempts,
                            startTime, timeout)

            # Can we keep trying
            now = time.time()
            if now >= endTime:
                logwrite("sendCommands: Giving up on connect failure")
                raise Exception("sendCommands could not connect.  "
                                "See info in %s" % DEFAULT_LOGFILE)

            # Keep trying
            sleepTime = SENDCMDS_SLEEP
            if now + sleepTime > endTime:
                sleepTime = int(endTime - now)
            time.sleep(sleepTime)
            continue

        # If we got here we have a connection
        try:
            # Send the commands to the server.
            sendRecvAttempts += 1
            sock.send(struct.pack('!L', len(cmds)))
            sock.send(cmds)

            # Read the server's response.
            (rlen,) = struct.unpack('!L', sock.recv(4))
            resp = ''
            while len(resp) < rlen:
                resp = resp + sock.recv(rlen - len(resp))

            # We got the response.  We're done.  Return it to the caller
            break

        except:
            # The connection is almost certainly gone, but make sure
            try:
                sock.close()
            except:
                pass

            # Log the failure, as above
            logSendFailure("send/recv", connAttempts, sendRecvAttempts,
                            startTime, timeout)

            # Handle timeouts as above
            now = time.time()
            if now >= endTime:
                logwrite("sendCommands: Giving up on send/recv failure")
                raise Exception("sendCommands could not send/recv.  "
                                "See info in %s" % DEFAULT_LOGFILE)
            sleepTime = SENDCMDS_SLEEP
            if now + sleepTime > endTime:
                sleepTime = int(endTime - now)
            time.sleep(sleepTime)
            continue

    # If we got here, we succeeded
    # If there were errors, log the success
    if connAttempts > 1 or sendRecvAttempts > 1:
        logSendFailure("Success after retry", connAttempts, sendRecvAttempts,
                        startTime, timeout)

    # Clean up and hand the server's response back to the caller.
    sock.close()
    return resp

#----------------------------------------------------------------------
# Wrap a command in a CdrCommandSet element. Command can be an etree node,
# or a UTF-8 string. Credentials can be a string or a tuple containing
# user name and password. In the latter case we insert logon and logoff
# commands around the commands passed to this function.
#
# Returns a string with one of the two following structures:
#
#               CdrCommandSet
#                  SessionId
#                  CdrCommand ...
#
#               CdrCommandSet
#                 CdrCommand
#                   CdrLogon ...
#                 CdrCommand ...
#                 CdrCommand
#                   CdrLogoff
#
#----------------------------------------------------------------------
def wrapCommand(command, credentials, host=DEFAULT_HOST):

    # Do we already have a session ID?
    if isinstance(credentials, tuple):
        if DEFAULT_HOST != "localhost" or host != DEFAULT_HOST:
            tier = host if host != DEFAULT_HOST else None
            sessionId = windowsLogin(credentials[0], credentials[1], tier)
        else:
            sessionId = None
    elif isinstance(credentials, unicode):
        sessionId = credentials.encode("ascii")
    else:
        sessionId = credentials

    # If the command is a node, we can build the command set with XML tools.
    # (We should have done all commands this way, but the tools weren't
    # as capable back when we started as they are now).
    if isinstance(command, etree._Element):
        commandSet = etree.Element("CdrCommandSet")
        if sessionId:
            etree.SubElement(commandSet, "SessionId").text = sessionId
        else:
            commandSet.append(createLoginCommand(*credentials))
        cdr_command = etree.SubElement(commandSet, "CdrCommand")
        cdr_command.append(command)
        if isinstance(credentials, tuple):
            logoff = etree.SubElement(commandSet, "CdrCommand")
            logoff.append(etree.Element("CdrLogoff"))
        return etree.tostring(commandSet)

    # Otherwise, we use string manipulation to build the command set.
    commandSet = ["<CdrCommandSet>"]
    if sessionId:
        commandSet.append("<SessionId>")
        commandSet.append(sessionId)
        commandSet.append("</SessionId>")
    else:
        commandSet.append(etree.tostring(createLoginCommand(*credentials)))
    commandSet.append("<CdrCommand>")
    commandSet.append(command)
    commandSet.append("</CdrCommand>")
    if isinstance(credentials, tuple):
        commandSet.append("<CdrCommand><CdrLogoff/></CdrCommand>")
    commandSet.append("</CdrCommandSet>")
    return "".join(commandSet)

#----------------------------------------------------------------------
# Validate date/time strings using strptime.
# Wraps the exception handling.
#----------------------------------------------------------------------
def strptime(str, format):
    """
    Wrap time.strptime() in a function that performs the exception
    handling and just returns None if an exception was generated.

    The actual ValueError message from Python may not always be
    understandable by non-programming users.

    Pass:
        str    - Date or datetime as a character string.
        format - Python strptime format string, e.g. '%Y-%m-%d %H:%M:%S'
    """
    tm = None
    try:
        tm = time.strptime(str, format)
    except ValueError:
        tm = None
    return tm

#----------------------------------------------------------------------
# Validate from/to date/time strings using strptime.
# Wraps the exception handling.
#----------------------------------------------------------------------
def valFromToDates(format, fromDate, toDate, minFrom=None, maxTo=None):
    """
    Turns out there are many places where we have from and to dates or
    datetimes.  This packages the validation to prevent invalid dates
    and obviate XSS security vulnerabilities.

    If this is too restrictive, at least check that integers are passed
    when integers are expected.

    Pass:
        format   - Python strptime format, e.g., '%Y-%m-%d'
        fromDate - Begining date
        toDate   - Ending date
        minFrom  - Minimum beginning date, if any checking desired.
        maxTo    - Maximum ending date, if any checking desired.

    Return:
        True  - Data passes validation.
        False - One or more failures.
    """
    if not strptime(fromDate, format) or not strptime(toDate, format):
        return False
    if fromDate > toDate:
        return False
    if minFrom and fromDate < minFrom:
        return False
    if maxTo and toDate > maxTo:
        return False

    return True

#----------------------------------------------------------------------
# Report elapsed time.
#----------------------------------------------------------------------
def getElapsed(startSecs, endSecs=None):
    """
    Human readable report of time between two points.
    This is taken from Bob's original in CdrLongReports.py.

    Pass:
        startSecs - Starting time from time.time().
        endSecs   - Ending time, use now if None.

    Return:
        String of "HH:MM:SS"
    """
    if endSecs is None:
        endSecs = time.time()

    # Convert to hours, minutes, seconds
    delta = endSecs - startSecs
    secs  = delta % 60
    delta /= 60
    mins  = delta % 60
    hours = delta / 60

    return "%02d:%02d:%02d" % (hours, mins, secs)


#----------------------------------------------------------------------
# Information about an error returned by the CDR server API.  Provides
# access to the new attributes attached to some Err elements.  As of
# this writing, only the CDR commands involving validation assign
# these attributes, and only when specifically requested by the client.
#
# Members:
#
#    message  - text description of the error
#    etype    - type of error ('validation' or 'other') (default 'other')
#    elevel   - 'error' | 'warning' | 'info' | 'fatal' (default 'fatal')
#    eref     - position of validation error in document (if appropriate)
#----------------------------------------------------------------------
class Error:
    def __init__(self, node):
        self.message = getTextContent(node)
        self.etype   = node.getAttribute('etype') or 'other'
        self.elevel  = node.getAttribute('elevel') or 'fatal'
        self.eref    = node.getAttribute('eref') or None
    def getMessage(self, asUtf8 = False):
        if asUtf8:
            return self.message.encode('utf-8')
        return self.message
    __pattern = None
    @classmethod
    def getPattern(cls):
        """
        Accessor for regular expression pattern for extracting the
        text content of <Err/> elements found in an XML fragment,
        in case the caller prefers not to incur the overhead of
        the DOM parser.  Note that we optimize by deferring the
        compilation of the regular expression pattern until it's
        actually requested, to avoid incurring the processing penalty
        every time this module is loaded.
        """
        if not cls.__pattern:
            cls.__pattern = re.compile("<Err(?:\\s+[^>]*)?>(.*?)</Err>",
                                       re.DOTALL)
        return cls.__pattern

#----------------------------------------------------------------------
# Extract a single error element from XML response.
#
# Pass:
#    resp      - XML fragment in which to look for <Err/> elements
#    asObject  - True if the return show be an Error object (see
#                above) with information contained in the Err
#                element's attributes; otherwise returns the text
#                content of the first Err element as a string
#
# Return:        An Error object or a string (as determined by the
#                value of the asObject parameter) for the first
#                Err element found, if any; otherwise, None
#----------------------------------------------------------------------
def checkErr(resp, asObject = False):
    errors = getErrors(resp, False, True, asObjects = asObject)
    return errors and errors[0] or None

#----------------------------------------------------------------------
# Extract error elements from XML.
#
# Pass:
#    xmlFragment   - string in which to look for serialized Err elements
#    errorExpected - if True (the default), then return a generic error
#                    string when no Err elements are found; otherwise
#                    return an empty string or list if no Err elements
#                    are found
#    asSequence    - if False (the default), return the entire <Errors/>
#                    element (serialized as a string) if present (for
#                    backward compatibility with older code which expects
#                    strings for error conditions); otherwise return
#                    a (possibly empty) list of Error objects or error
#                    strings; assumed to be True if asObjects is True (q.v.)
#    asObjects     - if False (the default) errors found are returned as
#                    strings; otherwise, errors are returned as Error
#                    objects
#    useDom        - if True a DOM parser is used to to extract the
#                    information from the Err elements; otherwise
#                    a regular expression is used to extract the error
#                    description strings; the performance penalty
#                    for using the DOM parser is roughly an order of
#                    magnitude compared to using regular expressions,
#                    but even in the worst case (a response containing
#                    multiple errors and the breast cancer HP summary),
#                    the call to getErrors with useDom = True takes
#                    less than .002 seconds; default value is True;
#                    if asObject is True, then useDom is assumed to
#                    be True; if an exception is raised by the attempt
#                    to get a DOM parse tree and asObjects is False,
#                    then the function will fall back on extracting
#                    the Err values using a regular expression
#    asUtf8        - if True (the default, for backward compatibility
#                    with existing code) the string(s) returned are
#                    encoded as utf-8; ignored when objects are
#                    requested (strings in Error objects are Unicode
#                    objects); also ignored when we're not returning
#                    a sequence (again, to preserve the original
#                    behavior of the function)
#----------------------------------------------------------------------
def getErrors(xmlFragment, errorsExpected = True, asSequence = False,
              asObjects = False, useDom = True, asUtf8 = True):

    if asSequence or asObjects:

        # Safety check.
        if type(xmlFragment) not in (str, unicode):
            return []

        if useDom or asObjects:
            if type(xmlFragment) == unicode:
                xmlFragment = xmlFragment.encode('utf-8')
            try:
                dom = xml.dom.minidom.parseString(xmlFragment)
                errors = [Error(node)
                          for node in dom.getElementsByTagName('Err')]
                if errorsExpected and not errors:
                    return ["Internal failure"]
                return asObjects and errors or [e.getMessage(asUtf8)
                                                for e in errors]
            except Exception, e:
                if asObjects:
                    raise Exception(u"getErrors(): %s" % e)
            except:
                if asObjects:
                    raise Exception(u"getErrors() failure")
        if asUtf8 and type(xmlFragment) == unicode:
            xmlFragment = xmlFragment.encode('utf-8')
        errors = Error.getPattern().findall(xmlFragment)
        if not errors and errorsExpected:
            return ["Internal failure"]
        return errors

    # Compile the pattern for the regular expression.
    pattern = re.compile("<Errors[>\s].*</Errors>", re.DOTALL)

    # Search for the <Errors> element.
    errors  =  pattern.search(xmlFragment)
    if errors:           return errors.group()
    elif errorsExpected: return "<Errors><Err>Internal failure</Err></Errors>"
    else:                return ""

#----------------------------------------------------------------------
# Extract a piece of the CDR Server's response.
#----------------------------------------------------------------------
def extract(pattern, response):

    # Compile the regular expression.
    expr = re.compile(pattern, re.DOTALL)

    # Search for the piece we want.
    piece = expr.search(response)
    if piece: return piece.group(1)
    else:     return getErrors(response)

#----------------------------------------------------------------------
# Extract several pieces of the CDR Server's response.
#----------------------------------------------------------------------
def extract_multiple(pattern, response):

    # Compile the regular expression.
    expr = re.compile(pattern, re.DOTALL)

    # Search for the piece we want.
    piece = expr.search(response)
    if piece: return piece.groups()
    else:     return getErrors(response)

#----------------------------------------------------------------------
# Create login command. If password is None or empty, name represents
# an NIH domain account name which has already been authenticated.
# Otherwise, the user is to be authenticated against credentials
# stored in the CDR database.
#----------------------------------------------------------------------
def createLoginCommand(user, password):
    command = etree.Element("CdrCommand")
    logon = etree.SubElement(command, "CdrLogon")
    etree.SubElement(logon, "UserName").text = user
    if password:
        etree.SubElement(logon, "Password").text = password
    return command

#----------------------------------------------------------------------
# Log in using Windows authentication verified through a connection
# to IIS.
#----------------------------------------------------------------------
def windowsLogin(user, password, tier=None):
    prefix = CBIIT_NAMES[2]
    if tier is not None:
        prefix = "https://%s" % h.getTierHostNames(tier.upper(), "APPC").qname
    url = "%s/cgi-bin/secure/login.py" % prefix
    requests.packages.urllib3.disable_warnings()
    auth = requests.auth.HTTPDigestAuth(user, password)
    response = requests.get(url, auth=auth, verify=False)
    return response.text.strip()

#----------------------------------------------------------------------
# Log in to the CDR Server.  Returns session ID.
# If passWord is None, userId is an NIH domain account name.
#----------------------------------------------------------------------
def login(userId, passWord, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # If we're not running directly on the CDR, we must log in using
    # Windows authentication.
    if DEFAULT_HOST != "localhost" or host != DEFAULT_HOST:
        tier = host if host != DEFAULT_HOST else None
        return windowsLogin(userId, passWord, tier)

    # Send the login request to the server.
    command = createLoginCommand(userId, passWord)
    cmds = "<CdrCommandSet>%s</CdrCommandSet>" % etree.tostring(command)
    resp = sendCommands(cmds, host, port)

    # Extract the session ID.
    return extract("<SessionId[^>]*>(.+)</SessionId>", resp)

#----------------------------------------------------------------------
# Identify the user associated with a session.
# Originally this was a reverse login, providing a userid and password
# enabling a program to re-login the same user with a new session.
#
# The function has been modified (March 2013) to only return a user id.
# Passwords are to be encrypted in the database and are hence not stored
# in any form that could be used to re-login.
#
# Pass:
#   mySession  - session for user doing the lookup - currently unused.
#   getSession - session to be looked up.
#
# Returns:
#   Tuple of (userid, '')
#   Or single error string.
#
#   The original tuple type return is retained so that the many programs
#   that use this function only to find a userid will work without
#   modification.
#----------------------------------------------------------------------
def idSessionUser(mySession, getSession):

    # Direct access to db.  May replace later with secure server function.
    try:
        conn   = cdrdb.connect()
        cursor = conn.cursor()
    except cdrdb.Error, info:
        return "Unable to connect to database to get session info: %s" %\
                info[1][0]

    # Search user/session tables
    try:
        cursor.execute (
            "SELECT u.name, '' AS pw"
            "  FROM usr u, session s "
            " WHERE u.id = s.usr "
            "   AND s.name = ?", getSession)
        usrRow = cursor.fetchone()
        if type(usrRow)==type(()) or type(usrRow)==type([]):
            return tuple(usrRow)
        else:
            return usrRow
    except cdrdb.Error, info:
        return "Error selecting usr for session: %s - %s" % \
                (repr(getSession), info[1][0])

#----------------------------------------------------------------------
# Find out if the user for a session is a member of the specified group.
#----------------------------------------------------------------------
def member_of_group(session, group):
    try:
        name = idSessionUser(session, session)[0]
        user = getUser(session, name)
        return group in user.groups
    except:
        return False

#----------------------------------------------------------------------
# Select the email address for the user.
# Pass:
#   mySession  - session for user doing the lookup
#
# Returns:
#   Email address
#   Or single error string.
#----------------------------------------------------------------------
def getEmail(mySession):
    try:
        conn   = cdrdb.connect()
        cursor = conn.cursor()
    except cdrdb.Error, info:
        return "Unable to connect to database to get email info: %s" %\
                info[1][0]

    # Search user/session tables
    try:
        query = """\
           SELECT u.email
             FROM session s
             JOIN usr u
               ON u.id   = s.usr
            WHERE s.name = '%s'
              AND ended   IS NULL
              AND expired IS NULL""" % mySession
        cursor.execute (query)
        rows = cursor.fetchall()
        if len(rows) < 1:
           return("ERROR: User not authorized to run this report!")
        elif len(rows) > 1:
           return("ERROR: User session not unique!")
        else:
           return rows[0][0]
    except cdrdb.Error, info:
        return "Error selecting email for session: %s - %s" % \
                (mySession, info[1][0])

#----------------------------------------------------------------------
# Find information about the last versions of a document.
# Returns tuple of:
#   Last version number, or -1 if no versions
#   Last publishable version number or -1, may be same as last version.
#   Is changed information:
#     'Y' = last version is different from current working doc.
#     'N' = last version is not different.
# These are pass throughs of the response from the CdrLastVersions command.
# Single error string returned if errors.
#----------------------------------------------------------------------
def lastVersions(session, docId, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = wrapCommand ("""
 <CdrLastVersions>
   <DocId>%s</DocId>
 </CdrLastVersions>
""" % normalize(docId), session, host)

    # Submit it
    resp = sendCommands (cmd, host, port)

    # Failed?
    errs = getErrors (resp, 0)
    if len (errs) > 0:
        return errs

    # Else get the parts we want
    lastAny   = extract ("<LastVersionNum>(.+)</LastVersionNum>", resp)
    lastPub   = extract ("<LastPubVersionNum>(.+)</LastPubVersionNum>", resp)
    isChanged = extract ("<IsChanged>(.+)</IsChanged>", resp)

    return (int(lastAny), int(lastPub), isChanged)

#----------------------------------------------------------------------
# Find the date that a current working document was created or modified.
#----------------------------------------------------------------------
def getCWDDate(docId, conn=None):
    """
    Find the latest date/time in the audit trail for a document.
    This is the date on the current working document.

    Pass:
        docId - Doc to process.
        conn  - Optional database connection.  Else create one.

    Return:
        Audit_trail date_time as a string.

    Raises:
        cdrdb.Error if database error.
        Exception if doc ID not found.
    """
    # If no connection, create one
    if not conn:
        conn = cdrdb.connect('CdrGuest')
    cursor = conn.cursor()

    # Normalize passed docId to a plain integer
    idNum = exNormalize(docId)[1]

    # Get date from audit trail
    cursor.execute("""
        SELECT max(at.dt)
          FROM audit_trail at, action act
         WHERE act.name in ('ADD DOCUMENT', 'MODIFY DOCUMENT')
           AND at.action = act.id
           AND at.document = %d""" % idNum)
    row = cursor.fetchone()
    cursor.close()

    # Caller should only pass docId for a real document
    if not row:
        raise Exception("cdr.getCWDDate: No document found for id=%d" % idNum)

    return row[0]

#----------------------------------------------------------------------
# Search the query term table for values
#----------------------------------------------------------------------
def getQueryTermValueForId(path, docId, conn=None):
    """
    Search for values pertaining to a particular path and id, or just
    for a particular path.
    Parameters:
        path  - query_term.path, i.e., name of an index.
        docId - limit search to specific id.
        conn  - use this connection, or create one if None.
    Return:
        Sequence of values.  May be None.
    Raises:
        Exception if any failure.
    """
    # Create connection if none available
    if not conn:
        try:
            conn = cdrdb.connect ("CdrGuest")
        except cdrdb.Error, info:
            raise Exception("getQueryTermValueForId: can't connect to DB: %s" %
                            info[1][0])

    # Normalize id to integer
    did = exNormalize(docId)[1]

    # Search table
    try:
        # Using % substitution because it should be completely safe and faster
        cursor = conn.cursor()
        cursor.execute (
          "SELECT value FROM query_term WHERE path = '%s' AND doc_id = %d" %
          (path, did))
        rows = cursor.fetchall()
        if len(rows) == 0:
            return None

        # Convert sequence of sequences to simple sequence
        retRows = []
        for row in rows:
            retRows.append (row[0])
        return retRows

    except cdrdb.Error, info:
        raise Exception("getQueryTermValueForId: database error: %s" %
                        info[1][0])

#----------------------------------------------------------------------
# Extract the text content of an lxml.etree node.
#----------------------------------------------------------------------
def getEtreeTextContent(node):
    return node.xpath("string()")

#----------------------------------------------------------------------
# Extract the text content of a DOM element.
#----------------------------------------------------------------------
def getTextContent(node, recurse=False, separator=''):
    """
    Get text content for a node, possibly including sub nodes.

    Pass:
        node      - Node to be checked.
        recurse   - Flag indicating that all subnodes must be processed.
        separator - If multiple nodes, put this between them if we wish
                    to avoid jamming words together.
                    The separator is applied even if recurse=False.  It
                    also appears after the end of the last node.

    Return:
        Text content as a single string.
    """
    text = ''
    for child in node.childNodes:
        if child.nodeType in (child.TEXT_NODE, child.CDATA_SECTION_NODE):
            text = text + child.nodeValue + separator
        elif recurse and child.nodeType == child.ELEMENT_NODE:
            text = text + getTextContent(child, recurse, separator)
    return text

#----------------------------------------------------------------------
# Encode a blob.
#----------------------------------------------------------------------
def makeDocBlob(blob=None, inFile=None, outFile=None, wrapper=None, attrs=""):
    """
    Encode a blob from either a string or a file in base64 with
    optional CdrDocBlob XML wrapper.

    This is a pretty trivial and probably unnecessary function, but
    it gives us a single point of control for constructing blobs.

    Parameters:
        blob=None       Blob as a string of bytes.  If None, use inFile.
                        An empty blob ("") is legal.  We return a null
                         string with the requested wrapper.
        inFile=None     Name of input file containing blob.  If None use blob.
        outFile=None    Write output to this file, overwriting whatever
                         may be there, if anything.  If None, return blob
                         as a string.
        wrapper=None    True=wrap blob in passed xml element tag.  Else not.
        attrs=None      Attribute string to include if passed wrapper.

    Returns:
        Base64 encoded blob if outFile not specified.
        Else returns empty string with output to file.

    Raises Exception if invalid parms or bad file i/o.
    """
    # Check parms
    if blob == None and not inFile:
        raise Exception("makeDocBlob: requires passed blob or inFile")
    if blob and inFile:
        raise Exception("makeDocBlob: pass blob or inFile, not both")

    if inFile:
        # Get blob from file
        try:
            fp = open(inFile, "rb")
            blob = fp.read()
            fp.close()
        except IOError, info:
            raise Exception("makeDocBlob: %s" % info)
        if not blob:
            raise Exception("makeDocBlob: no data read from file %s" % inFile)

    # Encode with or without wrapper
    startTag = endTag = ""
    if wrapper:
        startTag = "<" + wrapper
        if attrs:
            startTag += " " + attrs
        startTag += ">"
        endTag   = "</" + wrapper + ">"
    encodedBlob = startTag + base64.encodestring(blob) + endTag

    # Output
    if outFile:
        try:
            fp = open(outFile, "wb")
            fp.write(encodedBlob)
            fp.close()
        except IOError, info:
            raise Exception("makeDocBlob: %s" % info)
        return ""
    return encodedBlob

#----------------------------------------------------------------------
# Internal subroutine to add or replace DocComment element in CdrDocCtl.
#----------------------------------------------------------------------
def _addRepDocComment(doc, comment):

    """
    Add or replace DocComment element in CdrDocCtl.
    Done via XML parsing.

    Pass:
        doc - Full document in CdrDoc format, unicode or utf-8
        comment - Comment to insert, unicode or utf-8

    Return:
        Full CdrDoc as utf-8 with DocComment element inserted or replaced.
    """
    # Sanity checks.  Failure here means caller passed bad parms
    if not doc:
        raise Exception("_addRepDocComment(): missing doc argument")

    # Data must be utf-8
    if type(doc) == type(u""):
        doc = doc.encode('utf-8')
    if type(comment) == type(u""):
        comment = comment.encode('utf-8')

    # Parse doc wrapper, elements only include the CdrDoc elements
    # Actual content is all in a CDATA section, which must be recognized
    # Have to override the default lxml behavior to do that
    from lxml import etree as lx
    parser = lx.XMLParser(strip_cdata=False)
    tree = lx.fromstring(doc, parser=parser)

    # Parent element for DocComment
    found = tree.findall('CdrDocCtl')
    if len(found) == 0:
        raise Exception("_addRepDocComment: No CdrDocCtl in doc:\n%s" % doc)
    docCtl = found[0]

    # Create the new DocComment element, newline tail makes it prettier
    newCmt = lx.Element('DocComment')
    newCmt.text = comment
    newCmt.tail = "\n"

    # Find DocComment, if it exists
    docCmt = docCtl.findall('DocComment')
    if len(docCmt):
        docCtl.replace(docCmt[0], newCmt)
    else:
        docCtl.append(newCmt)

    # Return re-serialized doc
    return lx.tostring(tree, pretty_print=True)

#----------------------------------------------------------------------
# Internal subroutine to add or replace DocActiveStatus element in CdrDocCtl.
#----------------------------------------------------------------------
def _addRepDocActiveStatus(doc, newStatus):

    """
    Add or replace DocActiveStatus element in CdrDocCtl.
    Done by text manipulation.

    Pass:
        doc - Full document in CdrDoc format.
        newStatus - 'I' or 'A'.

    Return:
        Full CdrDoc with DocActiveStatus element inserted or replaced.

    Assumptions:
        Both doc and comment must be UTF-8.  (Else must add conversions here.)
    """

    # Sanity check.
    if not doc:
        raise Exception("_addRepDocActiveStatus(): missing doc argument")

    # Search for and delete existing DocComment
    delPat = re.compile (r"\n*<DocActiveStatus.*</DocActiveStatus>\n*",
                         re.DOTALL)
    newDoc = delPat.sub ('', doc).replace('<DocActiveStatus/>', '')

    # Search for CdrDocCtl to insert new DocComment after it
    newDoc = newDoc.replace('<CdrDocCtl/>', '<CdrDocCtl></CdrDocCtl>')
    insPat = re.compile (r"(?P<first>.*<CdrDocCtl[^>]*>)\n*(?P<last>.*)",
                         re.DOTALL)
    insRes = insPat.search (newDoc)
    if insRes:
        parts = insRes.group ('first', 'last')
    if not insRes or len (parts) != 2:
        # Should never happen unless there's a bug
        raise Exception("addRepDocActiveStatus: No CdrDocCtl in doc:\n%s" %
                        doc)

    # Comment must be compatible with CdrDoc utf-8
    if type(newStatus) == type(u""):
        newStatus = newStatus.encode('utf-8')

    # Insert new status
    return (parts[0] + "\n<DocActiveStatus>" + newStatus
            + "</DocActiveStatus>\n" + parts[1])

#----------------------------------------------------------------------
# Add a blob to a document, replacing existing blob if necessary
#----------------------------------------------------------------------
def _addDocBlob(doc, blob=None, blobFileName=None):
    """
    If either a blob (array of bytes) or the name of a file containing
    a blob is passed, then:

        Delete any existing CdrDocBlob in the doc.
        Add in the blob from the byte string or file as a base64
          encoded CdrDocBlob subelement of a CdrDoc.

    As a convenience, _addDocBlob accepts the case where blob and
    blobFileName are both None, returning doc unchanged.  This is
    so we don't have to check these parms in two different places.

    Pass:
        doc          - Document in CdrDoc utf-8 format.
        blob         - Optional blob as a string of bytes, NOT base64.
                        base64 conversion will be applied here.
                        May be None, may be empty.
                        An empty blob ("") causes an empty CdrDocBlob
                        element to be inserted in the doc, which in turn
                        causes any blob associated with this doc in the
                        database to be disassociated and, if it is not
                        versioned, deleted.
        blobFileName - Optional name of file containing binary bytes, not
                        in base64.  May be None.  May be the name of a
                        zero length file.

    Return:
        Possibly revised CdrDoc string.

    Raises:
        Exception if both blob and blobFileName are passed, or no
        CdrDoc end tag is found.
    """
    # Common case, we're just checking for the caller
    if (blob == None and not blobFileName):
        return doc

    # Check parms
    if (blob and blobFileName):
        raise Exception("_addDocBlob called with two blobs, one in "
                        "memory and one in named file")

    # Encode blob from memory or file
    encodedBlob = makeDocBlob(blob, blobFileName, wrapper='CdrDocBlob')

    # Delete any existing blob in doc.  We'll replace it
    delBlobPat  = re.compile(r"\n*<CdrDocBlob.*</CdrDocBlob>\n*", re.DOTALL)
    strippedDoc = delBlobPat.sub('', doc)

    # Prepare replacement
    encodedBlob = "\n" + encodedBlob + "\n</CdrDoc>\n"

    # Add the new blob just before the CdrDoc end tag
    addBlobPat = re.compile("\n*</CdrDoc>\n*", re.DOTALL)
    newDoc     = addBlobPat.sub(encodedBlob, strippedDoc)

    # Should never happen
    if newDoc == strippedDoc:
        raise Exception("_addDocBlob: could not find CdrDoc end tag")

    return newDoc

#----------------------------------------------------------------------
# Determine if a document is checked out without retrieving it
#----------------------------------------------------------------------
class lockedDoc(object):
    """
    Container object for information about the checkout status of a doc.
    """
    def __init__(self, userId, userAbbrev, userFullName,
                 docId, docVersion, docType, docTitle, dateOut):
        self.__userId       = userId
        self.__userAbbrev   = userAbbrev
        self.__userFullName = userFullName
        self.__docId        = docId
        self.__docVersion   = docVersion
        self.__docType      = docType
        self.__docTitle     = docTitle
        self.__dateOut      = dateOut

    # Read-only property accessors
    def getUserId(self): return self.__userId
    userId = property(getUserId)

    def getUserAbbrev(self): return self.__userAbbrev
    userAbbrev = property(getUserAbbrev)

    def getUserFullName(self): return self.__userFullName
    userFullName = property(getUserFullName)

    def getDocId(self): return self.__docId
    docId = property(getDocId)

    def getDocType(self): return self.__docType
    docType = property(getDocType)

    def getDocTitle(self):
        # Conversion for use in log files and messages
        if type(self.__docTitle) == type(u""):
            return self.__docTitle.encode('ascii', 'replace')
        return self.__docTitle
    docTitle = property(getDocTitle)

    def getDateOut(self): return self.__dateOut
    dateOut = property(getDateOut)

    def __str__(self):
        """ Human readable form """
        s = \
"""       docId: %s
     docType: %s
    docTitle: %s
  docVersion: %s
      userId: %s
  userAbbrev: %s
userFullName: %s
     dateOut: %s""" % (self.__docId, self.__docType, self.__docTitle,
     self.__docVersion, self.__userId, self.__userAbbrev,
     self.userFullName, self.dateOut)

        return s


def isCheckedOut(docId, conn=None):
    """
    Determine if a document is checked out.

    Pass:
        docId - Doc ID, any exNormal'izable format.
        conn  - Optional connection object, to optimize many checks in a row

    Return:
        If locked: returns a lockedDoc object.
        Else: returns None
    """
    # DB connection
    if not conn:
        conn = cdrdb.connect()
    cursor = conn.cursor()

    # Normalize id
    docId = exNormalize(docId)[1]

    # Data for lockedDoc object
    try:
        cursor.execute("""
        SELECT d.id, d.title, t.name,
               c.usr, c.version, c.dt_out, u.name, u.fullname
          FROM document d
          JOIN doc_type t on d.doc_type = t.id
          JOIN checkout c on d.id = c.id
          JOIN usr      u on c.usr = u.id
         WHERE c.id = ?
           AND c.dt_in IS NULL
        """, docId)

        row = cursor.fetchone()

        cursor.close()
    except cdrdb.Error, info:
        raise Exception("Database error in isCheckedOut. docId=%d:\n%s" %
                             (docId, str(info)))

    # No hits means not checked out
    if row is None:
        return None

    # Else return full info
    lockObj = lockedDoc(row[3], row[6], row[7], row[0], row[4], row[2],
                        row[1], row[5])
    return lockObj


#----------------------------------------------------------------------
# Checkout a document without retrieving it
#----------------------------------------------------------------------
def checkOutDoc(credentials, docId, force='N', comment='',
                host=DEFAULT_HOST, port=DEFAULT_PORT):
    """
    Checkout a document to the logged in user.

    Pass:
        credentials - returned form cdr.login().
        docId       - ID as number or string.
        force       - 'Y' = force checkout even if already out to another
                      user.  Requires that user have FORCE CHECKOUT
                      permission.
        comment     - checkout comment.
        host        - server.
        port        - TCP/IP port number.

    Return:
        Current version number or 0 if no version number returned.

    Raises:
        cdr.Exception if error.
    """
    docId = exNormalize(docId)[0]
    cmd = wrapCommand("""
<CdrCheckOut ForceCheckOut='%s'>
 <DocumentId>%s</DocumentId>
 <Comment>%s</Comment>
</CdrCheckOut>""" % (force, docId, comment), credentials, host)

    response = sendCommands(cmd, host, port)
    errs     = getErrors(response, False)
    if errs:
        raise Exception(errs)
    else:
        pattern = re.compile("<Version>(.*)</Version>", re.DOTALL)
        match   = pattern.search(response)
        if match:
            verNum  = match.group(1)
            return int(verNum)
        return 0

#----------------------------------------------------------------------
# Mark a CDR document as deleted.
#----------------------------------------------------------------------
def delDoc(credentials, docId, val='N', reason='',
           host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command.
    docId   = "<DocId>%s</DocId>" % docId
    val     = "<Validate>%s</Validate>" % val
    reason  = reason and ("<Reason>%s</Reason>" % reason) or ''
    cmd     = "<CdrDelDoc>%s%s%s</CdrDelDoc>" % (docId, val, reason)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)

    # Check for failure.
    errors = getErrors(resp, errorsExpected=False, asSequence=True,
                       asUtf8=False)
    if errors:
        return errors

    # Extract the document ID.
    return extract("<DocId.*>(CDR\d+)</DocId>", resp)

#----------------------------------------------------------------------
# Validate new and old docs
#----------------------------------------------------------------------
def valPair(session, docType, oldDoc, newDoc, host=DEFAULT_HOST,
            port=DEFAULT_PORT):
    """
    Validate the old and new versions of a document.
    If the old version is invalid, don't bother with the new.

    Used to ensure that a global change has not invalidated a
    previously valid doc.

    Pass:
        Logon credentials - must be authorized to validate this doctype.
        Document type.
        Old version of doc.
        New version of doc.
        Host.
        Connection port.

    Return:
        If oldDoc is valid and newDoc is not:
            Return list of de-duped errors, with multiples indicated.
        Else:
            Return None.
    """
    # Validate first document
    result = valDoc(session, docType, doc=oldDoc, host=host, port=port)

    # If no errors, check the new version
    if not getErrors(result, errorsExpected=0):
        result = valDoc(session, docType, doc=newDoc, host=host, port=port)
        return deDupErrs(result)

    # Else return empty list
    return []

#----------------------------------------------------------------------
# Update a document title
#----------------------------------------------------------------------
def updateTitle(credentials, docId, host=DEFAULT_HOST, port=DEFAULT_PORT):
    """
    Tell the CdrServer to re-run the title filter for this document,
    updating the title stored in the document table.

    No locking is done since the this action does not change the document
    itself.  If another user has the document checked out, no harm will
    be done when and if he saves it.

    Pass:
        credentials - Logon credentials or session.
        docId       - Document ID, any format is okay.
        host        - Update on this host.
        port        - Via this CdrServer port.

    Return:
        True  = Host says title was changed.
        False = Host says regenerated title is the same as the old one.
    """
    docIdStr = exNormalize(docId)[0]

    # Prepare transaction
    cmd = """
 <CdrUpdateTitle>
  <DocId>%s</DocId>
 </CdrUpdateTitle>
""" % docIdStr
    cmd = wrapCommand(cmd, credentials, host)

    # Interact with the host
    resp = sendCommands(cmd, host, port)

    # Check response
    if resp.find("unchanged") >= 0:
        return False
    if resp.find("changed") >= 0:
        return True

    # Should be here
    raise Exception("cdr.updateTitle: Unexpected return from server:\n%s\n" %
                    resp)

#----------------------------------------------------------------------
# De-duplicate and list a sequence of error messages
#----------------------------------------------------------------------
def deDupErrs(errXml):
    """
    Parse an error XML string returned by valDoc, de-duplicate the
    errors, and return them in a sequence of strings.

    Each error string is followed by optional number of occurrences
    in parens, e.g.:

        "An error" - Occurred once
        "Another error (3 times)" - Occurred three times

    Pass:
        errXml - Error XML string.
    Return:
        Sequence of error strings, may be empty
    """
    # If nothing passed, or empty string passed, then no errors
    if not errXml:
        return []

    # De-dup any errors
    errs = {}
    dom = xml.dom.minidom.parseString(errXml)
    for err in dom.getElementsByTagName('Err'):
        errString = getTextContent(err)
        errs[errString] = errs.get(errString, 0) + 1

    # Prepare results list
    result = []
    for err in errs.keys():
        errString = err
        if errs[err] > 1:
            errString += " (%d times)" % errs[err]
        result.append(errString)

    return result

#----------------------------------------------------------------------
# Request the output for a CDR report.
#----------------------------------------------------------------------
def report(credentials, name, parms, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command.
    cmd = "<CdrReport><ReportName>%s</ReportName>" % name

    # Add the parameters.
    if parms:
        cmd = cmd + "<ReportParams>"
        for parm in parms:
            cmd = cmd + '<ReportParam Name="%s" Value="%s"/>' % (
                cgi.escape(parm[0], 1), cgi.escape(parm[1], 1))
        cmd = cmd + "</ReportParams>"

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd + "</CdrReport>", credentials, host),
                        host, port)

    # Extract the report.
    return extract("(<ReportBody[>\s].*</ReportBody>)", resp)

#----------------------------------------------------------------------
# Class to contain one hit from query result set.
#----------------------------------------------------------------------
class QueryResult:
    def __init__(self, docId, docType, docTitle):
        self.docId      = docId
        self.docType    = docType
        self.docTitle   = docTitle
    def __repr__(self):
        return "%s (%s) %s\n" % (self.docId, self.docType, self.docTitle)

#----------------------------------------------------------------------
# Process a CDR query.  Returns a tuple with two members, the first of
# which is a list of tuples containing id, doctype and title for each
# document in the search result, and the second of which is an <Errors>
# element.  Exactly one of these two member of the tuple will be None.
#----------------------------------------------------------------------
def search(credentials, query, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command.
    cmd = ("<CdrSearch><Query>//CdrDoc[%s]/CdrCtl/DocId</Query></CdrSearch>"
            % query)

    # Submit the search.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)

    # Check for problems.
    err = checkErr(resp)
    if err: return err

    # Extract the results.
    results = extract("<QueryResults>(.*)</QueryResults>", resp)
    qrElemsPattern  = re.compile("<QueryResult>(.*?)</QueryResult>", re.DOTALL)
    docIdPattern    = re.compile("<DocId>(.*)</DocId>", re.DOTALL)
    docTypePattern  = re.compile("<DocType>(.*)</DocType>", re.DOTALL)
    docTitlePattern = re.compile("<DocTitle>(.*)</DocTitle>", re.DOTALL)
    ret = []
    for qr in qrElemsPattern.findall(results):
        docId    = docIdPattern.search(qr).group(1)
        docType  = docTypePattern.search(qr).group(1)
        docTitle = docTitlePattern.search(qr).group(1)
        ret.append(QueryResult(docId, docType, docTitle))
    return ret

#----------------------------------------------------------------------
# Return all all_docs info for a CDR document specified by ID
#----------------------------------------------------------------------
def getAllDocsRow(docId, conn=None):
    """
    Retrieve most info from the all_docs table for a document ID.
    Does not get the XML, which could be very large.

    Pass:
        docId - Document CDR ID, any format recognized by exNormalize().
        conn  - Optional connection object.

    Return:
        Dictionary containing column name = column value.
        Dictionary["doc_type"] is the doctype name string, not the id.
        Raises cdr:Exception if error.
    """
    # Convert to integer, raise exception if error
    idNum = exNormalize(docId)[1]

    # Fields of interest
    qry = """
SELECT d.title, t.name, d.active_status,
       d.val_status, d.val_date, d.first_pub
  FROM all_docs d
  JOIN doc_type t
    ON d.doc_type = t.id
 WHERE d.id = ?
 """
    # Connection
    if not conn:
        try:
            conn = cdrdb.connect()
        except cdrdb.Error, info:
            raise Exception("Error getting connection in getAllDocsRow(%s): %s"
                             % (docId, str(info)))

    # Get all the data
    try:
        cursor = conn.cursor()
        cursor.execute(qry, idNum)
        row    = cursor.fetchone()
        cursor.close()
    except cdrdb.Error, info:
        raise Exception("Database error in getAllDocsRow(%s): %s" %
                        (docId, str(info)))

    # Doc not found?
    if not row:
        raise Exception("getAllDocsRow() found no match for doc %s" % docId)

    # Parse the results for the caller
    docData = {}
    docData["id"]            = idNum
    docData["title"]         = row[0]
    docData["doc_type"]      = row[1]
    docData["active_status"] = row[2]
    docData["val_status"]    = row[3]
    docData["val_date"]      = row[4]
    docData["first_pub"]     = row[5]

    return docData


class Term:
    def __init__(self, id, name):
        self.id       = id
        self.name     = name
        self.parents  = []
        self.children = []

class TermSet:
    def __init__(self, error = None):
        self.terms = {}
        self.error = error

#----------------------------------------------------------------------
# Gets context information for term's position in terminology tree.
#----------------------------------------------------------------------
def getTree(credentials, docId, depth=1,
            host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = """\
<CdrGetTree><DocId>%s</DocId><ChildDepth>%d</ChildDepth></CdrGetTree>
""" % (normalize(docId), depth)

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)
    err = checkErr(resp)
    if err: return TermSet(error = err)

    # Parse the response.
    respExpr = re.compile("<CdrGetTreeResp>\s*"
                          "<Pairs>(.*)</Pairs>\s*"
                          "<Terms>(.*)</Terms>\s*"
                          "</CdrGetTreeResp>", re.DOTALL)
    pairExpr = re.compile("<Pair><Child>(.*?)</Child>\s*"
                          "<Parent>(.*?)</Parent></Pair>")
    termExpr = re.compile("<Term><Id>(.*?)</Id>\s*"
                          "<Name>(.*?)</Name></Term>")
    groups   = respExpr.search(resp)
    result   = TermSet()
    terms    = result.terms

    # Extract the names of all terms returned.
    for term in termExpr.findall(groups.group(2)):
        (trmId, name) = term
        terms[trmId]  = Term(id = trmId, name = name)

    # Extract the child-parent relationship pairs.
    for pair in pairExpr.findall(groups.group(1)):
        (child, parent) = pair
        terms[child].parents.append(terms[parent])
        terms[parent].children.append(terms[child])

    return result

#----------------------------------------------------------------------
# Gets the list of CDR users.
#----------------------------------------------------------------------
def getUsers(credentials, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListUsrs/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)
    err = checkErr(resp)
    if err: return err

    # Parse the response.
    users = re.findall("<UserName>(.*?)</UserName>", resp)
    users.sort()
    return users

#----------------------------------------------------------------------
# Gets the list of CDR document types.
#----------------------------------------------------------------------
def getDoctypes(credentials, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListDocTypes/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)
    err = checkErr(resp)
    if err: return err

    # Parse the response.
    types = re.findall("<DocType>(.*?)</DocType>", resp)
    if 'Filter' not in types: types.append('Filter')
    types.sort()
    return types

#----------------------------------------------------------------------
# Gets the list of currently known CDR document formats.
#----------------------------------------------------------------------
def getDocFormats(conn=None):
    """
    Gets document format names, in alpha order.

    Pass:
        conn - Optional existing db connection.

    Return:
        List of legal format names, unicode charset.

    Raises:
        Database exception if unable to get connection.
    """
    if conn is None:
        # Get a connection, raising exception if failed
        conn = cdrdb.connect()
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM format ORDER BY name")
    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        raise Exception("Unable to select document format names")

    formats = [row[0] for row in rows]
    return formats

#----------------------------------------------------------------------
# Fetch a value from the ctl table.
#----------------------------------------------------------------------
def getControlValue(group, name):
    query = cdrdb2.Query("ctl", "val")
    query.where(query.Condition("grp", group))
    query.where(query.Condition("name", name))
    query.where("inactivated IS NULL")
    row = query.execute().fetchone()
    return row and row[0] or None

#----------------------------------------------------------------------
# Update the ctl table.
#----------------------------------------------------------------------
def updateCtl(credentials, action,
              grp=None, name=None, val=None, comment=None,
              host=DEFAULT_HOST, port=DEFAULT_PORT):
    """
    Update the ctl table.  See CdrCtl.h/.cpp in the server for what this does.

    Caller must be logged in as user with SET_SYS_VALUE privilege.

    Pass:
        credentials - as elsewhere.  See cdr.login().
        action      - one of "Create", "Inactivate", "Install".
                      Create:     Creates a new row in the ctl table.  If
                                  another row exists with the same grp and
                                  name, it will be inactivated, effectively
                                  replaced by this new one.
                      Inactivate: Inactivate an existing grp/name/val
                                  without replacing it with a new one.
                      Install:    Causes the CdrServer to load the latest
                                  version of the ctl table into memory.  Until
                                  this is done, or the CdrServer restarted,
                                  the old values will still be active.
        grp         - grouping string for names in Create or Inactivate.
                       Example: 'Publishing'
        name        - name of the value for Create or Inactivate.
                       Example: 'ThreadCount'.
        val         - value itself, required for Create, else ignored.
                       Example: '6'.
        comment     - optional comment to store in the table.

        All parameters are strings.  Max length is defined in the database,
        currently as 255 chars each for grp, name, val, comment.

    Return:
        None

    Throws
        Exception if there is an error return from the CdrServer.
    """
    # Parameters are checked in the server.  Don't need to do it here.
    cmd = "<CdrSetCtl>\n <Ctl>\n  <Action>%s</Action>\n" % action
    if grp is not None:
        cmd += "  <Group>%s</Group>\n" % cgi.escape(grp)
    if name is not None:
        cmd += "  <Key>%s</Key>\n" % cgi.escape(name)
    if val is not None:
        cmd += "  <Value>%s</Value>\n" % cgi.escape(val)
    if comment is not None:
        cmd += "  <Comment>%s</Comment>\n" % cgi.escape(comment)
    cmd += " </Ctl>\n</CdrSetCtl>\n"

    # Wrap it with credentials and send it
    cmd  = wrapCommand(cmd, credentials, host)
    resp = sendCommands(cmd, host, port)

    # Did server report error?
    errs = getErrors(resp, 0)
    if len(errs) > 0:
        raise Exception("Server error on cdr.updateCtl:\n%s" % errs)

    return None

#----------------------------------------------------------------------
# Gets the list of CDR schema documents.
#----------------------------------------------------------------------
def getSchemaDocs(credentials, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListSchemaDocs/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)
    err = checkErr(resp)
    if err: return err

    # Parse the response.
    return re.findall("<DocTitle>(.*?)</DocTitle>", resp)

#----------------------------------------------------------------------
# Get a list of enumerated values for a CDR schema simpleType.
#----------------------------------------------------------------------
def getSchemaEnumVals(schemaTitle, typeName, sorted=False):
    """
    Read in a schema from the database.  Parse it.  Extract the enumerated
    values from a simpleType element.  Return them to the caller.

    Used for constructing picklists, checkboxes, valid values, etc.

    Pass:
        schemaTitle - document.title for the schema in the database.
                      Typical names are "SummarySchema.xml",
                      "CTGovProtocol.xml", etc.
        typeName    - Value of the name attribute for the simpleType.  Only
                      simpleTypes are supported.
        sorted      - True = sort values by value.  Else return in order
                      found in the schema.

    Return:
        Array of string values.

    Raises:
        cdr.Exception if schemaTitle or simpleName not found.
    """
    from lxml import etree as lx

    # Get the schema
    qry = """
     SELECT d.xml
       FROM document d
       JOIN doc_type t
         ON d.doc_type = t.id
      WHERE t.name = 'schema'
        AND d.title = ?
    """
    try:
        conn   = cdrdb.connect()
        cursor = conn.cursor()
    except cdrdb.Error, info:
        raise Exception(
            "Unable to connect to database in cdr.getSchemaEnumVals(): %s" %
             str(info))
    try:
        cursor.execute(qry, schemaTitle)
        row = cursor.fetchone()
        cursor.close()
    except cdrdb.Error, info:
        raise Exception(
            "Database error fetching schema in cdr.getSchemaEnumVals(): %s" %
             str(info))
    if not row:
        raise Exception("Schema %s not found in cdr.getSchemaEnumVals()" %
                         schemaTitle)

    # Parse it
    tree = lx.fromstring(row[0].encode('utf-8'))

    # Search for enumerations of the simple type
    # Note what we have to do with namespaces - won't work without that
    xmlns = {"xmlns": "http://www.w3.org/2001/XMLSchema"}
    path  = "//xmlns:simpleType[@name='%s']//xmlns:enumeration" % typeName
    nodes = tree.xpath(path, namespaces=xmlns)
    if not nodes:
        raise Exception(
            "simpleType %s not found in schema %s in cdr.getSchemaEnumVals()" %
            (typeName, schemaTitle))

    # Cumulate the values in a list
    valList = []
    for node in nodes:
        valList.append(node.get("value"))

    # Return in desired order
    if sorted:
        valList.sort()
    return valList

#----------------------------------------------------------------------
# Holds information about a single CDR user.
#----------------------------------------------------------------------
class User:
    def __init__(self,
                 name,
                 password = '',
                 fullname = None,
                 office   = None,
                 email    = None,
                 phone    = None,
                 groups   = None,
                 comment  = None,
                 authMode = "network"):
        self.name         = name
        self.password     = password
        self.fullname     = fullname
        self.office       = office
        self.email        = email
        self.phone        = phone
        self.groups       = groups or []
        self.comment      = comment
        self.authMode     = authMode

#----------------------------------------------------------------------
# Retrieves information about a CDR user.
#----------------------------------------------------------------------
def getUser(credentials, uName, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = etree.Element("CdrGetUsr")
    etree.SubElement(cmd, "UserName").text = uName
    cmd  = etree.tostring(cmd)

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)
    err = checkErr(resp)
    if err: return err

    # Parse the response.
    root = etree.XML(resp)
    for responseNode in root.findall("CdrResponse/CdrGetUsrResp"):
        for userNode in responseNode.findall("UserName"):
            user = User(userNode.text)
            for child in responseNode:
                if child.tag == "FullName":
                    user.fullname = child.text
                elif child.tag == "Office":
                    user.office = child.text
                elif child.tag == "Email":
                    user.email = child.text
                elif child.tag == "Phone":
                    user.phone = child.text
                elif child.tag == "Comment":
                    user.comment = child.text
                elif child.tag == "GrpName":
                    user.groups.append(child.text)
                elif child.tag == "AuthenticationMode":
                    user.authMode = child.text
            return user
    return "User %s not found" % repr(uName)

#----------------------------------------------------------------------
# Add or update the database record for a CDR user.
#
# Pass:
#   credentials - Session for person creating the user (not the user himself).
#   uName       - Name of user whose record should be modified.
#   user        - Object containing the values to be stored for the user
#   host        - Which server to use.
#   port        - Which port.
#
# Return:
#   string      - Error message if operation failed.
#   None        - Operation succeeded.
#----------------------------------------------------------------------
def putUser(credentials, uName, user, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    if uName:
        cmd = etree.Element("CdrModUsr")
        etree.SubElement(cmd, "UserName").text = uName
        if user.name and uName != user.name:
            etree.SubElement(cmd, "NewName").text = user.name
    else:
        cmd = etree.Element("CdrAddUsr")
        etree.SubElement(cmd, "UserName").text = user.name

    etree.SubElement(cmd, "AuthenticationMode").text = user.authMode
    etree.SubElement(cmd, "Password").text = user.password
    if user.fullname is not None:
        etree.SubElement(cmd, "FullName").text = user.fullname
    if user.office is not None:
        etree.SubElement(cmd, "Office").text = user.office
    if user.email is not None:
        etree.SubElement(cmd, "Email").text = user.email
    if user.phone is not None:
        etree.SubElement(cmd, "Phone").text = user.phone
    if user.comment is not None:
        etree.SubElement(cmd, "Comment").text = user.comment
    for group in user.groups:
        etree.SubElement(cmd, "GrpName").text = group

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)
    err = checkErr(resp)
    if err: return err

    # No errors to report.
    return None

#----------------------------------------------------------------------
# Deletes a CDR user.
#----------------------------------------------------------------------
def delUser(credentials, usr, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = etree.Element("CdrDelUsr")
    etree.SubElement(cmd, "UserName").text = usr

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)
    err = checkErr(resp)
    if err: return err

    # No errors to report.
    return None

#----------------------------------------------------------------------
# Holds information about a single CDR link type.
#----------------------------------------------------------------------
class LinkType:
    def __init__(self, name, linkSources = None,
                             linkTargets = None,
                             linkProps   = None,
                             comment     = None,
                             linkChkType = "P"):
        self.name        = name
        self.linkSources = linkSources or []
        self.linkTargets = linkTargets or []
        self.linkProps   = linkProps   or []
        self.comment     = comment
        self.linkChkType = linkChkType
    def __str__(self):
        return "LinkType(%s,\n%s,\n%s,\n%s,\n%s,\n%s)" % (self.name,
                                                 self.linkSources,
                                                 self.linkTargets,
                                                 self.linkProps,
                                                 self.linkChkType,
                                                 self.comment)

#----------------------------------------------------------------------
# Holds information about a single CDR link property.
#----------------------------------------------------------------------
class LinkProp:
    def __init__(self, name, comment = None):
        self.name        = name
        self.comment     = comment

#----------------------------------------------------------------------
# Retrieves list of CDR link type names.
#----------------------------------------------------------------------
def getLinkTypes(credentials, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListLinkTypes/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)
    err = checkErr(resp)
    if err: return err

    # Parse the response
    types = re.findall("<Name>(.*?)</Name>", resp)
    types.sort()
    return types

#----------------------------------------------------------------------
# Retrieves information from the CDR for a link type.
#----------------------------------------------------------------------
def getLinkType(credentials, name, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = "<CdrGetLinkType><Name>%s</Name></CdrGetLinkType>" % name

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)
    err = checkErr(resp)
    if err: return err

    # Parse the response
    name     = re.findall("<Name>(.*)</Name>", resp)[0]
    cmtExpr  = re.compile("<LinkTypeComment>(.*)</LinkTypeComment>", re.DOTALL)
    chkExpr  = re.compile("<LinkChkType>(.*?)</LinkChkType>", re.DOTALL)
    srcExpr  = re.compile("<LinkSource>(.*?)</LinkSource>", re.DOTALL)
    tgtExpr  = re.compile("<TargetDocType>(.*?)</TargetDocType>", re.DOTALL)
    prpExpr  = re.compile("<LinkProperties>(.*?)</LinkProperties>", re.DOTALL)
    sdtExpr  = re.compile("<SrcDocType>(.*)</SrcDocType>", re.DOTALL)
    fldExpr  = re.compile("<SrcField>(.*)</SrcField>", re.DOTALL)
    prnExpr  = re.compile("<LinkProperty>(.*)</LinkProperty>", re.DOTALL)
    prvExpr  = re.compile("<PropertyValue>(.*)</PropertyValue>", re.DOTALL)
    prcExpr  = re.compile("<PropertyComment>(.*)</PropertyComment>", re.DOTALL)
    comment  = cmtExpr.findall(resp)
    chkType  = chkExpr.findall(resp)
    sources  = srcExpr.findall(resp)
    targets  = tgtExpr.findall(resp)
    props    = prpExpr.findall(resp)
    linkType = LinkType(name)
    if comment:  linkType.comment     = comment[0]
    if targets:  linkType.linkTargets = targets
    if chkType:  linkType.linkChkType = chkType[0]
    for source in sources:
        srcDocType  = sdtExpr.search(source).group(1)
        srcField    = fldExpr.search(source).group(1)
        linkType.linkSources.append((srcDocType, srcField))
    for prop in props:
        propName    = prnExpr.search(prop).group(1)
        propVal     = prvExpr.search(prop)
        propComment = prcExpr.search(prop)
        propVal     = propVal and propVal.group(1) or None
        propComment = propComment and propComment.group(1) or None
        linkType.linkProps.append((propName, propVal, propComment))
    return linkType

#----------------------------------------------------------------------
# Stores information for a CDR link type.
#----------------------------------------------------------------------
def putLinkType(credentials, name, linkType, linkAct,
                host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    if linkAct == "modlink":
        cmd = "<CdrModLinkType><Name>%s</Name>" % name
        if linkType.name and name != linkType.name:
            cmd += "<NewName>%s</NewName>" % linkType.name
    else:
        cmd = "<CdrAddLinkType><Name>%s</Name>" % linkType.name

    # Add the target document version type to check against
    if not linkType.linkChkType:
        raise Exception("No linkChkType specified for link type %s:" %
                        linkType.name);
    cmd += "<LinkChkType>%s</LinkChkType>" % linkType.linkChkType

    # Add the comment, if present.
    if linkType.comment is not None:
        cmd += "<Comment>%s</Comment>" % linkType.comment

    # Add the link sources.
    for src in linkType.linkSources:
        cmd += "<LinkSource><SrcDocType>%s</SrcDocType>" % src[0]
        cmd += "<SrcField>%s</SrcField></LinkSource>" % src[1]

    # Add the link targets.
    for tgt in linkType.linkTargets:
        cmd += "<TargetDocType>%s</TargetDocType>" % tgt

    # Add the link properties.
    for prop in linkType.linkProps:
        cmd += "<LinkProperties><LinkProperty>%s</LinkProperty>" % prop[0]
        cmd += "<PropertyValue>%s</PropertyValue>" % prop[1]
        cmd += "<Comment>%s</Comment></LinkProperties>" % prop[2]

    # Submit the request.
    if linkAct == "modlink":
        cmd += "</CdrModLinkType>"
    else:
        cmd += "</CdrAddLinkType>"
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)
    err = checkErr(resp)
    if err: return err

    # No errors to report if we get here.
    return None

#----------------------------------------------------------------------
# Retrieves list of CDR link properties.
#----------------------------------------------------------------------
def getLinkProps(credentials, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListLinkProps/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)
    err = checkErr(resp)
    if err: return err

    # Parse the response
    propExpr = re.compile("<LinkProperty>(.*?)</LinkProperty>", re.DOTALL)
    nameExpr = re.compile("<Name>(.*)</Name>", re.DOTALL)
    cmtExpr  = re.compile("<Comment>(.*)</Comment>", re.DOTALL)
    ret      = []
    props    = propExpr.findall(resp)
    if props:
        for prop in props:
            name = nameExpr.findall(prop)[0]
            cmt  = cmtExpr.findall(prop)
            pr   = LinkProp(name)
            if cmt: pr.comment = cmt[0]
            ret.append(pr)
    return ret

#----------------------------------------------------------------------
# Returns a list of available query term rules.
#----------------------------------------------------------------------
def listQueryTermRules(session, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListQueryTermRules/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, session, host), host, port)

    # Check for problems.
    err = checkErr(resp)
    if err: return err

    # Extract the rules.
    return re.findall("<Rule>(.*?)</Rule>", resp)

#----------------------------------------------------------------------
# Returns a list of CDR query term definitions.
#----------------------------------------------------------------------
def listQueryTermDefs(session, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListQueryTermDefs/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, session, host), host, port)

    # Extract the definitions.
    defExpr      = re.compile("<Definition>(.*?)</Definition>", re.DOTALL)
    pathExpr     = re.compile("<Path>(.*)</Path>")
    ruleExpr     = re.compile("<Rule>(.*)</Rule>")
    err          = checkErr(resp)
    if err:
        return err
    definitions  = defExpr.findall(resp)
    rc           = []
    if definitions:
        for definition in definitions:
            path = pathExpr.search(definition).group(1)
            rule = ruleExpr.search(definition)
            rule = rule and rule.group(1) or None
            rc.append((path, rule))
    return rc

#----------------------------------------------------------------------
# Adds a new query term definition.
#----------------------------------------------------------------------
def addQueryTermDef(session, path, rule=None,
                    host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = "<CdrAddQueryTermDef><Path>%s</Path>" % path
    if rule: cmd += "<Rule>%s</Rule>" % rule
    cmd += "</CdrAddQueryTermDef>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, session, host), host, port)
    return checkErr(resp)

#----------------------------------------------------------------------
# Deletes an existing query term definition.
#----------------------------------------------------------------------
def delQueryTermDef(session, path, rule=None, host=DEFAULT_HOST,
                    port=DEFAULT_PORT):

    # Create the command
    cmd = "<CdrDelQueryTermDef><Path>%s</Path>" % path
    if rule: cmd += "<Rule>%s</Rule>" % rule
    cmd += "</CdrDelQueryTermDef>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, session, host), host, port)
    return checkErr(resp)

#----------------------------------------------------------------------
# Construct a string containing the description of the last exception.
#----------------------------------------------------------------------
def exceptionInfo():
    (eType, eValue) = sys.exc_info()[:2]
    if eType:
        eMsg = str(eType)
        if eValue:
            eMsg += (": %s" % str(eValue))
    else:
        eMsg = str(eValue) or "unable to find exception information"
    return eMsg

#----------------------------------------------------------------------
# Gets the email addresses for members of a group. Use cdrdb2 so
# we can work in a multi-threaded environment.
#----------------------------------------------------------------------
def getEmailList(groupName, tier=None):
    query = cdrdb2.Query("usr u", "u.email")
    query.join("grp_usr gu", "gu.usr = u.id")
    query.join("grp g", "g.id = gu.grp")
    query.where(query.Condition("g.name", groupName))
    return [row[0] for row in query.execute(tier=tier).fetchall()]

#----------------------------------------------------------------------
# Object for a mime attachment.
#----------------------------------------------------------------------
class EmailAttachment:
    def __init__(self, bytes=None, filepath=None, content_type=None):
        """
        Construct an email attachment object

        Pass:

            bytes         - optional sequence of bytes for the attachment
            filepath      - optional path where bytes for attachment can
                            be found; required if bytes is not specified;
                            can be specified even if bytes is also specified,
                            so that the Content-disposition header can use it
            content_type  - option slash-delimited mime type/subtype; e.g.:
                            text/rtf
        """
        if bytes is None and filepath is None:
            raise Exception("EmailAttachment: must specify bytes or filepath")
        if content_type is None:
            if filepath:
                import mimetypes
                content_type, encoding = mimetypes.guess_type(filepath)
                if content_type is None or encoding is not None:
                    content_type = "application/octet-stream"
        self.maintype, self.subtype = content_type.split("/")
        if bytes is None:
            mode = self.maintype == "text" and "r" or "rb"
            fp = open(filepath, mode)
            bytes = fp.read()
            fp.close()
        self.bytes = bytes
        self.filepath = filepath
        if self.maintype == "text":
            from email.mime.text import MIMEText
            self.mime_object = MIMEText(self.bytes, _subtype=self.subtype)
        elif self.maintype == "audio":
            from email.mime.audio import MIMEAudio
            self.mime_object = MIMEAudio(self.bytes, _subtype=self.subtype)
        elif self.maintype == "image":
            from email.mime.image import MIMEImage
            self.mime_object = MIMEImage(self.bytes, _subtype=self.subtype)
        else:
            from email.mime.base import MIMEBase
            from email import encoders
            self.mime_object = MIMEBase(self.maintype, self.subtype)
            self.mime_object.set_payload(self.bytes)
            encoders.encode_base64(self.mime_object)
        if filepath is not None:
            self.mime_object.add_header("Content-disposition", "attachment",
                                        filename=os.path.basename(filepath))

#----------------------------------------------------------------------
# Eventually we'll use this to send all email messages; for right
# now I'm using this for a selected set of applications to test
# it.
#----------------------------------------------------------------------
def sendMailMime(sender, recips, subject, body, bodyType='plain',
                 attachments=None):
    """
    Send an email message via SMTP to a list of recipients.  This
    version supports Unicode in the subject and body.  The message
    will be encoded with the most widely-supported encoding which
    results in no loss of information.  The encoding for the
    subject and body are determined separately.

    Pass:

        sender     - email address of sender; must contain only ASCII
                     characters
        recips     - sequence of recipient addresses; must contain only
                     ASCII characters
        subject    - string for subject header; must be a unicode object
                     or contain only ASCII characters or be UTF-8 encoded
        body       - payload for the message; must be a unicode object
                     or contain only ASCII characters or be UTF-8 encoded
        bodyType   - subtype for MIMEText object (e.g., 'plain', 'html')
        attachments - optional sequence of EmailAttachment objects
    """
    if not recips:
        raise Exception("sendMail: no recipients specified")
    if type(recips) not in (tuple, list):
        raise Exception("sendMail: recipients must be a sequence of "
                        "email addresses")
    recips = [recip.encode('US-ASCII') for recip in recips]
    sender = sender.encode('US-ASCII')

    from email.MIMEText import MIMEText
    from email.Header   import Header as EmailHeader

    # The Header class will try US-ASCII first, then the charset we
    # specify, then fall back to UTF-8.
    if type(subject) != unicode:
        subject = unicode(subject, 'utf-8')
    subject = EmailHeader(subject, 'ISO-8859-1')

    # The charset for the body must be set explicitly.
    if type(body) != unicode:
        body = unicode(body, 'utf-8')
    encodedBody = None
    for charset in ('US-ASCII', 'ISO-8859-1', 'UTF-8'):
        try:
            encodedBody = body.encode(charset)
        except UnicodeError:
            pass
        else:
            break
    if encodedBody is None:
        raise Exception("sendMailMime: failure determining body charset")

    # Create the message object.
    message = MIMEText(encodedBody, bodyType, charset)

    # Add attachments if present.
    if attachments:
        from email.mime.multipart import MIMEMultipart
        wrapper = MIMEMultipart()
        wrapper.preamble = "This is a multipart MIME message"
        wrapper.attach(message)
        for attachment in attachments:
            wrapper.attach(attachment.mime_object)
        message = wrapper

    # Plug in the headers.
    message['From']    = sender
    message['To']      = ",\n  ".join(recips)
    message['Subject'] = subject

    # Send it
    try:
        server = smtplib.SMTP(SMTP_RELAY)
        server.sendmail(sender, recips, message.as_string())
        server.quit()

    except Exception, e:

        # Log the error before re-throwing an exception.
        msg = "sendMail failure: %s" % e
        logwrite(msg, tback = True)
        raise Exception(msg)

#----------------------------------------------------------------------
# Send email to a list of recipients.
#----------------------------------------------------------------------
def sendMail(sender, recips, subject="", body="", html=False, mime=False,
             attachments=None):
    if mime or attachments:
        return sendMailMime(sender, recips, subject, body,
                            attachments=attachments)
    if not recips:
        raise Exception("sendMail: no recipients specified")
    if type(recips) not in (tuple, list):
        raise Exception("sendMail: recipients must be a sequence of "
                        "email addresses")
    recipList = recips[0]
    for recip in recips[1:]:
        recipList += (",\n  %s" % recip)
    try:
        # Headers
        message = """\
From: %s
To: %s
Subject: %s
""" % (sender, recipList, subject)

        # Set content type for html
        if html:
            message += "Content-type: text/html; charset=iso-8859-1\n"

        # Separator line + body
        message += "\n%s" % body

        # Send it
        server = smtplib.SMTP(SMTP_RELAY)
        server.sendmail(sender, recips, message)
        server.quit()
    except:
        # Log the error and return it to caller
        msg = "sendMail failure: %s" % exceptionInfo()
        logwrite(msg)
        return msg

#----------------------------------------------------------------------
# Check in a CDR document.
#----------------------------------------------------------------------
def unlock(credentials, docId, abandon='Y', force='Y', reason='',
           host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Normalize doc id
    docId = exNormalize(docId)[0]

    # Create the command.
    attrs   = "Abandon='%s' ForceCheckIn='%s'" % (abandon, force)
    docId   = "<DocumentId>%s</DocumentId>" % docId
    reason  = reason and ("<Comment>%s</Comment>" % reason) or ''
    cmd     = "<CdrCheckIn %s>%s%s</CdrCheckIn>" % (attrs, docId, reason)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)

    # Find any error messages.
    err = checkErr(resp)
    if err: return err
    return ""

#----------------------------------------------------------------------
# Get the most recent versions for a document.
#----------------------------------------------------------------------
def listVersions(credentials, docId, nVersions=-1,
                 host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command.
    cmd = "<CdrListVersions><DocId>%s</DocId>" \
          "<NumVersions>%d</NumVersions></CdrListVersions>" % (
          normalize(docId), nVersions)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)

    # Check for failure.
    if resp.find("<Errors") != -1:
        raise Exception(extract(r"(<Errors[\s>].*</Errors>)", resp))

    # Extract the versions.
    versions    = []
    versionExpr = re.compile("<Version>(.*?)</Version>", re.DOTALL)
    numExpr     = re.compile("<Num>(.*)</Num>")
    commentExpr = re.compile("<Comment>(.*)</Comment>", re.DOTALL)
    verList     = versionExpr.findall(resp)
    if verList:
        for ver in verList:
            numMatch     = numExpr.search(ver)
            commentMatch = commentExpr.search(ver)
            if not numMatch:
                raise Exception("listVersions: missing Num element")
            num = int(numMatch.group(1))
            comment = commentMatch and commentMatch.group(1) or None
            versions.append((num, comment))
    return versions

#----------------------------------------------------------------------
# Reindex the specified document.
#----------------------------------------------------------------------
def reindex(credentials, docId, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command.
    docId = normalize(docId)
    cmd = "<CdrReindexDoc><DocId>%s</DocId></CdrReindexDoc>" % docId

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)

    # Check for errors.
    if resp.find("<Errors") != -1:
        return extract(r"(<Errors[\s>].*</Errors>)", resp)
    return None

#----------------------------------------------------------------------
# Create a new publishing job.
#----------------------------------------------------------------------
def publish(credentials, pubSystem, pubSubset, parms=None, docList=None,
           email='', noOutput='N', allowNonPub='N', docTime=None,
           host=DEFAULT_HOST, port=DEFAULT_PORT, allowInActive='N'):

    # Create the command.
    pubSystem   = pubSystem and ("<PubSystem>%s</PubSystem>" % pubSystem) or ""
    pubSubset   = pubSubset and ("<PubSubset>%s</PubSubset>" % pubSubset) or ""
    email       = email and "<Email>%s</Email>" % email or ""
    noOutput    = noOutput and "<NoOutput>%s</NoOutput>" % noOutput
    allowNonPub = (allowNonPub == 'N') and 'N' or 'Y'
    allowNonPub = "<AllowNonPub>%s</AllowNonPub>" % allowNonPub
    allowInAct  = (allowInActive == 'N') and 'N' or 'Y'
    allowInAct  = "<AllowInActive>%s</AllowInActive>" % allowInAct
    parmElem    = ''
    docsElem    = ''
    if parms:
        parmElem = "<Parms>"
        for parm in parms:
            parmElem += "<Parm><Name>%s</Name><Value>%s</Value></Parm>" % (
                        parm[0], parm[1])
        parmElem += "</Parms>"
    if docList:
        expr = re.compile(r"CDR(\d+)(/(\d+))?")
        docsElem += "<DocList>"
        if docTime: docsElem += "<DocTime>%s</DocTime>" % docTime
        for doc in docList:
            match = expr.search(doc)
            if not match:
                return (None, "<Errors><Err>Malformed docList member '%s'"\
                              "</Err></Errors>" % cgi.escape(doc))
            docId = normalize(match.group(1))
            version = match.group(3) or "0"
            docsElem += "<Doc Id='%s' Version='%s'/>" % (docId, version)
        docsElem += "</DocList>"

    cmd = "<CdrPublish>%s%s%s%s%s%s%s%s</CdrPublish>" % (pubSystem,
                                                       pubSubset,
                                                       parmElem,
                                                       docsElem,
                                                       email,
                                                       noOutput,
                                                       allowNonPub,
                                                       allowInAct)

    # Log what we're doing to the publishing log
    logwrite('cdr.publish: Sending cmd to CdrServer: \n"%s"\n' % cmd,
                 PUBLOG)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)

    # And log response
    logwrite('cdr.publish: received response:\n"%s"\n' % resp, PUBLOG)

    # Return the job ID and any warnings/errors.
    jobId  = None
    errors = None
    if resp.find("<JobId") != -1:
        jobId  = extract(r"<JobId>([^<]*)</JobId>", resp)
    if resp.find("<Errors") != -1:
        errors = extract(r"(<Errors[\s>].*</Errors>)", resp)
    return (jobId, errors)

class PubStatus:
    def __init__(self, id, pubSystem, pubSubset, parms, userName, outputDir,
                 started, completed, status, messages, email, docList,
                 errors):
        self.id        = id
        self.pubSystem = pubSystem
        self.pubSubset = pubSubset
        self.parms     = parms
        self.userName  = userName
        self.outputDir = outputDir
        self.started   = started
        self.completed = completed
        self.status    = status
        self.messages  = messages
        self.email     = email
        self.docList   = docList
        self.errors    = errors

def pubStatus(self, jobId, getDocInfo=False):
    return "XXX this is a stub"

#----------------------------------------------------------------------
# Turn cacheing on or off in the CdrServer
#----------------------------------------------------------------------
def cacheInit(credentials, cacheOn, cacheType,
              host=DEFAULT_HOST, port=DEFAULT_PORT):
    """
    Submit a transaction to the server to turn cacheing on or off.
    At this time, cacheing is only of interest for publishing jobs,
    and the only type of cacheing we do is term denormalization
    cacheing - so that a given Term document id used in a protocol
    need only be looked up once, its document XML only parsed once,
    and the XML string for the denormalization need only be constructed
    once.  However the interface supports other types of cacheing if
    and when we create them.

    Pass:
        credentials - As usual.
        cacheOn     - true  = Turn cacheing on.
                      false = Turn it off.
        cacheType   - Currently known types are all synonyms of each other,
                      one of:
                        "term"
                        "pub"
                        "all"
        host / port - As usual.

    Return:
        Void.

    Raise:
        Standard error if error returned by host.  Possible errors
        are connection oriented, or invalid parameters.
    """
    # Attribute tells the server what to do
    cmdAttr = "off"
    if cacheOn:
        cmdAttr = "on"

    # Construct XML transaction
    cmd = "<CdrCacheing " + cmdAttr + "='" + cacheType + "'/>"

    # Send it
    resp = sendCommands(wrapCommand(cmd, credentials, host), host, port)

    # If error occurred, raise exception
    err = checkErr(resp)
    if err:
        raise Exception(err)

    return None

#----------------------------------------------------------------------
# Write messages to a logfile.
#----------------------------------------------------------------------
def logwrite(msgs, logfile=DEFAULT_LOGFILE, tback=False, stackTrace=False):
    """
    Append one or messages to a log file - closing the file when done.
    Can also record traceback information.

    Pass:
        msgs    - Single string or sequence of strings to write.
                   Should not contain binary data.
        logfile - Optional log file path, else uses default.
        tback   - True = log the latest traceback object.
                   False = do not.
                   See stack trace notes.
        stack   - True = log a stack trace even if there is no traceback
                   object.  Useful for logging the stack trace even though
                   no exception occurred.
                   See stack trace notes.

    Stack trace notes:
        For unconditional logging of a stack trace, use stackTrace=True,
        not tback=True.  tback will _only_ print a stack trace if there was
        an exception.

    Return:
        Void.  Does nothing at all if it can't open the logfile or
          append to it.
    """
    f = None
    try:
        f = open (logfile, "a", 0)

        # Write process id and timestamp
        f.write ("!%d %s: " % (os.getpid(), time.ctime()))

        # Sequence of messages or single message
        if type(msgs) == type(()) or type(msgs) == type([]):
            for msg in msgs:
                if (type(msg)) == type(u""):
                    msg = msg.encode ('utf-8')
                f.write (msg)
                f.write ("\n")
        else:
            if (type(msgs)) == type(u""):
                msgs = msgs.encode ('utf-8')
            f.write (msgs)
            f.write ("\n")

        # If traceback of the last exception is requested
        if tback:
            try:
                traceback.print_exc (999, f)
            except:
                pass

        # If an unconditional stack trace (no exception required) is requested
        if stackTrace:
            try:
                traceback.print_stack(file=f)
            except:
                pass

    except:
        pass

    # Close file if opened.  This ensures that caller will see his
    #   logged messages even if his program crashes
    if f:
        try:
            f.close()
        except:
            pass


#----------------------------------------------------------------------
# Manage a logfile.  Improved functionality compared to cdr.logwrite()
#----------------------------------------------------------------------
class Log:
    """
    Provides efficient logging to any file desired.

    Instantiate one of these to create, or append to an existing,
    logfile.
    """

    _DEFAULT_BANNER = "=========== Opening Log ==========="

    def __init__(self, filename,
                 dirname=DEFAULT_LOGDIR, banner=_DEFAULT_BANNER,
                 logTime=True, logPID=True, level=DEFAULT_LOGLVL,
                 logTier=False):
        """
        Creates log object.

        Pass:
            filename - All logging goes here.
            dirname  - Directory for log file.
            banner   - If present, write it to signify opening
                       the log.
            logTime  - Prepend date/time to each entry.
            logPID   - Prepend process ID.
            level    - Log any message at this level or lower.
                       (Possibly override with environment
                       variable or by calling function to change
                       level.)
            logTier  - Prepend tier ID (DEV, QA, etc.) to each log msg.
                       Tier will always be under the banner if there
                       is one.

        Raises:
            IOError if log cannot be opened.
        """

        # Defaults for banner
        self.__logTime  = True
        self.__logPID   = True
        self.__level    = level

        # Can get the PID once and save it, formatted
        self.__pid = "!%d: " % os.getpid()

        # Save parms
        self.__banner  = banner
        self.__logTime = logTime
        self.__logPID  = logPID
        self.__level   = level
        self.__fp      = None

        # Find the tier once and format it
        if logTier:
            self.__logTier = cdrutil.getTier(WORK_DRIVE + ":") + ':'
        else:
            self.__logTier = False

        # Open for append, unbuffered
        self.__filename = dirname + '/' + filename
        self.__fp = open(self.__filename, "a", 0)

        # If there's a banner, write it with stamps
        if banner:
            self.writeRaw("\n%s\nTIER: %s  DATETIME: %s\n" %
                          (banner, cdrutil.getTier(WORK_DRIVE + ":"),
                           time.ctime()))

    def write(self, msgs, level=DEFAULT_LOGLVL, tback=False,
              stdout=False, stderr=False):
        """
        Writes msg(s) to log file.
        Flushes after each write but does not close the file.

        Pass:
            msgs   - If type=string, write single message with
                     newline.
                   - If type=sequence, write each sequence in
                     string with newline (assuming raw = False).
            level  - See __init__().
            tback  - Write latest traceback object.
                     Use this when writing from an exception
                     handler if desired.
            stdout - True=Also write to stdout.
            stderr - True=Also write to stderr.
        """
        # No write if level too high
        if level > self.__level:
            return

        # Write process id and timestamp
        if self.__logTier:
            self.__fp.write(self.__logTier)
        if self.__logPID:
            self.__fp.write(self.__pid)
        if self.__logTime:
            self.__fp.write("%s: " % time.ctime())

        # Sequence of messages or single message
        if type(msgs) == type(()) or type(msgs) == type([]):
            for msg in msgs:
                if (type(msg)) == type(u""):
                    msg = msg.encode ('utf-8')
                self.__fp.write(msg)
                self.__fp.write("\n")
                if stdout:
                    print(msg)
                if stderr:
                    sys.stderr.write(msg + "\n")
        else:
            if (type(msgs)) == type(u""):
                msgs = msgs.encode('utf-8')
            self.__fp.write(msgs)
            self.__fp.write("\n")
            if stdout:
                print(msgs)
            if stderr:
                sys.stderr.write(msgs + "\n")

        # If traceback is requested, include the last one
        if tback:
            try:
                self.writeRaw("Traceback follows:\n")
                traceback.print_exc(999, self.__fp)
            except:
                pass

    def writeRaw(self, msg, level=DEFAULT_LOGLVL):
        """
        No processing of any kind.  But we do respect level.

        Caller can use this to dump data as he sees fit, but must
        take care about encoding and other issues.
        """
        # No write if level too high
        if level > self.__level:
            return

        self.__fp.write(msg)

    def __del__(self):
        """
        Final close of the log file.

        May write a closing banner - this tells when the program
        exited, or caller explicitly called del(log_object).
        """
        # If there's a banner, put one at the end
        if self.__banner:
            # Insure PID: date time on closing banner
            self.__logTime  = True
            self.__logPID   = True
            self.__level    = DEFAULT_LOGLVL
            if isinstance(self.__fp, file):
                self.writeRaw("\n%s\n" % time.ctime())
                self.writeRaw("=========== Closing Log ===========\n\n")

        # Can only close the file if we were able to open it earlier.
        # Permission problems exist and file pointer is None.
        # -----------------------------------------------------------
        if isinstance(self.__fp, file):
            self.__fp.close()

#----------------------------------------------------------------------
# Create an HTML table from a passed data
#----------------------------------------------------------------------
def tabularize (rows, tblAttrs=None):
    """
    Create an HTML table string from passed data.
    This looks like it should be in cdrcgi, but we also produce
    HTML email in batch programs - which aren't searching the
    cgi path.

    Pass:
        rows = Sequence of rows for the table, each containing
               a sequence of columns.
               If the number of columns is not the same in each row,
               then the caller gets whatever he gets, so it may be
               wise to add columns with content like "&nbsp;" if needed.
               No entity conversions are performed.

        tblAttrs = Optional string of attributes to put in table, e.g.,
               "align='center' border='1' width=95%'"

        We might add rowAttrs and colAttrs if this is worthwhile.
    Return:
        HTML as a string.
    """
    if not tblAttrs:
        html = "<table>\n"
    else:
        html = "<table " + tblAttrs + ">\n"

    for row in rows:
        html += " <tr>\n"
        for col in row:
            html += "  <td>%s</td>\n" % col
        html += " </tr>\n"
    html += "</table>"

    return html

#----------------------------------------------------------------------
# Log out from the CDR.
#----------------------------------------------------------------------
def logout(session, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = "<CdrLogoff/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, session, host), host, port)
    err = checkErr(resp)
    if err: return err

    # No errors to report.
    return None

#----------------------------------------------------------------------
# Object for results of an external command.
#----------------------------------------------------------------------
class CommandResult:
    def __init__(self, code, output, error=None):
        self.code   = code
        self.output = output
        self.error  = error

#----------------------------------------------------------------------
# Run an external command.
#----------------------------------------------------------------------
#def runCommand(command):
#    commandStream = os.popen('%s 2>&1' % command)
#    output = commandStream.read()
#    code = commandStream.close()
#    return CommandResult(code, output)

#----------------------------------------------------------------------
# Run an external command.
# The os.popen is depricated and replaced by the subprocess method.
# Note:  The return code is now 0 for a process without error and the
#        stdout and stderr output is split into output and error.
#----------------------------------------------------------------------
def runCommand(command, joinErr2Out=True, returnNoneOnSuccess=True):
    """
    Run a shell command

    The deprecated function os.popen() has been replaced with the
    subprocess method.  This method allows the stdout and stderr to
    be piped independently.

    Pass:
        command     - The string of the shell command to be run
        joinErr2Out - optional value, default TRUE
                      This parameter, when true (for downward compatibility)
                      pipes both, stdout and stderr to stdout.
                      Otherwise stdout and stderr are split.
        returnNoneOnSuccess
                    - optional value, default TRUE
                      This parameter, when true (for downward compatibility)
                      returns 'None' as a successful returncode.
                      Otherwise the returncode for a successful command is 0.
                      Note:  This is the return code of the command
                             submitted and not the return code of runCommand()
                             itself!
    Return:
        CommandResult object
            output  - output from stdout (or stdout and stderr if joinErr2Out
                      is True)
            error   - output from stderr (or a warning message if joinErr2Out
                      is True)
            code    - 0 or None (if osPopen is True) for successful completion
    """
    # Default mode - Pipe stdout and stderr to stdout
    # -----------------------------------------------
    if joinErr2Out:
        try:
            commandStream = subprocess.Popen(command, shell=True,
                                             stdin =subprocess.PIPE,
                                             stdout=subprocess.PIPE,
                                             stderr=subprocess.STDOUT)
            output, error = commandStream.communicate()
            code          = commandStream.returncode
            error         = '*** Warning: stderr piped to stdout'

            # For downward compatibility we return None for a successful
            # command return code
            # ----------------------------------------------------------
            if returnNoneOnSuccess and code == 0:
                return CommandResult(None, output)
        except Exception, info:
            logwrite("failure running command: %s\n%s" % (command, str(info)))
    else:
        try:
            commandStream = subprocess.Popen(command, shell=True,
                                             stdin =subprocess.PIPE,
                                             stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE)
            output, error = commandStream.communicate()
            code = commandStream.returncode

            # For downward compatibility we return None for a successful
            # command return code
            # ----------------------------------------------------------
            if returnNoneOnSuccess and code == 0:
                return CommandResult(None, output, error)
        except Exception, info:
            logwrite("failure running command: %s\n%s" % (command, str(info)))

    return CommandResult(code, output, error)


#----------------------------------------------------------------------
# Create a temporary working area.
#----------------------------------------------------------------------
def makeTempDir(basename="tmp", chdir=True):
    """
    Create a temporary directory.

    Uses deprecated mktemp function which is subject to race
    conditions, but it's not clear what would be better within
    the limits of the Windows tmpnam fuction used by Python for
    tmpnam().

    Pass:
        basename - suffix to put after name.
        chdir    - True = change to the new directory.

    Return:
        Absolute path to the directory.

    Caller must delete this directory himself if he wants it deleted
    when he's done.
    """
    if os.environ.has_key("TMP"):
        tempfile.tempdir = os.environ["TMP"]
    where = tempfile.mktemp(basename)
    abspath = os.path.abspath(where)
    try:
        os.mkdir(abspath)
    except:
        raise Exception("makeTempDir", "Cannot create directory %s" % abspath)
    if chdir:
        try:
            os.chdir(abspath)
        except:
            raise Exception("makeTempDir", "Cannot chdir to %s" % abspath)
    return abspath

#----------------------------------------------------------------------
# Object representing a CdrResponseNode.
#----------------------------------------------------------------------
class CdrResponseNode:
    def __init__(self, node, when):
        self.when            = when
        self.responseWrapper = node
        self.specificElement = None
        for child in node.childNodes:
            if child.nodeType == child.ELEMENT_NODE:
                if self.specificElement:
                    raise Exception("CdrResponseNode: too many children "
                                        "of CdrResponse element")
                self.specificElement = child
        if not self.specificElement:
            raise Exception("No element children found for CdrResponse")
        self.elapsed         = self.specificElement.getAttribute('Elapsed')

#----------------------------------------------------------------------
# Raise an exception using the text content of Err elements.
#----------------------------------------------------------------------
def wrapException(caller, errElems):
    args = [caller]
    for elem in errElems:
        args.append(getTextContent(elem))
    exception = Exception()
    exception.args = tuple(args)
    raise exception

#----------------------------------------------------------------------
# Extract main CdrResponse node from a response document.  This
# function will be called in one of two situations:
#  (a) a CDR session has already been established, and only
#      one CdrResponse element will be present; or
#  (b) the caller was given a login ID and password, in which case
#      there will be three CdrResponse elements present: one for
#      the CdrLogon command; one for the command submitted by the
#      original caller; and one for the CdrLogoff command.
# While we're at it, we check to make sure that the status of the
# command was success.
#----------------------------------------------------------------------
def extractResponseNode(caller, responseString):
    docElem = xml.dom.minidom.parseString(responseString).documentElement
    when = docElem.getAttribute('Time')
    cdrResponseElems = docElem.getElementsByTagName('CdrResponse')
    if not cdrResponseElems:
        errElems = docElem.getElementsByTagName('Err')
        if errElems:
            wrapException(caller, errElems)
        else:
            raise Exception(caller, 'No CdrResponse elements found')
    if len(cdrResponseElems) == 1:
        responseElem = cdrResponseElems[0]
    elif len(cdrResponseElems) == 3:
        responseElem = cdrResponseElems[2]
        raise Exception(caller, 'Found %d CdrResponse elements; expected '
                                'one or three' % len(cdrResponseElems))
    if responseElem.getAttribute('Status') == 'success':
        return CdrResponseNode(responseElem, when)
    errElems = cdrResponseElems[0].getElementsByTagName('Err')
    if not errElems:
        raise Exception(caller, 'call failed but Err elements missing')
    wrapException(caller, errElems)

    # wrapException does not return, but add a return to silence pychecker
    return None

#----------------------------------------------------------------------
# Mark tracking documents generated by failed mailer jobs as deleted.
#----------------------------------------------------------------------
def mailerCleanup(session, host=DEFAULT_HOST, port=DEFAULT_PORT):
    resp = sendCommands(wrapCommand("<CdrMailerCleanup/>", session, host),
                        host, port)
    dom = xml.dom.minidom.parseString(resp)
    docs = []
    errs = []
    for elem in dom.getElementsByTagName('DeletedDoc'):
        digits = re.sub(r'[^\d]', '', getTextContent(elem))
        docs.append(int(digits))
    for elem in dom.getElementsByTagName('Err'):
        errs.append(getTextContent(elem))
    return (docs, errs, resp)

#----------------------------------------------------------------------
# Used by normalizeDoc; treats string as writable file object.
#----------------------------------------------------------------------
class StringSink:
    def __init__(self, s = ""):
        self.__pieces = s and [s] or []
    def __repr__(self):
        return "".join(self.__pieces)
    def write(self, s):
        self.__pieces.append(s)
    def __getattr__(self, name):
        if name == 's':
            return "".join(self.__pieces)
        raise AttributeError

#----------------------------------------------------------------------
# Remove all lines from a multi-line string (e.g., an XML doc)
# that are empty or contain nothing but whitespace.
#----------------------------------------------------------------------
def stripBlankLines(s):
    # Make a sequence
    inSeq = s.split("\n")

    # Copy non blank lines to new sequence
    outSeq = []
    for line in inSeq:
        if len(string.lstrip(line)):
            outSeq.append(line)

    # Return them as a string with newlines at each line end
    return "\n".join(outSeq);

#----------------------------------------------------------------------
# Takes a utf-8 string for an XML document and creates a utf-8 string
# suitable for comparing two versions of XML documents by normalizing
# non-essential differences away.  Used by compareDocs() (below).
#----------------------------------------------------------------------
def normalizeDoc(utf8DocString):
    sFile = StringSink()
    dom = xml.dom.minidom.parseString(utf8DocString)
    dom.writexml(sFile)
    return sFile.__repr__()

#----------------------------------------------------------------------
# Extract the first CDATA section from a document.
# Simple version, only gets first CDATA, but we never use
#  more than one.
# If no CDATA, then returns None.
#----------------------------------------------------------------------
def getCDATA(utf8string):
    pat = re.compile(r"<!\[CDATA\[(.*?)]]>", re.DOTALL)
    data = pat.search(utf8string)
    if data:
        return data.group(1)
    return None

#----------------------------------------------------------------------
# Compares two XML documents by normalizing each.  Returns non-zero
# if documents are different; otherwise zero.  Expects each document
# to be passed as utf8-encoded documents.
#----------------------------------------------------------------------
def compareXmlDocs(utf8DocString1, utf8DocString2):
    if utf8DocString1 is utf8DocString2: return 0
    return cmp(normalizeDoc(utf8DocString1), normalizeDoc(utf8DocString2))

#----------------------------------------------------------------------
# Compare two XML documents by normalizing each.
# Returns the output of a textual differencer as a sequence of lines.
# See Python difflib.Differ.compare() for diff format.
#   Pass:
#     2 utf8 strings to compare
#     chgOnly  - True=only show changed lines, else show all.
#     useCDATA - True=call getCDATA on each string before compare.
#   Returns:
#     Difference, with or without context, as utf-8 string.
#     Context, if present, is pretty-printed with indentation.
#----------------------------------------------------------------------
def diffXmlDocs(utf8DocString1, utf8DocString2, chgOnly=True, useCDATA=False):
    # Extract data if needed
    if useCDATA:
        d1 = getCDATA(utf8DocString1)
        d2 = getCDATA(utf8DocString2)
    else:
        d1 = utf8DocString1
        d2 = utf8DocString2

    # Normalize
    doc1 = stripBlankLines(xml.dom.minidom.parseString(d1).toprettyxml("  "))
    doc2 = stripBlankLines(xml.dom.minidom.parseString(d2).toprettyxml("  "))

    # Compare
    diffObj = difflib.Differ()
    diffSeq = diffObj.compare(doc1.splitlines(1),doc2.splitlines(1))

    # If caller only wants changed lines, drop all lines with leading space
    if chgOnly:
        chgSeq = []
        for line in diffSeq:
            if line[0] != ' ':
                chgSeq.append (line)
        # Return them as a (possibly empty) string
        diffText = "".join(chgSeq)

    # Else return entire document as a string
    else:
        diffText = "".join(diffSeq)

    # Convert output back to utf-8.  toprettyxml made it unicode
    if type(diffText) == type(u''):
        diffText = diffText.encode('utf-8')

    return diffText

#----------------------------------------------------------------------
# Change the active_status column for a document.
#----------------------------------------------------------------------
def setDocStatus(credentials, docId, newStatus,
                 host=DEFAULT_HOST, port=DEFAULT_PORT, comment=None):
    docIdStr = u"<DocId>%s</DocId>" % normalize(docId)
    stat = u"<NewStatus>%s</NewStatus>" % newStatus
    cmt  = comment and (u"<Comment>%s</Comment>" % comment) or u""
    cmd  = u"<CdrSetDocStatus>%s%s%s</CdrSetDocStatus>" % (docIdStr, stat, cmt)
    resp = sendCommands(wrapCommand(cmd.encode('utf-8'), credentials, host),
                        host, port)
    errs = getErrors(resp, errorsExpected = False, asSequence = True)
    if errs:
        raise Exception(errs)

#----------------------------------------------------------------------
# Retrieve the active status for a document.
#----------------------------------------------------------------------
def getDocStatus(credentials, docId, tier=None):
    if tier:
        conn = cdrdb.connect('CdrGuest', tier)
    else:
        conn = cdrdb.connect('CdrGuest')
    cursor = conn.cursor()
    idTuple = exNormalize(docId)
    docId = idTuple[1]
    cursor.execute("SELECT active_status FROM all_docs WHERE id = ?", docId)
    rows = cursor.fetchall()
    if not rows:
        raise Exception(['Invalid document ID %s' % docId])
    return rows[0][0]

#----------------------------------------------------------------------
# Convenience wrapper for unblocking a document.
#----------------------------------------------------------------------
def unblockDoc(credentials, docId, host=DEFAULT_HOST, port=DEFAULT_PORT,
               comment=None):
    setDocStatus(credentials, docId, "A", host, port, comment)

#----------------------------------------------------------------------
# Determine the last date a versioned blob changed.
#----------------------------------------------------------------------
def getVersionedBlobChangeDate(credentials, docId, version, conn=None,
                               tier=None):
    if not conn:
        if tier:
            conn = cdrdb.connect("CdrGuest", tier)
        else:
            conn = cdrdb.connect("CdrGuest")
    cursor = conn.cursor()
    cursor.execute("""\
        SELECT blob_id
          FROM version_blob_usage
         WHERE doc_id = ?
           AND doc_version = ?""", (docId, version))
    rows = cursor.fetchall()
    if not rows:
        raise Exception(['no blob found for document %s version %s' %
                         (docId, version)])
    blobId = rows[0][0]
    cursor.execute("""\
        SELECT v.num, v.dt
          FROM doc_version v
          JOIN version_blob_usage u
            ON u.doc_id = v.id
           AND u.doc_version = v.num
         WHERE u.blob_id = ?
           AND u.doc_id = ?
           AND u.doc_version <= ?
      ORDER BY v.num DESC""", (blobId, docId, version))
    rows = cursor.fetchall()
    if not rows:
        raise Exception(['failure fetching rows for blob %s' % blobId])
    lastVersion, lastDate = rows[0]
    for prevVersion, prevDate in rows[1:]:
        if prevVersion != lastVersion - 1:
            break
        lastVersion, lastDate = prevVersion, prevDate
    return lastDate

#----------------------------------------------------------------------
# Returns the base URL for the current emailer CGI directory.
# Note: The CNAME for the GPMailer is only accessible from the
#       bastion host but not from the CDR Server (C-Mahler)
#----------------------------------------------------------------------
def emailerCgi(cname=True):
    if cname:
        return "https://%s.%s/cgi-bin" % h.host['EMAILERSC']
    else:
        return "https://%s.%s/cgi-bin" % h.host['EMAILERS']

#----------------------------------------------------------------------
# Standardize the email subject prefix used to
#    Host-Tier: SubjectLine
#----------------------------------------------------------------------
def emailSubject(text='No Subject'):
    return u"%s-%s: %s" % (h.org, h.tier, text)

#----------------------------------------------------------------------
# Create a file to use as an interprocess lockfile.
#----------------------------------------------------------------------
# Static dictionary of locked files
_lockedFiles = {}
def createLockFile(fname):
    """
    Create a named lock file to use in synchronizing processes.

    Tried msvcrt.locking() but couldn't get it to work reliably
    and the posix fnctl functions aren't available on Win32.  So
    this is a substitute in the spirit of Berkeley UNIX fcntl
    file locking functions.

    As in the UNIX world, everything is done by convention.
    Creating a lock file only locks something if another process
    also calls createLockFile() with the same filename.

    The caller may call removeLockFile when locking is done, but
    he can just ignore it and it will be called for him if he
    exits normally without doing it.

    An abnormal exit may not call deleteLockFile().  My tests of
    various exceptional exits worked fine.  The atexit was called.
    However the Python docs don't guarantee this for every
    possible case.

    Calls to createLockFile may NOT be nested for the same lockfile.
    Calling twice without an intervening call to removeLockFile, or
    a program exit, will raise an exception.

    Pass:
        fname - Path to lock file, should be unique to the particular
                  locking desired.
                The name of the file is arbitrary, but it must be a
                  legal file path and it must NOT be a file created for
                  other purposes.
                A good example of a lock file name might be something
                  like "/cdr/log/FileSweeper.lock".

    Return:
        True  = Success, lock file created.
        False = Lock file already exists.  If the process that created it
                is no longer running, it has to be removed manually.
    """
    # Nested calls are not allowed
    if _lockedFiles.has_key(fname):
        raise Exception('File "%s" locked twice without intervening unlock' %
                        fname)

    # If another process locked the file, caller loses
    if os.path.exists(fname):
        return False

    # Create the file, raises IOError if failed
    f = open(fname, "w")
    f.close()

    # Remember that we locked this file in this process
    _lockedFiles[fname] = True

    # Register a function to remove the file when the program exits
    atexit.register(removeAllLockFiles)

    return True

#----------------------------------------------------------------------
# Delete a lockfile.
#----------------------------------------------------------------------
def removeLockFile(fname):
    """
    Remove a file created by createLockFile.

    Need only be called if the caller wants to release the resource
    before ending his program.
    """
    # Only remove the file if we created it.
    # It is illegal to remove a lock created by another process
    if not _lockedFiles.has_key(fname):
        raise Exception('File "%s" not locked in this process' % fname)
    del(_lockedFiles[fname])

    # If we got here, this ought to work, propagate exception if it fails
    os.remove(fname)

#----------------------------------------------------------------------
# Remove any outstanding lockfiles for this process.
#----------------------------------------------------------------------
def removeAllLockFiles():
    """
    Remove any files that were created by createLockFile() for which
    removeLockFile() was not called.
    """
    for fname in _lockedFiles.keys():
        removeLockFile(fname)

#----------------------------------------------------------------------
# Wrapper for importing the most complete etree package available.
#----------------------------------------------------------------------
def importEtree():
    try:
        import lxml.etree as etree
    except:
        try:
            import xml.etree.cElementTree as etree
        except:
            import xml.etree.ElementTree as etree
    return etree

#----------------------------------------------------------------------
# Find a date a specified number of days in the future (or in the
# past, if a negative integer is passed).  We do this often enough
# that it's worth creating a function in this module.  Returns
# the value as an ISO-format date string, unless the optional
# second argument is False, in which case the 9-member tuple
# for the new date is returned.
#----------------------------------------------------------------------
def calculateDateByOffset(offset, referenceDate=None):
    """
    Find a date a specified number of days in the future (or in the
    past, if a negative offset is passed).  We do this often enough
    that it's worth creating a function in this module.  Returns
    a datetime.date object, which knows how to format itself as an
    ISO-formatted date string.  Example usage:

        deadline = str(cdr.calculateDateByOffset(30))

    Pass:

        offset        - integer representing the number of days
                        in the future (past for negative integers)
                        to calculate
        referenceDate - optional argument which can be passed
                        as the date from which to calculate the
                        returned date using the offset; this
                        can be a string or a datetime.date object;
                        defaults to the current date

    Returns:

        datetime.date object
    """

    if not referenceDate:
        referenceDate = datetime.date.today()
    elif type(referenceDate) in (str, unicode):
        y, m, d = referenceDate.split('-')
        referenceDate = datetime.date(int(y), int(m), int(d))
    elif not isinstance(referenceDate, datetime.date):
        raise Exception("invalid type for referenceDate")
    return referenceDate + datetime.timedelta(offset)


#----------------------------------------------------------------------
# Gets a list of all board names.
# This is frequently used to create reports by board.
#----------------------------------------------------------------------
def getBoardNames(boardType='all', display='full', tier=None):
    """
    Get the list of all board names (i.e. organizations with an
    organization type of 'PDQ Editorial Board' or 'PDQ Advisory Board').
    Usage:
         boardNames = cdr.getBoardNames()

    Pass:
         boardType:    optional string value of
                         editorial or advisory
                       will return just those board names.  Any other
                       entry will return a combined list.
         display:      optional string indicating the display format
                       of the board names
                         full   - display as is
                         short  - display with preceeding 'PDQ ' stripped
                         custom - same as short plus replace CAM
    Returns:
         Dictionary   {CdrI1:board1, CdrId2:board2, ...}
    """
    if boardType.lower() == 'editorial' or boardType.lower()== 'advisory':
        boardTypes = "('PDQ %s Board')" % boardType.lower()
    else:
        boardTypes = "('PDQ Editorial Board', 'PDQ Advisory Board')"

    # Get the board names and cdr-ids from the database
    # -------------------------------------------------
    if tier:
        conn = cdrdb.connect("CdrGuest", tier)
    else:
        conn = cdrdb.connect("CdrGuest")
    cursor = conn.cursor()
    cursor.execute("""\
     SELECT d.id,
            n.value  AS BoardName,
            t.value  AS BoardType
       FROM query_term n
       JOIN query_term t
         ON t.doc_id = n.doc_id
       JOIN document d
         ON n.doc_id = d.id
      WHERE t.value in %s
        AND t.path  = '/Organization/OrganizationType'
        AND n.path  = '/Organization/OrganizationNameInformation/' +
                       'OfficialName/Name'
        AND d.doc_type = 22
        AND active_status = 'A'
      ORDER BY t.value, n.value
""" % boardTypes)

    # Typically the full board name is rarely displayed
    # We're removing the PDQ prefix and/or modify the display of the CAM
    # ------------------------------------------------------------------
    if display == 'short':
        return dict((row[0], row[1].replace('PDQ ',''))
                            for row in cursor.fetchall())
    elif display == 'custom':
        return dict((row[0], row[1].replace('PDQ ', '').replace(
                            'Complementary and Alternative Medicine', 'CAM'))
                            for row in cursor.fetchall())

    return dict((row[0], row[1]) for row in cursor.fetchall())

#----------------------------------------------------------------------
# Gets a list of Summary languages.
#----------------------------------------------------------------------
def getSummaryLanguages():
    """
    Return a list of all languages that are used for Summaries.

    Implemented as a quick return of the constant tuple ('English','Spanish').

    This could be implemented as a database query but it adds a bit of time
    and the chances of it being more accurate are about equally good and bad
    since an additional language might be introduced experimentally but not
    intended to appear in all lists yet.

    If we add more languages, there's one place here to update for all
    scripts that use this function.
    """
    return ('English','Spanish')

#----------------------------------------------------------------------
# Gets a list of Audience values
#----------------------------------------------------------------------
def getSummaryAudiences():
    """
    Return a list of all Audience values that are used for Summaries.

    See getSummaryLanguages() for query vs. literals discussion.
    """
    return ('Health professionals', 'Patients')

#----------------------------------------------------------------------
# Gets a list of Summary CDR document IDs.
#----------------------------------------------------------------------
def getSummaryIds(language='all', audience='all', boards=[], status='A',
                  published=True, titles=False, sortby='title'):
    """
    Search the database for the Summary documents by the passed criteria.

    Pass:
        language - 'all' = Ignore language, get them all.
                    Else 'English' or 'Spanish' (or 'Klingon' if you want
                    no hits.)
        audience - 'all' = Ignore audience.
                    Else 'Health professionals' or 'Patients'.  Must exactly
                    match what's in:
                        '/Summary/SummaryMetaData/SummaryAudience'
        boards   - [] = Ignore board, get Summaries for any board.
                    Else a list of one or more board names or doc IDs.
                    If using names, use names like
                        'PDQ Adult Treatment Editorial Board'.
                    Must be an exact match for what's in:
                        'Summary/SummaryMetaData/PDQBoard/Board'
                    If using IDs, use integer docIds.
                    NOTE: I found a case where docIds included a Summary not
                          found by board name.  Name had not been denormalized
                          in the stored document.
        status   - 'A' = Active status for the document.  Else 'I' = Inactive
                    or 'all' = both.
        published- True = Only docs with publishable versions.
                   False = docs with or without publishable versions.
        titles   - True = return titles with the IDs.
        sortby   - 'id' = sort by CDR ID.
                   'title' = sort by title, can be done whether or not
                    titles are returned.
                    Can use any column or set of columns from the document
                    table if desired.
                    None = No sorting.

    Return:
        Array of arrays of document identifiers.
        If titles are not included, these are just CDR IDs of the form:
            [[docId,], [docId,], ...]
        Else there is an array of pair sequences of the form:
            [[docId, title], [docId, title], ...]
        Returned doc IDs are integers.
        If titles are returned, they are unicode strings.
        If there are no hits, an empty array [] is returned, presumably caused
            by an error in the passed parameters, e.g., invalid board names.

    Throws:
        cdrdb.Exception if database failure.
    """
    # Spanish Summaries have no board information.
    # If language is Spanish, we need to find the English equivalents,
    #  then get the Spanish with TranslationOf IDs.
    if language == 'Spanish' and boards:
        # Get IDs and flatten the results from [[id1],[id2]...] to [id1,id2...]
        listOfLists = getSummaryIds('English', audience, boards, status,
                                     published, titles=False, sortby=None)
        enSummaries = [item[0] for item in listOfLists]

        # Now find Spanish translations
        qtable  = published and "query_term_pub" or "query_term"
        columns = titles and ["d.id", "d.title"] or ["d.id"]
        spQry   = cdrdb.Query("document d", *columns)
        spQry.join("%s qlang" % qtable, "qlang.doc_id = d.id")
        spQry.where(spQry.Condition("qlang.path",
                                    "/Summary/TranslationOf/@cdr:ref"))
        spQry.where(spQry.Condition("qlang.int_val", enSummaries, "IN"))

        # It's possible for two English Summaries (e.g., one Active one not)
        #  to point to one Spanish translation.
        spQry.unique()

        # Apply same limits to Spanish as to English
        if status != "all":
            spQry.where(spQry.Condition("d.active_status" , status))
        if sortby:
            spQry.order(sortby)

        # Fetch and return
        cursor = spQry.execute()
        rows   = cursor.fetchall()
        cursor.close()
        return rows

    # Converted by Bob from my old code in previous version of cdr.py
    qtable = published and "query_term_pub" or "query_term"
    columns = titles and ["d.id", "d.title"] or ["d.id"]
    summary_restriction = False
    query = cdrdb.Query("document d", *columns)
    if language != "all":
        query.join("%s qlang" % qtable, "qlang.doc_id = d.id")
        query.where(query.Condition("qlang.path",
                    '/Summary/SummaryMetaData/SummaryLanguage'))
        query.where(query.Condition("qlang.value", language))
        summary_restriction = True
    if audience != "all":
        query.join("%s quad" % qtable, "quad.doc_id = d.id")
        query.where(query.Condition("quad.path",
                    '/Summary/SummaryMetaData/SummaryAudience'))
        query.where(query.Condition("quad.value", audience))
        summary_restriction = True
    if boards:
        query.join("%s qboard" % qtable, "qboard.doc_id = d.id")

        # Test whether we search by name or doc ID
        if type(boards[0]) == type(1):
            query.where(query.Condition("qboard.path",
                        '/Summary/SummaryMetaData/PDQBoard/Board/@cdr:ref'))
            query.where(query.Condition("qboard.int_val", boards, "IN"))
        else:
            query.where(query.Condition("qboard.path",
                        '/Summary/SummaryMetaData/PDQBoard/Board'))
            query.where(query.Condition("qboard.value", boards, "IN"))
        summary_restriction = True
    if not summary_restriction:
        # Summary restriction uses query_term path to get only Summary docs
        # Without it, we need a doc_type restriction
        query.join("doc_type t", "d.doc_type = t.id")
        query.where(query.Condition("t.name", 'Summary'))
    if status != "all":
        query.where(query.Condition("d.active_status" , status))
    if sortby:
        query.order(sortby)

    # DEBUG
    # logwrite("getSumaryIds:\n%s" % query, "foo")

    # Fetch and cleanup
    cursor = query.execute()
    rows   = cursor.fetchall()
    cursor.close()

    return rows

#----------------------------------------------------------------------
# Record an event which happened in a CDR client session.
#----------------------------------------------------------------------
def logClientEvent(session, desc, host=DEFAULT_HOST, port=DEFAULT_PORT):
    cmd = (u"<CdrLogClientEvent>"
           u"<EventDescription>%s</EventDescription>"
           u"</CdrLogClientEvent>" % cgi.escape(desc))
    resp = sendCommands(wrapCommand(cmd, session, host), host, port)
    errors = getErrors(resp, errorsExpected = False, asSequence = True)
    if errors:
        raise Exception(errors)
    match = re.search("<EventId>(\\d+)</EventId>", resp)
    if not match:
        raise Exception(u"malformed response: %s" % resp)
    return int(match.group(1))

#----------------------------------------------------------------------
# Transform URLs for DEV and QA
#----------------------------------------------------------------------
class MutateCGUrl:
    """
    Class for transforming URLs that point to cancer.gov pages to URLs that
    point to the equivalent pages on the DEV or QA versions of cancer.gov.

    This enables testing in the CBIIT DEV and QA CDR servers which are
    forbidden from talking to the cancer.gov servers.
    """
    def __init__(self):
        # Where are we?
        global h
        self.tier = h.tier

        # If in DEV or QA, we need info for the transmutations
        if self.tier in ("DEV", "QA"):

            # Where are the cancer.gov sites for this use?
            cgHostProp  = h.getHostNames("CG")
            mcgHostProp = h.getHostNames("CGMOBILE")

            # Corresponding names
            self.cgTargetUrlName  = "http://" + cgHostProp.qname
            self.mcgTargetUrlName = "http://" + mcgHostProp.qname

            self.cgpat  = re.compile("(https?://(www\.)?cancer.gov)/?.*")
            self.mcgpat = re.compile("(https?://m.cancer.gov)/?.*")
        else:
            self.cgtargetUrlName = None
            self.mcgtargetUrlName = None

    #------------------------------------------------------------------
    # Convert a URL for use on DEV or QA versions of cancer.gov
    #------------------------------------------------------------------
    def mutateUrl(self, url):
        """
        Convert a url from the production server if we're not running
        in production.

        Pass:
            url - Where we're trying to go

        Return:
            Transformed url or, if not going to cancer.gov or not on
            DEV or QA, the original url passed to us.
        """
        # Default returns unmodifed url
        retUrl = url

        if self.tier in ("DEV", "QA"):
            # Try standard browser url match
            m = self.cgpat.match(url)
            if m:
                # Substitute whatever we need for this tier
                retUrl = self.cgTargetUrlName + url[m.end(1):]

            # If failed, try mobile browser url match
            else:
                m = self.mcgpat.match(url)
                if m:
                    retUrl = self.mcgTargetUrlName + url[m.end(1):]

        # Return possibly transformed url
        return retUrl

#----------------------------------------------------------------------
# Pull out the portion of an editorial board name used for menu options.
#----------------------------------------------------------------------
def extract_board_name(doc_title):
    board_name = doc_title.split(";")[0].strip()
    board_name = board_name.replace("PDQ ", "").strip()
    board_name = board_name.replace(" Editorial Board", "").strip()
    if board_name.startswith("Cancer Complementary"):
        board_name = board_name.replace("Cancer ", "").strip()
    return board_name


'''