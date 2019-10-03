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
import os
import re
import sys
import traceback
import requests
from lxml import etree
from cdrapi import db as cdrdb
from cdrapi.settings import Tier
from cdrapi.users import Session
from cdrapi.docs import Doc as APIDoc
from cdrapi.docs import Doctype, GlossaryTermName, Schema, DTD
from cdrapi.docs import LinkType as APILinkType
from cdrapi.docs import FilterSet as APIFilterSet
from cdrapi.publishing import Job as PublishingJob
from cdrapi.reports import Report
from cdrapi.searches import QueryTermDef, Search


# ======================================================================
# CDR Board information
# ======================================================================

class Board:
    NAME_PATH = "/Organization/OrganizationNameInformation/OfficialName/Name"
    ORG_TYPE_PATH = "/Organization/OrganizationType"
    PREFIX = "PDQ "
    SUFFIXES = " Editorial Board", " Advisory Board"
    EDITORIAL = "Editorial"
    ADVISORY = "Advisory"
    BOARD_TYPES = EDITORIAL, ADVISORY
    def __init__(self, id, **opts):
        self.__id = id
        self.__opts = opts
    def __str__(self):
        return self.short_name
    def __repr__(self):
        return f"{self.name} (CDR{self.id})"
    @property
    def id(self):
        return self.__id
    @property
    def cursor(self):
        if not hasattr(self, "_cursor"):
            self._cursor = self.__opts.get("cursor")
            if not self._cursor:
                self._cursor = cdrdb.connect().cursor()
        return self._cursor
    @property
    def name(self):
        if not hasattr(self, "_name"):
            self._name = self.__opts.get("name")
            if not self._name and self.id:
                query = cdrdb.Query("query_term", "value")
                query.where(query.Condition("path", self.NAME_PATH))
                query.where(query.Condition("doc_id", self.id))
                for row in query.execute(self.cursor):
                    self._name = row.value
        return self._name
    @property
    def type(self):
        if not hasattr(self, "_type"):
            self._type = self.__opts.get("type")
            if not self._type:
                if self.name is not None:
                    if self.EDITORIAL.lower() in self.name.lower():
                        self._type = self.EDITORIAL
                    elif self.ADVISORY.lower() in self.name.lower():
                        self._type = self.ADVISORY
        return self._type
    @property
    def short_name(self):
        """Trimmed version of the name, suitable for web form picklists."""
        if not hasattr(self, "_short_name"):
            self._short_name = self.name
            if self._short_name:
                if self._short_name.startswith(self.PREFIX):
                    self._short_name = self._short_name[len(self.PREFIX):]
                for suffix in self.SUFFIXES:
                    if self._short_name.endswith(suffix):
                        self._short_name = self._short_name[:-len(suffix)]
        return self._short_name
    @property
    def tab_name(self):
        """Version of the name short enough to fit on an Excel tab."""
        if not self.short_name:
            return None
        if "alternative" in self.short_name.lower():
            return "IACT"
        return self.short_name
    def __lt__(self, other):
        return (self.name or "") < (other.name or "")
    @classmethod
    def get_boards(cls, board_type=EDITORIAL, cursor=None):
        """Dictionary of PDQ boards, indexed by Organization document ID.

        Pass:
            board_type:
                None for all boards
                "Editorial" for the editorial boards only (the default)
                "Advisory" for the advisory boards only
            cursor:
                optional database cursor object
        """
        query = cdrdb.Query("query_term n", "n.doc_id", "n.value").unique()
        query.join("query_term t", "t.doc_id = n.doc_id")
        query.join("active_doc a", "a.id = n.doc_id")
        query.where(query.Condition("n.path", cls.NAME_PATH))
        query.where(query.Condition("t.path", cls.ORG_TYPE_PATH))
        if board_type in cls.BOARD_TYPES:
            query.where(query.Condition("t.value", f"PDQ {board_type} Board"))
        else:
            types = [f"PDQ {bt} Board" for bt in cls.BOARD_TYPES]
            query.where(query.Condition("t.value", types, "IN"))
        boards = {}
        for board_id, board_name in query.execute():
            boards[board_id] = cls(board_id, name=board_name)
        return boards



# ======================================================================
# Manage CDR control values
# ======================================================================

def getControlValue(group, name, default=None, tier=None):
    """
    Fetch a value from the ctl table

    Pass:
      group - string naming group for which value is stored
      name - string for value's key withing the group
      default - optional value to return if no active value found
      tier - optional; one of DEV, QA, STAGE, PROD

    Return:
      string for control value if active value found; otherwise `default`
    """

    cursor = None
    if tier:
        cursor = cdrdb.connect(user="CdrGuest", tier=tier).cursor()
    query = cdrdb.Query("ctl", "val")
    query.where(query.Condition("grp", group))
    query.where(query.Condition("name", name))
    query.where("inactivated IS NULL")
    row = query.execute(cursor).fetchone()
    return row.val if row else default

def getControlGroup(group, tier=None):
    """
    Fetch a named group of CDR control values

    Pass:
      group - string naming group to be fetched
      tier - optional; one of DEV, QA, STAGE, PROD

    Return:
      dictionary of active values for the group, indexed by their names
    """

    cursor = cdrdb.connect(user="CdrGuest", tier=tier).cursor()
    query = cdrdb.Query("ctl", "name", "val")
    query.where(query.Condition("grp", group))
    query.where("inactivated IS NULL")
    group = dict()
    for name, value in query.execute(cursor).fetchall():
        group[name] = value
    return group

def updateCtl(credentials, action, **opts):
    """
    Update the `ctl` table

    The `ctl` table holds groups of named CDR system values used to
    control the behavior of the software at run time. This function
    is used to set or deactivate a value in this table.

    Pass:
      action - required string indicating what the function should do;
               one of:
                  "Create"
                      add a new row to the table, assigning a value for
                      a group/name combination; any existing rows for
                      that comibinaty will be inactivated
                  "Inactivate"
                      mark the row for a group/name combination as
                      inactivated
                  "Install"
                      obsolete action, used for managing the cache of
                      control values when the CDR used a Windows
                      service to handle all of the client/server
                      requests; currently ignored
      group - string naming the group for which the value is to be
              installed or inactivated (e.g., "Publishing"); required
              for the "Create" and "Inactivate" commands; otherwise ignored
      name - string for the value's key (unique within the group, but
             not necessarily withing the table); e.g., "ThreadCount";
             required for the "Create" and "Inactivate" commands;
             otherwise ignored
      value - string for the value to be added to the table (e.g., "6");
              required for the "Create" action; otherwise ignored
      comment - optional string describing the new value; ignored for
                all actions except "Create"
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
        None

    Throws
        Exception if there is an error return from the CdrServer.
    """

    group = opts.get("group")
    name = opts.get("name")
    value = opts.get("value")
    comment = opts.get("comment")
    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        if action == "Create":
            opts = dict(comment=comment)
            Tier.set_control_value(session, group, name, value, **opts)
        elif action == "Inactivate":
            Tier.inactivate_control_value(session, group, name)
        elif action != "Install":
            raise Exception("Invalid action {!r}".format(action))
    else:
        command = etree.Element("CdrSetCtl")
        wrapper = etree.SubElement(command, "Ctl")
        etree.SubElement(wrapper, "Action").text = action
        if group:
            etree.SubElement(wrapper, "Group").text = group
        if name:
            etree.SubElement(wrapper, "Key").text = name
        if value:
            etree.SubElement(wrapper, "Value").text = value
        if comment:
            etree.SubElement(wrapper, "Comment").text = value
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command.tag + "Resp":
                return
            raise Exception(";".join(response.errors) or "missing response")
        raise Exception("missing response")


# ======================================================================
# Manage CDR login sessions
# ======================================================================

def login(username, password="", **opts):

    """
    Create a CDR login session

    Pass:
      username - name of CDR use account
      password - password for the account (can be empty when logging in
                 from localhost with a network account)
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
            if response.node.tag == command.tag + "Resp":
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
# Manage CDR users/groups/actions/permissions
# ======================================================================

class User:
    """
    Information about a single CDR user account
    """

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


def getUser(credentials, name, **opts):
    """
    Retrieve information about a single CDR user account

    Required positional arguments:
      credentials - name of existing session or login credentials
      name - string for name of account to be fetched

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      `User` object
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        user = Session.User(session, name=name)
        user_opts = dict(
            fullname=user.fullname,
            office=user.office,
            email=user.email,
            phone=user.phone,
            groups=user.groups,
            comment=user.comment,
            authMode=user.authmode
        )
        return User(user.name, **user_opts)
    command = etree.Element("CdrGetUsr")
    etree.SubElement(command, "UserName").text = name
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            opts = dict(
                fullname=get_text(response.node.find("FullName")),
                office=get_text(response.node.find("Office")),
                email=get_text(response.node.find("Email")),
                phone=get_text(response.node.find("Phone")),
                comment=get_text(response.node.find("Comment")),
                authMode=get_text(response.node.find("AuthenticationMode")),
                groups=[get_text(g) for g in response.node.findall("GrpName")]
            )
            name = get_text(response.node.find("UserName"))
            return User(name, **opts)
        raise Exception(";".join(response.errors) or "missing response")
    raise Exception("missing response")

def putUser(credentials, name, user, **opts):
    """
    Add or update the database record for a CDR user

    Required positional arguments:
      credentials - name of existing session or login credentials
      name - string for name of account to be modified (None for new user)
      user - `User` object with values to be stored

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      None
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        user_opts = dict(
            name=user.name,
            fullname=user.fullname,
            office=user.office,
            email=user.email,
            phone=user.phone,
            groups=user.groups,
            comment=user.comment,
            authmode=user.authMode
        )
        if name:
            user_opts["id"] = Session.User(session, name=name).id
        Session.User(session, **user_opts).save(user.password)
    else:
        tag = "CdrModUsr" if name else "CdrAddUsr"
        command = etree.Element(tag)
        if name:
            etree.SubElement(command, "UserName").text = name
            if name != user.name:
                etree.SubElement(command, "NewName").text = user.name
        else:
            etree.SubElement(command, "UserName").text = user.name
        etree.SubElement(command, "AuthenticationMode").text = user.authMode
        etree.SubElement(command, "Password").text = user.password
        if user.fullname is not None:
            etree.SubElement(command, "FullName").text = user.fullname
        if user.office is not None:
            etree.SubElement(command, "Office").text = user.office
        if user.email is not None:
            etree.SubElement(command, "Email").text = user.email
        if user.phone is not None:
            etree.SubElement(command, "Phone").text = user.phone
        if user.comment is not None:
            etree.SubElement(command, "Comment").text = user.comment
        for group in user.groups:
            etree.SubElement(command, "GrpName").text = group
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == tag + "Resp":
                return
            raise Exception(";".join(response.errors) or "missing response")
        raise Exception("missing response")

def delUser(credentials, name, **opts):
    """
    Mark the user's account as expired

    Required positional arguments:
      credentials - name of existing session or login credentials
      name - string for name of account to be deleted

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      None
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        Session.User(session, name=name).delete()
    else:
        command = etree.Element("CdrDelUsr")
        etree.SubElement(command, "UserName").text = name
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command.tag + "Resp":
                return
            raise Exception(";".join(response.errors) or "missing response")
        raise Exception("missing response")

def getUsers(credentials, **opts):
    """
    Get the list of name for active CDR user accounts

    Required positional argument:
      credentials - name of existing session or login credentials

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      sequence of strings for user account names
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return session.list_users()
    command = etree.Element("CdrListUsrs")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            node = response.node
            return [get_text(child) for child in node.findall("UserName")]
        raise Exception(";".join(response.errors) or "missing response")
    raise Exception("missing response")

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
            if response.node.tag == command.tag + "Resp":
                return response.node.text == "Y"
            raise Exception(";".join(response.errors) or "missing response")
        raise Exception("missing response")

def checkAuth(session, pairs, **opts):
    """
    Determine which actions specified by the caller are allowed for this user

    Pass:
      session - name of session whose permissions are being checked
      pairs - sequence of action, doctype tuples; the wildcard string
              "*" can be given for the action to ask for information
              about all allowed actions; similarly "*" can be given
              the doctype member of a tuple to ask for all doctypes
              which are allowed to be operated on for a doctype-specific
              action (or as a placeholder for actions which are not
              doctype specific)

    Return:
      dictionary indexed by names of allowed actions, with values being
      sets of document type names on which the action can be performed
      (for doctype-specific actions), or the empty set (for actions which
      are not doctype specific)
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(session, tier)
    if isinstance(session, Session):
        return session.check_permissions(pairs)
    command = etree.Element("CdrCheckAuth")
    for action, doctype in pairs:
        wrapper = etree.SubElement(command, "Auth")
        etree.SubElement(wrapper, "Action").text = action
        if doctype.strip():
            etree.SubElement(wrapper, "DocType").text = doctype
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            auth = dict()
            for wrapper in response.node.findall("Auth"):
                action = get_text(wrapper.find("Action"))
                doctype = get_text(wrapper.find("DocType"))
                if action not in auth:
                    auth[action] = set()
                if doctype:
                    auth[action].add(doctype)
            return auth
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
        if response.node.tag == command.tag + "Resp":
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
        if response.node.tag == command.tag + "Resp":
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
        etree.SubElement(command, "Name").text = name
        if new_name and new_name != name:
            etree.SubElement(command, "NewName").text = new_name
        flag = action.doctype_specific
        etree.SubElement(command, "DoctypeSpecific").text = flag
        if action.comment is not None:
            etree.SubElement(command, "Comment").text = action.comment
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command_name + "Resp":
                return
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
            if response.node.tag == command.tag + "Resp":
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
        if response.node.tag == command.tag + "Resp":
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
        if response.node.tag == command.tag + "Resp":
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
            if response.node.tag == command.tag + "Resp":
                return
            raise Exception(";".join(response.errors) or "missing response")
        raise Exception("missing response")


# ======================================================================
# Manage CDR document types
# ======================================================================

class dtinfo:
    """Class to contain CDR document type information."""

    def __init__(self, **opts):
        names = ("type", "format", "versioning", "created", "schema_mod",
                 "dtd", "schema", "vvLists", "comment", "error", "active",
                 "title_filter")
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
        return f"""\
[CDR Document Type]
            Name: {self.type or ""}
          Format: {self.format or ""}
      Versioning: {self.versioning or ""}
         Created: {self.created or ""}
          Active: {self.active or ""}
    Title Filter: {self.title_filter or ""}
 Schema Modified: {self.schema_mod or ""}
          Schema:
{self.schema or ""}
             DTD:
{self.dtd or ""}
         Comment:
{self.comment or ""}
"""

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
        doctype = Doctype(session, name=name)
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
            active=doctype.active,
            title_filter=doctype.title_filter
        )
        return dtinfo(**args)
    command = etree.Element("CdrGetDocType", Type=name, GetEnumValues="Y")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
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
                elif child.tag == "TitleFilter":
                    args["title_filter"] = get_text(child)
                elif child.tag == "EnumSet":
                    values = [v.text for v in child.findall("ValidValue")]
                    args["vvLists"].append((child.get("Node"), values))
            return dtinfo(**args)
        else:
            raise Exception(";".join(response.errors) or "missing response")
    raise Exception("missing response")

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
            comment=info.comment,
            title_filter=info.title_filter
        )
        doctype = Doctype(session, **opts)
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
    if info.title_filter:
        etree.SubElement(command, "TitleFilter").text = info.title_filter
    if info.comment is not None:
        etree.SubElement(command, "Comment").text = info.comment

    # Submit the request.
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
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
            title_filter=info.title_filter
        )
        if info.active:
            opts["active"] = info.active
        doctype = Doctype(session, **opts)
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
    if info.title_filter:
        etree.SubElement(command, "TitleFilter").text = info.title_filter

    # Submit the request.
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
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
        doctype = Doctype(session, name=name)
        doctype.delete()
    else:
        command = etree.Element("CdrDelDocType", Type=name)
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command.tag + "Resp":
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
        return Doctype.list_doc_types(session)
    command = etree.Element("CdrListDocTypes")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
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
        return Doctype.list_schema_docs(session)
    command = etree.Element("CdrListSchemaDocs")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
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

    def __init__(self, xml, **opts):
        """
        An object encapsulating all the elements of a CDR document.

        NOTE: If the strings passed in for the constructor are encoded as
              anything other than latin-1, you MUST provide the name of
              the encoding used as the value of the `encoding' parameter!

        Required positional argument:
            xml         XML as utf-8 or Unicode string.

        Optional keyword arguments:
            doctype     Document type.
                         If passed, all other components of the Doc must
                          also be passed.
                         If none, then a CdrDoc must be passed with all
                          other components derived from the document string.
                         (also accepted for this option is "type" - an
                          unfortunately legacy naming of an argument using
                          a Python keyword)
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
        self.encoding = opts.get("encoding", "utf-8")
        doctype = opts.get("doctype") or opts.get("type")
        if doctype:
            self.type = doctype
            self.xml = xml
            self.id = opts.get("id")
            self.ctrl = opts.get("ctrl") or {}
            self.blob = opts.get("blob")
        # ... and the other for passing in a CdrDoc element to be parsed.
        else:
            if self.encoding.lower() != "utf-8":
                if isinstance(xml, str):
                    xml = xml.encode("utf-8")
                else:
                    xml = str(xml, self.encoding).encode("utf-8")
            root = etree.fromstring(xml)
            self.ctrl = {}
            self.xml = ""
            self.blob = None
            self.id = root.get("Id")
            self.type = root.get("Type")
            for node in root:
                if node.tag == "CdrDocCtl":
                    self.parseCtl(node)
                elif node.tag == "CdrDocXml":
                    self.xml = get_text(node, "").encode(self.encoding)
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

        self.blob = base64.decodebytes(get_text(node).encode("ascii"))

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
        xml = self.xml
        if not isinstance(xml, str):
            xml = xml.decode("utf-8")
        if "]]>" not in xml:
            xml = etree.CDATA(xml)
        etree.SubElement(doc, "CdrDocXml").text = xml
        if self.blob is not None:
            blob = base64.encodebytes(self.blob).decode("ascii")
            etree.SubElement(doc, "CdrDocBlob", encoding="base64").text = blob
        cdr_doc_xml = etree.tostring(doc, encoding="utf-8")
        return cdr_doc_xml.decode("utf-8")

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
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        etree.SubElement(control_wrapper, name).text = str(value)
    if isinstance(xml, bytes):
        xml = xml.decode("utf-8")
    etree.SubElement(doc, "CdrDocXml").text = etree.CDATA(str(xml))
    return etree.tostring(doc, encoding="utf-8")

def _put_doc(session, command_name, **opts):
    """
    Create and submit the XML command node for adding or replacing a CDR doc

    Factored tunneling code used by `addDoc()` and `repDoc()`.

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
    check_in = opts.get("check_in") == "Y" or opts.get("checkIn") == "Y"
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
    if isinstance(reason, bytes):
        reason = reason.decode("utf-8")
    etree.SubElement(command, "Reason").text = str(reason)

    # Get or create the CdrDoc node.
    filename = opts.get("doc_filename") or opts.get("file")
    try:
        if filename:
            cdr_doc = etree.parse(filename).getroot()
        else:
            doc = opts.get("doc")
            if isinstance(doc, str):
                doc = doc.encode("utf-8")
            cdr_doc = etree.fromstring(doc)
    except:
        error = cls.wrap_error("Unable to parse document")
        if opts.get("show_warnings") or opts.get("showWarnings"):
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
            if opts.get("show_warnings") or opts.get("showWarnings"):
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
        encoded_blob = base64.encodebytes(blob).decode("ascii")
        node = cdr_doc.find("CdrDocBlob")
        if node is not None:
            node.text = encoded_blob
        else:
            etree.SubElement(cdr_doc, "CdrDocBlob").text = encoded_blob

    # Plug in a new comment if appropriate.
    comment = opts.get("comment")
    if comment:
        if not isinstance(comment, str):
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
        LOGGER.exception("CdrRepDoc")
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
                  new `APIDoc` class objects
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
        doc = APIDoc(session, id=docId, version=version)
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
            if response.node.tag == command.tag + "Resp":
                doc = response.node.find("CdrDoc")
            else:
                error = ";".join(response.errors) or "missing response"
                raise Exception(error)
        if doc is None:
            raise Exception("missing response")
    doc_bytes = etree.tostring(doc, encoding="utf-8")
    if opts.get("getObject"):
        return Doc(doc_bytes, encoding="utf-8")
    return doc_bytes

def delDoc(credentials, doc_id, **opts):
    """
    Mark a CDR document as deleted

    Required positional arguments:
      credentials - name of existing session or login credentials
      doc_id - CDR ID string or integer

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
        doc = APIDoc(session, id=doc_id)
        opts = dict(reason=reason, validate=validate)
        doc.delete(**opts)
        if doc.errors:
            return etree.tostring(doc.errors_node, encoding="utf-8")
        return doc.cdr_id
    cdr_id = normalize(doc_id)
    command = etree.Element("CdrDelDoc")
    etree.SubElement(command, "DocId").text = cdr_id
    etree.SubElement(command, "Validate").text = "Y" if validate else "N"
    if reason:
        etree.SubElement(command, "Reason").text = reason
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            if response.errors:
                return response.errors
            return cdr_id
        else:
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
    raise Exception("missing response")

def lastVersions(credentials, doc_id, **opts):
    """
    Find information about the last versions of a document

    Required positional arguments:
      credentials - name of existing session or login credentials
      doc_id - CDR ID string or integer

    Keyword options
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      tuple of:
        * last version number, or -1 if no versions
        * last publishable version number or -1, may be same as last version
        * change information:
         'Y' = last version is different from current working doc.
         'N' = last version is not different.
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        doc = APIDoc(session, id=doc_id)
        return (
            doc.last_version or -1,
            doc.last_publishable_version or -1,
            doc.has_unversioned_changes and "Y" or "N"
        )
    command = etree.Element("CdrLastVersions")
    etree.SubElement(command, "DocId").text = normalize(doc_id)
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            return (
                int(get_text(response.node.find("LastVersionNum"))),
                int(get_text(response.node.find("LastPubVersionNum"))),
                get_text(response.node.find("IsChanged"))
            )
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def listVersions(credentials, doc_id, **opts):
    """
    Find information about the last versions of a document

    Required positional arguments:
      credentials - name of existing session or login credentials
      doc_id - CDR ID string or integer

    Keyword options
      limit - maximum number of version tuples to return (default=None)
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      sequence of tuples, latest versions first, each tuple containing:
        * integer for the version number
        * date/time the version was saved
        * comment for the version, if any (otherwise None)
    """

    limit = opts.get("limit")
    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        versions = APIDoc(session, id=doc_id).list_versions(limit)
        return [(v.number, v.saved, v.comment) for v in versions]
    command = etree.Element("CdrListVersions")
    etree.SubElement(command, "DocId").text = normalize(doc_id)
    if limit:
        etree.SubElement(command, "NumVersions").text = str(limit)
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            versions = []
            for wrapper in response.node.findall("Version"):
                number = int(get_text(wrapper.find("Num")))
                saved = get_text(wrapper.find("Date"))
                comment = get_text(wrapper.find("Comment"))
                versions.append((number, saved, comment))
            return versions
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
      no_output - if 'Y', retrieve messages but no filtered document
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
      encoding - return document and errors as bytes with specified encoding

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
    encoding = opts.get("encoding") or "unicode"
    output = not no_output
    parms = opts.get("parms") or opts.get("parm")
    parms = dict(parms) if parms else {}
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        args = credentials, filter, docId, opts
        session.logger.debug("filterDoc(%r, %r, %r, %r)", *args)
        doc = APIDoc(session, id=docId, version=ver, before=date, xml=xml)
        options = dict(
            parms=parms,
            output=output,
            version=filter_ver,
            date=filter_date
        )
        if opts.get("inline"):
            options["filter"] = filter
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
            messages = etree.tostring(messages, encoding=encoding)
        else:
            messages = ""
        if output:
            # Don't know why, but lxml isn't consistent in filter results.
            if isinstance(result.result_tree, etree._Element):
                result = etree.tostring(result.result_tree, encoding=encoding)
            else:
                result = str(result.result_tree)
                if encoding.lower() != "unicode":
                    result = result.encode(encoding)
            return result, messages
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
        if not isinstance(filter, str):
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
        if not isinstance(xml, str):
            xml = xml.decode("utf-8")
        node.text = etree.CDATA(xml)
    else:
        raise Exception("nothing to filter")

    if parms:
        wrapper = etree.SubElement(command, "Parms")
        for name in parms:
            parm = etree.SubElement(wrapper, "Parm")
            etree.SubElement(parm, "Name").text = str(name)
            etree.SubElement(parm, "Value").text = str(parms[name] or "")

    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            document = get_text(response.node.find("Document"))
            messages = response.node.find("Messages")
            if messages is not None:
                messages = etree.tostring(messages, encoding=encoding)
            else:
                messages = ""
            if output:
                if encoding.lower() != "unicode":
                    document = document.encode(encoding)
                return document, messages
            else:
                return messages
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def create_label(credentials, label, **opts):
    """
    Create a name which can be used to tag a set of document versions

    Pass:
      credentials - results of login
      label - string for the label name
      comment - option string describing the label's usage
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      None
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        APIDoc.create_label(session, label, comment=opts.get("comment"))
    else:
        command = etree.Element("CdrCreateLabel")
        etree.SubElement(command, "Name").text = label
        comment = opts.get("comment")
        if comment:
            etree.SubElement(command, "Comment").text = comment
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command.tag + "Resp":
                return
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
        raise Exception("missing response")

def delete_label(credentials, label, **opts):
    """
    Remove a name previously made available for tagging document versions

    Pass:
      credentials - results of login
      label - string for the label name
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      None
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        APIDoc.delete_label(session, label)
    else:
        command = etree.Element("CdrDeleteLabel")
        etree.SubElement(command, "Name").text = label
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command.tag + "Resp":
                return
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
        raise Exception("missing response")

def label_doc(credentials, doc_id, version, label, **opts):
    """
    Create a name which can be used to tag a set of document version

    Pass:
      credentials - results of login
      doc_id - unique identifier for CDR document
      version - which version should be labeled
      label - string for the label name to be applied
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      None
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        doc = APIDoc(session, id=doc_id, version=version)
        doc.label(label)
    else:
        command = etree.Element("CdrLabelDocument")
        etree.SubElement(command, "DocumentId").text = normalize(doc_id)
        etree.SubElement(command, "DocumentVersion").text = str(version)
        etree.SubElement(command, "LabelName").text = label
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command.tag + "Resp":
                return
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
        raise Exception("missing response")

def unlabel_doc(credentials, doc_id, label, **opts):
    """
    Create a name which can be used to tag a set of document version

    Pass:
      credentials - results of login
      doc_id - unique identifier for CDR document
      label - string for the label name to be removed
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      None
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        doc = APIDoc(session, id=doc_id)
        doc.unlabel(label)
    else:
        command = etree.Element("CdrUnlabelDocument")
        etree.SubElement(command, "DocumentId").text = normalize(doc_id)
        etree.SubElement(command, "LabelName").text = label
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command.tag + "Resp":
                return
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
        raise Exception("missing response")

def setDocStatus(credentials, docId, newStatus, **opts):
    """
    Change the active_status column for a document.

    Required positional arguments:
      credentials - result of login()
      docId - unique ID for document to be modified
      newStatus - "I" (inactive) or "A" (active)

    Optional keyword arguments:
      comment - optional string to be written to the audit table
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier
    """

    comment = opts.get("comment")
    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        APIDoc(session, id=docId).set_status(newStatus, comment=comment)
    else:
        command = etree.Element("CdrSetDocStatus")
        etree.SubElement(command, "DocId").text = normalize(docId)
        etree.SubElement(command, "NewStatus").text = newStatus
        if comment:
            etree.SubElement(command, "Comment").text = comment
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command.tag + "Resp":
                return
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
        raise Exception("missing response")

def getDocStatus(credentials, docId, tier=None):
    """
    Retrieve the active status for a document

    Pass:
      credentials - ignored
      docId - unique ID for CDR document
      tier - optional keyword argument
    """

    cursor = cdrdb.connect(user="CdrGuest", tier=tier).cursor()
    cdr_id, int_id, frag_id = exNormalize(docId)
    query = cdrdb.Query("all_docs", "active_status")
    query.where(query.Condition("id", int_id))
    row = query.execute(cursor).fetchone()
    if not row:
        raise Exception("Invalid document ID {!r}".format(docId))
    return row.active_status

def unblockDoc(credentials, docId, **opts):
    """
    Set document status to "A" (active)

    This is a convenience wrapper for cdr.setDocStatus(..., "A")

    Required positional arguments:
      credentials - result of login()
      docId - unique ID for document to be modified
      newStatus - "I" (inactive) or "A" (active)

    Optional keyword arguments:
      comment - optional string to be written to the audit table
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier
    """

    setDocStatus(credentials, docId, "A", **opts)

def updateTitle(credentials, docId, **opts):
    """
    Update a document title

    Tell the CdrServer to re-run the title filter for this document,
    updating the title stored in the document table.

    No locking is done since the this action does not change the document
    itself.  If another user has the document checked out, no harm will
    be done when and if he saves it.

    Pass:
      credentials - Logon credentials or session.
      docId       - Document ID, any format is okay.
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
        True  = Host says title was changed.
        False = Host says regenerated title is the same as the old one.
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return APIDoc(session, id=docId).update_title()
    command = etree.Element("CdrUpdateTitle")
    etree.SubElement(command, "DocId").text = normalize(docId)
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            return get_text(response.node) == "changed"
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
        if isinstance(doc, str):
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
        validation_opts = dict(types=val_types, locators=locators, store=store)
        if doc_id:
            doc = APIDoc(session, id=doc_id)
        else:
            xml = get_text(doc.find("CdrDocXml"))
            level = doc.get("RevisionFilterLevel")
            if level:
                validation_opts["level"] = level
            doc = APIDoc(session, xml=xml, doctype=doctype)
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
        if response.node.tag == command.tag + "Resp":
            parent = response.node.getparent()
            return etree.tostring(parent, encoding="utf-8")
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def reindex(credentials, doc_id, **opts):
    """
    Reindex the specified document

    Required positional arguments:
      credentials - result of login
      doc_id - unique identifier for document to be reindexed

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        APIDoc(session, id=doc_id).reindex()
    else:
        command = etree.Element("CdrReindexDoc")
        etree.SubElement(command, "DocId").text = normalize(doc_id)
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command.tag + "Resp":
                return
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
        raise Exception("missing response")

def checkOutDoc(credentials, doc_id, **opts):
    """
    Checkout a document to the logged in user without retrieving it

    Required positional arguments:
      credentials - result of login
      doc_id - unique identifier for document to be checked out

    Optional keyword arguments:
      force - if "Y" break another user's lock if necessary (default "N")
      comment - optional string explaining why we're locking the document
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      current version number or 0 if no version number returned.
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        opts = {
            "force": True if opts.get("force") == "Y" else False,
            "comment": opts.get("reason")
        }
        doc = APIDoc(session, id=doc_id)
        doc.check_out(**opts)
        return doc.last_version or 0
    else:
        command = etree.Element("CdrCheckOut")
        command.set("ForceCheckOut", opts.get("force", "N") or "N")
        etree.SubElement(command, "DocumentId").text = normalize(doc_id)
        comment = opts.get("comment")
        if comment:
            etree.SubElement("Comment").text = comment
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command.tag + "Resp":
                return int(get_text(response.node.find("Version")) or "0")
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
        raise Exception("missing response")

def unlock(credentials, doc_id, **opts):
    """
    Check in a CDR document

    Required positional arguments:
      credentials - result of login
      doc_id - unique identifier for document to be unlocked

    Optional keyword arguments:
      abandon - if "N" version unsaved changes (default "Y")
      force - if "N" don't break another user's lock (default "Y")
      reason - optional string explaining why we're releasing the lock
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      None
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        opts = {
            "abandon": False if opts.get("abandon") == "N" else True,
            "force": False if opts.get("force") == "N" else True,
            "comment": opts.get("reason")
        }
        APIDoc(session, id=doc_id).check_in(**opts)
    else:
        command = etree.Element("CdrCheckIn")
        command.set("Abandon", opts.get("abandon", "Y") or "Y")
        command.set("ForceCheckIn", opts.get("force", "Y") or "Y")
        comment = opts.get("reason")
        etree.SubElement(command, "DocumentId").text = normalize(doc_id)
        if comment:
            etree.SubElement(command, "Comment").text = comment
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command.tag + "Resp":
                return
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
        raise Exception("missing response")


class LockedDoc(object):
    """
    Container object for information about the checkout status of a doc.
    """

    def __init__(self, row):
        self.__userId       = row.uid
        self.__userAbbrev   = row.username
        self.__userFullName = row.fullname
        self.__docId        = row.id
        self.__docVersion   = row.version
        self.__docType      = row.doctype
        self.__docTitle     = row.title
        self.__dateOut      = row.dt_out

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
        return self.__docTitle
        # Conversion for use in log files and messages XXX - no, don't
        if isinstance(self.__docTitle, type("")):
            return self.__docTitle.encode('ascii', 'replace')
        return self.__docTitle
    docTitle = property(getDocTitle)

    def getDateOut(self): return self.__dateOut
    dateOut = property(getDateOut)

    def __str__(self):
        """ Human readable form """

        return f"""\
       docId: {self.docId}
     docType: {self.docType}
    docTitle: {self.docTitle}
  docVersion: {self.__docVersion}
      userId: {self.userId}
  userAbbrev: {self.userAbbrev}
userFullName: {self.userFullName}
     dateOut: {self.dateOut}"""


def isCheckedOut(doc_id, conn=None):
    """
    Determine if a document is checked out.

    Pass:
        docId - Doc ID, any exNormalizable format.
        conn  - Optional connection object, to optimize many checks in a row

    Return:
        If locked: returns a `LockedDoc` object.
        Else: returns None
    """

    cursor = conn.cursor() if conn else None
    fields = ("u.id AS uid", "u.name AS username", "u.fullname",
              "d.id", "d.title", "t.name AS doctype",
              "c.version", "c.dt_out")
    query = cdrdb.Query("document d", *fields)
    query.join("doc_type t", "t.id = d.doc_type")
    query.join("checkout c", "c.id = d.id")
    query.join("open_usr u", "u.id = c.usr")
    query.where("c.dt_in IS NULL")
    query.where(query.Condition("d.id", exNormalize(doc_id)[1]))
    row = query.execute(cursor).fetchone()
    if cursor:
        cursor.close()
    return LockedDoc(row) if row else None

def get_links(credentials, doc_id, **opts):
    """
    Find the links to a CDR document

    Required positional arguments:
      credentials - result of login
      doc_id - unique identifier for target of links

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      sequence of strings describing the links to this document
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return APIDoc(session, id=doc_id).link_report()
    command = etree.Element("CdrGetLinks")
    etree.SubElement(command, "DocId").text = normalize(doc_id)
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            nodes = response.node.findall("LnkList/LnkItem")
            return [get_text(node) for node in nodes]
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

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

def getTree(credentials, doc_id, **opts):
    """
    Fetch context information for document's position in the terminology tree

    Required positional arguments:
      credentials - result of login
      doc_id - unique identifier for Term document

    Optional keyword arguments:
      depth - number of levels (default=1) to descend for child terms
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      `TermSet` object; for compatibility with the existing software
      which uses this function, the `Term` and `TermSet` objects
      use integer strings instead of integers for term document IDs;
      to fix this to use integers instead, modifications to at least
      the TermHierarchy.py CGI script will need to be made (possibly
      elsewhere as well); if this is done, we can dispense with the
      `TermSet` class and just return the dictionary of `Term` objects,
      since we now throw an exception for errors instead of returning
      an error string (and of course the unittest code will need to
      be modified as well)
    """

    depth = int(opts.get("depth", 1))
    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    term_set = TermSet()
    terms = term_set.terms
    if isinstance(session, Session):
        tree = APIDoc(session, id=doc_id).get_tree(depth)
        for term_id in tree.names:
            terms[str(term_id)] = Term(str(term_id), tree.names[term_id])
        for relationship in tree.relationships:
            parent = str(relationship.parent)
            child = str(relationship.child)
            terms[parent].children.append(terms[child])
            terms[child].parents.append(terms[parent])
        return term_set
    command = etree.Element("CdrGetTree")
    etree.SubElement(command, "DocId").text = normalize(doc_id)
    etree.SubElement(command, "ChildDepth").text = str(depth)
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            for wrapper in response.node.findall("Terms/Term"):
                term_id = get_text(wrapper.find("Id"))
                term_name = get_text(wrapper.find("Name"))
                terms[term_id] = Term(term_id, term_name)
            for wrapper in response.node.findall("Pairs/Pair"):
                parent = get_text(wrapper.find("Parent"))
                child = get_text(wrapper.find("Child"))
                terms[parent].children.append(terms[child])
                terms[child].parents.append(terms[parent])
            return term_set
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
        files = Doctype.get_css_files(session)
        return [CssFile(name, files[name]) for name in sorted(files)]
    command = etree.Element("CdrGetCssFiles")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            files = []
            for node in response.node.findall("File"):
                name = get_text(node.find("Name"))
                data = get_text(node.find("Data"))
                data = base64.decodebytes(data.encode("ascii"))
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

    if isinstance(usage, bytes):
        usage = usage.decode("utf-8")
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        doc = APIDoc(session, id=opts.get("doc_id"))
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
        if response.node.tag == command.tag + "Resp":
            return int(response.node.get("MappingId"))
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def get_glossary_map(credentials, lang, **opts):
    """
    Fetch the mappings of phrases to English or Spanish glossary term names

    Required positional argument:
      credentials - result of login
      lang - "en" or "es"

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      sequence of `GlossaryTermName` objects
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return GlossaryTermName.get_mappings(session, lang)
    tag = "CdrGetGlossaryMap" if lang == "en" else "CdrGetSpanishGlossaryMap"
    command = etree.Element(tag)
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == tag + "Resp":
            terms = []
            for node in response.node.findall("Term"):
                doc_id = int(node.get("id"))
                name = get_text(node.find("Name"))
                term = GlossaryTermName(doc_id, name)
                for child in node.findall("Phrase"):
                    term.phrases.add(get_text(child))
                terms.append(term)
            return terms
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
        filters = APIFilterSet.get_filters(session)
        return [IdAndName(doc.cdr_id, doc.title) for doc in filters]
    command = etree.Element("CdrGetFilters")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
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
        return [IdAndName(*s) for s in APIFilterSet.get_filter_sets(session)]
    command = etree.Element("CdrGetFilterSets")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
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
        self.notes = toUnicode(notes)
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
                if isinstance(m.id, (str, bytes)):
                    member = APIDoc(session, id=m.id, title=self.name)
                else:
                    member = APIFilterSet(session, id=m.id, name=self.name)
                members.append(member)
            set_opts = dict(
                name=self.name,
                description=self.desc,
                notes=self.notes,
                members=members
            )
            filter_set = APIFilterSet(session, **set_opts)
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
            member_id = member.id
            if isinstance(member_id, bytes):
                member_id = member_id.decode("utf-8")
            if isinstance(member_id, str):
                etree.SubElement(command, "Filter", DocId=member_id)
            else:
                etree.SubElement(command, "FilterSet", SetId=str(member_id))
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

        lines = [f"name={self.name}", f"desc={self.desc}"]
        if self.notes:
            lines.append("notes={}".format(self.notes))
        if self.expanded:
            lines.append("Expanded list of filters:")
        for member in self.members:
            member_id, member_name = member.id, member.name
            if isinstance(member_id, bytes):
                member_id = member_id.decode("utf-8")
            if isinstance(member_name, bytes):
                member_name = member_name.decode("utf-8")
            member_type = "filter set"
            if self.expanded or isinstance(member_id, str):
                member_type = "filter"
            lines.append(f"{member_type} {member_id} ({member_name})")
        return "\n".join(lines) + "\n"

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
        filter_set = APIFilterSet(session, name=name)
        for member in filter_set.members:
            if isinstance(member, APIDoc):
                title = member.title or "*** DOCUMENT NOT FOUND ***"
                members.append(IdAndName(member.cdr_id, title))
            else:
                members.append(IdAndName(member.id, member.name))
        return FilterSet(filter_set.name, filter_set.description,
                         filter_set.notes, members)
    command = etree.Element("CdrGetFilterSet")
    etree.SubElement(command, "FilterSetName").text = name
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
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

_expandedFilterSetCache = {}
def expandFilterSet(session, name, level=0, **opts):
    """
    Find all of the filters loaded for a filter set.

    Recursively rolls out the list of filters invoked by a named filter
    set.  In contrast with getFilterSet, which returns a list of nested
    filter sets and filters intermixed, all of the members of the list
    returned by this function represent filters.  Since there is no need
    to distinguish filters from nested sets by the artifice of
    representing filter IDs as strings, the id member of each object
    in this list is an integer.

    Takes the name of the filter set as input.  Returns a FilterSet
    object, with the members attribute as described above.

    Note: since it is possible for bad data to trigger infinite
    recursion, we throw an exception if the depth of nesting exceeds
    a reasonable level.

    WARNING: treat the returned objects as read-only, otherwise you'll
    corrupt the cache used for future calls.
    """

    global _expandedFilterSetCache
    if level > 100:
        raise Exception('expandFilterSet', 'infinite nesting of sets')
    if name in _expandedFilterSetCache:
        return _expandedFilterSetCache[name]
    filterSet = getFilterSet(session, name, host, port)
    newSetMembers = []
    for member in filterSet.members:
        if isinstance(member.id, type(9)):
            nestedSet = expandFilterSet(session, member.name, level + 1)
            newSetMembers += nestedSet.members
        else:
            newSetMembers.append(member)
    filterSet.members = newSetMembers
    filterSet.expanded = 1
    _expandedFilterSetCache[name] = filterSet
    return filterSet

def expandFilterSets(session, **opts):
    """
    Perform the filter set expansion for all filter sets in the system

    Returns a dictionary containing all of the CDR filter sets, rolled
    out by the expandFilterSet() function above, indexed by the filter
    set names.
    """

    sets = {}
    opts = dict(host=host, port=port)
    for fSet in getFilterSets(session):
        sets[fSet.name] = expandFilterSet(session, fSet.name, **opts)
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
        filter_set = APIFilterSet(session, name=name)
        filter_set.delete()
        return
    command = etree.Element("CdrDelFilterSet")
    etree.SubElement(command, "FilterSetName").text = name
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            return
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")


# ======================================================================
# Manage CDR link types
# ======================================================================


class LinkType:
    """
    Information about a single CDR link type definition
    """

    LISTS = "linkSources", "linkTargets", "linkProps"

    def __init__(self, name, **opts):
        self.name = name
        for attr_name in self.LISTS:
            setattr(self, attr_name, opts.get(attr_name) or [])
        self.comment = opts.get("comment")
        self.linkChkType = opts.get("linkChkType", "P")
        if self.comment == "None":
            self.comment = None

    def __str__(self):
        lines = ["LinkType("]
        for name in self.LISTS:
            lines.append(str(getattr(self, name)))
        lines += [self.linkChkType or "?", self.comment or "[NO COMMENT]"]
        return ",\n    ".join(lines) + "\n)"

class LinkPropType:
    """
    Custom property type for a CDR link types

    Currently there is only one type of property supported by the
    CDR, named LinkTargetContains, which checks the query index
    tables to narrow the set of available link target to those
    which have (or do not have) certain values or combinations
    of values.
    """

    def __init__(self, name, comment=None):
        self.name = name
        self.comment = comment

def getLinkTypes(credentials, **opts):
    """
    Fetch the list of CDR link type names

    Required positional argument:
      credentials - result of login

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      sequence of strings for link type names
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return APILinkType.get_linktype_names(session)
    command = etree.Element("CdrListLinkTypes")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            return [get_text(node) for node in response.node.findall("Name")]
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def getLinkType(credentials, name, **opts):
    """
    Fetch detailed information about a single link type

    Required positional arguments:
      credentials - result of login
      name - string for unique link type name

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      `LinkType` object
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        t = APILinkType(session, name=name)
        props = t.properties or []
        opts = {
            "linkTargets": [v.name for v in t.targets.values()],
            "linkSources": [(s.doctype.name, s.element) for s in t.sources],
            "linkProps": [(p.name, p.value, p.comment) for p in props],
            "linkChkType": t.chk_type,
            "comment": t.comment,
        }
        return LinkType(t.name, **opts)
    command = etree.Element("CdrGetLinkType")
    etree.SubElement(command, "Name").text = name
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            name = get_text(response.node.find("Name"))
            opts = dict(
                linkTargets=[],
                linkSources=[],
                linkProps=[],
                linkChkType=get_text(response.node.find("LinkChkType")),
                comment=get_text(response.node.find("LinkTypeComment")),
            )
            for wrapper in response.node.findall("LinkSource"):
                doctype = get_text(wrapper.find("SrcDocType"))
                element = get_text(wrapper.find("SrcField"))
                opts["linkSources"].append((doctype, element))
            for node in response.node.findall("TargetDocType"):
                opts["linkTargets"].append(get_text(node))
            tags = "LinkProperty", "PropertyValue", "PropertyComment"
            for wrapper in response.node.findall("LinkProperties"):
                prop = tuple([get_text(wrapper.find(tag)) for tag in tags])
                opts["linkProps"].append(prop)
            return LinkType(name, **opts)
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def putLinkType(credentials, name, linktype, action, **opts):
    """
    Add a new CDR link type or update an existing one

    Required positional argument:
      credentials - result of login
      name - string for name of existing link type (ignored for new type)
      link_type - `LinkType` object
      action - 'addlink' or 'modlink'

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      None
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        opts = dict(
            name=linktype.name,
            sources=[],
            targets={},
            properties=[],
            chk_type=linktype.linkChkType,
            comment=linktype.comment
        )
        if action == "modlink":
            opts["id"] = APILinkType(session, name=name).id
            if opts["id"] is None:
                raise Exception("Can't find link type {}".format(name))
        for doctype_name, element in linktype.linkSources:
            doctype = Doctype(session, name=doctype_name)
            opts["sources"].append(APILinkType.LinkSource(doctype, element))
        for doctype_name in linktype.linkTargets:
            doctype = Doctype(session, name=doctype_name)
            assert doctype.id, "doctype {!r} not found".format(doctype_name)
            opts["targets"][doctype.id] = doctype
        message = "Property type {!r} not supported"
        for name, value, comment in linktype.linkProps:
            try:
                cls = getattr(APILinkType, name)
                property = cls(session, name, value, comment)
            except:
                raise Exception(message.format(name))
            if not isinstance(property, APILinkType.Property):
                raise Exception(message.format(name))
            opts["properties"].append(property)
        linktype = APILinkType(session, **opts)
        linktype.save()
    else:
        tag = "CdrModLinkType" if action == "modlink" else "CdrAddLinkType"
        command = etree.Element(tag)
        if action == "modlink":
            etree.SubElement(command, "Name").text = name
            if linktype.name and name != linktype.name:
                etree.SubElement(command, "NewName").text = linktype.name
        else:
            etree.SubElement(command, "Name").text = linktype.name
        etree.SubElement(command, "LinkChkType").text = linktype.linkChkType
        if linktype.comment is not None:
            etree.SubElement(command, "Comment").text = linktype.comment
        for doctype, element in linktype.linkSources:
            wrapper = etree.SubElement(command, "LinkSource")
            etree.SubElement(wrapper, "SrcDocType").text = doctype
            etree.SubElement(wrapper, "SrcField").text = element
        for doctype in linktype.linkTargets:
            etree.SubElement(command, "TargetDocType").text = doctype
        for name, value, comment in linktype.linkProps:
            wrapper = etree.SubElement(command, "LinkProperties")
            etree.SubElement(wrapper, "LinkProperty").text = name
            etree.SubElement(wrapper, "PropertyValue").text = value
            etree.SubElement(wrapper, "Comment").text = comment or ""
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == tag + "Resp":
                return
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
        raise Exception("missing response")

def delLinkType(credentials, name, **opts):
    """
    Remove a link type from the CDR

    Required positional arguments:
      credentials - result of login
      name - string for unique link type name to be deleted

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      None
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        APILinkType(session, name=name).delete()
    else:
        command = etree.Element("CdrDelLinkType")
        etree.SubElement(command, "Name").text = name
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command.tag + "Resp":
                return
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
        raise Exception("missing response")

def getLinkProps(credentials, **opts):
    """
    Fetch the information on available custom property types for link types

    Required positional argument:
      credentials - result of login

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      sequence of `LinkPropType` objects
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        prop_types = APILinkType.get_property_types(session)
        return [LinkPropType(p.name, p.comment) for p in prop_types]
    command = etree.Element("CdrListLinkProps")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            prop_types = []
            for wrapper in response.node.findall("LinkProperty"):
                name = get_text(wrapper.find("Name"))
                comment = get_text(wrapper.find("Comment"))
                prop_types.append(LinkPropType(name, comment))
            return prop_types
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def search_links(credentials, source_type, element, **opts):
    """
    Find candidate target documents for a link type

    Pass:
      credentials - result of login
      source_type - string for linking document type
      element - string for linking element

    Optional keyword arguments:
      pattern - string for title pattern to be matched (may contain wildcards)
      limit - integer to constrain the size of the result set
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      sequence of zero or more `IdAndName` objects

    Raise:
      Exception if linking from the specified element is now allowed
      for documents of the specified source type
    """

    limit = opts.get("limit")
    pattern = opts.get("pattern")
    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        message = "Link from {} elements of {} documents not permitted"
        doctype = Doctype(session, name=source_type)
        link_type = APILinkType.lookup(session, doctype, element)
        if link_type is None:
            raise Exception(message.format(element, source_type))
        opts = dict(limit=limit, pattern=pattern)
        docs = link_type.search(**opts)
        return [IdAndName(doc.id, doc.title) for doc in docs]
    command = etree.Element("CdrSearchLinks")
    if limit:
        command.set("MaxDocs", str(limit))
    etree.SubElement(command, "SourceDocType").text = source_type
    etree.SubElement(command, "SourceElementType").text = element
    if pattern:
        etree.SubElement(command, "TargetTitlePattern").text = pattern
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            results = []
            for result in response.node.findall("QueryResults/QueryResult"):
                doc_id = get_text(result.find("DocId"))
                title = get_text(result.find("DocTitle"))
                results.append(IdAndName(doc_id, title))
            return results
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def check_proposed_link(credentials, source_type, element, target, **opts):
    """
    Verify that proposed link is allowed and fetch its denormalized text

    Pass:
      credentials - result of login
      source_type - string for linking document type
      element - string for linking element
      target - CDR document ID for target of link

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      title of target document for denormalizing link

    Raise:
      Exception if proposed link is not allowed
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        message = "Link from {} elements of {} documents"
        tail = " not permitted"
        doctype = Doctype(session, name=source_type)
        link_type = APILinkType.lookup(session, doctype, element)
        if link_type is None:
            raise Exception(message.format(element, source_type) + tail)
        doc = APIDoc(session, id=target)
        if doc.doctype.id not in link_type.targets:
            message += " to document {}" + tail
            raise Exception(message.format(element, source_type, doc.cdr_id))
        return doc.title
    command = etree.Element("CdrPasteLink")
    etree.SubElement(command, "SourceDocType").text = source_type
    etree.SubElement(command, "SourceElementType").text = element
    etree.SubElement(command, "TargetDocId").text = normalize(target)
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            return get_text(response.node.find("DenormalizedContent"))
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")


# ======================================================================
# Manage CDR publishing
# ======================================================================

def publish(credentials, pubSystem, pubSubset, **opts):
    """
    Create a new CDR publishing job

    Required positional arguments:
      credentials - result of login
      pubSystem - string for name of publishing system (e.g., "Primary")
      pubSubset - string for name of publishing subset (e.g., "Export")

    Optional keyword arguments:
      parms - sequence of name, value tuples to override defaults
      docList - sequence of document ID strings with optional version suffix
                (in the form CDR0000999999 or CDR0000999999/99)
      email - string for address to which reports should be sent
      noOutput - if "Y" job will not write published docs to the file system
      allowNonPub - if "Y" non-publishable versions can be specified
      allowInActive - set to "Y" for unpublishing blocked documents
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      tuple of job ID string (None for failure) and serialized (utf-8)
      `Errors` XML node (None if no errors)
    """

    # Log what we're doing to the publishing log
    logger = Logging.get_logger("publish")
    logger.info("cdr.publish(opts=%s)", opts)

    # Parse the doc list
    doc_list = opts.get("docList", [])
    if isinstance(doc_list, (str, bytes)):
        doc_list = [doc_list]
    docs = []
    for doc in doc_list:
        if isinstance(doc, bytes):
            doc_string = doc.decode("utf-8")
        else:
            doc_string = str(doc)
        if "/" in doc_string:
            doc_id, version = doc_string.split("/", 1)
        else:
            doc_id, version = doc_string, None
        try:
            docs.append((doc_id, version))
        except:
            return None, "<Errors><Err>Invalid version</Err></Errors>"

    # Handle the request locally if possible.
    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        pub_opts = dict(
            system=pubSystem,
            subsystem=pubSubset,
            parms=dict(opts.get("parms") or []),
            docs=[APIDoc(session, id=doc[0], version=doc[1]) for doc in docs],
            email=opts.get("email"),
            no_output=opts.get("noOutput", "N") == "Y",
            permissive=opts.get("allowNonPub", "N") == "Y",
            force=opts.get("allowInActive", "N") == "Y"
        )
        try:
            job_id = PublishingJob(session, **pub_opts).create()
            logger.info("Job %s created", job_id)
            return (str(job_id), None)
        except Exception as e:
            session.logger.exception("publish() failed")
            logger.exception("failure: %s", e)
            errors = etree.Element("Errors")
            etree.SubElement(errors, "Err").text = str(e)
            return (None, etree.tostring(errors, encoding="utf-8"))

    # Can't do it locally, so use the HTTPS tunnel.
    command = etree.Element("CdrPublish")
    etree.SubElement(command, "PubSystem").text = pubSystem or ""
    etree.SubElement(command, "PubSubset").text = pubSubset or ""
    parms = opts.get("parms", [])
    email = opts.get("email")
    no_output = opts.get("noOutput")
    allow_non_pub = opts.get("allowNonPub")
    allow_inactive = opts.get("allowInActive")
    if parms:
        wrapper = etree.SubElement(command, "Parms")
        for name, value in parms:
            parm = etree.SubElement(wrapper, "Parm")
            etree.SubElement(parm, "Name").text = name
            etree.SubElement(parm, "Value").text = str(value)
    if docs:
        wrapper = etree.SubElement(command, "DocList")
        for doc_id, doc_version in docs:
            cdr_id = normalize(doc_id)
            version = str(doc_version or "0")
            etree.SubElement(wrapper, "Doc", Id=cdr_id, Version=version)
    if email:
        etree.SubElement(command, "Email").text = email
    if no_output:
        etree.SubElement(command, "NoOutput").text = no_output
    if allow_non_pub:
        etree.SubElement(command, "AllowNonPub").text = allow_non_pub
    if allow_inactive:
        etree.SubElement(command, "AllowInActive").text = allow_inactive
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            parent = response.node.getparent()
            node_bytes = etree.tostring(parent, encoding="utf-8")
            node_string = node_bytes.decode("utf-8")
            logger.info("cdr.publish() returned %s", node_string)
            job_id = get_text(response.node.find("JobId"))
            return (job_id, None)

        # Break from the normal pattern of raising exceptions for
        # errors, so we don't break the publishing system. When
        # we buckle down to rewrite cdrpub.py (see OCECDR-2324)
        # we can fix this.
        error = ";".join(response.errors) or "missing response"
        return (None, error)
    return (None, "missing response")

def clear_cache(credentials):
    """
    Clear the cache for filters, filter sets, and term documents

    Used by the publishing system to make sure publishing jobs have
    access to the latest documents. Most CDR scripts don't need to
    worry about the cache, as different processes have their own
    caches. However, the scheduler runs for long stretches (usually
    at least days, if not weeks) in the same process, and publishing
    jobs are processed under the scheduler.

    Pass:
      credentials - from cdr.login()

    Return:
      None
    """

    session = _Control.get_session(credentials)
    session.cache.clear()


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


# ======================================================================
# Manage CDR reports
# ======================================================================

def report(credentials, name, **opts):
    """
    Request the output for a CDR report

    Required positional argument:
      credentials - result of login
      name - required string for report name

    Optional keyword arguments:
      parms - dictionary of named parameter values to be passed to report
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      `ReportBody` DOM node
    """

    parms = opts.get("parms", {})
    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return Report(session, name, **parms).run()
    command = etree.Element("CdrReport")
    etree.SubElement(command, "ReportName").text = name
    if parms:
        wrapper = etree.SubElement(command, "ReportParams")
        for name in parms:
            value = parms[name]
            etree.SubElement(wrapper, "ReportParam", Name=name, Value=value)
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            body = response.node.find("ReportBody")
            if body is None:
                raise Exception("ReportBody missing")
            return body
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")


# ======================================================================
# Manage CDR searching
# ======================================================================


class QueryResult:
    """
    Class to contain one hit from query result set.
    """

    def __init__(self, docId, docType, docTitle):
        self.docId      = docId
        self.docType    = docType
        self.docTitle   = docTitle
    def __repr__(self):
        return "%s (%s) %s\n" % (self.docId, self.docType, self.docTitle)


def search(credentials, *tests, **opts):
    """
    Process a CDR document search request

    Pass:
      credentials - results of login()
      tests - one or more assertion strings
      limit - optional integer keyword argument limiting number of results
      doctypes - optional sequence of document type name strings to limit set

    Each valid test assertion string contains exactly three tokens:

       * a path, which can be one of

         - CdrCtl/Title

         - the xpath (starting with a single forward slash) for an
           element or attribute, with /value or /int_val appended to
           indicate which column of the query_term table should be
           used for the test

       * an operator; (one of eq, ne, lt, lte, gt, gte, begins, contains)

       * a value to be used in the test; wildcards are added as
         appropriate if the operator is "contains" or "begins"

    The three tokens are separated by whitespace. The first two tokens
    cannot contain whitespace, but there are no whitespace restrictions
    on the value component of the test, which should not be enclosed
    in quote marks.

    Return:
      sequence of `QueryResult` object (possibly empty)
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        results = []
        for doc in Search(session, *tests, **opts).run():
            result = QueryResult(doc.cdr_id, doc.doctype.name, doc.title)
            results.append(result)
        return results
    command = etree.Element("CdrSearch")
    query = etree.SubElement(command, "Query")
    if "limit" in opts:
        query.set("MaxDocs", str(opts["limit"]))
    for doctype in opts.get("doctypes", []):
        etree.SubElement(query, "DocType").text = doctype
    for test in tests:
        etree.SubElement(query, "Test").text = test
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            results = []
            for node in response.node.findall("QueryResults/QueryResult"):
                doc_id = get_text(node.find("DocId"))
                doc_type = get_text(node.find("DocType"))
                doc_title = get_text(node.find("DocTitle"))
                results.append(QueryResult(doc_id, doc_type, doc_title))
            return results
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def listQueryTermRules(credentials, **opts):
    """
    Return the list of available query term rules

    Required positional argument:
      credentials - result of login

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      sequence of custom rule name strings
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return QueryTermDef.get_rules(session)
    command = etree.Element("CdrListQueryTermRules")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            return [get_text(n) for n in response.node.findall("Rule")]
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def listQueryTermDefs(credentials, **opts):
    """
    Return the list of CDR query term definitions

    Required positional argument:
      credentials - result of login

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      sequence of path, rule tuples
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        definitions = QueryTermDef.get_definitions(session)
        return [(d.path, d.rule) for d in definitions]
    command = etree.Element("CdrListQueryTermDefs")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            definitions = []
            for wrapper in response.node.findall("Definition"):
                path = get_text(wrapper.find("Path"))
                rule = get_text(wrapper.find("Rule")) or None
                definitions.append((path, rule))
            return definitions
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def addQueryTermDef(credentials, path, rule=None, **opts):
    """
    Add a new query term definition to the CDR

    Required positional arguments:
      credentials - result of login
      path - string for the location of information to be indexed

    Optional position argument:
      rule - string for the definition's custom indexing rule name (if any)

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      None
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        QueryTermDef(session, path, rule).add()
    else:
        command = etree.Element("CdrAddQueryTermDef")
        etree.SubElement(command, "Path").text = path
        if rule is not None:
            etree.SubElement(command, "Rule").text = rule
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command.tag + "Resp":
                return
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
        raise Exception("missing response")

def delQueryTermDef(credentials, path, rule=None, **opts):
    """
    Delete an existing query term definition from the CDR

    Required positional arguments:
      credentials - result of login
      path - string for the path of the defintion to be deleted

    Optional position argument:
      rule - string for the custom indexing rule name (if any)

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      None
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        QueryTermDef(session, path, rule).delete()
    else:
        command = etree.Element("CdrDelQueryTermDef")
        etree.SubElement(command, "Path").text = path
        if rule is not None:
            etree.SubElement(command, "Rule").text = rule
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command.tag + "Resp":
                return
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
        raise Exception("missing response")

def log_client_event(credentials, description, **opts):
    """
    Capture a record of something that happened in the XMetaL client

    Useful for troubleshooting.

    Pass:
      credentials - result of login
      description - string describing what happened

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      None
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        session.log_client_event(description)
    else:
        command = etree.Element("CdrLogClientEvent")
        etree.SubElement(command, "EventDescription").text = description
        for response in _Control.send_command(session, command, tier):
            if response.node.tag == command.tag + "Resp":
                return
            error = ";".join(response.errors) or "missing response"
            raise Exception(error)
        raise Exception("missing response")

def save_client_trace_log(credentials, log_data, **opts):
    """
    Capture the contents of the trace debugging log from the XMetaL client

    Useful for troubleshooting.

    Pass:
      credentials - result of login
      description - string holding the trace log's currentcontent

    Optional keyword arguments:
      tier - optional; one of DEV, QA, STAGE, PROD
      host - deprecated alias for tier

    Return:
      None
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        return session.save_client_trace_log(log_data)
    command = etree.Element("CdrSaveClientTraceLog")
    etree.SubElement(command, "LogData").text = log_data
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            return int(get_text(response.node.find("LogId")))
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def mailerCleanup(credentials, **opts):
    """
    Mark tracking documents generated by failed mailer jobs as deleted
    """

    tier = opts.get("tier") or opts.get("host") or None
    session = _Control.get_session(credentials, tier)
    if isinstance(session, Session):
        report = APIDoc.delete_failed_mailers(session)
        return (report.deleted, report.errors)
    command = etree.Element("CdrMailerCleanup")
    for response in _Control.send_command(session, command, tier):
        if response.node.tag == command.tag + "Resp":
            doc_ids = []
            errors = []
            for node in response.node.findall("DeletedDoc"):
                doc_ids.append(re.sub(r"[^\d]", "", get_text(node)))
            for node in reponse.node.findall("Errors/Err"):
                errors.append(text_text(node))
            return (doc_ids, errors)
        error = ";".join(response.errors) or "missing response"
        raise Exception(error)
    raise Exception("missing response")

def bail(why):
    """Complain to web client and exit."""
    print(f"Content-type: text/plain\n\n{why}\n")
    exit(0)

class Logging:
    """
    Use the Python standard library support for logging the CDR activity
    This class is basically used as a namespace wrapper, with a factory
    method for instantiating logging objects.
    """

    FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
    LEVELS = dict(
        info=logging.INFO,
        debug=logging.DEBUG,
        warn=logging.WARN,
        warning=logging.WARNING,
        critical=logging.CRITICAL,
        error=logging.ERROR
    )

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
    DUMP = os.environ.get("DUMP_REMOTE_CDR_EXCHANGE")

    try:
        # This isn't actually used to talk to the database. We're only
        # trying to find out if we have local database access. If we
        # don't we'll be using tunneling over HTTPS for CDR commands.
        conn = cdrdb.connect(timeout=10)
        HAVE_LOCAL_DB_ACCESS = True
        conn.close()
        del conn
    except:
        HAVE_LOCAL_DB_ACCESS = False

    @classmethod
    def tunneling(cls, tier=None):
        return tier or not cls.HAVE_LOCAL_DB_ACCESS

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
            if isinstance(credentials, str):
                return credentials
            elif isinstance(credentials, bytes):
                return credentials("utf-8")
            else:
                username, password = credentials
                if isinstance(username, bytes):
                    username = username.decode("utf-8")
                if isinstance(password, bytes):
                    password = password.decode("utf-8")
                return cls.windows_login(username, password, tier)
        if isinstance(tier, bytes):
            tier = tier.decode("utf-8")
        if isinstance(comment, bytes):
            comment = comment.decode("utf-8")
        if isinstance(credentials, bytes):
            credentials = credentials.decode("utf-8")
        if isinstance(credentials, str):
            return Session(credentials, tier)
        user, password = credentials
        if isinstance(user, bytes):
            user = user.decode("utf-8")
        if isinstance(password, bytes):
            password = password.decode("utf-8")
        opts = dict(comment=comment, password=password, tier=tier)
        return Session.create_session(user, **opts)

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

        tier = Tier(tier) if tier else cls.TIER
        url = "https://{}/cgi-bin/secure/login.py".format(tier.hosts["APPC"])
        auth = requests.auth.HTTPDigestAuth(username, password)
        response = requests.get(url, auth=auth)
        if not response.ok:
            raise Exception(response.reason)
        return response.text.strip()

    class ResponseSet:
        def __init__(self, node):
            import dateutil.parser
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
        import random
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
        tier = Tier(tier) if tier else cls.TIER
        url = "https://" + tier.hosts["API"]
        request = etree.tostring(commands, encoding="utf-8")
        if cls.DUMP:
            print(request)
        response = requests.post(url, request)
        if not response.ok:
            raise Exception(response.reason)
        if cls.DUMP:
            print(response.text)
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
                if isinstance(doc, str):
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
                encoded_blob = get_text(root.find("CdrDocBlob"))
                if encoded_blob is not None:
                    blob = base64.decodebytes(encoded_blob.encode("ascii"))
                elif opts.get("del_blob") or opts.get("delBlob"):
                    blob = b""
        doc_opts = {
            "id": root.get("Id"),
            "doctype": root.get("Type"),
            "xml": get_text(root.find("CdrDocXml")),
            "blob": blob
        }
        doc = APIDoc(session, **doc_opts)
        comment = opts.get("comment")
        reason = opts.get("reason")
        if comment and not isinstance(comment, str):
            comment = comment.decode("utf-8")
        if reason and not isinstance(reason, str):
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



# ======================================================================
# Legacy functions, classes, and module-level values
# The stuff below here was cleaned up a little bit, but is essentially
# what it was before the Gauss release.
# ======================================================================

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
        with open(f"{ETC}/cdrpw") as fp:
            for line in fp:
                n, p = line.strip().split(":", 1)
                if n == name:
                    return p
    except:
        pass
    return None

def toUnicode(value, default=""):
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
    if not isinstance(value, (str, bytes)):
        return str(value)
    if isinstance(value, str):
        return value
    for encoding in ("ascii", "utf-8", "iso-8859-1"):
        try:
            return value.decode(encoding)
        except:
            pass
    raise Exception(f"unknown encoding for {value!r}")

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
    return "".join(node.itertext("*"))

#----------------------------------------------------------------------
# Normalize a document id to form 'CDRnnnnnnnnnn'.
#----------------------------------------------------------------------
def normalize(id):
    if id is None: return None
    if isinstance(id, type(9)):
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

    if isinstance(id, type(9)):
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
BASEDIR = _Control.TIER.basedir
ETC = _Control.TIER.etc
APPC = _Control.TIER.hosts["APPC"]
FQDN = open(f"{BASEDIR}/etc/hostname").read().strip()
HOST_NAMES = FQDN.split(".")[0], FQDN, "https://" + FQDN
CBIIT_NAMES = APPC.split(".")[0], APPC, "https://" + APPC
OPERATOR = "NCIPDQoperator@mail.nih.gov"
DOMAIN_NAME = FQDN.split(".", 1)[1]
PUB_NAME = HOST_NAMES[0]
URDATE = "2002-06-22"
PYTHON = sys.executable
SMTP_RELAY = "MAILFWD.NIH.GOV"
DEFAULT_LOGDIR = f"{BASEDIR}/Log"
DEFAULT_LOGFILE = f"{DEFAULT_LOGDIR}/debug.log"
PUBLOG = f"{DEFAULT_LOGDIR}/publish.log"
MAILER_LOGFILE = f"{DEFAULT_LOGDIR}/mailer.log"
MANIFEST_NAME = "CdrManifest.xml"
CLIENT_FILES_DIR = f"{BASEDIR}/ClientFiles"
MANIFEST_PATH = f"{CLIENT_FILES_DIR}/{MANIFEST_NAME}"
SENDCMDS_TIMEOUT = 300
SENDCMDS_SLEEP = 3
PDQDTDPATH = f"{BASEDIR}/licensee".replace("/", os.path.sep)
DEFAULT_DTD = f"{PDQDTDPATH}/pdqCG.dtd".replace("/", os.path.sep)
NAMESPACE = "cips.nci.nih.gov/cdr"
LOGGER = Logging.get_logger("cdr-client", level="debug", console=True)
Group = Session.Group
Action = Session.Action
try:
    WORK_DRIVE = _Control.TIER.drive
except:
    WORK_DRIVE = None
TMP = f"{WORK_DRIVE}:/tmp" if WORK_DRIVE else "/tmp"

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
    # 'CTGovProtocol':
    #     ["set:QC CTGovProtocol Set"],
    'DrugInformationSummary':
        ["set:QC DrugInfoSummary Set"],
    # 'GlossaryTerm':
    #     ["set:QC GlossaryTerm Set"],
    # 'GlossaryTerm:rs':            # Redline/Strikeout
    #     ["set:QC GlossaryTerm Set (Redline/Strikeout)"],
    'GlossaryTermConcept':
        ["name:Glossary Term Concept QC Report Filter"],
    'GlossaryTermName':
        ["set:QC GlossaryTermName"],
    'GlossaryTermName:gtnwc':
        ["set:QC GlossaryTermName with Concept Set"],
    # 'InScopeProtocol':
    #     ["set:QC InScopeProtocol Set"],
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
#
# 2017-12-13: This was a mistake, because it masked our ability
#             to catch standard Exception objects. Export the
#             name as part of the cdr module so we don't break
#             existing code more than we have to.
#----------------------------------------------------------------------
Exception = Exception
"""
_baseException = Exception
class Exception(_baseException):
    __baseException = _baseException
    def __str__(self):
        if len(self.args) == 1:
            return unicode(self.args[0])
        else:
            return Exception.__baseException.__str__(self)
del _baseException
"""

#----------------------------------------------------------------------
# Validate date/time strings using strptime.
# Wraps the exception handling.
#----------------------------------------------------------------------
def strptime(str, format):
    """
    Wrap datetime.strptime() in a function that performs the exception
    handling and just returns None if an exception was generated.

    The actual ValueError message from Python may not always be
    understandable by non-programming users.

    Pass:
        str    - Date or datetime as a character string.
        format - Python strptime format string, e.g. '%Y-%m-%d %H:%M:%S'
    """
    tm = None
    try:
        tm = datetime.datetime.strptime(str, format)
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
        self.message = get_text(node)
        self.etype   = node.get('etype', 'other')
        self.elevel  = node.get('elevel', 'fatal')
        self.eref    = node.get('eref')
    def getMessage(self, asUtf8=False):
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
def checkErr(resp, asObject=False):
    opts = dict(errorsExpected=False, asSequence=True, asObjects=asObject)
    errors = getErrors(resp, **opts)
    return errors and errors[0] or None

#----------------------------------------------------------------------
# Extract error elements from XML.
#
# Pass:
#    xmlFragment   - string in which to look for serialized Err elements
#    errorsExpected - if True (the default), then return a generic error
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
#                    2019-09-01: the default is now False (so by
#                                default we return Unicode strings)
#----------------------------------------------------------------------
def getErrors(xmlFragment, **opts):

    # Pull out the options.
    expected = opts.get("errorsExpected", True)
    as_objects = opts.get("asObjects", False)
    as_utf8 = opts.get("asUtf8", False)
    use_dom = as_objects or opts.get("useDom", True)
    as_sequence = opts.get("asSequence", False)

    # Try get a parsed node for the fragment if appropriate.
    if use_dom:
        root = None
        if isinstance(xmlFragment, str):
            xmlFragment = xmlFragment.encode("utf-8")
        try:
            root = etree.fromstring(xmlFragment)
        except Exception as e:
            if as_objects:
                raise Exception(f"getErrors(): {e}")

    if as_sequence or as_objects:

        # Safety check.
        if not isinstance(xmlFragment, (str, bytes)):
            return []

        if use_dom and root is not None:
            errors = [Error(node) for node in root.iter("Err")]
            if expected and not errors:
                return ["Internal failure"]
            if as_objects:
                return errors
            return [e.getMessage(as_utf8) for e in errors]
        if not isinstance(xmlFragment, str):
            xmlFragment = xmlFragment.decode("utf-8")
        errors = Error.getPattern().findall(xmlFragment)
        if not errors and expected:
            errors = ["Internal failure"]
        if as_utf8:
            return [e.encode("utf-8") for e in errors]
        return errors

    elif use_dom and root is not None:
        for node in root.iter("Errors"):
            errors = etree.tostring(node, encoding="utf-8")
        if not errors and expected:
            errors = b"<Errors><Err>Internal failure</Err></Errors>"
        else:
            errors = b""
        return errors if as_utf8 else errors.decode("utf-8")

    else:
        # Compile the pattern for the regular expression.
        pattern = re.compile("<Errors[>\\s].*</Errors>", re.DOTALL)

        # Search for the <Errors> element.
        if not isinstance(xmlFragment, str):
            xmlFragment = xmlFragment.decode("utf-8")
        errors = pattern.search(xmlFragment)
        if errors:
            errors = errors.group()
        elif expected:
            errors = "<Errors><Err>Internal failure</Err></Errors>"
        else:
            errors = ""
        return errors.encode("utf-8") if as_utf8 else errors

#----------------------------------------------------------------------
# Find out if the user for a session is a member of the specified group.
#----------------------------------------------------------------------
def member_of_group(session, group):
    try:
        user = getUser(session, Session(session).user_name)
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
def getEmail(session):
    query = cdrdb.Query("usr u", "u.email")
    query.join("session s", "s.usr = u.id")
    query.where(query.Condition("s.name", session))
    query.where("s.ended IS NULL")
    query.where("u.expired IS NULL")
    try:
        row = query.execute().fetchone()
    except Exception as e:
        return "Error selecting email for session %r: %s".format(session, e)
    if not row:
        return "Session not found or unauthorized"
    return row.email

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

    query = cdrdb.Query("query_term", "value")
    query.where(query.Condition("path", path))
    query.where(query.Condition("doc_id", exNormalize(docId)[1]))
    if conn is not None:
        cursor = conn.cursor()
        rows = query.execute(cursor).fetchall()
        cursor.close()
    else:
        rows = query.execute().fetchall()
    return [row[0] for row in rows]

#----------------------------------------------------------------------
# Extract the text content of a DOM element.
# For nodes parsed by the ancient dom.minidom package; we don't use
# that package any more for new code, but there's still code out there
# which does. At least we don't have to import the old package here.
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
# Validate new and old docs
#----------------------------------------------------------------------
def valPair(session, docType, oldDoc, newDoc, **opts):
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
        Optional tier

    Return:
        If oldDoc is valid and newDoc is not:
            Return list of de-duped errors, with multiples indicated.
        Else:
            Return None.
    """

    # Find out where we are running.
    tier = opts.get("tier")

    # Validate first document
    result = valDoc(session, docType, doc=oldDoc, tier=tier)

    # If no errors, check the new version
    if not getErrors(result, errorsExpected=False):
        result = valDoc(session, docType, doc=newDoc, tier=tier)
        return deDupErrs(result)

    # Else return empty list
    return []

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
    root = etree.fromstring(errXml)
    for err in root.iter("Err"):
        errString = get_text(err)
        errs[errString] = errs.get(errString, 0) + 1

    # Prepare results list
    result = []
    for err in sorted(errs):
        errString = err
        if errs[err] > 1:
            errString += " (%d times)" % errs[err]
        result.append(errString)

    return result

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

    # Create the query
    fields = (
        "d.title",
        "t.name",
        "d.active_status",
        "d.val_status",
        "d.val_date",
        "d.first_pub"
    )
    query = cdrdb.Query("all_docs d", *fields)
    query.join("doc_type t", "t.id = d.doc_type")
    query.where(query.Condition("d.id", idNum))

    # Run the query
    if conn is None:
        row = query.execute().fetchone()
    else:
        cursor = conn.cursor()
        row = query.execute(cursor).fetchone()
        cursor.close()

    # Doc not found?
    if not row:
        raise Exception("getAllDocsRow() found no match for doc %s" % docId)

    # Package the results for the caller
    return dict(
        id=idNum,
        title=row.title,
        doc_type=row.name,
        active_status=row.active_status,
        val_status=row.val_status,
        val_date=row.val_date,
        first_pub=row.first_pub
    )

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

    query = cdrdb.Query("format", "name").order("name")
    if conn is None:
        rows = query.execute().fetchall()
    else:
        cursor = conn.cursor()
        rows = query.execute(cursor).fetchall()
        cursor.close()
    return [row.name for row in rows]

#----------------------------------------------------------------------
# Get a list of enumerated values for a CDR schema simpleType.
#----------------------------------------------------------------------
def getSchemaEnumVals(schemaTitle, typeName, **opts):
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
                      found in the schema (optional keyword argument)

    Return:
        Array of string values.

    Raises:
        Exception if schemaTitle or simpleName not found.
    """

    # Fetch and parse the schema
    cursor = cdrdb.connect().cursor()
    xml = APIDoc.get_schema_xml(schemaTitle, cursor)
    root = etree.fromstring(xml.encode('utf-8'))

    # Search for enumerations of the simple type
    # Note what we have to do with namespaces - won't work without that
    path  = "//xsd:simpleType[@name='%s']//xsd:enumeration" % typeName
    nodes = root.xpath(path, namespaces=dict(xsd=Schema.NS))
    if not nodes:
        message = "type %r not found in %r" % (typeName, schemaTitle)
        raise Exception(message)

    # Pull the values into a string sequence
    valList = [node.get("value") for node in nodes]

    # Return in desired order
    if opts.get("sorted"):
        return sorted(valList)
    return valList

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
# Gets the email addresses for members of a group.
#----------------------------------------------------------------------
def getEmailList(groupName):
    query = cdrdb.Query("usr u", "u.email")
    query.join("grp_usr gu", "gu.usr = u.id")
    query.join("grp g", "g.id = gu.grp")
    query.where(query.Condition("g.name", groupName))
    return [row.email for row in query.execute().fetchall() if row.email]


class EmailAttachment:
    """Object for a mime attachment"""
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


class EmailMessage:
    """Wrap the Python mail services"""

    def __init__(self, sender, recips, **opts):
        """
        Assemble and send an SMTP message

        Required positional arguments:
          sender - string in the form user@example.com (only ascii characters);
                   we can't do anything fancier for now because of bugs in
                   the Python libraries
                     - https://bugs.python.org/issue33398
                     - https://bugs.python.org/issue24218
                     - https://bugs.python.org/issue34424
          recips - one or more recipient addresses; same format (and
                   issues) as for the sender

        Optional keyword arguments:
          subject - Unicode string for the message subject
          body - Unicode string for the message body; if there are no
                 attachments, the default is a single period (because
                 NIH's mail server won't deliver messages whose content
                 consists of only whitespace)
          subtype - set to 'html' to override the default mime type of
                    'text/plain'
          attachments - optional sequence of cdr.EmailAttachment objects
        """

        # Enforce required positional arguments.
        if not sender:
            raise Exception("EmailMessage(): missing sender")
        if not recips:
            raise Exception("EmailMessage(): missing recipients")

        # Capture the caller's values.
        self.__sender = sender
        self.__recips = recips
        self.__opts = opts

    def send(self):
        """Use the NIH mail server to deliver the message"""

        try:
            from smtplib import SMTP
            with SMTP(SMTP_RELAY) as server:
                args = self.sender, self.recips, self.smtp_message.as_string()
                server.sendmail(*args)
        except Exception as e:
            LOGGER.exception("send_mail failure")
            raise Exception(f"send_mail: {e}")

    @property
    def attachments(self):
        """Make sure the attachments are the right type"""

        if not hasattr(self, "_attachments"):
            self._attachments = self.__opts.get("attachments")
            if self._attachments is not None:
                if not isinstance(self._attachments, (list, tuple)):
                    self._attachments = [self._attachments]
                for attachment in self._attachments:
                    if not isinstance(attachment, EmailAttachment):
                        t = type(attachment)
                        raise Exception(f"EmailMessage(): attachment is {t}")
        return self._attachments

    @property
    def body(self):
        """
        Make sure we have either a body or at least one attachment

        If we have a body, make sure it is Unicode.
        """

        if not hasattr(self, "_body"):
            self._body = self.__opts.get("body")
            if self._body is None or not self._body.strip():
                if not self.attachments:
                    self._body = "."
            if isinstance(self._body, bytes):
                self._body = str(self._body, "utf-8")
        return self._body

    @property
    def recips(self):
        """Coerce recips to sequence of Unicode strings"""

        if not hasattr(self, "_recips"):
            self._recips = self.__recips
            if isinstance(self._recips, tuple):
                self._recips = list(self._recips)
            elif not isinstance(self._recips, list):
                self._recips = [self._recips]
            for i, recip in enumerate(self._recips):
                if isinstance(recip, bytes):
                    self._recips[i] = str(recip, "ascii")
        return self._recips

    @property
    def sender(self):
        """Force sender to Unicode string"""

        if not hasattr(self, "_sender"):
            self._sender = self.__sender
            if isinstance(self._sender, bytes):
                self._sender = str(self._sender, "ascii")
        return self._sender

    @property
    def smtp_message(self):
        """
        Assemble the SMTP message object
        """

        # Create the object if not already cached.
        if not hasattr(self, "_smtp_message"):
            from email.message import EmailMessage as EM
            message = EM()
            if self.body:
                message.set_content(self.body, subtype=self.subtype)

            # Add attachments if present.
            if self.attachments:
                from email.mime.multipart import MIMEMultipart
                wrapper = MIMEMultipart()
                wrapper.preamble = "This is a multipart MIME message"
                wrapper.attach(message)
                for attachment in self.attachments:
                    wrapper.attach(attachment.mime_object)
                message = wrapper

            # Plug in the headers.
            message["From"] = self.sender
            message["To"] = ", ".join(self.recips)
            if self.subject:
                message["Subject"] = self.subject

            # Cache the object.
            self._smtp_message = message

        # Return the (possibly cached) object.
        return self._smtp_message

    @property
    def subtype(self):
        if not hasattr(self, "_subtype"):
            stp = self.__opts.get("subtype", "plain")
            if isinstance(stp, bytes):
                stp = str(stp, "utf-8")
            if stp not in ("plain", "html"):
                raise Exception(f"EmailMessage(): invalid subtype {stp!r}")
            self._subtype = stp
        return self._subtype

    @property
    def subject(self):
        """Coerce subject to Unicode string"""

        if not hasattr(self, "_subject"):
            self._subject = self.__opts.get("subject")
            if isinstance(self._subject, bytes):
                self._subject = str(self._subject, "utf-8")
        return self._subject

    def __str__(self):
        """Serialize the message"""

        return str(self.smtp_message)


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
def run_command(command, **opts):
    """
    Run a shell command

    Required positional argument:

        command
             string of the command to be run, with arguments as appropriate

    Optional keyword arguments:

        binary
            if True, use bytes for io instead of text

        merge_output
            if True, combine stdin and stderr into a single stream; default
            is False

        shell
            if False, don't invoke a separate command shell to process
            the command (safer); default is True

    Return:
        subprocess.CompletedProcess object, with properties:
           - args
           - returncode
           - stdout
           - stderr
    """

    import subprocess

    spopts = dict(stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    if not opts.get("binary"):
        spopts["encoding"] = "utf-8"
    if opts.get("merge_output"):
        spopts["stderr"] = subprocess.STDOUT
    else:
        spopts["stderr"] = subprocess.PIPE
    if opts.get("shell", True):
        spopts["shell"] = True
    try:
        return subprocess.run(command, **spopts)
    except:
        LOGGER.exception("failure running command %s", command)
        raise

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

    import tempfile
    if "TMP" in os.environ:
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
        raise AttributeError()

#----------------------------------------------------------------------
# Remove all lines from a multi-line string (e.g., an XML doc)
# that are empty or contain nothing but whitespace.
#----------------------------------------------------------------------
def stripBlankLines(s):
    # Make a sequence
    inSeq = s.replace("\r", "").split("\n")

    # Copy non blank lines to new sequence
    outSeq = []
    for line in inSeq:
        if line.strip():
            outSeq.append(line)

    # Return them as a string with newlines at each line end
    return "\n".join(outSeq);

class Normalizer:
    ELEMENT_ONLY = {}

    def __init__(self):
        self.session = Session("guest")
    def transform(self, xml):
        try:
            root = etree.fromstring(xml)
        except:
            if isinstance(xml, str):
                root = etree.fromstring(xml.encode("utf-8"))
            else:
                raise
        element_only = Normalizer.ELEMENT_ONLY.get(root.tag)
        if element_only is None:
            element_only = set()
            schema = Doctype(self.session, name=root.tag).schema
            for line in str(DTD(self.session, name=schema)).splitlines():
                if line.startswith("<!ELEMENT") and "#PCDATA" not in line:
                    element_only.add(line.split()[1])
            Normalizer.ELEMENT_ONLY[root.tag] = element_only
        if element_only:
            for node in root.iter("*"):
                if node.tag in element_only:
                    self.scrub(node)
        xml = etree.tostring(root, pretty_print=True, encoding="unicode")
        return etree.canonicalize(xml)
    def scrub(self, node):
        node.text = None
        for child in node.findall("*"):
            child.tail = None

#----------------------------------------------------------------------
# Takes a utf-8 string for an XML document and creates a utf-8 string
# suitable for comparing two versions of XML documents by normalizing
# non-essential differences away.  Used by compareDocs() (below).
#----------------------------------------------------------------------
def normalizeDoc(utf8DocString):
    return etree.tostring(etree.fromstring(utf8DocString))

#----------------------------------------------------------------------
# Compares two XML documents by normalizing each.  Returns non-zero
# if documents are different; otherwise zero.  Expects each document
# to be passed as utf8-encoded documents.
# 2017-12-13: Python 3 dropped the cmp() function; this is what Guido
#             recommends as it's replacement.
#----------------------------------------------------------------------
def compareXmlDocs(utf8DocString1, utf8DocString2):
    if utf8DocString1 is utf8DocString2: return 0
    a, b = normalizeDoc(utf8DocString1), normalizeDoc(utf8DocString2)
    return (a > b) - (a < b)

#----------------------------------------------------------------------
# Compare two XML documents by normalizing each.
# Returns the output of a textual differencer as a sequence of lines.
# See Python difflib.Differ.compare() for diff format.
#   Pass:
#     2 utf8 strings to compare
#     chgOnly  - True=only show changed lines, else show all (optional
#                keyword argument; default is True)
#   Returns:
#     Difference, with or without context, as utf-8 string.
#     Context, if present, is pretty-printed with indentation.
#----------------------------------------------------------------------
def diffXmlDocs(utf8DocString1, utf8DocString2, **opts):

    import difflib

    # Normalize
    serialize_opts = dict(pretty_print=True, with_tail=True, encoding="utf-8")
    parser = etree.XMLParser(remove_comments=True)
    root1 = etree.fromstring(utf8DocString1, parser=parser)
    root2 = etree.fromstring(utf8DocString2, parser=parser)
    xml1 = etree.tostring(root1, **serialize_opts).decode("utf-8")
    xml2 = etree.tostring(root2, **serialize_opts).decode("utf-8")

    # Compare
    diffObj = difflib.Differ()
    diffSeq = diffObj.compare(xml1.splitlines(1),xml2.splitlines(1))

    # If caller only wants changed lines, drop all lines with leading space
    if opts.get("chgOnly", True):
        chgSeq = []
        for line in diffSeq:
            if line[0] != ' ':
                chgSeq.append (line)
        # Return them as a (possibly empty) string
        diffText = "".join(chgSeq)

    # Else return entire document as a string
    else:
        diffText = "".join(diffSeq)

    # Convert output back to utf-8; normalization made it unicode
    if isinstance(diffText, str):
        diffText = diffText.encode('utf-8')

    return diffText

def getVersionedBlobChangeDate(credentials, doc_id, version, **opts):
    """
    Determine the last date a versioned blob changed

    Pass:
      credentials - result of login
      doc_id - unique ID for the CDR document connected with the blob
      version - version number of the document
      tier - optional name of hosting tier (e.g., 'DEV')
      conn - optional connection to the database
    """

    tier = opts.get("tier")
    conn = opts.get("conn") or cdrdb.connect(user="CdrGuest", tier=tier)
    cursor = conn.cursor()
    query = cdrdb.Query("version_blob_usage", "blob_id")
    query.where(query.Condition("doc_id", doc_id))
    query.where(query.Condition("doc_version", version))
    row = query.execute(cursor).fetchone()
    if row:
        blob_id = row[0]
        join_conditions  ="u.doc_id = v.id", "u.doc_version = v.num"
        query = cdrdb.Query("doc_version v", "v.num", "v.dt")
        query.join("version_blob_usage u", *join_conditions)
        query.where(query.Condition("u.blob_id", blob_id))
        query.where(query.Condition("u.doc_id", doc_id))
        query.where(query.Condition("u.doc_version", version, "<="))
        rows = query.order("v.num DESC").execute(cursor).fetchall()
    cursor.close()
    if not opts.get("conn"):
        conn.close()
    if not row:
        message = "No blob found for document %s version %s"
        raise Exception(message % (doc_id, version))
    if not rows:
        raise Exception("No versions for CDR%s blob" % doc_id)
    last_version, last_date = rows[0]
    for prev_version, prev_date in rows[1:]:
        if prev_version != last_version - 1:
            break
        last_version, last_date = prev_version, prev_date
    return last_date

def emailSubject(text='No Subject'):
    """
    Standardize the email subject format

    Pass:
      text - string for the subject's main content

    Return:
      passed string prefixed by "[TIER-NAME] "
    """

    return "[%s] %s" % (_Control.TIER.name, text)

_lockedFiles = {}
""" Static dictionary of locked files"""

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
    import atexit

    # Nested calls are not allowed
    if fname in _lockedFiles:
        message = 'File "%s" locked twice without intervening unlock' % fname
        raise Exception(message)

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

def removeLockFile(fname):
    """
    Remove a file created by createLockFile.

    Need only be called if the caller wants to release the resource
    before ending his program.
    """
    # Only remove the file if we created it.
    # It is illegal to remove a lock created by another process
    if fname not in _lockedFiles:
        raise Exception('File "%s" not locked in this process' % fname)
    del(_lockedFiles[fname])

    # If we got here, this ought to work, propagate exception if it fails
    os.remove(fname)

def removeAllLockFiles():
    """
    Remove any outstanding lockfiles for this process.

    Removes any files that were created by createLockFile() for which
    removeLockFile() was never called.
    """
    for fname in list(_lockedFiles.keys()):
        removeLockFile(fname)

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
    elif isinstance(referenceDate, (str, bytes)):
        separator = "-" if isinstance(referenceDate, str) else b"-"
        y, m, d = referenceDate.split(separator)
        referenceDate = datetime.date(int(y), int(m), int(d))
    elif not isinstance(referenceDate, datetime.date):
        raise Exception("invalid type for referenceDate")
    return referenceDate + datetime.timedelta(offset)

def getBoardNames(boardType='all', display='full', tier=None):
    """
    Get a list of all the PDQ board names

    Gets the list of names of all organizations with an organization
    type of 'PDQ Editorial Board' or 'PDQ Advisory Board').

    This is frequently used to create reports by board.

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

    n_path = "/Organization/OrganizationNameInformation/OfficialName/Name"
    query = cdrdb.Query("query_term n", "n.doc_id", "n.value")
    query.join("query_term t", "t.doc_id = n.doc_id")
    query.join("document d", "d.id = n.doc_id")
    if boardType.lower() == 'editorial' or boardType.lower()== 'advisory':
        type_string = "PDQ %s Board" % boardType.capitalize()
        query.where(query.Condition("t.value", type_string))
    else:
        types = "PDQ Editorial Board", "PDQ Advisory Board"
        query.where(query.Condition("t.value", types, "IN"))
    query.where("t.path = '/Organization/OrganizationType'")
    query.where(query.Condition("n.path", n_path))
    query.where("d.active_status = 'A'")
    conn = cdrdb.connect(name="CdrGuest", tier=tier)
    cursor = conn.cursor()
    rows = query.execute(cursor).fetchall()
    cursor.close()
    conn.close()
    if display == "short":
        pairs = [(row.doc_id, row.value.replace("PDQ ", "")) for row in rows]
    elif display == "custom":
        pairs = []
        IACT = "Integrative, Alternative, and Complementary Therapies"
        for row in rows:
            name = row.value.replace("PDQ ", "").replace(IACT, "IACT")
            pairs.append((row.doc_id, name))
    else:
        pairs = [tuple(row) for row in rows]
    return dict(pairs)

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

    return 'English', 'Spanish'

def getSummaryAudiences():
    """
    Return a list of all Audience values that are used for Summaries.

    See getSummaryLanguages() for query vs. literals discussion.
    """

    return 'Health professionals', 'Patients'

def extract_board_name(doc_title):
    """
    Pull out the portion of an editorial board name used for menu options.

    Pass:
      doc_title - string pulled from the `all_docs.title` column

    Return:
      String for the board's canonical name
    """

    board_name = doc_title.split(";")[0].strip()
    board_name = board_name.replace("PDQ ", "").strip()
    board_name = board_name.replace(" Editorial Board", "").strip()
    if board_name.startswith("Cancer Complementary"):
        board_name = board_name.replace("Cancer ", "").strip()
    return board_name

def get_image(doc_id, **opts):
    """
    Get the bytes for a CDR image, possibly transformed

    Pass:
      doc_id - required positional argument for CDR document ID
      width - optional integer restraining maximum width in pixels
      height - optional integer restraining maximum height in pixels
      quality - optional positive number (max 100, default 85)
      sharpen - optional floating point number for enhancing sharpness
      return_image - if True, return Image object instead of bytes
      return_stream - if True, return BytesIO object instead of bytes

    Return:
      bytes for image binary, unless return_... option specified
    """

    # Load the object's bytes
    session = opts.get("session", Session("guest"))
    if not isinstance(session, Session):
        session = Session(session)
    doc = APIDoc(session, id=doc_id, version=opts.get("version"))

    # If no transformations are requested, we're done.
    mods = ("width", "height", "quality", "sharpen", "return_image",
            "return_stream")
    if not any([opts.get(name) for name in mods]):
        return doc.blob

    # Get an image object so we can apply the requested modifications.
    from PIL import Image, ImageEnhance
    from io import BytesIO
    image = Image.open(BytesIO(doc.blob))

    # Scale the image if requested.
    if opts.get("width") or opts.get("height"):
        width, height = image_width, image_height = image.size
        max_width, max_height = opts.get("width"), opts.get("height")
        if max_width is not None and width > max_width:
            ratio = 1.0 * image_height / image_width
            width = max_width
            height = int(round(width * ratio))
        if max_height is not None and height > max_height:
            ratio = 1.0 * image_width / image_height
            height = max_height
            width = int(round(height * ratio))
        image = image.resize((width, height), Image.ANTIALIAS)

    # Apply sharpening if requested.
    if opts.get("sharpen"):
        tool = ImageEnhance.Sharpness(image)
        image = tool.enhance(float(opts.get("sharpen")))

    # Return what the caller asked for.
    if opts.get("return_image"):
        return image
    fp = BytesIO()
    image.save(fp, "JPEG", quality=int(opts.get("quality", 85)))
    if opts.get("return_stream"):
        return fp
    return bytes(fp.getvalue())

def prepare_pubmed_article_for_import(node):
    """
    Transform XML from NLM's PubMed for insertion into a CDR Citation doc

    We used to pretty much accept what NLM gave us, but that broke our
    software every time they updated their schemas and DTD (which
    happened a lot), so we now just cherry-pick the pieces we need,
    which means our own schema can be much more stable.

    Pass:
      node - parsed XML object for a PubmedArticle block

    Return:
      transformed PubmedArticle node object
    """

    for citation in node.findall("MedlineCitation"):
        namespace = "http://www.w3.org/1998/Math/MathML"
        mml_math = "{{{}}}math".format(namespace)
        namespaces = dict(mml=namespace)
        for child in citation.xpath("//mml:math", namespaces=namespaces):
            if child.tail is None:
                child.tail = "[formula]"
            else:
                child.tail = "[formula]" + child.tail
        etree.strip_elements(citation, mml_math, with_tail=False)
        xslt = APIDoc.load_single_filter(Session("guest"), "Import Citation")
        citation = etree.fromstring(str(xslt(citation)))
        article = etree.Element("PubmedArticle")
        article.append(citation)
        return article
    raise Exception("MedlineCitation child not found")
