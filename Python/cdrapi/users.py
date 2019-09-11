"""
Control for who can use the CDR and what they can do
"""

import binascii
import datetime
import hashlib
import logging
import random
import re
import socket
import string
import threading
import time
from cdrapi import db
from cdrapi.settings import Tier


class Session:
    """
    Information from a row in the `session` table of the cdr database

    Attributes:
      tier - instance of the `session.Tier` class
      name - unique (for this tier) string identifier for the session
      conn - connection to the cdr database with read/write privileges
      cursor - database cursor created for this connection
      active - True as long as the session has not expired
      id - primary key for the session's row in the the session table
      user_id - primary key in the usr table for this session's user
      user_name - account name for this user
      cache - session-specific cache of filtering objects
    """

    INACTIVE = "DATEDIFF(hour, last_act, GETDATE()) > 24"
    CONDITIONS = "ended IS NULL", "name <> 'guest'", INACTIVE
    CONDITIONS = " AND ".join(CONDITIONS)
    SELECT = "SELECT COUNT(*) FROM session WHERE {}".format(CONDITIONS)
    UPDATE = "UPDATE session SET ended = GETDATE() WHERE {}".format(CONDITIONS)
    CLEAR_FAILURE_LOGGED = False

    def __init__(self, name, tier=None, loglevel="INFO"):
        """
        Populate the attributes for the object if the session is active

        This constructor does not create a new session. That task is
        handled by the class factory method `create_session()` below.

        First mark any sessions which have been inactive for 24 hours
        as expired. This way we prevent the (unlikely) loophole for
        a session which should have been expired from renewing itself.

        Pass:
          name - unique (for this tier) string identifier for the session
          tier - optional string or `Tier` object identifying which server
                 the session lives on
          loglevel - logging level to use (default "INFO")

        Raise
          `Exception` if the session does not exist or is expired
        """

        self.name = name
        self.tier = tier if isinstance(tier, Tier) else Tier(tier)
        opts = dict(level=loglevel, rolling=True, tier=self.tier)
        self.local = self.Local(**opts)
        opts["dbconn"] = self.LoggingDBConnection()
        self.logger = self.tier.get_logger("session", **opts)
        try:
            self.cursor.execute(self.SELECT)
            if self.cursor.fetchone()[0] > 0:
                self.cursor.execute(self.UPDATE)
                self.conn.commit()
        except:
            if not Session.CLEAR_FAILURE_LOGGED:
                self.logger.exception("Unable to clear stale sessions")
                Session.CLEAR_FAILURE_LOGGED = True
        query = db.Query("session s", "s.id", "u.id", "u.name")
        query.join("open_usr u", "u.id = s.usr")
        query.where(query.Condition("s.name", name))
        query.where("s.ended IS NULL")
        row = query.execute(self.cursor).fetchone()
        if row is None:
            self.logger.warning("query: %s (%s)", query, name)
            raise Exception("Invalid or expired session: {!r}".format(name))
        self.active = True
        self.id, self.user_id, self.user_name = row
        self.cache = self.Cache()
        update = "UPDATE session SET last_act = GETDATE() WHERE id = ?"
        try:
            self.cursor.execute(update, (self.id,))
            self.conn.commit()
        except:
            self.logger.exception("Unable to set last_act")
            raise

    @property
    def conn(self):
        return self.local.conn

    @property
    def cursor(self):
        return self.local.cursor

    def log(self, what):
        """
        Record what we're doing
        """

        self.logger.info("%s calling %s", self.name, what)

    def logout(self):
        """
        Close the current session on request

        Called by:
          cdr.logout()
          client XML wrapper command CdrLogoff

        Return:
          None
        """

        if not self.active:
            raise Exception("session expired")
        update = "UPDATE session SET ended = GETDATE() WHERE id = ?"
        self.cursor.execute(update, (self.id,))
        self.conn.commit()
        self.active = False
        self.log("Session.logout({})".format(self.name))

    def duplicate(self):
        """
        Create a new session for the same user account

        Useful when a long-running job is requested by a user who
        might log out from the session from which the request was
        submitted before the job had completed.

        Called by:
          cdr.dupSession()
          client XML wrapper command CdrDupSession

        Return:
          `Session` object
        """

        self.log("Session.duplicate()")
        query = db.Query("usr u", "u.id", "u.name")
        query.join("session s", "s.usr = u.id")
        query.where("s.ended IS NULL")
        query.where(query.Condition("s.name", self.name))
        row = query.execute(self.cursor).fetchone()
        if not row:
            raise Exception("Can't duplicate invalid or expired session")
        name = row.name
        comment = "Session duplicated from id={}".format(self.name)
        opts = dict(comment=comment, tier=self.tier.name)
        return self.__create_session(self.conn, row.id, **opts)

    def can_do(self, action, doctype=None):
        """
        Determine whether the account can perform a specific action

        Called by:
          cdr.canDo()
          client XML wrapper command CdrCanDo
          numerous internal API methods

        Pass:
          action - string for the name of the action
          doctype - optional string for the document type on which
                    the action is to be performed

        Return:
          True if the account is permitted to do what is proposed
        """

        if not self.active:
            self.logger.warning("session {} expired".format(self.name))
            raise Exception("session expired")
        if doctype:
            self.log("Session.can_do({}, {})".format(action, doctype))
        else:
            self.log("Session.can_do({})".format(action))
        query = db.Query("grp_usr", "COUNT(*)")
        query.join("grp_action", "grp_action.grp = grp_usr.grp")
        query.join("action", "action.id = grp_action.action")
        query.join("doc_type", "doc_type.id = grp_action.doc_type")
        query.where(query.Condition("grp_usr.usr", self.user_id))
        query.where(query.Condition("action.name", action))
        query.where(query.Condition("doc_type.name", doctype or ""))
        return query.execute(self.cursor).fetchone()[0] > 0

    def get_permissions(self):
        """
        Build a dictionary showing what this user is allowed to do

        Return:
          dictionary indexed by action name, with sets of document type
          names on which the user is allowed to perform the action as
          dictionary values; for actions which are not document-type
          specific, the value is an empty set
        """
        query = db.Query("action a", "a.name AS action", "t.name AS doctype")
        query.join("grp_action g", "g.action = a.id")
        query.join("doc_type t", "t.id = g.doc_type")
        query.join("grp_usr u", "u.grp = g.grp")
        query.where(query.Condition("u.usr", self.user_id))
        rows = query.execute(self.cursor).fetchall()
        permissions = dict()
        for action, doctype in rows:
            if action not in permissions:
                permissions[action] = set()
            doctype = doctype.strip()
            if doctype:
                permissions[action].add(doctype)
        return permissions

    def check_permissions(self, pairs):
        """
        Filter the results of `get_permissions()` for requested subset

        Called by:
          cdr.checkAuth()
          client XML wrapper command CdrCheckAuth

        Pass:
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

        self.log("Session.check_permissions({!r})".format(pairs))
        result = dict()
        permissions = self.get_permissions()
        for action, doctype in pairs:
            if action == "*":
                for a in permissions:
                    if doctype == "*":
                        result[a] = set(permissions[a])
                    elif doctype in permissions[a]:
                        if a not in result:
                            result[a] = set()
                        result[a].add(doctype)
            elif action:
                if doctype == "*":
                    result[action] = set(permissions[action])
                elif doctype in permissions[action]:
                    if action not in result:
                        result[action] = set()
                    result[action].add(doctype)
        return result

    def list_actions(self):
        """
        Return a sorted sequence of the names of CDR actions

        Called by:
          cdr.getActions()
          client XML wrapper command CdrListActions

        Return:
          dictionary of authorizable actions, indexed by action name, with
          the value of a flag ("Y" or "N") indicating whether the action
          must be authorized on a per-doctype basis
        """

        if not self.can_do("LIST ACTIONS"):
            raise Exception("LIST ACTIONS action not authorized for this user")
        self.log("Session.list_actions()")
        query = db.Query("action", "name", "doctype_specific").order("name")
        rows = query.execute(self.cursor).fetchall()
        return [self.Action(*row) for row in rows]

    def get_action(self, name):
        """
        Fetch the information stored in a row of the `action` table

        Called by:
          cdr.getAction()
          client XML wrapper command CdrGetAction

        Return:
          `Session.Action` object
        """

        if not self.can_do("GET ACTION"):
            raise Exception("GET ACTION action not authorized for this user")
        self.log("Session.get_action({})".format(name))
        query = db.Query("action", "id", "name", "doctype_specific", "comment")
        query.where(query.Condition("name", name))
        row = query.execute(self.cursor).fetchone()
        if not row:
            return None
        action = self.Action(*row[1:])
        action.id = row[0]
        return action

    def get_group(self, name):
        """
        Fetch information about a CDR group

        Called by:
          cdr.getGroup()
          client XML wrapper command CdrGetGrp

        Pass:
          session - `Session` object requesting the operation

        Return:
          `Session.Group` object
        """

        if not self.can_do("GET GROUP"):
            raise Exception("GET GROUP action not authorized for this user")
        self.log("Session.get_group({})".format(name))
        query = db.Query("grp", "id", "name", "comment")
        query.where(query.Condition("name", name))
        row = query.execute(self.cursor).fetchone()
        if not row:
            return None
        group_id, group_name, comment = row
        query = db.Query("usr u", "name").order("name")
        query.join("grp_usr g", "g.usr = u.id")
        query.where(query.Condition("g.grp", group_id))
        users = [row[0] for row in query.execute(self.cursor).fetchall()]
        query = db.Query("grp_action g", "a.name", "t.name")
        query.join("action a", "a.id = g.action")
        query.join("doc_type t", "t.id = g.doc_type")
        query.where(query.Condition("g.grp", group_id))
        query.order("a.name", "t.name")
        actions = {}
        for action, doctype in query.execute(self.cursor).fetchall():
            if action not in actions:
                actions[action] = []
            actions[action].append(doctype or None)
        return self.Group(id=group_id, name=group_name, comment=comment,
                          users=users, actions=actions)

    def list_groups(self):
        """
        Return a sorted sequence of the names of CDR groups

        Called by:
          cdr.getGroups()
          client XML wrapper command CdrListGrps

        Return:
          sorted sequence of CDR group name strings
        """

        if not self.can_do("LIST GROUPS"):
            raise Exception("LIST GROUPS action not authorized for this user")
        self.log("Session.list_groups()")
        query = db.Query("grp", "name").order("name")
        return [row.name for row in query.execute(self.cursor).fetchall()]

    def list_users(self):
        """
        Fetch the names for the active CDR user accounts

        Called by:
          cdr.getUsers()
          client XML wrapper command CdrListUsrs

        Return:
          sequence of strings for user account names
        """

        self.log("Session.list_users()")
        query = db.Query("usr", "name").where("expired is NULL").order("name")
        return [row.name for row in query.execute(self.cursor).fetchall()]

    def log_client_event(self, description):
        """
        Record client action (typically local save of a document)

        Useful for (among other things) assisting users find backup copies
        of their work in the event of a computer failure, as well as for
        verify their accounts of what happened during troubleshooting
        exercises.

        Called by:
          cdr.log_client_event()
          client XML wrapper command CdrLogClientEvent

        Pass:
          description - required string capturing the details of a local
                        save action
        """

        self.log("Session.log_client_event()")
        fields = "event_time, event_desc, session"
        insert = "INSERT INTO client_log ({}) VALUES (GETDATE(), ?, ?)"
        self.cursor.execute(insert.format(fields), (description, self.id))
        self.conn.commit()

    def save_client_trace_log(self, log_data):
        """
        Allows the CDR client software to post a copy of the local log

        Called by:
          cdr.save_client_trace_log()
          client XML wrapper command CdrSaveClientTraceLog

        Pass:
          log_data - required string containing contents of log file
                     populated by the CDR client loader program
        """

        self.log("Session.save_client_trace_log()")
        user = session = None
        match = re.search(r"logon\(([^,]+), ([^)]+)\)", log_data)
        if match:
            user = match.group(1)
            session = match.group(2)
        fields = "log_saved, cdr_user, session_id, log_data"
        values = user, session, log_data
        insert = "INSERT INTO dll_trace_log ({}) VALUES (GETDATE(), ?, ?, ?)"
        self.cursor.execute(insert.format(fields), values)
        self.conn.commit()
        self.cursor.execute("SELECT @@IDENTITY AS id")
        return int(self.cursor.fetchone().id)

    def __str__(self):
        """
        Support code which thinks it's got a string instead of an object
        """

        return self.name or ""

    def __repr__(self):
        """
        Support code which thinks it's got a string instead of an object
        """

        return self.name or ""

    @classmethod
    def create_session(cls, user, **opts):
        """
        Insert a new row into the `session` database table

        Assumes the account's credentials have been verified.
        For requests coming from external clients (without read/write
        database access) this will have been handled by Windows
        through a protected IIS folder.

        Pass:
          user - unique string naming the account
          password - not needed for network accounts logging on locally
          comment - optional string describing the session request
          tier - optional string identifying which server should
                 be used for the session

        Return:
          `Session` object
        """

        tier = opts.get("tier")
        conn = cls.LoggingDBConnection()
        logger = Tier(tier).get_logger("session", rolling=True, dbconn=conn)
        conn = db.connect(tier=tier)
        with conn.cursor() as cursor:
            query = db.Query("usr", "id", "hashedpw")
            query.where(query.Condition("name", user))
            query.where("expired IS NULL")
            row = query.execute(cursor).fetchone()
        if not row:
            raise Exception("Unknown or expired user: {}".format(user))
        uid, hashedpw = row
        if hashedpw is not None:
            hexhash = binascii.hexlify(hashedpw).upper()
            if not isinstance(hexhash, str):
                hexhash = hexhash.decode("ascii")
            if hexhash != Session.User.EMPTY_PW:
                password = opts.get("password")
                if not password:
                    raise Exception("Missing password")
                if isinstance(password, str):
                    password = password.encode("utf-8")
                submitted = hashlib.sha1(password).hexdigest().upper()
                if hexhash != submitted:
                    logger.warning("%s vs %s", hexhash, submitted)
                    raise Exception("Invalid credentials")
        return cls.__create_session(conn, uid, **opts)

    @classmethod
    def __create_session(cls, conn, uid, **opts):
        """
        Insert a row in the `session` table

        Pass:
          conn - reference to database connection object
          uid - required integer for the CDR user account
          tier - optional name of tier (e.g., "DEV")
          comment - optional string for comment to store in the session row
        """

        secs, msecs = [int(n) for n in "{:.9f}".format(time.time()).split(".")]
        secs = secs & 0xFFFFFFFF
        msecs = msecs & 0xFFFFFF
        letters = string.ascii_uppercase + string.digits
        suffix = "".join([random.choice(letters) for i in range(12)])
        name = "{:08X}-{:06X}-{:03d}-{}".format(secs, msecs, uid, suffix)
        ip_address = socket.gethostbyname(socket.gethostname()) or None
        cols = "name, usr, comment, initiated, last_act, ip_address"
        vals = "?, ?, ?, GETDATE(), GETDATE(), ?"
        insert = "INSERT INTO session({}) VALUES({})".format(cols, vals)
        cursor = conn.cursor()
        cursor.execute(insert, (name, uid, opts.get("comment"), ip_address))
        conn.commit()
        session = Session(name, opts.get("tier"))
        session.log("login({})".format(name))
        return session

    class Action:
        """
        Information about a permission-controlled CDR action

        Attributes:
          name - unique display name for the action
          doctype_specific - True iff permissions are specified separately
                             for each document type
          comment - optional string describing the action
          id - primary key for the action in the `action` table
        """

        def __init__(self, name, doctype_specific, comment=None):
            """
            Save the passed values for the object

            Initialize the primary key `id` to `None` (will be
            populated by the `Session.get_action()` method)
            """

            self.name = name
            self.doctype_specific = doctype_specific
            self.comment = comment
            self.id = None

        def add(self, session):
            """
            Create a row in the `action` database table

            Called by:
              cdr.putAction()
              client XML wrapper command CdrAddAction

            Pass:
              session - `Session` object requesting the operation
            """

            session.log("Action.add({!r})".format(self.name))
            if not session.can_do("ADD ACTION"):
                message = "ADD ACTION action not authorized for this user"
                raise Exception(message)
            if not self.name or not self.name.strip():
                raise Exception("Missing action name")
            query = db.Query("action", "COUNT(*)")
            query.where(query.Condition("name", self.name.strip()))
            if query.execute(session.cursor).fetchone()[0] > 0:
                raise Exception("Action already exists")
            if self.doctype_specific not in "YN":
                error = "DoctypeSpecific element must contain 'Y' or 'N'"
                raise Exception(error)
            insert = "INSERT INTO action(name, doctype_specific, comment) "
            insert += "VALUES(?, ?, ?)"
            values = self.name, self.doctype_specific, self.comment
            session.cursor.execute(insert, (values))
            session.conn.commit()

        def modify(self, session):
            """
            Update a row in the `action` database table

            Called by:
              cdr.putAction()
              client XML wrapper command CdrRepAction

            Pass:
              session - `Session` object requesting the operation
            """

            session.log("Action.modify({!r})".format(self.name))
            if not session.can_do("MODIFY ACTION"):
                message = "MODIFY ACTION action not authorized for this user"
                raise Exception(message)
            if not getattr(self, "id"):
                raise Exception("action id missing")
            if self.doctype_specific == "N":
                query = db.Query("grp_action", "COUNT(*)")
                query.where("doc_type <> 1")
                query.where(query.Condition("action", self.id))
                if query.execute(session.cursor).fetchone()[0] > 0:
                    raise Exception("Cannot set doctype_specific flag to 'N' "
                                    "because action has been assigned to "
                                    "groups for specific doctypes")
            elif self.doctype_specific == "Y":
                query = db.Query("grp_action", "COUNT(*)")
                query.where("doc_type = 1")
                query.where(query.Condition("action", self.id))
                if query.execute(session.cursor).fetchone()[0] > 0:
                    raise Exception("Cannot set doctype_specific flag to 'Y' "
                                    "because doctype-independent assignments "
                                    "of this action have been made to groups")
            else:
                error = "DoctypeSpecific element must contain 'Y' or 'N'"
                raise Exception(error)
            update = "UPDATE action SET name = ?, doctype_specific = ?, "
            update += "comment = ? WHERE id = ?"
            values = self.name, self.doctype_specific, self.comment, self.id
            session.cursor.execute(update, values)
            session.conn.commit()

        def delete(self, session):
            """
            Remove a row from the `action` database table

            Called by:
              cdr.delAction()
              client XML wrapper command CdrDelAction

            Database relational integrity will prevent the deletion
            if there are foreign key constraint violations.

            Pass:
              session - `Session` object requesting the operation
            """

            if not session.can_do("DELETE ACTION"):
                message = "DELETE ACTION action not authorized for this user"
                raise Exception(message)
            session.log("Action.delete({!r})".format(self.name))
            cursor = session.cursor
            delete = "DELETE FROM action WHERE name = '{}'".format(self.name)
            cursor.execute(delete)
            session.conn.commit()


    class Group:
        """
        Named CDR group representing zero or more CDR accounts

        Groups are used primarily for controlling permission to
        perform specific actions. Actions can also be used for
        identifying accounts to be included in specific system
        actions (for example, email distribution lists, or admin
        menu options).

        Attributes:
          id - primary key for the `group` database table
          name - unique display string for the group
          comment - optional string describing the group's role
          users - sequence of names of accounts in the group
          actions - dictionary of actions, keyed by the names
                    of the actions, with values consisting of
                    a sequence of names of document types on
                    which the group is authorized to perform
                    the action (a list containing a single
                    empty string is used for actions which are
                    not controlled separately for each document
                    type)
        """

        def __init__(self, **opts):
            """
            Save the caller's values for the `Group` object
            """

            self.id = opts.get("id")
            self.name = opts.get("name")
            self.comment = opts.get("comment")
            self.users = opts.get("users") or []
            self.actions = opts.get("actions") or {}

        def add(self, session):
            """
            Create a row in the `group` database table (and related tables)

            Also populates the `grp_action` and `grp_usr` tables.

            Called by:
              cdr.putGroup()
              client XML wrapper command CdrAddGrp

            Pass:
              session - `Session` object requesting the operation
            """

            if not session.can_do("ADD GROUP"):
                message = "ADD GROUP action not authorized for this user"
                raise Exception(message)
            session.log("Group.add({!r})".format(self.name))
            if self.id:
                raise Exception("group already in database")
            query = db.Query("grp", "COUNT(*)")
            if not self.name or not self.name.strip():
                raise Exception("Missing group name")
            query.where(query.Condition("name", self.name.strip()))
            if query.execute(session.cursor).fetchone()[0] > 0:
                raise Exception("Group name already exists")
            insert = "INSERT INTO grp(name, comment) VALUES(?, ?)"
            session.cursor.execute(insert, (self.name, self.comment))
            session.cursor.execute("SELECT @@IDENTITY AS id")
            self.id = session.cursor.fetchone().id
            self.save_users(session)
            self.save_actions(session)
            session.conn.commit()

        def modify(self, session):
            """
            Update a row in the `group` database table (and related tables)

            Also re-populates the `grp_action` and `grp_usr` tables.

            Called by:
              cdr.putGroup()
              client XML wrapper command CdrModGrp

            Pass:
              session - `Session` object requesting the operation
            """

            if not session.can_do("MODIFY GROUP"):
                message = "MODIFY GROUP action not authorized for this user"
                raise Exception(message)
            if not self.id:
                raise Exception("group not in database")
            name = self.name.strip()
            if not name:
                raise Exception("Missing group name")
            session.log("Group.modify({!r})".format(self.name))
            cursor = session.cursor
            update = "UPDATE grp SET name = ?, comment = ? WHERE id = ?"
            cursor.execute(update, (name, self.comment, self.id))
            cursor.execute("DELETE grp_usr WHERE grp = ?", (self.id,))
            cursor.execute("DELETE grp_action WHERE grp = ?", (self.id,))
            self.save_users(session)
            self.save_actions(session)
            session.conn.commit()

        def delete(self, session):
            """
            Drop database rows representing a CDR group

            Called by:
              cdr.DelGroup()
              client XML wrapper command CdrDelGrp

            Pass:
              session - `Session` object requesting the operation
            """

            if not session.can_do("DELETE GROUP"):
                message = "DELETE GROUP action not authorized for this user"
                raise Exception(message)
            session.log("Group.delete({!r})".format(self.name))
            cursor = session.cursor
            cursor.execute("DELETE grp_usr WHERE grp = ?", (self.id,))
            cursor.execute("DELETE grp_action WHERE grp = ?", (self.id,))
            cursor.execute("DELETE grp WHERE id = ?", (self.id,))
            session.conn.commit()

        def save_users(self, session):
            """
            Create rows in the `grp_usr` table for a CDR group

            Pass:
              session - `Session` object requesting the operation
            """

            cursor = session.cursor
            for user in self.users:
                message = "storing user {} for group {}"
                session.logger.debug(message.format(user, self.name))
                query = db.Query("usr", "id")
                query.where(query.Condition("name", user.strip()))
                row = query.execute(cursor).fetchone()
                if not row:
                    raise Exception("Unknown user {}".format(user))
                insert = "INSERT INTO grp_usr(grp, usr) VALUES(?, ?)"
                cursor.execute(insert, (self.id, row[0]))

        def save_actions(self, session):
            """
            Create rows in the `grp_action` table for a CDR group

            Pass:
              session - `Session` object requesting the operation
            """

            cursor = session.cursor
            for action in self.actions:
                doctypes = self.actions[action] or [""]
                for doctype in self.actions[action]:
                    if doctype:
                        what = "action {} ({})".format(action, doctype)
                    else:
                        what = "action " + action
                    message = "storing {} for group {}"
                    session.logger.debug(message.format(what, self.name))
                    query = db.Query("action", "id")
                    query.where(query.Condition("name", action))
                    row = query.execute(cursor).fetchone()
                    if not row:
                        raise Exception("Unknown action: {}".format(action))
                    action_id = row[0]
                    query = db.Query("doc_type", "id")
                    query.where(query.Condition("name", doctype or ""))
                    row = query.execute(cursor).fetchone()
                    if not row:
                        raise Exception("Unknown doc type: {}".format(doctype))
                    doctype_id = row[0]
                    insert = "INSERT INTO grp_action(grp, action, doc_type)"
                    insert += " VALUES(?, ?, ?)"
                    cursor.execute(insert, (self.id, action_id, doctype_id))


    class User:
        """
        Login account for a CDR user

        Properties:
          id - primary key into the `usr` table
          name - string for short account name; e.g., "klem"
          fullname - e.g., "Klem Kadiddlehopper"
          office - optional string indicating where the user works
          email - optional string for user's email address
          phone - optional string for the user's telephone number
          comment - optional string describing the user's rule in the CDR
          groups - sequence of strings naming groups of which the user
                   is a member (supporting, among other things, determining
                   which actions the user is authorized to perform)
          authmode - "local" for machine account which run on localhost, or
                     "network" for users authenticated using their NIH
                     domain credentials
        """

        # Used by 'network' accounts, which are authenticated outside
        # the CDR by the NIH Active Directory.
        EMPTY_PW = hashlib.sha1(b"").hexdigest().upper()

        def __init__(self, session, **opts):
            """
            Construct an object for a CDR user account

            Called by:
              cdr.getUser()
              client XML wrapper command CdrGetUsr

            Required positional argument:
              session - object representing current login

            Optional keyword arguments:
              id - primary key into the `usr` table
              name - string for the short account name (e.g., 'klem')
              fullname - longer string for the account name (e.g.,
                         'Klem Kadiddlehopper')
              authmode - 'network' or 'local'
              office - string identifying where the user works
              email - string for the user's email address
              phone - string for the user's telephone number
              groups - sequence of strings naming groups of which the
                       user is a member
            """

            self.__session = session
            self.__opts = opts

        @property
        def session(self):
            """
            Reference to `Session` object representing the current login
            """

            return self.__session

        @property
        def id(self):
            """
            Primary key for the user's row in the `usr` table
            """

            if not hasattr(self, "_id"):
                if "id" in self.__opts:
                    self._id = self.__opts["id"]
                else:
                    if hasattr(self, "_name"):
                        name = self._name
                    else:
                        name = self.__opts.get("name")
                    if name:
                        query = db.Query("usr", "id")
                        query.where(query.Condition("name", name))
                        row = query.execute(self.session.cursor).fetchone()
                        self._id = row.id if row else None
                    else:
                        self._id = None
                        self.session.logger.warning("User.id: NO NAME!!!")
            return self._id

        @property
        def name(self):
            """
            Short login name string for the accout (e.g., "klem")
            """

            if not hasattr(self, "_name"):
                self._name = self.__opts.get("name")
                if not self._name and self.id:
                    query = db.Query("usr", "name")
                    query.where(query.Condition("id", self.id))
                    row = query.execute(self.session.cursor).fetchone()
                    self._name = row.name if row else None
            return self._name

        @property
        def authmode(self):
            """
            How the account is authenticated ("local" or "network")

            Local accounts have a hashed password stored in the database.
            The other accounts have a dummy password stored in the database,
            and are authenticated by the NIH domain.
            """

            if not hasattr(self, "_authmode"):
                if "authmode" in self.__opts:
                    self._authmode = self.__opts["authmode"]
                elif self.id:
                    lines = [
                        "SELECT authmode = CASE",
                        "WHEN hashedpw IS NULL THEN 'network'",
                        "WHEN hashedpw = HASHBYTES('SHA1', '') THEN 'network'",
                        "ELSE 'local' END FROM usr WHERE id = ?"
                    ]
                    self.session.cursor.execute(" ".join(lines), (self.id,))
                    row = self.session.cursor.fetchone()
                    self._authmode = row.authmode if row else None
                else:
                    self._authmode = None
            return self._authmode

        @property
        def fullname(self):
            """
            User's real name (e.g., "Klem Kadiddlehopper")
            """

            if not hasattr(self, "_fullname"):
                if "fullname" in self.__opts:
                    self._fullname = self.__opts["fullname"]
                elif self.id:
                    query = db.Query("usr", "fullname")
                    query.where(query.Condition("id", self.id))
                    row = query.execute(self.session.cursor).fetchone()
                    self._fullname = row.fullname if row else None
                else:
                    self._fullname = None
            return self._fullname

        @property
        def office(self):
            """
            Optional string for the location where the user works
            """

            if not hasattr(self, "_office"):
                if "office" in self.__opts:
                    self._office = self.__opts["office"]
                elif self.id:
                    query = db.Query("usr", "office")
                    query.where(query.Condition("id", self.id))
                    row = query.execute(self.session.cursor).fetchone()
                    self._office = row.office if row else None
                else:
                    self._office = None
            return self._office

        @property
        def email(self):
            """
            Optional string for the user's email address
            """

            if not hasattr(self, "_email"):
                if "email" in self.__opts:
                    self._email = self.__opts["email"]
                elif self.id:
                    query = db.Query("usr", "email")
                    query.where(query.Condition("id", self.id))
                    row = query.execute(self.session.cursor).fetchone()
                    self._email = row.email if row else None
                else:
                    self._email = None
            return self._email

        @property
        def phone(self):
            """
            Optional string for the user's telephone number
            """

            if not hasattr(self, "_phone"):
                if "phone" in self.__opts:
                    self._phone = self.__opts["phone"]
                elif self.id:
                    query = db.Query("usr", "phone")
                    query.where(query.Condition("id", self.id))
                    row = query.execute(self.session.cursor).fetchone()
                    self._phone = row.phone if row else None
                else:
                    self._phone = None
            return self._phone

        @property
        def comment(self):
            """
            Optional string describing the account's role in the CDR
            """

            if not hasattr(self, "_comment"):
                if "comment" in self.__opts:
                    self._comment = self.__opts["comment"]
                elif self.id:
                    query = db.Query("usr", "comment")
                    query.where(query.Condition("id", self.id))
                    row = query.execute(self.session.cursor).fetchone()
                    self._comment = row.comment if row else None
                else:
                    self._comment = None
            return self._comment

        @property
        def groups(self):
            """
            Sequence of string for the names of groups user belongs to
            """

            if not hasattr(self, "_groups"):
                if "groups" in self.__opts:
                    self._groups = self.__opts["groups"]
                elif self.id:
                    query = db.Query("grp g", "g.name")
                    query.join("grp_usr u", "u.grp = g.id")
                    query.where(query.Condition("u.usr", self.id))
                    rows = query.execute(self.session.cursor).fetchall()
                    self._groups = [row.name for row in rows]
                else:
                    self._groups = None
            return self._groups

        def save(self, password=None):
            """
            Add or update the user's row in the `usr` table

            Called by:
              cdr.putUser()
              client XML wrapper command CdrAddUsr
              client XML wrapper command CdrModUsr

            Pass:
              password - optional string for changing the password
                         of a machine ("local") account
            """

            self.session.log("User.save({!r})".format(self.name))
            action = "MODIFY USER" if self.id else "CREATE USER"
            if not self.session.can_do(action):
                message = "{} action not authorized for this user"
                raise Exception(message.format(action))
            if not self.name:
                raise Exception("Missing user name")
            if self.authmode == "local":
                if not password:
                    if not self.id:
                        raise Exception("Missing password")
                    else:
                        password = None
            else:
                password = ""
            if isinstance(password, str):
                password = password.encode("utf-8")
            query = db.Query("usr", "COUNT(*) AS n")
            query.where(query.Condition("name", self.name))
            if self.id:
                query.where(query.Condition("id", self.id, "<>"))
            if query.execute(self.session.cursor).fetchone().n > 0:
                raise Exception("Name used by another account")
            try:
                self.__save(password)
                self.session.conn.commit()
            except:
                self.session.logger.exception("User.save() failure")
                self.session.cursor.execute("SELECT @@TRANCOUNT AS tc")
                if self.session.cursor.fetchone().tc:
                    self.session.cursor.execute("ROLLBACK TRANSACTION")
                raise

        def __save(self, password):
            """
            Do the actual database writes for the `usr` row

            This is siphoned off separately to make it easier to
            wrap it in a try block and roll back in the case of failure.

            Pass:
              password - string for login password; will be empty for
                         non-local users, who are authenticated separately
                         using their NIH domain credentials; can be set to
                         None for updates of local user accounts to leave
                         the password unchanged, but must be a non-empty
                         string for a new local account
            """

            fields = dict(
                name=self.name,
                fullname=self.fullname,
                office=self.office,
                email=self.email,
                phone=self.phone,
                comment=self.comment
            )
            names = sorted(fields)
            values = [fields[name] for name in names]
            if password is not None:
                values.append(password)
                hashbytes = "HASHBYTES('SHA1', ?)"
            if self.id:
                user_id = self.id
                delete = "DELETE FROM grp_usr WHERE usr = ?"
                self.session.cursor.execute(delete, (self.id,))
                assignments = ["{} = ?".format(name) for name in names]
                if password is not None:
                    assignments.append("hashedpw = {}".format(hashbytes))
                assignments = ", ".join(assignments)
                values.append(self.id)
                sql = "UPDATE usr SET {} WHERE id = ?".format(assignments)
            else:
                names = ", ".join(names + ["hashedpw", "created", "password"])
                extras = [hashbytes, "GETDATE()", "''"]
                placeholders = ", ".join(["?"] * len(fields) + extras)
                args = names, placeholders
                sql = "INSERT INTO usr ({}) VALUES ({})".format(*args)
            self.session.cursor.execute(sql, tuple(values))
            if not self.id:
                self.session.cursor.execute("SELECT @@IDENTITY AS id")
                user_id = self.session.cursor.fetchone().id
            insert = "INSERT INTO grp_usr(grp, usr) VALUES(?, ?)"
            groups = set()
            for name in (self.groups or []):
                query = db.Query("grp", "id")
                query.where(query.Condition("name", name))
                row = query.execute(self.session.cursor).fetchone()
                if not row:
                    raise Exception("Unknown group {}".format(name))
                if row.id not in groups:
                    self.session.cursor.execute(insert, (row.id, user_id))
                    groups.add(row.id)
            self._id = user_id

        def delete(self):
            """
            Mark the user's account as expired

            Called by:
              cdr.delUser()
              client XML wrapper command CdrDelUsr
            """

            self.session.log("User.delete({!r})".format(self.name))
            if not self.session.can_do("DELETE USER"):
                raise Exception("DELETE USER action not allowed for this user")
            delete = "UPDATE usr SET expired = GETDATE() WHERE id = ?"
            self.session.cursor.execute(delete, (self.id,))
            self.session.conn.commit()


    class Cache:
        """
        Optimization for retrieval of filters, filter sets, and terms

        The fetching and caching of terms (which is complicated) seems
        to be obsolete, as the users just told us that the denormalization
        filters for protocol documents, which are the only users of this
        term caching, are no longer actively invoked.
        """

        def __init__(self):
            """
            Initialize caching dictionaries and locks protecting the cache
            """

            self.terms = {}
            self.filters = {}
            self.filter_sets = {}
            self.term_lock = threading.Lock()
            self.filter_lock = threading.Lock()
            self.filter_set_lock = threading.Lock()

        def clear(self):
            """
            Empty the cache dictionaries.
            """

            with self.term_lock:
                self.terms = {}
            with self.filter_lock:
                self.filters = {}
            with self.filter_set_lock:
                self.filter_sets = {}


    class Local(threading.local):
        """
        Thread-specific storage for session

        Attribute:
          conn - database connection
          cursor - database cursor
        """

        LOG_FORMAT = "%(asctime)s [%(levelname)s-%(thread)04d] %(message)s"

        def __init__(self, **kw):
            self.__dict__.update(kw)
            self.conn = db.connect(tier=self.tier.name)
            self.cursor = self.conn.cursor()
            #self.logger = self.tier.get_logger("session", **kw)
            #formatter = Session.Formatter(self.LOG_FORMAT)
            #for handler in self.logger.handlers:
            #    handler.setFormatter(formatter)


    class LoggingDBConnection(threading.local):
        def __init__(self):
            self.conn = db.connect()
            self.cursor = self.conn.cursor()
