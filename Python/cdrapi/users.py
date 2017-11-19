"""
Control for who can use the CDR and what they can do
"""

import random
import threading
import time
from cdrapi import db
from cdrapi.settings import Tier


try:
    basestring
except:
    basestring = str, bytes
    unicode = str


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

    def __init__(self, name, tier=None):
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

        Raise
          `Exception` if the session does not exist or is expired
        """

        self.name = name
        self.tier = tier if isinstance(tier, Tier) else Tier(tier)
        self.logger = self.tier.get_logger("session")
        self.conn = db.connect(tier=self.tier.name)
        self.cursor = self.conn.cursor()
        update = """\
                UPDATE session
                   SET ended = GETDATE()
                 WHERE ended IS NULL
                   AND name <> 'guest'
                   AND DATEDIFF(hour, last_act, GETDATE()) > 24"""
        self.cursor.execute(update)
        self.conn.commit()
        query = db.Query("session s", "s.id", "u.id", "u.name")
        query.join("open_usr u", "u.id = s.usr")
        query.where(query.Condition("s.name", name))
        query.where("s.ended IS NULL")
        row = query.execute(self.cursor).fetchone()
        if row is None:
            raise Exception("Invalid or expired session: {!r}".format(name))
        self.active = True
        self.id, self.user_id, self.user_name = row
        self.cache = self.Cache()
        update = "UPDATE session SET last_act = GETDATE() WHERE id = ?"
        self.cursor.execute(update, (self.id,))
        self.conn.commit()

    def log(self, what):
        """
        Record what we're doing
        """

        self.logger.info("%s running command %s", self.name, what)

    def logout(self):
        """
        Close the current session on request
        """

        if not self.active:
            raise Exception("session expired")
        update = "UPDATE session SET ended = GETDATE() WHERE id = ?"
        self.cursor.execute(update, (self.id,))
        self.conn.commit()
        self.active = False
        self.log("logout({})".format(self.name))

    def duplicate(self):
        """
        Create a new session for the same user account

        Useful when a long-running job is requested by a user who
        might log out from the session from which the request was
        submitted before the job had completed.

        Return:
          `Session` object
        """

        self.log("duplicate()")
        query = db.Query("usr u", "u.name")
        query.join("session s", "s.usr = u.id")
        query.where("s.ended IS NULL")
        query.where(query.Condition("s.name", self.name))
        row = query.execute(self.cursor).fetchone()
        if not row:
            raise Exception("Can't duplicate invalid or expired session")
        name = row[0]
        comment = "Session duplicated from id={}".format(self.name)
        return self.create_session(name, comment, self.tier.name)

    def can_do(self, action, doctype=None):
        """
        Determine whether the account can perform a specific action

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
            self.log("can_do({}, {})".format(action, doctype))
        else:
            self.log("can_do({})".format(action))
        query = db.Query("grp_usr", "COUNT(*)")
        query.join("grp_action", "grp_action.grp = grp_usr.grp")
        query.join("action", "action.id = grp_action.action")
        query.join("doc_type", "doc_type.id = grp_action.doc_type")
        query.where(query.Condition("grp_usr.usr", self.user_id))
        query.where(query.Condition("action.name", action))
        query.where(query.Condition("doc_type.name", doctype or ""))
        return query.execute(self.cursor).fetchone()[0] > 0

    def list_actions(self):
        """
        Return a sorted sequence of the names of CDR actions
        """

        if not self.can_do("LIST ACTIONS"):
            raise Exception("LIST ACTIONS action not authorized for this user")
        self.log("list_actions()")
        query = db.Query("action", "name", "doctype_specific").order("name")
        rows = query.execute(self.cursor).fetchall()
        return [self.Action(*row) for row in rows]

    def get_action(self, name):
        """
        Fetch the information stored in a row of the `action` table

        Return:
          `Session.Action` object
        """

        if not self.can_do("GET ACTION"):
            raise Exception("GET ACTION action not authorized for this user")
        self.log("get_action({})".format(name))
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

        Pass:
          session - `Session` object requesting the operation

        Return:
          `Session.Group` object
        """

        if not self.can_do("GET GROUP"):
            raise Exception("GET GROUP action not authorized for this user")
        self.log("get_group({})".format(name))
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
        """

        if not self.can_do("LIST GROUPS"):
            raise Exception("LIST GROUPS action not authorized for this user")
        self.log("list_groups()")
        query = db.Query("grp", "name").order("name")
        return [row[0] for row in query.execute(self.cursor).fetchall()]

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
    def create_session(cls, user, comment=None, tier=None):
        """
        Insert a new row into the `session` database table

        Assumes the account's credentials have been verified.
        For requests coming from external clients (without read/write
        database access) this will have been handled by Windows
        through a protected IIS folder.

        Pass:
          user - unique string naming the account
          comment - optional string describing the session request
          tier - optional string identifying which server should
                 be used for the session

        Return:
          `Session` object
        """

        conn = db.connect(tier=tier)
        cursor = conn.cursor()
        query = db.Query("usr", "id")
        query.where(query.Condition("name", user))
        query.where("expired IS NULL")
        row = query.execute(cursor).fetchone()
        if not row:
            raise Exception("unknown or expired user: {}".format(user))
        uid = row[0]
        secs, msecs = [int(n) for n in "{:.9f}".format(time.time()).split(".")]
        secs = secs & 0xFFFFFFFF
        msecs = msecs & 0xFFFFFF
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        suffix = "".join([random.choice(letters) for i in range(12)])
        name = "{:08X}-{:06X}-{:03d}-{}".format(secs, msecs, uid, suffix)
        cols = "name, usr, comment, initiated, last_act"
        vals = "?, ?, ?, GETDATE(), GETDATE()"
        insert = "INSERT INTO session({}) VALUES({})".format(cols, vals)
        cursor.execute(insert, (name, uid, comment))
        conn.commit()
        session = Session(name, tier)
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

            Pass:
              session - `Session` object requesting the operation
            """

            session.log("add action {}".format(self.name))
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

            Pass:
              session - `Session` object requesting the operation
            """

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

            Database relational integrity will prevent the deletion
            if there are foreign key constraint violations.

            Pass:
              session - `Session` object requesting the operation
            """

            if not session.can_do("DELETE ACTION"):
                message = "DELETE ACTION action not authorized for this user"
                raise Exception(message)
            session.log("delete action {}".format(self.name))
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

        def __init__(self, **args):
            """
            Save the caller's values for the `Group` object
            """

            self.id = args.get("id")
            self.name = args.get("name")
            self.comment = args.get("comment")
            self.users = args.get("users") or []
            self.actions = args.get("actions") or {}

        def add(self, session):
            """
            Create a row in the `group` database table (and related tables)

            Also populates the `grp_action` and `grp_usr` tables.

            Pass:
              session - `Session` object requesting the operation
            """

            if not session.can_do("ADD GROUP"):
                message = "ADD GROUP action not authorized for this user"
                raise Exception(message)
            session.log("add group {}".format(self.name))
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
            session.cursor.execute("SELECT @@IDENTITY")
            self.id = session.cursor.fetchone()[0]
            self.save_users(session)
            self.save_actions(session)
            session.conn.commit()

        def modify(self, session):
            """
            Update a row in the `group` database table (and related tables)

            Also re-populates the `grp_action` and `grp_usr` tables.

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
            session.log("modify group {}".format(name))
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

            Pass:
              session - `Session` object requesting the operation
            """

            if not session.can_do("DELETE GROUP"):
                message = "DELETE GROUP action not authorized for this user"
                raise Exception(message)
            session.log("delete group {}".format(self.name))
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

    class Cache:
        def __init__(self):
            self.terms = {}
            self.filters = {}
            self.filter_sets = {}
            self.term_lock = threading.Lock()
            self.filter_lock = threading.Lock()
            self.filter_set_lock = threading.Lock()
        def clear(self):
            with self.term_lock:
                self.terms = {}
            with self.filter_lock:
                self.filters = {}
            with self.filter_set_lock:
                self.filter_sets = {}
