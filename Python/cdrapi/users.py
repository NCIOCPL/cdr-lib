"""
Control for who can use the CDR and what they can do
"""

import random
import time
from cdrapi import db
from cdrapi.settings import Tier


try:
    basestring
except:
    basestring = str


class Session:

    def __init__(self, name, tier=None):
        self.tier = tier if isinstance(tier, Tier) else Tier(tier)
        self.logger = self.tier.get_logger("session")
        self.conn = db.connect(tier=self.tier.name)
        self.cursor = self.conn.cursor()
        self.name = name
        query = db.Query("session s", "s.id", "u.id", "u.name")
        query.join("open_usr u", "u.id = s.usr")
        query.where(query.Condition("s.name", name))
        query.where("s.ended IS NULL")
        row = query.execute(self.cursor).fetchone()
        if row is None:
            raise Exception("Invalid or expired session: {!r}".format(name))
        self.active = True
        self.id, self.user_id, self.user_name = row
        update = "UPDATE session SET last_act = GETDATE() WHERE id = ?"
        self.cursor.execute(update, (self.id,))
        update = """UPDATE session
                       SET ended = GETDATE()
                     WHERE ended IS NULL
                       AND name <> 'guest'
                       AND DATEDIFF(hour, last_act, GETDATE()) > 24"""
        self.cursor.execute(update)
        self.conn.commit()
    def log(self, what):
        self.logger.info("%s running command %s", self.name, what)
    def logout(self):
        if not self.active:
            raise Exception("session expired")
        update = "UPDATE session SET ended = GETDATE() WHERE id = ?"
        self.cursor.execute(update, (self.id,))
        self.conn.commit()
        self.active = False
        self.log("logout({})".format(self.name))
    def duplicate(self):
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
        if not self.can_do("LIST ACTIONS"):
            raise Exception("LIST ACTIONS action not authorized for this user")
        self.log("list_actions()")
        query = db.Query("action", "name", "doctype_specific").order("name")
        rows = query.execute(self.cursor).fetchall()
        return [self.Action(*row) for row in rows]

    def get_action(self, name):
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

    class Action:
        def __init__(self, name, doctype_specific, comment=None):
            self.name = name
            self.doctype_specific = doctype_specific
            self.comment = comment
            self.id = None
        def add(self, session):
            if not session.can_do("ADD ACTION"):
                message = "ADD ACTION action not authorized for this user"
                raise Exception(message)
            session.log("add action {}".format(self.name))
            query = db.Query("grp", "COUNT(*)")
            if not self.name or not self.name.strip():
                raise Exception("Missing action name")
            query.where(query.Condition("name", self.name.strip()))
            if query.execute(session.cursor).fetchone()[0] > 0:
                raise Exception("Action already exists")
            if self.doctype_specific not in "YN":
                error = "DoctypeSpecific element must contain 'Y' or 'N'"
                raise Exception(error)
            cols = "name, comment"
            insert = "INSERT INTO action(name, doctype_specific, comment) "
            insert += "VALUES(?, ?, ?)"
            values = self.name, self.doctype_specific, self.comment
            session.cursor.execute(insert, (values))
            session.conn.commit()
        def modify(self, session):
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
            session.logger.info("checking DELETE ACTION")
            if not session.can_do("DELETE ACTION"):
                message = "DELETE ACTION action not authorized for this user"
                raise Exception(message)
            session.log("delete action {}".format(self.name))
            cursor = session.cursor
            session.logger.info("submitting DELETE FROM")
            cursor.execute("DELETE FROM action WHERE name = '{}'".format(self.name))
            session.logger.info("committing transaction")
            session.conn.commit()
            session.logger.info("back from commit")

    def get_group(self, name):
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
        if not self.can_do("LIST GROUPS"):
            raise Exception("LIST GROUPS action not authorized for this user")
        self.log("list_groups()")
        query = db.Query("grp", "name").order("name")
        return [row[0] for row in query.execute(self.cursor).fetchall()]
    class Group:
        def __init__(self, **args):
            self.id = args.get("id")
            self.name = args.get("name")
            self.comment = args.get("comment")
            self.users = args.get("users") or []
            self.actions = args.get("actions") or {}
        def add(self, session):
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
    def __str__(self):
        return self.name or ""
    def __repr__(self):
        return self.name or ""

    @classmethod
    def create_session(cls, user, comment=None, tier=None):
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
