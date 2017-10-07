#!/usr/bin/env python3

import os
import unittest
import cdr
from cdrapi.users import Session
from cdrapi import db

class Tests(unittest.TestCase):
    USERNAME = "tester"
    TIER = os.environ.get("TEST_CDR_TIER")

    def setUp(self):
        opts = dict(comment="unit testing", tier=self.TIER)
        Tests.session = Session.create_session(self.USERNAME, **opts).name

    def tearDown(self):
        cdr.logout(self.session, tier=self.TIER)

class SessionTests(Tests):
    def test_01_login(self):
        opts = dict(comment="unit testing", tier=self.TIER)
        SessionTests.session2 = Session.create_session(self.USERNAME, **opts)
        self.assertEqual(len(SessionTests.session2.name), 32)
        self.assertTrue(SessionTests.session2.active)
    def test_02_logout(self):
        self.assertTrue(SessionTests.session2.active)
        self.assertIsNone(cdr.logout(SessionTests.session2, tier=self.TIER))
        self.assertFalse(SessionTests.session2.active)
    def test_03_dup_session(self):
        session = cdr.dupSession(self.session, tier=self.TIER)
        if isinstance(session, Session):
            session = session.name
        self.assertEqual(len(session), 32)
        self.assertEqual(len(self.session), 32)
        self.assertNotEqual(session, self.session)
        self.assertIsNone(cdr.logout(session, tier=self.TIER))

class UserPermissionTests(Tests):
    def delete_action(self, name, **opts):
        disable = "ALTER TABLE {} NOCHECK CONSTRAINT ALL"
        enable = "ALTER TABLE {} WITH CHECK CHECK CONSTRAINT ALL"
        conn = db.connect(tier=opts.get("tier"))
        cursor = conn.cursor()
        fk_tables = (
            "audit_trail",
            "audit_trail_added_action",
            "external_map_usage",
            "grp_action"
        )
        try:
            for table in fk_tables:
                cursor.execute(disable.format(table))
            conn.commit()
            cdr.delAction(self.session, name, **opts)
        finally:
            for table in fk_tables:
                cursor.execute(enable.format(table))
            conn.commit()

    def test_01_can_do(self):
        opts = dict(tier=self.TIER)
        self.assertTrue(cdr.canDo("guest", "ADD DOCUMENT", "xxtest", **opts))
        self.assertFalse(cdr.canDo("guest", "ADD DOCUMENT", "Summary", **opts))
        self.assertTrue(cdr.canDo("guest", "LIST DOCTYPES", **opts))
        self.assertFalse(cdr.canDo("guest", "LIST USERS", **opts))
    def test_02_add_action(self):
        opts = dict(tier=self.TIER)
        actions = cdr.getActions(self.session, **opts)
        for name in "gimte", "dada":
            if name in actions:
                self.delete_action(name, **opts)
        action = cdr.Action("dada", "Y", "gimte")
        self.assertIsNone(cdr.putAction(self.session, None, action, **opts))
    def test_03_mod_action(self):
        opts = dict(tier=self.TIER)
        action = cdr.getAction(self.session, "dada", **opts)
        action.name = "gimte"
        action.comment = "dada"
        action.doctype_specific = "N"
        self.assertIsNone(cdr.putAction(self.session, "dada", action, **opts))
        action = cdr.getAction(self.session, "ADD DOCUMENT", **opts)
        action.doctype_specific = "N"
        with self.assertRaises(Exception):
            cdr.putAction(self.session, "ADD DOCUMENT", action, **opts)
    def test_04_get_action(self):
        opts = dict(tier=self.TIER)
        action = cdr.getAction(self.session, "gimte", **opts)
        self.assertEqual(action.comment, "dada")
        self.assertEqual(action.doctype_specific, "N")
    def test_05_del_action(self):
        opts = dict(tier=self.TIER)
        self.assertIsNone(self.delete_action("gimte", **opts))
    def test_06_get_actions(self):
        opts = dict(tier=self.TIER)
        actions = cdr.getActions(self.session, **opts)
        self.assertEqual(actions["ADD DOCUMENT"], "Y")
        self.assertEqual(actions["LIST USERS"], "N")

class GroupTests(Tests):
    NAME = "Test Group"
    NEWNAME = "Test Group (MOD)"
    USERS = ["tester"]
    NEWUSERS = ["tester", "CdrGuest"]
    def test_01_add_group(self):
        opts = dict(tier=self.TIER)
        groups = cdr.getGroups(self.session, **opts)
        for name in self.NAME, self.NEWNAME:
            if name in groups:
                cdr.delGroup(self.session, name, **opts)
        name = self.NAME
        users = self.USERS
        actions = {"LIST USERS": ""}
        args = dict(name=name, comment="dada", users=users, actions=actions)
        group = cdr.Group(**args)
        group.actions = {"LIST USERS": ""}
        self.assertIsNone(cdr.putGroup(self.session, None, group, **opts))
    def test_02_get_group(self):
        group = cdr.getGroup(self.session, self.NAME, tier=self.TIER)
        self.assertIsNotNone(group)
        self.assertEqual(set(group.users), set(self.USERS))
        self.assertEqual(group.comment, "dada")
    def test_03_mod_group(self):
        opts = dict(tier=self.TIER)
        group = cdr.getGroup(self.session, self.NAME, **opts)
        group.name = self.NEWNAME
        group.users = self.NEWUSERS
        self.assertIsNone(cdr.putGroup(self.session, self.NAME, group, **opts))
        group = cdr.getGroup(self.session, self.NEWNAME, **opts)
        self.assertEqual(set(group.users), set(self.NEWUSERS))
    def test_04_get_groups(self):
        opts = dict(tier=self.TIER)
        self.assertIn(self.NEWNAME, cdr.getGroups(self.session, **opts))
    def test_05_del_group(self):
        opts = dict(tier=self.TIER)
        self.assertIsNone(cdr.delGroup(self.session, self.NEWNAME, **opts))

if __name__ == "__main__":
    unittest.main()
