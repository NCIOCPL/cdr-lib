#!/usr/bin/env python

"""Add the xxtest document type following a DB refresh from PROD.
"""

from cdrapi import db

GROUP_ACTIONS = (
    ('ADD ACTION', ''),
    ('ADD DOCTYPE', ''),
    ('ADD DOCUMENT', 'Person'),
    ('ADD DOCUMENT', 'Filter'),
    ('ADD DOCUMENT', 'schema'),
    ('ADD DOCUMENT', 'Summary'),
    ('ADD DOCUMENT', 'Mailer'),
    ('ADD DOCUMENT', 'xxtest'),
    ('ADD FILTER SET', ''),
    ('ADD GROUP', ''),
    ('ADD LINKTYPE', ''),
    ('ADD QUERY TERM DEF', ''),
    ('BLOCK DOCUMENT', ''),
    ('CREATE USER', ''),
    ('DELETE ACTION', ''),
    ('DELETE DOCTYPE', ''),
    ('DELETE DOCUMENT', 'Person'),
    ('DELETE DOCUMENT', 'Filter'),
    ('DELETE DOCUMENT', 'schema'),
    ('DELETE DOCUMENT', 'Summary'),
    ('DELETE DOCUMENT', 'Mailer'),
    ('DELETE DOCUMENT', 'xxtest'),
    ('DELETE FILTER SET', ''),
    ('DELETE GROUP', ''),
    ('DELETE LINKTYPE', ''),
    ('DELETE QUERY TERM DEF', ''),
    ('DELETE USER', ''),
    ('EDIT GLOSSARY MAP', ''),
    ('FILTER DOCUMENT', 'Person'),
    ('FILTER DOCUMENT', 'schema'),
    ('FILTER DOCUMENT', 'Summary'),
    ('FILTER DOCUMENT', 'xxtest'),
    ('FORCE CHECKIN', 'Person'),
    ('FORCE CHECKIN', 'schema'),
    ('FORCE CHECKIN', 'Mailer'),
    ('FORCE CHECKIN', 'xxtest'),
    ('FORCE CHECKOUT', 'Person'),
    ('FORCE CHECKOUT', 'schema'),
    ('FORCE CHECKOUT', 'Mailer'),
    ('FORCE CHECKOUT', 'xxtest'),
    ('GET ACTION', ''),
    ('GET DOCTYPE', 'Person'),
    ('GET DOCTYPE', 'Filter'),
    ('GET DOCTYPE', 'Mailer'),
    ('GET DOCTYPE', 'xxtest'),
    ('GET GROUP', ''),
    ('GET LINKTYPE', ''),
    ('GET SCHEMA', 'Person'),
    ('GET SCHEMA', 'Mailer'),
    ('GET SCHEMA', 'xxtest'),
    ('GET SYS CONFIG', ''),
    ('GET TREE', ''),
    ('GET USER', ''),
    ('LIST ACTIONS', ''),
    ('LIST DOCTYPES', ''),
    ('LIST GROUPS', ''),
    ('LIST LINKTYPES', ''),
    ('LIST USERS', ''),
    ('MAKE GLOBAL CHANGES', 'xxtest'),
    ('MODIFY ACTION', ''),
    ('MODIFY DOCTYPE', 'Person'),
    ('MODIFY DOCTYPE', 'xxtest'),
    ('MODIFY DOCUMENT', 'Person'),
    ('MODIFY DOCUMENT', 'Filter'),
    ('MODIFY DOCUMENT', 'schema'),
    ('MODIFY DOCUMENT', 'Summary'),
    ('MODIFY DOCUMENT', 'Mailer'),
    ('MODIFY DOCUMENT', 'xxtest'),
    ('MODIFY FILTER SET', ''),
    ('MODIFY GROUP', ''),
    ('MODIFY LINKTYPE', ''),
    ('MODIFY USER', ''),
    ('PUBLISH DOCUMENT', 'xxtest'),
    ('REPLACE CWD WITH VERSION', ''),
    ('RUN LONG REPORT', ''),
    ('SET_SYS_VALUE', ''),
    ('SUMMARY MAILERS', ''),
    ('UNBLOCK DOCUMENT', ''),
    ('UNLOCK', ''),
    ('USE PUBLISHING SYSTEM', ''),
    ('VALIDATE DOCUMENT', 'Person'),
    ('VALIDATE DOCUMENT', 'Filter'),
    ('VALIDATE DOCUMENT', 'Summary'),
    ('VALIDATE DOCUMENT', 'Mailer'),
    ('VALIDATE DOCUMENT', 'xxtest'),
)

conn = db.connect()
cursor = conn.cursor()
cursor.execute("SELECT id, expired FROM usr WHERE name = 'tester'")
row = cursor.fetchone()
if not row:
    raise Exception("tester account missing")
uid, expired = list(row)
if expired:
    print("re-activating tester account")
    cursor.execute("UPDATE usr SET expired = NULL WHERE id = ?", (uid,))
cursor.execute("SELECT id, active FROM doc_type WHERE name = 'xxtest'")
row = cursor.fetchone()
if not row:
    raise Exception("xxtest document missing")
tid, active = list(row)
if active != "Y":
    print("re-activating xxtest document type")
    cursor.execute("UPDATE doc_type SET active = 'Y' WHERE id = ?", (tid,))
cursor.execute("SELECT id FROM grp WHERE name = 'Regression Testers'")
row = cursor.fetchone()
if not row:
    raise Exception("Regression Testers group missing")
gid = row.id
cursor.execute("SELECT name, id FROM doc_type")
doc_types = dict(list(row) for row in cursor.fetchall())
cursor.execute("SELECT name, id FROM action")
actions = dict(list(row) for row in cursor.fetchall())
cursor.execute("SELECT action, doc_type FROM grp_action WHERE grp = ?", (gid,))
loaded = {tuple(row) for row in cursor.fetchall()}
for action, doc_type in GROUP_ACTIONS:
    if action not in actions:
        raise Exception(f"action {action!r} not found")
    elif doc_type not in doc_types:
        raise Exception(f"doc_type {doc_type!r} not found")
    else:
        key = actions[action], doc_types[doc_type]
        if key not in loaded:
            print(f"adding action {action!r} for doctype {doc_type!r}")
            cursor.execute("""\
INSERT INTO grp_action (grp, action, doc_type)
     VALUES (?, ?, ?)""", (gid, *key))
conn.commit()
