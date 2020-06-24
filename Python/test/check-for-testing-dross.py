"""Report any residual cruft left behind by the regression tests.
"""

from argparse import ArgumentParser
from cdrapi import db

parser = ArgumentParser()
parser.add_argument("--tier", "-t")
opts = parser.parse_args()
cursor = db.connect(tier=opts.tier, user="CdrGuest").cursor()
cursor.execute("SELECT id FROM usr WHERE name = 'tester'")
tester = cursor.fetchone().id
cursor.execute("SELECT id FROM doc_type WHERE name = 'xxtest'")
xxtest = cursor.fetchone().id
cursor.execute("SELECT id FROM doc_type WHERE name = 'Mailer'")
mailer = cursor.fetchone().id
cursor.execute("SELECT id FROM action WHERE name = 'ADD DOCUMENT'")
add_doc = cursor.fetchone().id
select = "SELECT name, initiated FROM session WHERE usr = ? AND ended IS NULL"
cursor.execute(select, (tester,))
for row in cursor.fetchall():
    print(f"open tester session {row.name} (started {row.initiated})")
cursor.execute("SELECT name FROM action WHERE name IN ('gimte', 'dada')")
for row in cursor.fetchall():
    print(f"action {row.name} left in database")
cursor.execute("SELECT name FROM grp WHERE name LIKE 'Test Group%'")
for row in cursor.fetchall():
    print(f"group {row.name} left in database")
names = "xxtest.xml", "dada.xml"
cursor.execute("SELECT id, title FROM document WHERE title = 'dada.xml'")
for row in cursor.fetchall():
    print(f"CDR{row.id} ({row.title}) not deleted after testing")
cursor.execute("SELECT id FROM doc_type WHERE name = 'dada'")
for row in cursor.fetchall():
    print(f"document type dada (id {row.id}) left in database")
cursor.execute("SELECT id, title FROM document where doc_type = ?", (xxtest,))
for row in cursor.fetchall():
    print(f"xxtest doc CDR{row.id} ({row.title}) not deleted")
cursor.execute("SELECT value FROM external_map WHERE value LIKE 'test bogus%'")
for row in cursor.fetchall():
    print(f"mapping for {row.value!r} still in database")
cursor.execute("SELECT name FROM version_label")
for row in cursor.fetchall():
    print(f"version label {row.name!r} still in database")
select = "SELECT id FROM pub_proc WHERE usr = ? AND status <> 'Failure'"
cursor.execute(select, (tester,))
for row in cursor.fetchall():
    print(f"publishing job {row.id} not marked as failure")
args = tester, mailer, add_doc
cursor.execute("""\
SELECT d.id FROM document d JOIN audit_trail a ON a.document = d.id
WHERE a.usr = ? AND d.doc_type = ? AND a.action = ?""", args)
for row in cursor.fetchall():
    print(f"mailer CDR{row.id}")
cursor.execute("SELECT id, name FROM filter_set WHERE name LIKE 'Test%'")
for row in cursor.fetchall():
    print(f"filter set {row.id} ({row.name}) still in database")
cursor.execute("SELECT id, name FROM link_type WHERE name LIKE 'Test%'")
for row in cursor.fetchall():
    print(f"link type {row.id} ({row.name}) still in database")
paths = "/Term/Gimte", "/XXX"
cursor.execute("SELECT path FROM query_term_def WHERE path in (?, ?)", paths)
for row in cursor.fetchall():
    print(f"query term definition for {row.path} left in database")
select = "SELECT * FROM usr WHERE name LIKE 'testuser%' AND expired IS NULL"
cursor.execute(select)
for row in cursor.fetchall():
    print(f"user {row.name} still active")
cursor.execute("SELECT * FROM ctl WHERE grp = 'test' AND inactivated IS NULL")
for row in cursor.fetchall():
    print(f"{row.grp} control value {row.name} still active")
