#!/usr/bin/env python3

"""
Run these tests whenever changes are made to the API code or client wrapper

The numbers in the names guarantee the order in which the tests will be run.
The extra underscores in the names make the verbose output much easier to read.

For details about what's being tested, read the documentation of the
individual functions in `cdr.py` being invoked here. For a deeper dive,
read the documentation in the `cdrapi` package.
"""

import datetime
import os
import random
import string
import time
import unittest
from lxml import etree
import cdr
from cdrapi.users import Session
from cdrapi import db

try:
    basestring
except:
    basestring = (str, bytes)
    unicode = str


class Tests(unittest.TestCase):
    USERNAME = "tester"
    TIER = os.environ.get("TEST_MODE") == "remote" and cdr.Tier().name or None

    def setUp(self):
        password = cdr.getpw(self.USERNAME)
        opts = dict(comment="unit testing", tier=self.TIER, password=password)
        Tests.session = Session.create_session(self.USERNAME, **opts).name
        Tests.TEST_DIR = os.path.dirname(os.path.realpath(__file__))
    def tearDown(self):
        cdr.logout(self.session, tier=self.TIER)

    @staticmethod
    def make_password(length=16):
        chars = string.ascii_letters + string.digits + string.punctuation
        return "".join(random.choice(chars) for _ in range(length))

# Set FULL to False temporarily when adding new tests so you can get
# the new ones working without having to grind through the entire set.

FULL = True
#FULL = False
if FULL:

    class _01SessionTests___(Tests):

        def test_01_login_______(self):
            opts = dict(comment="unit testing", tier=self.TIER)
            password = cdr.getpw(self.USERNAME)
            if self.TIER:
                try:
                    test = self.assertRaisesRegex
                except:
                    test = self.assertRaisesRegexp
                with test(Exception, "Unauthorized"):
                    cdr.login(self.USERNAME, password, **opts)
                opts["password"] = password
                session = Session.create_session(self.USERNAME, **opts)
            else:
                session = cdr.login(self.USERNAME, password, **opts)
            self.__class__.session2 = session
            self.assertEqual(len(self.__class__.session2.name), 32)
            self.assertTrue(self.__class__.session2.active)
        def test_02_logout______(self):
            self.assertTrue(self.__class__.session2.active)
            opts = dict(tier=self.TIER)
            self.assertIsNone(cdr.logout(self.__class__.session2, **opts))
            self.assertFalse(self.__class__.session2.active)

        def test_03_dup_session_(self):
            session = cdr.dupSession(self.session, tier=self.TIER)
            if isinstance(session, Session):
                session = session.name
            self.assertEqual(len(session), 32)
            self.assertEqual(len(self.session), 32)
            self.assertNotEqual(session, self.session)
            self.assertIsNone(cdr.logout(session, tier=self.TIER))


    class _02PermissionTests(Tests):

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

        def test_04_can_do______(self):
            opts = dict(tier=self.TIER)
            action = "ADD DOCUMENT"
            self.assertTrue(cdr.canDo("guest", action , "xxtest", **opts))
            self.assertFalse(cdr.canDo("guest", action, "Summary", **opts))
            self.assertTrue(cdr.canDo("guest", "LIST DOCTYPES", **opts))
            self.assertFalse(cdr.canDo("guest", "LIST USERS", **opts))

        def test_05_add_action__(self):
            opts = dict(tier=self.TIER)
            actions = cdr.getActions(self.session, **opts)
            for name in "gimte", "dada":
                if name in actions:
                    self.delete_action(name, **opts)
            action = cdr.Action("dada", "Y", "gimte")
            result = cdr.putAction(self.session, None, action, **opts)
            self.assertIsNone(result)

        def test_06_mod_action__(self):
            opts = dict(tier=self.TIER)
            action = cdr.getAction(self.session, "dada", **opts)
            action.name = "gimte"
            action.comment = "dada"
            action.doctype_specific = "N"
            result = cdr.putAction(self.session, "dada", action, **opts)
            self.assertIsNone(result)
            action = cdr.getAction(self.session, "ADD DOCUMENT", **opts)
            action.doctype_specific = "N"
            expression = "Cannot set doctype_specific flag to 'N'"
            try:
                test = self.assertRaisesRegex
            except:
                test = self.assertRaisesRegexp
            with test(Exception, expression):
                cdr.putAction(self.session, "ADD DOCUMENT", action, **opts)

        def test_07_get_action__(self):
            opts = dict(tier=self.TIER)
            action = cdr.getAction(self.session, "gimte", **opts)
            self.assertEqual(action.comment, "dada")
            self.assertEqual(action.doctype_specific, "N")

        def test_08_del_action__(self):
            opts = dict(tier=self.TIER)
            self.assertIsNone(self.delete_action("gimte", **opts))

        def test_09_get_actions_(self):
            opts = dict(tier=self.TIER)
            actions = cdr.getActions(self.session, **opts)
            self.assertEqual(actions["ADD DOCUMENT"], "Y")
            self.assertEqual(actions["LIST USERS"], "N")

        def test_10_check_auth__(self):
            pairs = [("*", "*")]
            auth = cdr.checkAuth(self.session, pairs, tier=self.TIER)
            self.assertTrue("ADD ACTION" in auth)
            self.assertTrue("schema" in auth["ADD DOCUMENT"])
            self.assertTrue("xxtest" in auth["ADD DOCUMENT"])
            pairs = [("*", "xxtest")]
            auth = cdr.checkAuth(self.session, pairs, tier=self.TIER)
            self.assertTrue("xxtest" in auth["ADD DOCUMENT"])
            self.assertFalse("schema" in auth["ADD DOCUMENT"])
            pairs = [("ADD DOCUMENT", "*")]
            auth = cdr.checkAuth(self.session, pairs, tier=self.TIER)
            self.assertTrue("ADD DOCUMENT" in auth)
            self.assertTrue(len(auth) == 1)
            pairs = [("ADD DOCUMENT", "Summary"), ("LIST GROUPS", "*")]
            auth = cdr.checkAuth(self.session, pairs, tier=self.TIER)
            self.assertTrue(len(auth) == 2)


    class _03GroupTests_____(Tests):

        NAME = "Test Group"
        NEWNAME = "Test Group (MOD)"
        USERS = ["tester"]
        NEWUSERS = ["tester", "CdrGuest"]

        def test_11_add_group___(self):
            opts = dict(tier=self.TIER)
            groups = cdr.getGroups(self.session, **opts)
            for name in self.NAME, self.NEWNAME:
                if name in groups:
                    cdr.delGroup(self.session, name, **opts)
            name = self.NAME
            users = self.USERS
            actions = {"LIST USERS": ""}
            args = dict(
                name=name,
                comment="dada",
                users=users,
                actions=actions
            )
            group = cdr.Group(**args)
            group.actions = actions
            self.assertIsNone(cdr.putGroup(self.session, None, group, **opts))

        def test_12_get_group___(self):
            group = cdr.getGroup(self.session, self.NAME, tier=self.TIER)
            self.assertIsNotNone(group)
            self.assertEqual(set(group.users), set(self.USERS))
            self.assertEqual(group.comment, "dada")

        def test_13_mod_group___(self):
            opts = dict(tier=self.TIER)
            group = cdr.getGroup(self.session, self.NAME, **opts)
            group.name = self.NEWNAME
            group.users = self.NEWUSERS
            result = cdr.putGroup(self.session, self.NAME, group, **opts)
            self.assertIsNone(result)
            group = cdr.getGroup(self.session, self.NEWNAME, **opts)
            self.assertEqual(set(group.users), set(self.NEWUSERS))

        def test_14_get_groups__(self):
            opts = dict(tier=self.TIER)
            self.assertIn(self.NEWNAME, cdr.getGroups(self.session, **opts))

        def test_15_del_group___(self):
            opts = dict(tier=self.TIER)
            self.assertIsNone(cdr.delGroup(self.session, self.NEWNAME, **opts))

    class _04DoctypeTests___(Tests):

        def __fix_xxtest_schema(self):
            with open("{}/{}".format(self.TEST_DIR, "xxtest.xsd"), "rb") as fp:
                xsd = fp.read().strip()
            query = db.Query("document", "id", "xml")
            query.where("title = 'xxtest.xml'")
            row = query.execute().fetchone()
            if row:
                id, xml = row
                if xml.strip() == xsd:
                    return
                opts = dict(checkout="Y", tier=self.TIER, getObject=True)
                doc = cdr.getDoc(self.session, id, **opts)
                doc.xml = xsd
                opts = dict(tier=self.TIER, doc=str(doc), check_in="Y")
                cdr.repDoc(self.session, **opts)
            else:
                ctrl = {"DocTitle": "xxtest.xml"}
                doc = cdr.makeCdrDoc(xsd, "schema", None, ctrl)
                cdr.addDoc(self.session, doc=doc, check_in="Y", tier=self.TIER)

        def test_16_add_doctype_(self):
            with open("{}/{}".format(self.TEST_DIR, "dada.xsd"), "rb") as fp:
                xsd = fp.read().decode("utf-8")
            ctrl = {"DocTitle": "dada.xml"}
            doc = cdr.makeCdrDoc(xsd, "schema", None, ctrl)
            response = cdr.addDoc(self.session, doc=doc, tier=self.TIER)
            self.assertTrue(response.startswith("CDR"))
            comment = "test of CdrAddDocType"
            opts = {"type": "dada", "schema": "dada.xml", "comment": comment}
            info = cdr.dtinfo(**opts)
            response = cdr.addDoctype(self.session, info, tier=self.TIER)
            self.assertEqual(response.active, "Y")
            self.assertEqual(response.format, "xml")
            self.assertEqual(response.comment, comment)

        def test_17_get_doctypes(self):
            types = cdr.getDoctypes(self.session, tier=self.TIER)
            self.assertIn("dada", types)

        def test_18_mod_doctype_(self):
            info = cdr.getDoctype(self.session, "dada", tier=self.TIER)
            info.comment = None
            info.active = "N"
            info = cdr.modDoctype(self.session, info, tier=self.TIER)
            self.assertEqual(info.active, "N")
            self.assertIsNone(info.comment)

        def test_19_list_schemas(self):
            titles = cdr.getSchemaDocs(self.session, tier=self.TIER)
            self.assertIn("dada.xml", titles)

        def test_20_get_doctype_(self):
            self.__fix_xxtest_schema()
            doctype = cdr.getDoctype(self.session, "xxtest", tier=self.TIER)
            self.assertIn("Generated from xxtest", doctype.dtd)
            doctype = cdr.getDoctype(self.session, "Summary", tier=self.TIER)
            self.assertIn("AvailableAsModule", doctype.dtd)
            self.assertEqual(doctype.format, "xml")
            self.assertEqual(doctype.versioning, "Y")
            self.assertEqual(doctype.active, "Y")
            opts = dict(tier=self.TIER)
            vv_list = cdr.getVVList(self.session, "dada", "gimte", **opts)
            self.assertIn("Niedersachsen", vv_list)
            self.assertIn(u"K\xf6ln", vv_list)

        def test_21_del_doctype_(self):
            try:
                result = cdr.delDoctype(self.session, "dada", tier=self.TIER)
                self.assertIsNone(result)
                types = cdr.getDoctypes(self.session, tier=self.TIER)
                self.assertNotIn("dada", types)
            finally:
                query = db.Query("document d", "d.id")
                query.join("doc_type t", "t.id = d.doc_type")
                query.where(query.Condition("d.title", "dada.xml"))
                query.where(query.Condition("t.name", "schema"))
                for row in query.execute().fetchall():
                    cdr.delDoc(self.session, row.id, tier=self.TIER)

        def test_22_get_css_____(self):
            files = cdr.getCssFiles(self.session, tier=self.TIER)
            self.assertTrue(isinstance(files, list))
            if files:
                names = set([f.name for f in files])
                self.assertTrue("Summary.css" in names)


    class _05DocumentTests__(Tests):

        CURSOR = db.connect(tier=Tests.TIER).cursor()
        LABEL = "Test Label"
        COMMENT = "Test comment"
        BREAST_CANCER = "38832"
        MALE_BREAST_CANCER = "41338"
        ADULT_SOLID_TUMOR = "40460"

        doc = None
        doc_ids = []

        def __make_doc(self, doc_filename, doc_id=None):
            with open("{}/{}".format(self.TEST_DIR, doc_filename), "rb") as fp:
                xml = fp.read()
            ctrl = {"DocTitle": "test doc"}
            return cdr.makeCdrDoc(xml, "xxtest", doc_id, ctrl)

        def __get_opts(self, doc):
            return {
                "doc": doc,
                "comment": "sauve qui peut",
                "reason": "pourquoi pas?",
                "val": "Y",
                "ver": "Y",
                "show_warnings": True,
                "tier": self.TIER
            }

        def __fix_cdr5000(self):
            path = "{}/{}".format(self.TEST_DIR, "CDR5000.xml")
            with open(path, "rb") as fp:
                xml = fp.read().strip()
            query = db.Query("document", "xml")
            query.where("id = 5000")
            row = query.execute().fetchone()
            if row.xml.strip() != xml:
                opts = dict(checkout="Y", tier=self.TIER, getObject=True)
                doc = cdr.getDoc(self.session, 5000, **opts)
                doc.xml = xml
                opts = dict(tier=self.TIER, doc=str(doc), check_in="Y")
                cdr.repDoc(self.session, **opts)

        def test_23_add_doc_____(self):
            doc = self.__make_doc("001.xml")
            response = cdr.addDoc(self.session, **self.__get_opts(doc))
            doc_id, errors = response
            self.assertTrue(b"not accepted by the pattern" in errors)
            self.assertTrue(doc_id.startswith("CDR"))
            self.__class__.doc_ids.append(doc_id)
            self.__class__.doc = doc_id
            ctrl = dict(DocTitle="bad")
            doc = cdr.makeCdrDoc(u"<foo>\uEBAD<foo>", "xxtest", ctrl=ctrl)
            doc_id = cdr.addDoc(self.session, doc=doc, tier=self.TIER)
            self.assertTrue(doc_id.startswith("CDR"))
            self.__class__.doc_ids.append(doc_id)
            opts = dict(getObject=True, tier=self.TIER)
            doc = cdr.getDoc(self.session, doc_id, **opts)
            self.assertEqual(doc.ctrl.get("DocValStatus"), b"M")

        def test_24_get_doc_____(self):
            self.__fix_cdr5000()
            opts = dict(tier=self.TIER, getObject=True)
            doc = cdr.getDoc(self.session, 5000, **opts)
            self.assertEqual(doc.type, "Person")
            doc = cdr.getDoc(self.session, self.__class__.doc, **opts)
            self.__class__.doc = doc
            self.assertEqual(doc.type, "xxtest")

        def test_25_rep_doc_____(self):
            doc = self.__class__.doc
            original_id = doc.id
            with open("{}/{}".format(self.TEST_DIR, "002.xml"), "rb") as fp:
                doc.xml = fp.read()
            opts = self.__get_opts(str(doc))
            opts["blob"] = b"blob"
            opts["publishable"] = "Y"
            response = cdr.repDoc(self.session, **opts)
            doc_id, errors = response
            self.assertEqual(doc_id, original_id)
            self.assertTrue(not errors)
            opts = dict(
                blob="Y",
                tier=self.TIER,
                getObject=True,
                version="lastp"
            )
            doc = cdr.getDoc(self.session, original_id, **opts)
            self.assertEqual(doc.blob, b"blob")
            self.assertEqual(doc.ctrl["DocVersion"], b"2")

        def test_26_filter_doc__(self):
            filt = ["set:QC Summary Set"]
            result = cdr.filterDoc(self.session, filt, 62902, tier=self.TIER)
            self.assertTrue(b"small intestine cancer" in result[0])

        def test_27_val_doc_____(self):
            opts = dict(doc_id=5000, locators=True, tier=self.TIER)
            result = cdr.valDoc(self.session, "Person", **opts).decode("utf-8")
            expected = (
                "Element 'ProfessionalSuffix': This element is not expected.",
                "eref="
            )
            for e in expected:
                self.assertIn(e, result)
            opts = dict(doc=u"<x>\uEBAD<x>", tier=self.TIER)
            result = cdr.valDoc(self.session, "xxtest", **opts).decode("utf-8")
            for expected in ("private use char", "malformed"):
                self.assertIn(expected, result)

        def test_28_del_doc_____(self):
            for cdr_id in self.__class__.doc_ids:
                response = cdr.delDoc(self.session, cdr_id, tier=self.TIER)
                self.assertTrue(response.startswith("CDR"))
        def test_29_add_mapping_(self):
            usage = "GlossaryTerm Phrases"
            value = "test bogus mapping {}".format(datetime.datetime.now())
            args = usage, value
            opts = dict(bogus="Y", mappable="N", tier=self.TIER)
            mapping_id = cdr.addExternalMapping(self.session, *args, **opts)
            conn = db.connect(tier=self.TIER)
            cursor = conn.cursor()
            query = db.Query("external_map", "value", "bogus", "mappable")
            query.where(query.Condition("id", mapping_id))
            mapping = query.execute(cursor).fetchone()
            self.assertEqual(mapping.value, value)
            self.assertEqual(mapping.bogus, "Y")
            self.assertEqual(mapping.mappable, "N")
            delete = "DELETE FROM external_map WHERE id = ?"
            cursor.execute(delete, (mapping_id,))
            conn.commit()

        def test_30_lock_doc____(self):
            ctrl = dict(DocTitle="lock test")
            doc = cdr.makeCdrDoc(u"<xxtest/>", "xxtest", ctrl=ctrl)
            opts = dict(doc=doc, check_in="Y", tier=self.TIER)
            self.__class__.doc_id = doc_id = cdr.addDoc(self.session, **opts)
            lock = cdr.isCheckedOut(doc_id)
            self.assertIsNone(lock)
            result = cdr.checkOutDoc(self.session, doc_id, tier=self.TIER)
            self.assertEqual(result, 0)
            lock = cdr.isCheckedOut(doc_id)
        def test_31_unlock_doc__(self):
            doc_id = self.__class__.doc_id
            cdr.unlock(self.session, doc_id, tier=self.TIER)
            lock = cdr.isCheckedOut(doc_id)
            self.assertIsNone(lock)
            cdr.delDoc(self.session, doc_id, tier=self.TIER)
        def test_32_create_label(self):
            opts = dict(comment=self.COMMENT, tier=self.TIER)
            result = cdr.create_label(self.session, self.LABEL, **opts)
            self.assertIsNone(result)
            query = db.Query("version_label", "comment")
            query.where(query.Condition("name", self.LABEL))
            row = query.execute(self.CURSOR).fetchone()
            self.assertEqual(row.comment, self.COMMENT)
        def test_33_label_doc___(self):
            ctrl = dict(DocTitle="label test")
            doc = cdr.makeCdrDoc(u"<xxtest/>", "xxtest", ctrl=ctrl)
            opts = dict(doc=doc, ver="Y", check_in="Y", tier=self.TIER)
            self.__class__.doc_id = doc_id = cdr.addDoc(self.session, **opts)
            args = self.session, doc_id, 1, self.LABEL
            result = cdr.label_doc(*args, tier=self.TIER)
            self.assertIsNone(result)
            version = "label {}".format(self.LABEL)
            opts = dict(version=version, xml="N", tier=self.TIER)
            doc = cdr.getDoc(self.session, doc_id, **opts)
            root = etree.fromstring(doc)
            version = cdr.get_text(root.find("CdrDocCtl/DocVersion"))
            self.assertEqual(version, "1")
        def test_34_unlabel_doc_(self):
            args = self.session, self.__class__.doc_id, self.LABEL
            result = cdr.unlabel_doc(*args, tier=self.TIER)
            self.assertIsNone(result)
            version = "label {}".format(self.LABEL)
            try:
                test = self.assertRaisesRegex
            except:
                test = self.assertRaisesRegexp
            expected = "(?i)no version labeled {}".format(self.LABEL)
            doc_id = self.__class__.doc_id
            opts = dict(version=version, xml="N", tier=self.TIER)
            with test(Exception, expected):
                doc = cdr.getDoc(self.session, doc_id, **opts)
        def test_35_delete_label(self):
            opts = dict(tier=self.TIER)
            result = cdr.delete_label(self.session, self.LABEL, **opts)
            self.assertIsNone(result)
            query = db.Query("version_label", "comment")
            query.where(query.Condition("name", self.LABEL))
            row = query.execute(self.CURSOR).fetchone()
            self.assertIsNone(row)
            cdr.delDoc(self.session, self.__class__.doc_id, tier=self.TIER)
        def test_36_set_doc_stat(self):
            ctrl = dict(DocTitle="doc status test")
            xml = u"<xxtest><a>dada</a></xxtest>"
            doc = cdr.makeCdrDoc(xml, "xxtest", None, ctrl)
            doc_id = cdr.addDoc(self.session, doc=doc, tier=self.TIER)
            self.assertTrue(doc_id.startswith("CDR"))
            doc_id = int(doc_id[3:])
            status = cdr.getDocStatus(self.session, doc_id, tier=self.TIER)
            self.assertEqual(status, "A")
            opts = dict(comment="testing setDocStatus()", tier=self.TIER)
            result = cdr.setDocStatus(self.session, doc_id, "I", **opts)
            self.assertIsNone(result)
            status = cdr.getDocStatus(self.session, doc_id, tier=self.TIER)
            self.assertEqual(status, "I")
            query = db.Query("audit_trail", "COUNT(*) AS n")
            query.where(query.Condition("document", doc_id))
            self.assertEqual(query.execute().fetchone().n, 2)
            query = db.Query("audit_trail_added_action", "COUNT(*) AS n")
            query.where(query.Condition("document", doc_id))
            self.assertEqual(query.execute().fetchone().n, 1)
            cdr.delDoc(self.session, doc_id, tier=self.TIER)
        def test_37_update_title(self):
            with open("{}/{}".format(self.TEST_DIR, "004.xml"), "rb") as fp:
                filter1 = fp.read()
            with open("{}/{}".format(self.TEST_DIR, "005.xml"), "rb") as fp:
                filter2 = fp.read()
            conn = db.connect()
            cursor = conn.cursor()
            query = db.Query("doc_type", "title_filter")
            query.where("name = 'xxtest'")
            filter_id = query.execute(cursor).fetchone().title_filter
            if filter_id is None:
                ctrl = dict(DocTitle="Filter title: DocTitle for xxtest")
                doc = cdr.makeCdrDoc(filter1, "Filter", None, ctrl)
                doc_id = cdr.addDoc(self.session, doc=doc, tier=self.TIER)
                self.assertTrue(doc_id.startswith("CDR"))
                filter_id = int(doc_id[3:])
                update = "UPDATE doc_type SET title_filter = ? WHERE name = ?"
                cursor.execute(update, (filter_id, "xxtest"))
                conn.commit()
            else:
                opts = dict(getObject=True, tier=self.TIER)
                doc = cdr.getDoc(self.session, filter_id, **opts)
                doc.xml = filter1
                doc_id = cdr.repDoc(self.session, doc=str(doc), tier=self.TIER)
                self.assertTrue(doc_id.startswith("CDR"))
            xml = u"<xxtest><a>gimte</a></xxtest>"
            doc = cdr.makeCdrDoc(xml, "xxtest")
            doc_id = cdr.addDoc(self.session, doc=doc, tier=self.TIER)
            self.assertTrue(doc_id.startswith("CDR"))
            doc_id = int(doc_id[3:])
            query = db.Query("document", "title")
            query.where(query.Condition("id", doc_id))
            title = query.execute(cursor).fetchone().title
            self.assertEqual(title, "This is a test gimte")
            opts = dict(getObject=True, tier=self.TIER)
            doc = cdr.getDoc(self.session, filter_id, **opts)
            doc.xml = filter2
            filter_id = cdr.repDoc(self.session, doc=str(doc), tier=self.TIER)
            self.assertTrue(filter_id.startswith("CDR"))
            result = cdr.updateTitle(self.session, doc_id, tier=self.TIER)
            self.assertTrue(result)
            title = query.execute(cursor).fetchone().title
            self.assertEqual(title, "Modified title for gimte")
        def test_38_english_map_(self):
            name = u"stage II cutaneous T-cell lymphoma"
            phrase = u"STAGE IIA CUTANEOUS T CELL LYMPHOMA"
            names = cdr.get_glossary_map(self.session, "en", tier=self.TIER)
            self.assertTrue(isinstance(names, list))
            index = dict([(n.id, n) for n in names])
            self.assertEqual(index[43966].name, name)
            self.assertTrue(phrase in index[43966].phrases)
        def test_39_spanish_map_(self):
            name = u"microscopio electr\xf3nico"
            phrase = u"ELECTR\xd3NICA"
            names = cdr.get_glossary_map(self.session, "es", tier=self.TIER)
            self.assertTrue(isinstance(names, list))
            index = dict([(n.id, n) for n in names])
            self.assertEqual(index[44025].name, name)
            self.assertTrue(phrase in index[44025].phrases)
        def test_40_last_doc_ver(self):
            ctrl = dict(DocTitle="last version test")
            xml = u"<xxtest><a>dada</a></xxtest>"
            doc = cdr.makeCdrDoc(xml, "xxtest", ctrl=ctrl)
            opts = dict(doc=doc, check_in="N", ver="N", tier=self.TIER)
            doc_id = cdr.addDoc(self.session, **opts)
            versions = cdr.lastVersions(self.session, doc_id, tier=self.TIER)
            self.assertEqual(versions, (-1, -1, "Y"))
            opts["ver"] = "Y"
            opts["doc"] = cdr.makeCdrDoc(xml, "xxtest", doc_id, ctrl=ctrl)
            cdr.repDoc(self.session, **opts)
            versions = cdr.lastVersions(self.session, doc_id, tier=self.TIER)
            self.assertEqual(versions, (1, -1, "N"))
            opts["ver"] = "N"
            cdr.repDoc(self.session, **opts)
            versions = cdr.lastVersions(self.session, doc_id, tier=self.TIER)
            self.assertEqual(versions, (1, -1, "Y"))
            opts["ver"] = "Y"
            opts["val"] = "Y"
            opts["publishable"] = "Y"
            cdr.repDoc(self.session, **opts)
            versions = cdr.lastVersions(self.session, doc_id, tier=self.TIER)
            self.assertEqual(versions, (2, 2, "N"))
            cdr.delDoc(self.session, doc_id, tier=self.TIER)
        def test_41_list_docvers(self):
            ctrl = dict(DocTitle="list versions test")
            xml = u"<xxtest><a>dada</a></xxtest>"
            doc = cdr.makeCdrDoc(xml, "xxtest", ctrl=ctrl)
            opts = dict(doc=doc, check_in="N", ver="Y", tier=self.TIER)
            doc_id = cdr.addDoc(self.session, **opts)
            self.assertTrue(doc_id.startswith("CDR"))
            opts["doc"] = cdr.makeCdrDoc(xml, "xxtest", doc_id, ctrl=ctrl)
            opts["comment"] = "this is version two"
            result = cdr.repDoc(self.session, **opts)
            self.assertEqual(result, doc_id)
            opts["comment"] = "this is version three"
            result = cdr.repDoc(self.session, **opts)
            self.assertEqual(result, doc_id)
            opts = dict(limit=2, tier=self.TIER)
            versions = cdr.listVersions(self.session, doc_id, **opts)
            self.assertEqual(len(versions), 2)
            version_number, date, comment = versions[0]
            last_version_date = date
            self.assertEqual(version_number, 3)
            self.assertEqual(comment, "this is version three")
            version_number, date, comment = versions[1]
            penultimate_version_date = date
            self.assertEqual(version_number, 2)
            self.assertEqual(comment, "this is version two")
            self.assertFalse(last_version_date < penultimate_version_date)
            cdr.delDoc(self.session, doc_id, tier=self.TIER)
        def test_42_publish_docs(self):
            args = self.session, "Primary", "Hotfix-Remove"
            opts = dict(docs=["CDR5000"], allowInActive="Y", tier=self.TIER)
            result = cdr.publish(*args, **opts)
            self.assertTrue(isinstance(result, tuple))
            self.assertEqual(len(result), 2)
            job_id, errors = result
            self.assertIsNotNone(job_id)
            self.assertIsNone(errors)
            conn = db.connect()
            cursor = conn.cursor()
            sql = "UPDATE pub_proc SET status = 'Failure' WHERE id = ?"
            cursor.execute(sql, (int(job_id),))
            conn.commit()
        def test_43_mailrcleanup(self):
            cdr.mailerCleanup(self.session)
            query = db.Query("pub_proc", "id").limit(2)
            query.where("status = 'Failure'")
            query.where("output_dir LIKE 'd:/cdr/Output/Mailer%'")
            failed_jobs = [row.id for row in query.execute().fetchall()]
            ctrl = dict(DocTitle="mailer cleanup test")
            template = "<Mailer><JobId>{}</JobId></Mailer>"
            xml = template.format(failed_jobs[0])
            doc = cdr.makeCdrDoc(xml, "Mailer", ctrl=ctrl)
            opts = dict(doc=doc, check_in="N", tier=self.TIER)
            will_fail = cdr.addDoc(self.session, **opts)
            self.assertTrue(will_fail.startswith("CDR"))
            xml = template.format(failed_jobs[1])
            doc = cdr.makeCdrDoc(xml, "Mailer", ctrl=ctrl)
            opts["check_in"] = "Y"
            will_succeed = cdr.addDoc(self.session, **opts)
            self.assertTrue(will_succeed.startswith("CDR"))
            session = cdr.login("cdrmailers", cdr.getpw("cdrmailers"))
            deleted, errors = cdr.mailerCleanup(session)
            self.assertTrue(len(deleted) == 1)
            self.assertEqual(cdr.normalize(deleted[0]), will_succeed)
            self.assertTrue(len(errors) == 1)
            self.assertTrue("checked out by another user" in errors[0])
            cdr.delDoc(self.session, will_fail)

    class _06FilterTests____(Tests):
        num_filters = 3
        set_name = "Test Filter Set {}".format(datetime.datetime.now())
        set_desc = "Test Filter Set Description"
        set_notes = "Quo usque tandem abutere Catalina patientia nostra?"
        def test_44_get_filters_(self):
            filters = cdr.getFilters(self.session, tier=self.TIER)
            self.assertTrue(filters[0].id.startswith("CDR"))
            self.__class__.filters = dict([(f.name, f.id) for f in filters])
            self.assertTrue("Vendor Filter: Summary" in self.filters)
        def test_45_add_flt_set_(self):
            members = []
            for name in sorted(self.filters)[:self.num_filters]:
                members.append(cdr.IdAndName(self.filters[name], name))
            args = self.set_name, self.set_desc, None, members
            filter_set = cdr.FilterSet(*args)
            result = cdr.addFilterSet(self.session, filter_set, tier=self.TIER)
            self.assertEqual(result, len(members))
        def test_46_get_flt_sets(self):
            sets = cdr.getFilterSets(self.session, tier=self.TIER)
            self.assertTrue(isinstance(sets, list))
            self.assertTrue(isinstance(sets[0], cdr.IdAndName))
            self.__class__.filter_sets = dict([(s.name, s.id) for s in sets])
            self.assertTrue("Vendor Summary Set" in self.filter_sets)
            self.assertTrue(self.set_name in self.filter_sets)
        def test_47_get_flt_set_(self):
            name = "Vendor Summary Set"
            filter_set = cdr.getFilterSet(self.session, name, tier=self.TIER)
            self.assertTrue(isinstance(filter_set, cdr.FilterSet))
            last_filter = filter_set.members.pop()
            self.assertEqual(last_filter.name, "Vendor Filter: Final")
            name = self.set_name
            filter_set = cdr.getFilterSet(self.session, name, tier=self.TIER)
            self.assertEqual(filter_set.name, name)
            self.assertEqual(filter_set.desc, self.set_desc)
            self.assertIsNone(filter_set.notes)
            self.assertEqual(len(filter_set.members), 3)
        def test_48_rep_flt_set_(self):
            name = self.set_name
            filter_set = cdr.getFilterSet(self.session, name, tier=self.TIER)
            filter_set.notes = self.set_notes
            set_name = sorted(self.filter_sets)[0]
            set_id = self.filter_sets[set_name]
            member = cdr.IdAndName(set_id, set_name)
            filter_set.members.append(member)
            result = cdr.repFilterSet(self.session, filter_set, tier=self.TIER)
            self.assertEqual(result, self.num_filters + 1)
            filter_set = cdr.getFilterSet(self.session, name, tier=self.TIER)
            self.assertEqual(filter_set.name, name)
            self.assertEqual(filter_set.desc, self.set_desc)
            self.assertEqual(filter_set.notes, self.set_notes)
            self.assertTrue(isinstance(filter_set.members[0], cdr.IdAndName))
            self.assertTrue(isinstance(filter_set.members[0].id, basestring))
            self.assertTrue(isinstance(filter_set.members[-1], cdr.IdAndName))
            self.assertFalse(isinstance(filter_set.members[-1].id, basestring))
        def test_49_del_flt_set_(self):
            name = self.set_name
            result = cdr.delFilterSet(self.session, name, tier=self.TIER)
            self.assertIsNone(result)
            filter_sets = cdr.getFilterSets(self.session, tier=self.TIER)
            names = set([s.name for s in filter_sets])
            self.assertFalse(name in names)


    class _07LinkTests______(Tests):

        PROP_VALUE = '/Term/TermType/TermTypeName == "Semantic type"'
        PROP = "LinkTargetContains", PROP_VALUE, "test property"
        NAME = "Test Link Type"
        NEW_NAME = "Test Link Type With New Name"
        COMMENT = "This is a test link type"
        NEW_COMMENT = "This is a different comment"
        BREAST_CANCER = "38832"
        MALE_BREAST_CANCER = "41338"
        ADULT_SOLID_TUMOR = "40460"

        def __get_term_doc_id(self, cursor, semantic_type=False):
            query = db.Query("query_term_pub", "doc_id")
            query.where("path = '/Term/TermType/TermTypeName'")
            query.where("value = 'Semantic type'")
            if semantic_type:
                return query.limit(1).execute(cursor).fetchone().doc_id
            sub_query = query
            query = db.Query("document d", "d.id")
            query.join("doc_type t", "t.id = d.doc_type")
            query.join("pub_proc_cg c", "c.id = d.id")
            query.where("t.name = 'Term'")
            query.where(query.Condition("d.id", sub_query, "NOT IN"))
            return query.limit(1).execute(cursor).fetchone().id
        def __make_test_xml(self, doc_id):
            pattern = '<xxtest xmlns:cdr="{}"><a cdr:ref="{}">x</a></xxtest>'
            return pattern.format(cdr.NAMESPACE, cdr.normalize(doc_id))
        def test_50_add_linktype(self):
            opts = dict(
                comment=self.COMMENT,
                linkChkType="P",
                linkTargets=["Term"],
                linkSources=[("xxtest", "a")],
                linkProps=[self.PROP]
            )
            linktype = cdr.LinkType(self.NAME, **opts)
            opts = dict(tier=self.TIER)
            action = "addlink"
            rc = cdr.putLinkType(self.session, None, linktype, action, **opts)
            self.assertIsNone(rc)
            cursor = db.connect(tier=self.TIER).cursor()
            doc_id = self.__get_term_doc_id(cursor, semantic_type=True)
            opts["link_validation"] = True
            opts["schema_validation"] = False
            opts["doc"] = self.__make_test_xml(doc_id)
            result = cdr.valDoc(self.session, "xxtest", **opts).decode("utf-8")
            self.assertTrue("<Errors" not in result)
            self.assertTrue("Failed link target rule" not in result)
            doc_id = self.__get_term_doc_id(cursor, semantic_type=False)
            opts["doc"] = self.__make_test_xml(doc_id)
            result = cdr.valDoc(self.session, "xxtest", **opts).decode("utf-8")
            self.assertTrue("<Errors" in result)
            self.assertTrue("Failed link target rule" in result)
        def test_51_linktypes___(self):
            types = cdr.getLinkTypes(self.session, tier=self.TIER)
            self.assertTrue(self.NAME in types)
        def test_52_get_linktype(self):
            linktype = cdr.getLinkType(self.session, self.NAME, tier=self.TIER)
            self.assertEqual(linktype.name, self.NAME)
            self.assertEqual(linktype.linkChkType, "P")
            self.assertEqual(linktype.linkTargets, ["Term"])
            self.assertEqual(linktype.linkSources, [("xxtest", "a")])
            self.assertEqual(linktype.linkProps, [self.PROP])
            self.assertEqual(linktype.comment, self.COMMENT)
        def test_53_rep_linktype(self):
            opts = dict(tier=self.TIER)
            action = "modlink"
            lt = cdr.getLinkType(self.session, self.NAME, **opts)
            lt.name = self.NEW_NAME
            lt.comment = self.NEW_COMMENT
            rc = cdr.putLinkType(self.session, self.NAME, lt, action, **opts)
            self.assertIsNone(rc)
            lt = cdr.getLinkType(self.session, self.NEW_NAME, **opts)
            self.assertEqual(lt.name, self.NEW_NAME)
            self.assertEqual(lt.comment, self.NEW_COMMENT)
            self.assertEqual(lt.linkChkType, "P")
            self.assertEqual(lt.linkTargets, ["Term"])
            self.assertEqual(lt.linkSources, [("xxtest", "a")])
            self.assertEqual(lt.linkProps, [self.PROP])
            lt.linkProps = [("BogusPropType", self.PROP_VALUE, "should fail")]
            expression = "^Property type '.*' not supported$"
            try:
                test = self.assertRaisesRegex
            except:
                test = self.assertRaisesRegexp
            with test(Exception, expression):
                name = self.NEW_NAME
                cdr.putLinkType(self.session, name, lt, action, **opts)
        def test_54_del_linktype(self):
            opts = dict(tier=self.TIER)
            types = cdr.getLinkTypes(self.session, tier=self.TIER)
            for name in (self.NAME, self.NEW_NAME):
                if name in types:
                    rc = cdr.delLinkType(self.session, self.NEW_NAME, **opts)
                    self.assertIsNone(rc)
            types = cdr.getLinkTypes(self.session, tier=self.TIER)
            self.assertTrue(self.NAME not in types)
            self.assertTrue(self.NEW_NAME not in types)
        def test_55_proptypes___(self):
            types = cdr.getLinkProps(self.session, tier=self.TIER)
            self.assertEqual(len(types), 1)
            self.assertEqual(types[0].name, "LinkTargetContains")
            self.assertTrue(isinstance(types[0].comment, basestring))
        def test_56_get_tree____(self):
            opts = dict(tier=self.TIER)
            tree = cdr.getTree(self.session, self.BREAST_CANCER, **opts)
            term = tree.terms[self.BREAST_CANCER]
            parents = [parent.id for parent in term.parents]
            children = [child.id for child in term.children]
            self.assertTrue(len(tree.terms) > 10)
            self.assertTrue(term.name.startswith("breast cancer"))
            self.assertTrue(self.MALE_BREAST_CANCER in children)
            self.assertTrue(self.ADULT_SOLID_TUMOR in parents)
        def test_57_paste_link__(self):
            query = db.Query("query_term", "doc_id").limit(1)
            query.where("path = '/Term/PreferredName'")
            query.where("value = 'bevacizumab'")
            target = query.execute().fetchone().doc_id
            opts = dict(tier=self.TIER)
            args = self.session, "Summary", "Intervention", target
            result = cdr.check_proposed_link(*args, **opts)
            self.assertTrue(result.startswith("bevacizumab"))
            args = self.session, "Summary", "Para", target
            try:
                test = self.assertRaisesRegex
            except:
                test = self.assertRaisesRegexp
            expected = ("Link from Para elements of Summary documents"
                        " not permitted")
            with test(Exception, expected):
                cdr.check_proposed_link(*args, **opts)
            args = self.session, "Summary", "GlossaryTermLink", target
            expected = ("Link from GlossaryTermLink elements of Summary "
                        "documents to document {} not permitted")
            with test(Exception, expected.format(cdr.normalize(target))):
                cdr.check_proposed_link(*args, **opts)
        def test_58_get_links___(self):
            links = cdr.get_links(self.session, 5000, tier=self.TIER)
            self.assertTrue(len(links) >= 4)
            self.assertTrue("Physician-Annual remail" in "".join(links))
            self.assertTrue(links[0].startswith("Document "))
            self.assertTrue("links to this document" in links[-1])
        def test_59_search_links(self):
            args = "guest", "Person", "PDQEditorialBoard"
            opts = dict(tier=self.TIER, limit=1, pattern="PDQ Adult%")
            docs = cdr.search_links(*args, **opts)
            self.assertTrue(isinstance(docs, (list, tuple)))
            self.assertEqual(len(docs), 1)
            self.assertTrue(isinstance(docs[0], cdr.IdAndName))
            self.assertIn("PDQ Adult", docs[0].name)
            args = "guest", "Summary", "Term"
            opts = dict(tier=self.TIER, pattern="BRAF%inhibitor%")
            docs = cdr.search_links(*args, **opts)
            for doc in docs:
                self.assertTrue(doc.name.upper().startswith("BRAF"))
                self.assertIn("inhibitor", doc.name.lower())

    class _07SearchTests____(Tests):
        PATH = "/Term/Gimte"
        RULE = "Test Rule Number One"
        def test_60_list_rules__(self):
            rules = cdr.listQueryTermRules(self.session, tier=self.TIER)
            self.assertTrue(self.RULE in rules)
        def test_61_add_qt_def__(self):
            opts = dict(tier=self.TIER)
            path = self.PATH
            rule = self.RULE
            response = cdr.addQueryTermDef(self.session, path, rule, **opts)
            self.assertIsNone(response)
            expression = "Duplicate query term definition"
            try:
                test = self.assertRaisesRegex
            except:
                test = self.assertRaisesRegexp
            with test(Exception, expression):
                cdr.addQueryTermDef(self.session, path, None, **opts)
            with test(Exception, "Unknown query term rule"):
                cdr.addQueryTermDef(self.session, "/XXX", "bogus", **opts)
        def test_62_list_defs___(self):
            defs = cdr.listQueryTermDefs(self.session, tier=self.TIER)
            self.assertTrue((self.PATH, self.RULE) in defs)
            self.assertFalse((self.PATH, None) in defs)
        def test_63_del_qt_def__(self):
            opts = dict(tier=self.TIER)
            rule = self.RULE
            path = self.PATH
            response = cdr.delQueryTermDef(self.session, path, None, **opts)
            self.assertIsNone(response)
            try:
                test = self.assertRaisesRegex
            except:
                test = self.assertRaisesRegexp
            with test(Exception, "Query term definition not found"):
                cdr.delQueryTermDef(self.session, path, rule, **opts)
            defs = cdr.listQueryTermDefs(self.session, tier=self.TIER)
            self.assertFalse((self.PATH, self.RULE) in defs)
            self.assertFalse((self.PATH, None) in defs)
        def test_64_reindex_doc_(self):
            self.assertIsNone(cdr.reindex(self.session, 5000, tier=self.TIER))
        def test_65_search______(self):
            opts = dict(doctypes=["PublishingSystem"], tier=self.TIER)
            docs = cdr.search("guest", **opts)
            self.assertIn("Primary", str(docs))
            query = "CdrCtl/Title begins genetics"
            opts["doctypes"] = ["Summary"]
            docs = cdr.search("guest", query, **opts)
            for doc in docs:
                self.assertEqual("Summary", doc.docType)
                self.assertTrue(doc.docTitle.lower().startswith("genetics"))
                self.assertTrue(doc.docId.startswith("CDR0"))


    class _07UserTests______(Tests):
        STAMP = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        NAME = "testuser{}".format(STAMP)
        FULLNAME = "Test User {}".format(STAMP)
        PASSWORD = Tests.make_password()
        OFFICE = "Starbucks"
        PHONE = "Butterfield 8"
        EMAIL = "test@example.com"
        GROUPS = ["Regression Testers"]
        AUTHMODE = "local"
        COMMENT = "no comment"
        NEW_COMMENT = "record has been modified"
        NEW_NAME = NAME + "_mod"
        OPTS = dict(
            password=PASSWORD,
            fullname=FULLNAME,
            office=OFFICE,
            phone=PHONE,
            email=EMAIL,
            groups=GROUPS,
            comment=COMMENT,
            authMode=AUTHMODE
        )
        def test_66_add_user____(self):
            user = cdr.User(self.NAME, **self.OPTS)
            result = cdr.putUser(self.session, None, user, tier=self.TIER)
            self.assertIsNone(result)
        def test_67_mod_user____(self):
            opts = dict(self.OPTS)
            opts["comment"] = self.NEW_COMMENT
            user = cdr.User(self.NEW_NAME, **opts)
            result = cdr.putUser(self.session, self.NAME, user, tier=self.TIER)
            self.assertIsNone(result)
        def test_68_get_user____(self):
            user = cdr.getUser(self.session, self.NEW_NAME, tier=self.TIER)
            self.assertEqual(user.name, self.NEW_NAME)
            self.assertEqual(user.comment, self.NEW_COMMENT)
            self.assertEqual(user.office, self.OFFICE)
            self.assertEqual(user.phone, self.PHONE)
            self.assertEqual(user.groups, self.GROUPS)
            self.assertEqual(user.authMode, self.AUTHMODE)
        def test_69_list_users__(self):
            users = cdr.getUsers(self.session, tier=self.TIER)
            self.assertTrue(self.NEW_NAME in users)
            self.assertFalse(self.NAME in users)
        def test_70_del_user____(self):
            result = cdr.delUser(self.session, self.NEW_NAME, tier=self.TIER)
            self.assertIsNone(result)
            users = cdr.getUsers(self.session, tier=self.TIER)
            self.assertFalse(self.NEW_NAME in users)
            self.assertFalse(self.NAME in users)
        def test_71_log_cli_evnt(self):
            description = "This is a test event description"
            cdr.log_client_event(self.session, description, tier=self.TIER)
            query = db.Query("client_log", "event_time", "event_desc")
            query.limit(1).order("event_time DESC")
            row = query.execute().fetchone()
            self.assertEqual(description, row.event_desc)
            delta = datetime.datetime.now() - row.event_time
            self.assertTrue(abs(delta.total_seconds()) < 5)
        def test_72_save_trcelog(self):
            opts = dict(tier=self.TIER)
            args = datetime.datetime.now(), self.session
            log_data = "{} logon(joe, {}) test trace log data".format(*args)
            log_id = cdr.save_client_trace_log(self.session, log_data, **opts)
            self.assertTrue(isinstance(log_id, int))
            query = db.Query("dll_trace_log", "cdr_user", "session_id")
            query.where(query.Condition("log_id", log_id))
            row = query.execute().fetchone()
            self.assertEqual(row.cdr_user, "joe")
            self.assertEqual(row.session_id, self.session)


    class _08ReportingTests_(Tests):
        def test_73_report______(self):
            with open("{}/{}".format(self.TEST_DIR, "003.xml"), "rb") as fp:
                xml = fp.read()
            #ctrl = {"DocTitle": "reporting test"}
            doc = cdr.makeCdrDoc(xml, "Person")#, None, ctrl)
            doc_id = cdr.addDoc(self.session, doc=doc, tier=self.TIER)
            self.assertTrue(doc_id.startswith("CDR"))
            name = "Dated Actions"
            opts = dict(parms=dict(DocType="Person"), tier=self.TIER)
            report = cdr.report(self.session, name, **opts)
            report = etree.tostring(report, encoding="utf-8").decode("utf-8")
            self.assertIn(doc_id, report)
            self.assertIn("Test the reports module", report)
            name = "Person Locations Picklist"
            opts["parms"] = dict(DocId=doc_id)
            report = cdr.report(self.session, name, **opts)
            report = etree.tostring(report, encoding="utf-8").decode("utf-8")
            self.assertIn("Kalamazoo", report)
            name = "Person Address Fragment"
            opts["parms"] = dict(Link="{}#_3".format(doc_id))
            report = cdr.report(self.session, name, **opts)
            report = etree.tostring(report, encoding="utf-8").decode("utf-8")
            self.assertIn("Shady Grove", report)
            cdr.delDoc(self.session, doc_id, tier=self.TIER)

    class _09SystemCtlTests_(Tests):
        def test_74_set_control_(self):
            opts = dict(group="test", name="n", value="v", comment="c")
            opts["tier"] = self.TIER
            result = cdr.updateCtl(self.session, "Create", **opts)
            self.assertIsNone(result)
            value = cdr.getControlValue("test", "n", tier=self.TIER)
            self.assertEqual("v", value)
            result = cdr.updateCtl(self.session, "Inactivate", **opts)
            self.assertIsNone(result)
            value = cdr.getControlValue("test", "n", tier=self.TIER)
            self.assertIsNone(value)

if __name__ == "__main__":
    unittest.main()
