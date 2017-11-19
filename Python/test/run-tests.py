#!/usr/bin/env python3

"""
Run these tests whenever changes are made to the API code or client wrapper

The numbers in the names guarantee the order in which the tests will be run.
The extra underscores in the names make the verbose output much easier to read.
"""

import datetime
import os
import unittest
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
    TIER = os.environ.get("TEST_CDR_TIER")

    def setUp(self):
        opts = dict(comment="unit testing", tier=self.TIER)
        Tests.session = Session.create_session(self.USERNAME, **opts).name

    def tearDown(self):
        cdr.logout(self.session, tier=self.TIER)

# Set FULL to False temporarily when adding new tests so you can get
# the new ones working without having to grind through the entire set.

FULL = True
#FULL = False
if FULL:

    class _01SessionTests___(Tests):

        def test_01_login_______(self):
            opts = dict(comment="unit testing", tier=self.TIER)
            session = Session.create_session(self.USERNAME, **opts)
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

        def test_01_can_do______(self):
            opts = dict(tier=self.TIER)
            action = "ADD DOCUMENT"
            self.assertTrue(cdr.canDo("guest", action , "xxtest", **opts))
            self.assertFalse(cdr.canDo("guest", action, "Summary", **opts))
            self.assertTrue(cdr.canDo("guest", "LIST DOCTYPES", **opts))
            self.assertFalse(cdr.canDo("guest", "LIST USERS", **opts))

        def test_02_add_action__(self):
            opts = dict(tier=self.TIER)
            actions = cdr.getActions(self.session, **opts)
            for name in "gimte", "dada":
                if name in actions:
                    self.delete_action(name, **opts)
            action = cdr.Action("dada", "Y", "gimte")
            result = cdr.putAction(self.session, None, action, **opts)
            self.assertIsNone(result)

        def test_03_mod_action__(self):
            opts = dict(tier=self.TIER)
            action = cdr.getAction(self.session, "dada", **opts)
            action.name = "gimte"
            action.comment = "dada"
            action.doctype_specific = "N"
            result = cdr.putAction(self.session, "dada", action, **opts)
            self.assertIsNone(result)
            action = cdr.getAction(self.session, "ADD DOCUMENT", **opts)
            action.doctype_specific = "N"
            with self.assertRaises(Exception):
                cdr.putAction(self.session, "ADD DOCUMENT", action, **opts)

        def test_04_get_action__(self):
            opts = dict(tier=self.TIER)
            action = cdr.getAction(self.session, "gimte", **opts)
            self.assertEqual(action.comment, "dada")
            self.assertEqual(action.doctype_specific, "N")

        def test_05_del_action__(self):
            opts = dict(tier=self.TIER)
            self.assertIsNone(self.delete_action("gimte", **opts))

        def test_06_get_actions_(self):
            opts = dict(tier=self.TIER)
            actions = cdr.getActions(self.session, **opts)
            self.assertEqual(actions["ADD DOCUMENT"], "Y")
            self.assertEqual(actions["LIST USERS"], "N")


    class _03GroupTests_____(Tests):

        NAME = "Test Group"
        NEWNAME = "Test Group (MOD)"
        USERS = ["tester"]
        NEWUSERS = ["tester", "CdrGuest"]

        def test_01_add_group___(self):
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

        def test_02_get_group___(self):
            group = cdr.getGroup(self.session, self.NAME, tier=self.TIER)
            self.assertIsNotNone(group)
            self.assertEqual(set(group.users), set(self.USERS))
            self.assertEqual(group.comment, "dada")

        def test_03_mod_group___(self):
            opts = dict(tier=self.TIER)
            group = cdr.getGroup(self.session, self.NAME, **opts)
            group.name = self.NEWNAME
            group.users = self.NEWUSERS
            result = cdr.putGroup(self.session, self.NAME, group, **opts)
            self.assertIsNone(result)
            group = cdr.getGroup(self.session, self.NEWNAME, **opts)
            self.assertEqual(set(group.users), set(self.NEWUSERS))

        def test_04_get_groups__(self):
            opts = dict(tier=self.TIER)
            self.assertIn(self.NEWNAME, cdr.getGroups(self.session, **opts))

        def test_05_del_group___(self):
            opts = dict(tier=self.TIER)
            self.assertIsNone(cdr.delGroup(self.session, self.NEWNAME, **opts))


    class _04DoctypeTests___(Tests):

        def test_01_add_doctype_(self):
            directory = os.path.dirname(os.path.realpath(__file__))
            with open("{}/{}".format(directory, "dada.xsd"), "rb") as fp:
                xsd = fp.read().decode("utf-8")
            ctrl = {"DocTitle": "dada.xsd"}
            doc = cdr.makeCdrDoc(xsd, "schema", None, ctrl)
            response = cdr.addDoc(self.session, doc=doc, tier=self.TIER)
            self.assertTrue(response.startswith("CDR"))
            comment = "test of CdrAddDocType"
            opts = {"type": "dada", "schema": "dada.xsd", "comment": comment}
            info = cdr.dtinfo(**opts)
            response = cdr.addDoctype(self.session, info, tier=self.TIER)
            self.assertEqual(response.active, "Y")
            self.assertEqual(response.format, "xml")
            self.assertEqual(response.comment, comment)

        def test_02_get_doctypes(self):
            types = cdr.getDoctypes(self.session, tier=self.TIER)
            self.assertIn("dada", types)

        def test_03_mod_doctype_(self):
            info = cdr.getDoctype(self.session, "dada", tier=self.TIER)
            info.comment = None
            info.active = "N"
            info = cdr.modDoctype(self.session, info, tier=self.TIER)
            self.assertEqual(info.active, "N")
            self.assertIsNone(info.comment)

        def test_04_list_schemas(self):
            titles = cdr.getSchemaDocs(self.session, tier=self.TIER)
            self.assertIn("dada.xsd", titles)

        def test_05_get_doctype_(self):
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

        def test_06_del_doctype_(self):
            try:
                result = cdr.delDoctype(self.session, "dada", tier=self.TIER)
                self.assertIsNone(result)
                types = cdr.getDoctypes(self.session, tier=self.TIER)
                self.assertNotIn("dada", types)
            finally:
                query = db.Query("document d", "d.id")
                query.join("doc_type t", "t.id = d.doc_type")
                query.where(query.Condition("d.title", "dada.xsd"))
                query.where(query.Condition("t.name", "schema"))
                for row in query.execute().fetchall():
                    cdr.delDoc(self.session, row.id, tier=self.TIER)

        def test_07_get_css_____(self):
            files = cdr.getCssFiles(self.session, tier=self.TIER)
            self.assertTrue(isinstance(files, list))
            if files:
                names = set([f.name for f in files])
                self.assertTrue("Summary.css" in names)


    class _05DocumentTests__(Tests):

        doc = None
        doc_ids = []

        def __make_doc(self, doc_filename, doc_id=None):
            directory = os.path.dirname(os.path.realpath(__file__))
            with open("{}/{}".format(directory, doc_filename), "rb") as fp:
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

        def test_01_add_doc_____(self):
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

        def test_02_get_doc_____(self):
            opts = dict(tier=self.TIER, getObject=True)
            doc = cdr.getDoc(self.session, 5000, **opts)
            self.assertEqual(doc.type, "Person")
            doc = cdr.getDoc(self.session, self.__class__.doc, **opts)
            self.__class__.doc = doc
            self.assertEqual(doc.type, "xxtest")

        def test_03_rep_doc_____(self):
            doc = self.__class__.doc
            original_id = doc.id
            directory = os.path.dirname(os.path.realpath(__file__))
            with open("{}/{}".format(directory, "002.xml"), "rb") as fp:
                doc.xml = fp.read()
            opts = self.__get_opts(str(doc))
            opts["blob"] = b"blob"
            opts["publishable"] = "Y"
            response = cdr.repDoc(self.session, **opts)
            doc_id, errors = response
            #print(errors)
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

        def test_04_filter_doc__(self):
            filt = ["set:QC Summary Set"]
            result = cdr.filterDoc(self.session, filt, 62902, tier=self.TIER)
            self.assertTrue(b"small intestine cancer" in result[0])

        def test_05_val_doc_____(self):
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

        def test_06_del_doc_____(self):
            for cdr_id in self.__class__.doc_ids:
                response = cdr.delDoc(self.session, cdr_id, tier=self.TIER)
                self.assertTrue(response.startswith("CDR"))

        def test_07_add_mapping_(self):
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


    class _06FilterTests____(Tests):
        num_filters = 3
        set_name = "Test Filter Set {}".format(datetime.datetime.now())
        set_desc = "Test Filter Set Description"
        set_notes = "Quo usque tandem abutere Catalina patientia nostra?"
        def test_01_get_filters_(self):
            filters = cdr.getFilters(self.session, tier=self.TIER)
            self.assertTrue(filters[0].id.startswith("CDR"))
            self.__class__.filters = dict([(f.name, f.id) for f in filters])
            self.assertTrue("Vendor Filter: Summary" in self.filters)
        def test_02_add_flt_set_(self):
            members = []
            for name in sorted(self.filters)[:self.num_filters]:
                members.append(cdr.IdAndName(self.filters[name], name))
            args = self.set_name, self.set_desc, None, members
            filter_set = cdr.FilterSet(*args)
            result = cdr.addFilterSet(self.session, filter_set, tier=self.TIER)
            self.assertEqual(result, len(members))
        def test_03_get_flt_sets(self):
            sets = cdr.getFilterSets(self.session, tier=self.TIER)
            self.assertTrue(isinstance(sets, list))
            self.assertTrue(isinstance(sets[0], cdr.IdAndName))
            self.__class__.filter_sets = dict([(s.name, s.id) for s in sets])
            self.assertTrue("Vendor Summary Set" in self.filter_sets)
            self.assertTrue(self.set_name in self.filter_sets)
        def test_04_get_flt_set_(self):
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
        def test_05_rep_flt_set_(self):
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
        def test_06_del_flt_set_(self):
            name = self.set_name
            result = cdr.delFilterSet(self.session, name, tier=self.TIER)
            self.assertIsNone(result)
            filter_sets = cdr.getFilterSets(self.session, tier=self.TIER)
            names = set([s.name for s in filter_sets])
            self.assertFalse(name in names)


if __name__ == "__main__":
    unittest.main()
