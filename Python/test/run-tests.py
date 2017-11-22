#!/usr/bin/env python3

"""
Run these tests whenever changes are made to the API code or client wrapper

The numbers in the names guarantee the order in which the tests will be run.
The extra underscores in the names make the verbose output much easier to read.
"""

import datetime
import os
import random
import string
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
        password = cdr.getpw(self.USERNAME)
        opts = dict(comment="unit testing", tier=self.TIER, password=password)
        Tests.session = Session.create_session(self.USERNAME, **opts).name

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

        def test_16_add_doctype_(self):
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
            self.assertIn("dada.xsd", titles)

        def test_20_get_doctype_(self):
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
                query.where(query.Condition("d.title", "dada.xsd"))
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
            opts = dict(tier=self.TIER, getObject=True)
            doc = cdr.getDoc(self.session, 5000, **opts)
            self.assertEqual(doc.type, "Person")
            doc = cdr.getDoc(self.session, self.__class__.doc, **opts)
            self.__class__.doc = doc
            self.assertEqual(doc.type, "xxtest")

        def test_25_rep_doc_____(self):
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


    class _06FilterTests____(Tests):
        num_filters = 3
        set_name = "Test Filter Set {}".format(datetime.datetime.now())
        set_desc = "Test Filter Set Description"
        set_notes = "Quo usque tandem abutere Catalina patientia nostra?"
        def test_30_get_filters_(self):
            filters = cdr.getFilters(self.session, tier=self.TIER)
            self.assertTrue(filters[0].id.startswith("CDR"))
            self.__class__.filters = dict([(f.name, f.id) for f in filters])
            self.assertTrue("Vendor Filter: Summary" in self.filters)
        def test_31_add_flt_set_(self):
            members = []
            for name in sorted(self.filters)[:self.num_filters]:
                members.append(cdr.IdAndName(self.filters[name], name))
            args = self.set_name, self.set_desc, None, members
            filter_set = cdr.FilterSet(*args)
            result = cdr.addFilterSet(self.session, filter_set, tier=self.TIER)
            self.assertEqual(result, len(members))
        def test_32_get_flt_sets(self):
            sets = cdr.getFilterSets(self.session, tier=self.TIER)
            self.assertTrue(isinstance(sets, list))
            self.assertTrue(isinstance(sets[0], cdr.IdAndName))
            self.__class__.filter_sets = dict([(s.name, s.id) for s in sets])
            self.assertTrue("Vendor Summary Set" in self.filter_sets)
            self.assertTrue(self.set_name in self.filter_sets)
        def test_33_get_flt_set_(self):
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
        def test_34_rep_flt_set_(self):
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
        def test_35_del_flt_set_(self):
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
        def test_36_add_linktype(self):
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
        def test_37_linktypes___(self):
            types = cdr.getLinkTypes(self.session, tier=self.TIER)
            self.assertTrue(self.NAME in types)
        def test_38_get_linktype(self):
            linktype = cdr.getLinkType(self.session, self.NAME, tier=self.TIER)
            self.assertEqual(linktype.name, self.NAME)
            self.assertEqual(linktype.linkChkType, "P")
            self.assertEqual(linktype.linkTargets, ["Term"])
            self.assertEqual(linktype.linkSources, [("xxtest", "a")])
            self.assertEqual(linktype.linkProps, [self.PROP])
            self.assertEqual(linktype.comment, self.COMMENT)
        def test_39_rep_linktype(self):
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
        def test_40_del_linktype(self):
            opts = dict(tier=self.TIER)
            types = cdr.getLinkTypes(self.session, tier=self.TIER)
            for name in (self.NAME, self.NEW_NAME):
                if name in types:
                    rc = cdr.delLinkType(self.session, self.NEW_NAME, **opts)
                    self.assertIsNone(rc)
            types = cdr.getLinkTypes(self.session, tier=self.TIER)
            self.assertTrue(self.NAME not in types)
            self.assertTrue(self.NEW_NAME not in types)
        def test_41_proptypes___(self):
            types = cdr.getLinkProps(self.session, tier=self.TIER)
            self.assertEqual(len(types), 1)
            self.assertEqual(types[0].name, "LinkTargetContains")
            self.assertTrue(isinstance(types[0].comment, basestring))

    class _07SearchTests____(Tests):
        PATH = "/Term/Gimte"
        RULE = "Test Rule Number One"
        def test_42_list_rules__(self):
            rules = cdr.listQueryTermRules(self.session, tier=self.TIER)
            self.assertTrue(self.RULE in rules)
        def test_43_add_qt_def__(self):
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
        def test_44_list_defs___(self):
            defs = cdr.listQueryTermDefs(self.session, tier=self.TIER)
            self.assertTrue((self.PATH, self.RULE) in defs)
            self.assertFalse((self.PATH, None) in defs)
        def test_45_del_qt_def__(self):
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
        def test_46_add_user____(self):
            user = cdr.User(self.NAME, **self.OPTS)
            result = cdr.putUser(self.session, None, user, tier=self.TIER)
            self.assertIsNone(result)
        def test_47_mod_user____(self):
            opts = dict(self.OPTS)
            opts["comment"] = self.NEW_COMMENT
            user = cdr.User(self.NEW_NAME, **opts)
            result = cdr.putUser(self.session, self.NAME, user, tier=self.TIER)
            self.assertIsNone(result)
        def test_48_get_user____(self):
            user = cdr.getUser(self.session, self.NEW_NAME, tier=self.TIER)
            self.assertEqual(user.name, self.NEW_NAME)
            self.assertEqual(user.comment, self.NEW_COMMENT)
            self.assertEqual(user.office, self.OFFICE)
            self.assertEqual(user.phone, self.PHONE)
            self.assertEqual(user.groups, self.GROUPS)
            self.assertEqual(user.authMode, self.AUTHMODE)
        def test_49_list_users__(self):
            users = cdr.getUsers(self.session, tier=self.TIER)
            self.assertTrue(self.NEW_NAME in users)
            self.assertFalse(self.NAME in users)
        def test_50_del_user____(self):
            result = cdr.delUser(self.session, self.NEW_NAME, tier=self.TIER)
            self.assertIsNone(result)
            users = cdr.getUsers(self.session, tier=self.TIER)
            self.assertFalse(self.NEW_NAME in users)
            self.assertFalse(self.NAME in users)


if __name__ == "__main__":
    unittest.main()
