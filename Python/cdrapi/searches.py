"""
Search support in the CDR
"""

from cdrapi.db import Query

class QueryTermDef:
    def __init__(self, session, path, rule=None):
        self.session = session
        self.path = path
        self.rule = rule

    @property
    def rule_id(self):
        if not hasattr(self, "_rule_id"):
            if not self.rule:
                self._rule_id = None
            else:
                query = Query("query_term_rule", "id")
                query.where(query.Condition("name", self.rule))
                row = query.execute(self.session.cursor).fetchone()
                if not row:
                    message = "Unknown query term rule: {}".format(self.rule)
                    raise Exception(message)
                self._rule_id = row.id
        return self._rule_id

    def add(self):
        if not self.session.can_do("ADD QUERY TERM DEF"):
            message = "User not authorized to add query term definitions"
            raise Exception(message)
        if not self.path:
            raise Exception("Missing required path")
        query = Query("query_term_def", "COUNT(*) AS n")
        query.where(query.Condition("path", self.path))
        if query.execute(self.session.cursor).fetchone().n > 0:
            raise Exception("Duplicate query term definition")
        names = "path, term_rule"
        values = self.path, self.rule_id
        insert = "INSERT INTO query_term_def ({}) VALUES (?, ?)".format(names)
        self.session.cursor.execute(insert, values)
        self.session.conn.commit()

    def delete(self):
        if not self.session.can_do("DELETE QUERY TERM DEF"):
            message = "User not authorized to delete query term definitions"
            raise Exception(message)
        if not self.path:
            raise Exception("Missing required path")
        delete = "DELETE FROM query_term_def WHERE path = ?"
        self.session.cursor.execute(delete, (self.path,))
        if self.session.cursor.rowcount != 1:
            self.session.cursor.execute("ROLLBACK TRANSACTION")
            raise Exception("Query term definition not found")
        self.session.conn.commit()

    @classmethod
    def get_rules(cls, session):
        query = Query("query_term_rule", "name").order("name")
        return [row.name for row in query.execute(session.cursor).fetchall()]

    @classmethod
    def get_definitions(cls, session):
        query = Query("query_term_def d", "d.path", "r.name")
        query.outer("query_term_rule r", "r.id = d.term_rule")
        query.order("d.path", "r.name")
        definitions = []
        for row in query.execute(session.cursor).fetchall():
            definitions.append(QueryTermDef(session, row.path, row.name))
        return definitions
