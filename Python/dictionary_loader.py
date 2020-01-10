"""Base class for drug and glossary (and possibly other?) dictionary loaders.
"""

from argparse import ArgumentParser
from datetime import datetime
from json import load, loads
from re import compile
from sys import stderr
from unicodedata import normalize, combining
from elasticsearch5 import Elasticsearch
from lxml import etree
from cdr import Logging, getControlValue
from cdrapi import db
from cdrapi.docs import Doc
from cdrapi.settings import Tier


class DictionaryAPILoader:
    """Scaffolding for a nightly job to index a dictionary for its API.

    The derived class must override the ids() method. It must also
    provide a class constant for ALIAS, HOST, PORT, TYPE, and INDEXDEF,
    and/or implement the corresponding methods. It can optionally give
    a different LOGNAME than that provided by this class.
    """

    LOGNAME = "dictionary_loader"
    Query = db.Query

    def __init__(self, **opts):
        """Save the caller's runtime options for a rainy day."""
        self.__opts = opts

    def run(self):
        """Load the dictionary into a new index and adjust the alias."""

        self.__started = datetime.now()
        self.logger.info("Loading %d %s terms", len(self.ids), self.type)
        done = 0
        for term_id in self.ids:
            doc = self.Doc(self, term_id)
            doc.index()
            self.logger.debug("loaded CDR%d", term_id)
            if self.verbose:
                done += 1
                stderr.write(f"\rindexed {done} of {len(self.ids)} terms")
        self.create_alias()
        self.logger.info("aliased %s as %s", self.index, self.alias)
        self.logger.info("elapsed: %s", self.elapsed)
        if self.verbose:
            stderr.write("\ndone")

    def create_alias(self):
        """Point the canonical name for this dictionary to our new index."""

        try:
            actions = []
            if self.es.indices.exists_alias(name=self.alias):
                aliases = self.es.indices.get_alias(self.alias)
                for index in aliases:
                    if "aliases" in aliases[index]:
                        if self.alias in aliases[index]["aliases"]:
                            actions.append(dict(
                                remove=dict(
                                    index=index,
                                    alias=self.alias,
                                )
                            ))
            actions.append(dict(add=dict(index=self.index, alias=self.alias)))
            self.es.indices.update_aliases(body=dict(actions=actions))
        except Exception as e:
            self.logger.exception("failure redirecting %s", self.alias)
            raise

    @property
    def alias(self):
        """Canonical name for the dictionary's index."""

        try:
            return self.ALIAS
        except:
            raise Exception("derived class must provide alias name")

    @property
    def conn(self):
        """Read-only connection to the CDR database."""

        if not hasattr(self, "_conn"):
            self._conn = db.connect(user="CdrGuest", tier=self.tier.name)
        return self._conn

    @property
    def cursor(self):
        """Access to the CDR database tables."""

        if not hasattr(self, "_cursor"):
            self._cursor = self.conn.cursor()
        return self._cursor

    @property
    def doctype(self):
        """Elasticsearch name for the type (plural is intentional)."""
        return "terms"

    @property
    def elapsed(self):
        """Amount of time since this job was started."""
        return datetime.now() - self.started

    @property
    def es(self):
        """Connection to the Elasticsearch server."""

        if not hasattr(self, "_es"):
            self._es = Elasticsearch([dict(host=self.host, port=self.port)])
        return self._es

    @property
    def host(self):
        """Name of the Elasticsearch server (override and/or define HOST)."""

        if not hasattr(self, "_host"):
            self._host = self.opts.get("host")
            if not self._host and hasattr(self, "HOST"):
                self._host = self.HOST
            if not self._host:
                self._host = self.tier.hosts.get("DICTIONARY")
            if not self._host:
                raise Exception("no database host specified")
        return self._host

    @property
    def ids(self):
        """Sequence of integers for the CDR documents to be indexed."""
        raise Exception("derived class must override ids() method")

    @property
    def index(self):
        """String for the name of the newly created index."""

        if not hasattr(self, "_index"):
            self._index = f"{self.ALIAS}-{self.stamp}"
            self.es.indices.create(index=self._index, body=self.indexdef)
        return self._index

    @property
    def indexdef(self):
        """Schema for our index.

        INDEXDEF can be a filename or a string
        """

        if not hasattr(self, "_indexdef"):
            if not hasattr(self, "INDEXDEF"):
                raise Exception("no schema provided")
            try:
                indexdef = getControlValue("dictionary", self.INDEXDEF)
                if indexdef:
                    self._indexdef = loads(indexdef)
                    return self._indexdef
                with open(self.INDEXDEF) as fp:
                    self._indexdef = load(fp)
            except FileNotFoundError:
                try:
                    self._indexdef = loads(self.INDEXDEF)
                except:
                    name = self.INDEXDEF
                    self.logger.exception("Loading schema from string")
                    raise Exception("can't load index schema")
            except Exception:
                self.logger.exception("Loading schema from %s", self.INDEXDEF)
                raise Exception(f"can't load schema from {self.INDEXDEF}")
        return self._indexdef

    @property
    def logger(self):
        """Tool for recording what we do."""

        if not hasattr(self, "_logger"):
            opts = dict(level=self.loglevel)
            self._logger = Logging.get_logger(self.LOGNAME, **opts)
        return self._logger

    @property
    def loglevel(self):
        """Defaults to 'INFO'."""
        return (self.opts.get("loglevel") or "INFO").upper()

    @property
    def opts(self):
        """Runtime options."""
        return self.__opts

    @property
    def port(self):
        """TCP/IP port on which we connect."""

        if not hasattr(self, "_port"):
            port = self.opts.get("")
            if not port and hasattr(self, "PORT"):
                port = self.PORT
            if not port:
                port = self.tier.ports.get("dictionary")
            if not port:
                raise Exception("no database port specified")
            try:
                self._port = int(port)
            except Exception:
                raise Exception("invalid port value")
        return self._port

    @property
    def stamp(self):
        """Date/time string used to create unique names."""

        if not hasattr(self, "_stamp"):
            self._stamp = self.started.strftime("%Y%m%d%H%M%S")
        return self._stamp

    @property
    def started(self):
        """Read-only property for when this job began."""
        return self.__started

    @property
    def tier(self):
        """Which CDR server are we using?"""

        if not hasattr(self, "_tier"):
            self._tier = Tier(self.opts.get("tier"))
        return self._tier

    @property
    def type(self):
        """String for the type of dictionary ("drug" or "glossary")."""

        try:
            return self.TYPE
        except:
            raise Exception("derived class must define type() method or TYPE")

    @property
    def verbose(self):
        """Should we display progress?"""
        return True if self.opts.get("verbose") else False

    class Doc:
        """Override for specific dictionary types.

        Internal class values are for transforming names to pretty URLs.
        See `Doc.clean()` below.
        """

        _FROM = "\u03b1\u03b2\u03bc;_&\u2013/"
        _TO = "abu-----"
        _STRIP = "\",+().\xaa'\u2019[\uff1a:*\\]"
        _TRANS = str.maketrans(_FROM, _TO, _STRIP)
        _SPACES = compile(r"\s+")
        AUDIENCE = {
            "Health_professionals": "Health professional",
            "Health_professional": "Health professional",
            "Health professional": "Health professional",
            "Health Professional": "Health professional",
            "Patients": "Patient",
            "Patient": "Patient",
        }

        def __init__(self, loader, id):
            """Save the caller's values.

            Pass:
                loader - access to the database and the current CDR session
                id - integer for the document's CDR ID
            """

            self.__loader = loader
            self.__id = id

        @property
        def cdr_id(self):
            """String for the canonical format of the CDR ID."""
            return f"CDR{self.id:010d}"

        @property
        def id(self):
            """Integer for the document's CDR ID."""
            return self.__id

        @property
        def loader(self):
            """Access to the database and the current CDR session."""
            return self.__loader

        @property
        def root(self):
            """Top node for the document's parsed XML tree."""

            if not hasattr(self, "_root"):
                query = self.__loader.Query("pub_proc_cg", "xml")
                query.where(query.Condition("id", self.id))
                xml = query.execute(self.__loader.cursor).fetchone().xml
                try:
                    self._root = etree.fromstring(xml)
                except:
                    self._root = etree.fromstring(xml.encode("utf-8"))
            return self._root

        @classmethod
        def clean(cls, name):
            """Prepare a term name for use as a pretty URL.

            Uses logic implemented by Bryan P. in JavaScript as part of
            github.com/NCIOCPL/wcms-cts-term-map-gen/blob/master/pdq_index.js
            (see getFriendlyUrlForDisplayName() at line 44 ff.).

            Pass:
                name - string for the term name

            Return:
                scrubbed name string
            """

            name = cls._SPACES.sub("-", name).lower().translate(cls._TRANS)
            nfkd = normalize("NFKD", name)
            pretty_url = "".join([c for c in nfkd if not combining(c)])
            return pretty_url.replace("%", "pct")
