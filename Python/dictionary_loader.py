"""Base class for drug and glossary (and possibly other?) dictionary loaders.
"""

from argparse import ArgumentParser
import datetime
from json import dumps, load, loads
from re import compile
from string import ascii_lowercase
from sys import stderr
from unicodedata import normalize, combining
from elasticsearch7 import Elasticsearch
from lxml import etree
from cdr import Logging, getControlValue
from cdrapi import db
from cdrapi.docs import Doc
from cdrapi.settings import Tier
from cdrapi.users import Session


class DictionaryAPILoader:
    """Scaffolding for a nightly job to index a dictionary for its API.

    The derived class must override the ids() method. It must also
    provide a class constant for ALIAS, HOST, PORT, TYPE, and INDEXDEF,
    and/or implement the corresponding methods. It can optionally give
    a different LOGNAME than that provided by this class.

    January 2020: added constructor to save override options for some of
    these values.
    """

    LOGNAME = "dictionary_loader"
    Query = db.Query

    def __init__(self, **opts):
        """Save the caller's runtime options for a rainy day."""
        self.__opts = opts

    def run(self):
        """Load the dictionary into a new index and adjust the alias."""

        self.__started = datetime.datetime.now()
        args = len(self.ids), self.type, self.host, self.port
        self.logger.info("Loading %d %s terms to %s:%s", *args)
        if self.testing:
            self.logger.info("Running in test mode")
            if self.verbose:
                stderr.write("Running in test mode")
        done = 0
        ids = self.opts.get("ids") or self.ids
        for term_id in ids:
            doc = self.Doc(self, term_id)
            doc.index()
            self.logger.debug("loaded CDR%d", term_id)
            if self.verbose:
                done += 1
                stderr.write(f"\rindexed {done} of {len(self.ids)} terms")
            if done >= self.limit:
                break

        if not self.testing:

            # Optimize the index.
            opts = dict(max_num_segments=1, index=self.index)
            self.es.indices.forcemerge(**opts)
            self.logger.info("New index optimized")

            # Point the canonical name to the new index.
            self.create_alias()

            # Housekeeping.
            self.cleanup()

        self.logger.info("aliased %s as %s", self.index, self.alias)
        self.logger.info("elapsed: %s", self.elapsed)
        if self.verbose:
            stderr.write("\ndone")

    def cleanup(self):
        """Drop old indices, keeping the latest ones as backup."""

        pattern = f"{self.alias}-20"
        date = datetime.date.today() - datetime.timedelta(self.days_to_keep)
        stamp = date.strftime("%Y%m%d")
        cutoff = f"{self.alias}-{stamp}"
        self.logger.info("Cleanup cutoff: %s", cutoff)
        indices = self.es.cat.indices(format="json")
        candidates = []
        for index in indices:
            name = index["index"]
            if name.startswith(pattern):
                candidates.append(name)
        kept = min(len(candidates), abs(self.indices_to_keep))
        candidates = sorted(candidates)[:-abs(self.indices_to_keep)]
        for name in candidates:
            if name < cutoff and name != self.index:
                self.logger.info("dropping index %s", name)
                self.es.indices.delete(name)
            else:
                kept += 1
        if kept == 1:
            self.logger.info("Kept one index")
        else:
            self.logger.info("Kept %d indices", kept)

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
    def days_to_keep(self):
        """Age in days for retention of older indices."""
        return 5

    @property
    def doctype(self):
        """Elasticsearch name for the type (plural is intentional)."""
        return "terms"

    @property
    def elapsed(self):
        """Amount of time since this job was started."""
        return datetime.datetime.now() - self.started

    @property
    def es(self):
        """Connection to the Elasticsearch server."""

        if not hasattr(self, "_es"):
            opts = dict(host=self.host, port=self.port, timeout=300)
            auth = self.opts.get("auth")
            if auth:
                opts["http_auth"] = auth.split(",")
            self._es = Elasticsearch([opts])
        return self._es

    @property
    def host(self):
        """Name of the Elasticsearch server (override and/or define HOST)."""

        if not hasattr(self, "_host"):
            self._host = self.opts.get("host")
            if self.testing:
                self._host = "example.com"
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
            if not self.testing:
                self.es.indices.create(index=self._index, body=self.indexdef)
        return self._index

    @property
    def indexdef(self):
        """Schema for our index.

        INDEXDEF can be the name of a control value or a JSON serialization
        of the index mappings. If the former, the serialization will be
        fetched from the ctl table of the CDR database, which will have
        been populated for this row in the table from "dictionary--{name}.json"
        in the Database/Loader directory of the `cdr-server` git repository.
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
    def indices_to_keep(self):
        """Minimum number of indices to retain during housekeeping."""
        return 5

    @property
    def limit(self):
        """Throttle for testing."""

        if not hasattr(self, "_limit"):
            limit = self.opts.get("limit")
            self._limit = int(limit) if limit else float("inf")
        return self._limit

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
            port = self.opts.get("port")
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
    def testing(self):
        """True if we should just dump the json instead of sending it."""

        if not hasattr(self, "_testing"):
            self._testing = True if self.opts.get("test") else False
        return self._testing

    @property
    def tier(self):
        """Which CDR server are we using?"""

        if not hasattr(self, "_tier"):
            self._tier = Tier(self.opts.get("tier"))
        return self._tier

    @property
    def transform(self):
        """XSL/T filter used for this load."""

        if not hasattr(self, "_transform"):
            title = f"Index {self.type.capitalize()} Dictionary"
            doc_id = Doc.id_from_title(title, self.cursor)
            doc = Doc(Session("guest", tier=self.tier), id=doc_id)
            self._transform = etree.XSLT(doc.root)
            self.logger.info("Loaded %r filter", title)
        return self._transform

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
        """Override for specific dictionary types."""

        def __init__(self, loader, id):
            """Save the caller's values.

            Pass:
                loader - access to the database and runtime settings
                id - integer for the document's CDR ID
            """

            self.__loader = loader
            self.__id = id

        def index(self):
            """Add nodes for this document to the ElasticSearch database."""

            args = len(self.nodes), self.cdr_id
            self.loader.logger.debug("%d nodes for %s", *args)
            for node in self.nodes:
                opts = dict(
                    index=self.loader.index,
                    body=node.values,
                )
                if node.id:
                    opts["id"] = node.id
                if self.loader.testing:
                    print(dumps(opts, indent=2))
                else:
                    self.loader.es.index(**opts)

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
            """Access to the database and runtime options."""
            return self.__loader

        @property
        def nodes(self):
            """Nodes to be added to the ElasticSearch database."""

            if not hasattr(self, "_nodes"):
                query = self.__loader.Query("pub_proc_cg", "xml")
                query.where(query.Condition("id", self.id))
                xml = query.execute(self.__loader.cursor).fetchone().xml
                try:
                    root = etree.fromstring(xml)
                except:
                    root = etree.fromstring(xml.encode("utf-8"))
                tier = f"'{self.loader.tier.name}'"
                result = self.loader.transform(root, tier=tier)
                self._nodes = []
                for node in result.getroot().findall("node"):
                    self._nodes.append(self.Node(node))
            return self._nodes


        class Node:
            """Information for a record to be sent to ElasticSearch.

            Class values are for transforming names to pretty URLs.
            See `clean_pretty_url()` method, below, which uses logic
            implemented by Bryan P. in JavaScript as part of
            github.com/NCIOCPL/wcms-cts-term-map-gen/blob/master/pdq_index.js
            (getFriendlyUrlForDisplayName() at line 44 ff.).

            See https://github.com/NCIOCPL/glossary-api/issues/98 for
            explanation of MAX_PRETTY_URL_LENGTH.
            """

            _FROM = "\u03b1\u03b2\u03bc;_&\u2013/"
            _TO = "abu-----"
            _STRIP = "\",+().\xaa'\u2019[\uff1a:*\\]"
            TRANS = str.maketrans(_FROM, _TO, _STRIP)
            SPACES = compile(r"\s+")
            MAX_PRETTY_URL_LENGTH = 75

            def __init__(self, node):
                """Save the caller's information.

                Pass:
                    node - object for parsed node in the filtered result XML
                """

                self.__node = node

            @property
            def doc_type(self):
                """String for the type of this record."""

                if not hasattr(self, "_doc_type"):
                    self._doc_type = self.__node.get("doc_type")
                return self._doc_type

            @property
            def id(self):
                """Optional unique ID for the record."""

                if not hasattr(self, "_id"):
                    self._id = self.__node.get("id")
                return self._id

            @property
            def values(self):
                """Dictionary of values for the _source of the record."""

                if not hasattr(self, "_values"):
                    self._values = self.__get_values(self.__node)
                return self._values

            def __get_values(self, node):
                """Recursively extract values from the filtered XML.

                Nodes which need further processing by Python to achieve
                results which are difficult or impossible in XSL/T have
                a `processor` attribute identifying the method to be
                applied to the node to produce the desired value.
                A node can also have a `type` attribute with one of the
                following values:
                  - array-member
                      this value is to be enclosed in an array with
                      all values having the same name as this one
                  - false
                      use Boolean 'False` as the value
                  - true
                      use Boolean `True` as the value
                  - null
                      use `None` (`null` in json)
                  - int
                      convert the value to an integer

                The "array-member" value can be combined with "int"
                (or any of the other values, though it would not be
                useful to have multiple True, False, or None instances
                in an array). In that case, separate them (in either
                order) with a space. For example:

                  <width type="array-member int">571</width>
                  <width type="array-member int">750</width>

                which would be be converted by the code below to

                  { "width": [571, 750] }

                Pass:
                    node - object for parsed node in the filtered result XML

                Return:
                    dictionary of values or scalar
                """

                processor = node.get("processor")
                if processor:
                    return getattr(self, processor)(node)
                values = {}
                for child in node:
                    value = self.__get_values(child)
                    child_types = child.get("type", "").split()
                    if "array-member" in child_types:
                        if child.tag not in values:
                            values[child.tag] = [value]
                        else:
                            values[child.tag].append(value)
                    elif "false" in child_types:
                        values[child.tag] = False
                    elif "true" in child_types:
                        values[child.tag] = True
                    elif "null" in child_types:
                        values[child.tag] = None
                    elif "int" in child_types:
                        values[child.tag] = int(value)
                    else:
                        values[child.tag] = value
                if values:
                    return values
                if node.text is None or not node.text:
                    return None
                return node.text

            @classmethod
            def clean_pretty_url(cls, node):
                """Prepare a term name for use as a pretty URL.

                Pass:
                    node - XML node needing further processing

                Return:
                    scrubbed name string
                """

                name = node.text
                name = cls.SPACES.sub("-", name).lower().translate(cls.TRANS)
                nfkd = normalize("NFKD", name)
                pretty_url = "".join([c for c in nfkd if not combining(c)])
                pretty_url = pretty_url.replace("%", "pct")
                if len(pretty_url) > cls.MAX_PRETTY_URL_LENGTH:
                    return ""
                return pretty_url

            @classmethod
            def lowercase_first_letter(cls, node):
                """Get the first letter of the node's text value.

                Pass:
                    node - XML node needing further processing

                Return:
                    lowercase version of first character ("#" if not ASCII)
                """

                name = node.text
                if name is None:
                    return "#"
                name = name.strip()
                if not name:
                    return "#"
                nfkd = normalize("NFKD", name)
                name = "".join([c for c in nfkd if not combining(c)])
                letter = name[0].lower()
                if letter not in ascii_lowercase:
                    return "#"
                return letter
