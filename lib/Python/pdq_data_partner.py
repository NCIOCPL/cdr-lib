#----------------------------------------------------------------------
# Support for PDQ data partner management interface.
# JIRA::OCECDR-4025
#----------------------------------------------------------------------

# Standard library modules
import datetime
import json
import os
import urllib

# Third-party modules
import lxml.etree as etree

# Custom/application-specific modules
import cdr
import cdrdb
import cdrcgi

class TableRecord:
    """
    Base class for each of the types of database records. Knows how to
    load a record, save a record
    """

    cursor = cdrdb.connect("CdrGuest").cursor()
    "Read-only database cursor used by the subclasses."

    LAST_MOD = "last_mod"
    "The last column in each table."

    def __init__(self, *values):
        """
        Populate the properties of the object using the column names.
        The number of values passed in must match the number of names
        in the COLS class member, and the position of the values must
        match the COLS names as well.

        Pass:
            values - result of a call to fetchall() for a SELECT query
        """
        for i, name in enumerate(self.COLS):
            setattr(self, name, values[i])

    def to_row_node(self):
        "Create an ElementTree node from this record."
        key = getattr(self, self.KEY)
        name = getattr(self, self.NAME)
        node = etree.Element("row", key=unicode(key), name=unicode(name))
        for name in self.COLS:
            child = etree.SubElement(node, "col", name=name)
            value = getattr(self, name)
            if value is None:
                child.set("null", "True")
            else:
                child.set("value", unicode(value))
        return node

    @classmethod
    def edit_url(cls, parms):
        "Build the URL for editing a new or existing record."
        return "%s?%s" % (cls.EDIT, urllib.urlencode(parms))

    @classmethod
    def get_rows(cls):
        "Fetch all of the database table's rows."
        query = cdrdb.Query(cls.TABLE, *cls.COLS)
        return query.execute(cls.cursor).fetchall()

    @classmethod
    def to_table_node(cls):
        node = etree.Element("table", name=cls.TABLE, key_col=cls.KEY,
                             name_col=cls.NAME)
        for record in sorted([cls(*row) for row in cls.get_rows()]):
            node.append(record.to_row_node())
        return node

    @classmethod
    def load(cls, key):
        "Returns an object of the class by primary key."
        if key:
            query = cdrdb.Query(cls.TABLE, *cls.COLS)
            query.where(query.Condition(cls.KEY, key))
            row = query.execute(cls.cursor).fetchone()
            if row:
                return cls(*row)
        return None

    @classmethod
    def save(cls, field_values):
        """
        Save a database record using values collected by get_form_values()
        and validated by FormValues.check_value(). Does an INSERT if we
        don't have a primary key already, and an UPDATE if we do.
        """

        cols = sorted(field_values.values)
        values = [field_values.values[name] for name in cols]
        if field_values.key:
            sets = ["%s = ?" % col for col in cols]
            sets.append("%s = GETDATE()" % cls.LAST_MOD)
            values.append(field_values.key)
            query = """\
UPDATE %s
   SET %s
 WHERE %s = ?""" % (cls.TABLE,
                    ",\n       ".join(sets),
                    cls.KEY)
        else:
            query = """\
INSERT INTO %s (%s)
     VALUES (%s, GETDATE())""" % (cls.TABLE,
                                  ", ".join(cols + [cls.LAST_MOD]),
                                  ",".join("?" * len(cols)))
        # DEBUGGING cdrcgi.bail("\n%s\n%s" % (query, values))
        # This is the only place in the interface where we need a connection
        # that can write to the database tables.
        conn = cdrdb.connect()
        cursor = conn.cursor()
        cursor.execute(query, values)
        conn.commit()

    @classmethod
    def picklist(cls, inactive_col=None):
        "Returns a sorted sequence of ID and name for the table's objects."
        query = cdrdb.Query(cls.TABLE, cls.KEY, cls.NAME)
        if inactive_col:
            query.where("%s IS NULL" % inactive_col)
        return query.order(2).execute(cls.cursor).fetchall()

    @classmethod
    def FormValues(cls, fields):
        """
        Factory method, used so that _FormValues know which record class
        is using it. Looks to the caller like it's invoking a constructor
        directly.

        Pass:
            fields  - cgi.FieldStorage() object

        Return:
            Populated _FormValues object
        """

        return cls._FormValues(cls, fields)

    class _FormValues:
        """
        Collection of values from a CGI form, with validation functionality.

        Attributes:
            key         - primary key for the record, if there is one
            values      - dictionary of field values mapped by field name
            check_value - invoked to validate a single field's value
            record_type - class for the record type
            name        - name for the record; must be unique for some
                          record types
        """

        def __init__(self, cls, fields):
            """
            Extract the values which match the columns over which the user
            has control (excluding the primary key and internal last_mod
            column).

            Pass:
                fields  - cgi.FieldStorage() object
            """

            self.key = self.name = None
            self.values = {}
            self.record_type = cls
            for name in cls.COLS:
                value = fields.getvalue(name)
                if name == cls.KEY:
                    self.key = value
                elif name != cls.LAST_MOD:
                    self.values[name] = value
                    if name == cls.NAME:
                        self.name = value

        def check_value(self, name, max_len=255, ftype="text", required=True):
            """
            Check for validity of one of the form's field values.
            Bail out if validation fails. Uses a map of field type
            names to bound object methods.

            Pass:
                name         - name of the field
                max_len      - integer specifying the maximum number of
                               characters allowed for the field; defaults
                               to 255
                ftype        - text, name, uint, date, datetime, email,
                               or a sequence of valid values; defaults to text
                required     - True if there must be a value for this field
            """

            val = self._get_value(name, required)
            if val:
                if type(ftype) in (list, tuple, set):
                    self._check_valid_values(name, val, ftype)
                else:
                    checker = {
                        "date": self._check_date,
                        "datetime": self._check_datetime,
                        "email": self._check_email,
                        "uint": self._check_uint,
                        "name": self._check_name
                    }.get(ftype, self._check_length)
                    checker(name, val, max_len)

        def _get_value(self, name, required):
            """
            Extract the value from the object's dictionary and return
            it, first making sure that if the value is required it is
            present.

            Pass:
                name     -  name of the field
                required -  True if the field must have a value

            Return:
                string for the field's value if a value is present;
                otherwise None (unless the field is required, in
                which case we've bailed out with an error message)
            """

            val = self.values.get(name)
            if not val:
                if not required:
                    self.values[name] = None
                    return None
                cdrcgi.bail("%s field is required" % name)
            return val

        def _check_valid_values(self, name, val, valid_values):
            "Make sure the field's value is one of an accepted list."
            if val not in valid_values:
                # DEBUGGING cdrcgi.bail(repr((val, valid_values)))
                cdrcgi.bail(cdrcgi.TAMPERING)

        def _check_name(self, name, val, max_len):
            """
            Find out if the name field has a value taken by another record.
            For some record types, the name must be unique.
            """

            query = cdrdb.Query(self.record_type.TABLE, self.record_type.NAME)
            query.where(query.Condition(self.record_type.NAME, self.name))
            if self.key:
                query.where(query.Condition(self.record_type.KEY,
                                            self.key, "<>"))
            if query.execute(self.record_type.cursor).fetchall():
                cdrcgi.bail("Another %s record named %s already exists in the "
                            "%s table." % (self.record_type.__name__, repr(val),
                                           self.record_type.TABLE))
            self._check_length(name, val, max_len)

        def _check_date(self, name, val, max_len):
            "Make sure the value is a valid date in ISO format."
            cdrcgi.valParmDate(val, msg="Invalid date in %s field." % name)

        def _check_datetime(self, name, val, max_len):
            "Make sure the value is a valid date/time in ISO format."
            msg = "Invalid date/time value in %s field." % name
            cdrcgi.valParmVal(val, regex=cdrcgi.VP_DATETIME, msg=msg)

        def _check_email(self, name, val, max_len):
            "Ensure that the value has valid email address syntax."
            msg = "Invalid address in %s field." % name
            cdrcgi.valParmEmail(val, msg=msg)
            self._check_length(name, val, max_len)

        def _check_uint(self, name, val, max_len):
            "Ensure that the string value represents an unsigned integer."
            regex = cdrcgi.VP_UNSIGNED_INT
            msg = "%s field must contain a positive integer" % name
            cdrcgi.valParmVal(val, regex=regex, msg=msg)

        def _check_length(self, name, val, max_len):
            "Make sure the value's length does not exceed the maximum allowed."
            if len(val) > max_len:
                cdrcgi.bail("%s field has %d characters; max length is %d" %
                            (name, len(val), max_len))

class Product(TableRecord):
    """
    Represents a class of consumers of PDQ data. Over the years there
    have been many of these. Currently there is only one (CDR) in
    production, and another for testing/development (TEST).
    """

    EDIT = "edit-data-product.py"
    "Script for editing an existing product record or creating a new one."

    COLS = ("prod_id", "prod_name", "prod_desc", "inactivated",
            TableRecord.LAST_MOD)
    "Database column names for Product records."

    KEY = COLS[0]
    "Primary key for the database table."

    NAME = COLS[1]
    "Database column for object's name."

    INACTIVATED = COLS[3]
    "Column indicating the row has been taken out of commission."

    TABLE = "data_partner_product"
    "Name of DBMS table for Product records."

    BUTTON = "Manage Data Products"
    "Button on the landing page."

    CREATE = "Create Product"
    "Button on the product landing page."

    MANAGE = "manage-pdq-data-products.py"
    "Displays existing PDQ data products, with links for editing/adding."

    def __sort__(self, other):
        "Support ordering of the products."
        return cmp(self.prod_name, other.prod_name)

    def show(self, form, parms):
        """
        Draw a link to the edit page for the product on the landing page
        for managing the PDQ data products.
        """

        parms = dict(parms)
        parms["id"] = self.prod_id
        url = Product.edit_url(parms)
        title = "Click to edit"
        display = u"%s (%s)" % (self.prod_name,
                                self.inactivated and "inactive" or "active")
        link = form.B.A(display, href=url, title=title)
        form.add(form.B.LI(link))

    def active(self):
        "Should this product show up on the forms for managing data partners?"
        return not getattr(self, self.INACTIVATED)

    @classmethod
    def active_name(cls, name):
        "Look up the active status of a product by name."
        return cls.by_name[name].active()

    @classmethod
    def _load(cls):
        """
        Load the sorted sequence of product objects, and populate
        the indexes for the class.

        Class attributes set:
            products   - ordered sequence of all Product objects
            ids        - dictionary mapping prod_id -> prod_value
            by_id      - dictionary mapping prod_id -> Product object
            names      - dictionary mapping prod_name -> prod_id
            by_name    - dictionary mapping prod_name -> Product object
        """
        cls.products = sorted([cls(*row) for row in cls.get_rows()])
        if not cls.products:
            raise Exception("No data products found")
        cls.ids = {}
        cls.names = {}
        cls.by_id = {}
        cls.by_name = {}
        for product in cls.products:
            cls.by_id[product.prod_id] = product
            cls.ids[product.prod_id] = product.prod_name
            cls.by_name[product.prod_name] = product
            cls.names[product.prod_name] = product.prod_id

class Org(TableRecord):
    """
    Represents a PDQ data partner organization. Each PDQ data product
    can have many partner organizations (or none), and each partner
    organization can have zero or more contact records. Currently,
    the organization record must have a unique name across all
    products. If requirements change, and that restriction must
    be lifted, the database table definition will have to be altered.
    """

    BUTTON = "Create New Partner"
    "Button on the landing page."

    EDIT = "edit-data-partner-org.py"
    "Script for editing an existing org record or creating a new one."

    TABLE = "data_partner_org"
    "Name of database table for Org records."

    COLS = ("org_id", "org_name", "prod_id", "org_status", "activated",
            "ftp_username", "terminated", "renewal", TableRecord.LAST_MOD)
    "Database column names for Org records."

    KEY = COLS[0]
    "Primary key for the database table."

    NAME = COLS[1]
    "Database column for object's name."

    HISTORY_FIELDS = ("activated", "terminated", "renewal")
    "Columns with dates representing the history of the partner organization."

    STATUS_LABELS = ("Active", "Test", "Special")
    "Form display versions of partner org status valid values."

    STATUSES = [value[0] for value in STATUS_LABELS]
    "Internal status valid values."

    def __init__(self, *cols):
        """
        After populating the attributes for the column values pulled from
        the database table, the contact records connected with this partner
        are loaded. For convenience, a 'product' attribute is also provided
        with the string name for this partner's PDQ data product.
        """

        TableRecord.__init__(self, *cols)
        query = cdrdb.Query(Contact.TABLE, *Contact.COLS)
        query.where(query.Condition("org_id", self.org_id))
        rows = query.execute(self.cursor).fetchall()
        self.product = Product.ids[self.prod_id]
        self.contacts = sorted([Contact(*row) for row in rows])

    def __cmp__(self, other):
        "Support sorting, with deactivated partners dropping to the bottom."
        terminated = self.terminated and 1 or 0
        return cmp((self.terminated and 1 or 0, self.org_name.lower()),
                   (other.terminated and 1 or 0, other.org_name.lower()))

    def show(self, form, parms):
        """
        Show a link for editing this record on the Manager Partners landing
        page, along with links to each of the partner's contacts.
        """
        status = self.terminated and "inactive" or "active"
        classes = ["partner", "prod-%s" % self.product, status]
        if self.hidden(parms["product"], parms["included"]):
            classes.append("hidden")
        form.add("<li class=\"%s\">" % " ".join(classes))
        parms = dict(parms)
        parms["id"] = self.org_id
        parms["product"] = self.product
        url = Org.edit_url(parms)
        display = u"%s (%s)" % (self.org_name, status)
        title = "Click to edit"
        form.add(form.B.A(form.B.STRONG(display), href=url, title=title))
        if self.contacts:
            form.add("<ul>")
            for contact in self.contacts:
                contact.show(form, parms)
            form.add("</ul>")
        form.add("</li>")

    def hidden(self, product, included):
        """
        Determine whether, given the current filtering settings on the
        landing page for managing PDQ data partners, this partner should
        be initially hidden when the page is brought up.
        """

        if product != self.product:
            return True
        if included == "active" and self.terminated:
            return True
        return included == "inactive" and not self.terminated

class Contact(TableRecord):
    """
    Individual to whom correspondence about the product's activity is sent.
    Contacts can by primary, secondary, internal or deleted.
    """

    TABLE = "data_partner_contact"
    "Name of database table for Contact records."

    EDIT = "edit-data-partner-contact.py"
    "Script for editing an existing contact record or creating a new one."

    BUTTON = "Add New Contact"
    "Button on the partner organization editing page."

    COLS = ("contact_id", "org_id", "email_addr", "person_name", "phone",
            "notif_count", "contact_type", "notif_date", TableRecord.LAST_MOD)
    "Database column names for Contact records."

    KEY = COLS[0]
    "Primary key for the database table."

    NAME = COLS[3]
    "Database column for object's name."

    TYPE_LABELS = ("Primary", "Secondary", "Internal", "Deleted")
    "Display versions of the valid values for the contact_type column."

    TYPES = dict([(v[0], i) for i, v in enumerate(TYPE_LABELS)])
    "Internal values for contact, with sorting support."

    def __cmp__(self, other):
        "Support order of contact, first by type, then by person's name."
        delta = cmp(self.TYPES.get(self.contact_type),
                    self.TYPES.get(other.contact_type))
        if not delta:
            delta = cmp(self.person_name.lower(), other.person_name.lower())
        return delta

    def show(self, form, parms):
        """
        Show a link for editing this record on the Manager Partners landing
        page or on the Partner Editing page. The markup differs for those
        two locations, partly for visual appearance, partly to support
        returning to the page from which the editing form was invoked,
        and partly to support warning the user if she is discarding
        edits to the current Partner Editing form.
        """

        parms = dict(parms)
        parms["id"] = self.contact_id
        url = Contact.edit_url(parms)
        title = "Click to edit"
        display = u"%s (%s)" % (self.person_name, self.email_addr)
        if self.contact_type:
            display += " [%s]" % self.contact_type
        if form.get_action() == Org.EDIT:
            onclick = "confirm_nav('%s')" % url
            link = form.B.A(display, href="#", title=title, onclick=onclick)
        else:
            link = form.B.A(form.B.EM(display), href=url, title=title)
        form.add(form.B.LI(link))

class Control(cdrcgi.Control):
    """
    Common behavior for the pages in the PDQ data partner management system.
    Each page implements its own derived class, customizing the behavior
    further.
    """

    MANAGE = "manage-pdq-data-partners.py"
    "Script for PDQ data partner landing page."

    BUTTON = "Manage Partners"
    "Returns to top-level landing page for this interface."

    SAVE = "Save"
    "Button on the editing pages."

    CANCEL = "Cancel"
    "Button on the editing pages."

    STATUSES = ("all", "active", "inactive")
    "Used for filtering which partners are shown on the landing page."

    ACTION = "MANAGE PDQ DATA PARTNERS"
    "Permission checked for using this interface."

    DENIED = "Account not authorized to Manage PDQ data partners."
    "Error message displayed for lack of adequate permissions."

    TODAY = str(datetime.date.today())
    "Default date value for fields."

    TAMPERING = cdrcgi.TAMPERING
    "Error message left intentionally uninformative."

    CSS = """\
fieldset a { text-decoration: none; color: black; }
fieldset li {list-style-type: none; }
fieldset > ul { margin-left: -30px; }"""
    "Custom display for partner/contact lists."

    def __init__(self, title):
        """
        Carry the filtering choices from the partner management landing
        page as we navigate around, so we remember the user's choices
        when we come back to it. Load the Product class's data.
        Make sure the user is authorized to use these pages.
        """

        cdrcgi.Control.__init__(self, title)
        if not self.session or not cdr.canDo(self.session, self.ACTION):
            cdrcgi.bail(self.DENIED)
        Product._load()
        self.included = self.fields.getvalue("included") or "active"
        self.products = self.get_products()
        self.product = self.get_product()
        self.parms = { "product": self.product, "included": self.included }
        msg = self.TAMPERING
        cdrcgi.valParmVal(self.product, valList=self.products, msg=msg)
        cdrcgi.valParmVal(self.included, valList=self.STATUSES, msg=msg)

    def navigate_to(self, url):
        "Shortcut for jumping between pages."
        cdrcgi.navigateTo(url, self.session, **self.parms)

    def get_products(self):
        "Get only the names of products which are in current use."
        return sorted([p.prod_name for p in Product.products if p.active()])

    def get_product(self):
        "Make sure the selected product is valid; use default if needed."
        product = self.fields.getvalue("product")
        if product not in self.products:
            product = self.products[0]
        return product

    def run(self):
        "Customize the navigation for these page."
        if self.request == self.BUTTON:
            self.navigate_to(self.MANAGE)
        elif self.request == self.CANCEL:
            url = self.script == Product.EDIT and Product.MANAGE or self.MANAGE
            self.navigate_to(url)
        elif self.request == Org.BUTTON:
            self.navigate_to(Org.EDIT)
        elif self.request == Product.BUTTON:
            self.navigate_to(Product.MANAGE)
        elif self.request == Product.CREATE:
            self.navigate_to(Product.EDIT)
        elif self.request == self.SAVE:
            self.save()
        else:
            self.parms[cdrcgi.SESSION] = self.session
            cdrcgi.Control.run(self)

    def add_field(self, form, record, name, prompt, is_date_field=False):
        "Common code for adding a text or date field to the form."
        if record:
            value = getattr(record, name)
        else:
            value = name == "notif_count" and "0" or ""
        if value is None:
            value = ""
        if is_date_field:
            form.add_date_field(name, prompt, value=value)
        else:
            form.add_text_field(name, prompt, value=value)

    def get_partners(self):
        """
        Collect the objects for the partners which match the filtering
        parameters on the Partner management landing page.
        """

        return [Org(*row) for row in Org.get_rows()]
        query = cdrdb.Query(Org.TABLE, *Org.COLS)
        prod_id = Product.by_name[self.product].prod_id
        query.where(query.Condition("prod_id", prod_id))
        if self.included == "active":
            query.where("terminated IS NULL")
        elif self.included == "inactive":
            query.where("terminated IS NOT NULL")
        rows = query.execute(self.cursor).fetchall()
        return [Org(*row) for row in rows]

    def make_script(self, values):
        """
        Create Javascript methods which will allow us to make sure
        the user really wants to abandon changes to the current partner
        record when clicking on a link/button to edit/create a contact
        record for the partner.
        """

        add_contact = Contact.edit_url(self.parms)
        return """\
function data_changed() {
    var original_values = %s;
    var text_fields = [
        "org_name",
        "activated",
        "terminated",
        "renewal",
        "ftp_username"
    ];
    for (var i = 0; i < text_fields.length; ++i) {
        var name = text_fields[i];
        if (jQuery("#" + name).val() != original_values[name])
            return true;
    }
    var radio_fields = [ "prod_id", "org_status" ];
    for (var i = 0; i < radio_fields.length; ++i) {
        var name = radio_fields[i];
        var val = original_values[name];
        if (jQuery("input[name=" + name + "]:checked").val() != val)
            return true;
    }
    return false;
}
function confirm_nav(url) {
    if (data_changed()) {
        var ask = "You have edited the data on this form. Abandon it?";
        if (!confirm(ask))
            return false;
    }
    window.location.href = url;
        return false;
}
jQuery(function() {
    jQuery("input[value='%s']").click(function() {
        return confirm_nav("%s");
    });
});""" % (json.dumps(values), Contact.BUTTON, add_contact)

    def show_report(self):
        "None of these pages has a report; it's all forms."
        self.show_form()

    def validate_unsigned_int(self, val, optional=True):
        "See if a hacker has messed with a parameter which should be a key."
        regex = cdrcgi.VP_UNSIGNED_INT
        msg = self.TAMPERING
        cdrcgi.valParmVal(val, regex=regex, empty_ok=optional, msg=msg)

    def debug_log(where, what):
        "Not currently used, but keeping it here in case it's needed later."
        fp = open("d:/tmp/%s" % where, "a")
        fp.write("%s\n" % what)
        fp.close()
