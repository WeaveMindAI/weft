"""The credential container's pure parts: what it reads, what it writes
for the pooler, and what the card shows. Run with the standard library
alone (`python3 -m unittest`), so it needs no install."""

import json
import os
import sys
import tempfile
import unittest

# The settings the node hands the container, so the module imports here
# as it does in the unit.
os.environ.update({
    "WEFT_SECRET_DIR": "/store/secret", "WEFT_PASSWORD_FILE": "/store/secret/password", "WEFT_CREDENTIAL_PORT": "8099",
    "WEFT_SOCKET_DIR": "/var/run/postgresql", "WEFT_POSTGRES_PORT": "5433", "WEFT_ADMIN_USER": "weft",
    "WEFT_APP_USER": "app", "WEFT_DATABASE": "my shop", "WEFT_POOLER_DATABASE": "shop", "WEFT_WATCH_DATABASE": "shop_watch",
    "WEFT_EXTENSIONS": '["pg_trgm", "nope"]',
    "WEFT_ROLES": '[{"name": "tenant_app", "login": false, "bypassRowLevelSecurity": false},'
                  ' {"name": "tenant_site", "login": true, "bypassRowLevelSecurity": false},'
                  ' {"name": "tenant_admin", "login": false, "bypassRowLevelSecurity": true}]',
    "WEFT_POSTGRES_MAJOR": "17", "WEFT_CLOSES_IDLE_SESSIONS_FROM": "14",
    "WEFT_POSTGRES_NOTE": "", "WEFT_DATA_DIR": "/store/pgdata",
    "WEFT_IDLE_SESSION_SECONDS": "60", "WEFT_POOLER_DIR": "/run/pooler", "WEFT_POOLER_PORT": "5432",
    "WEFT_POOLER_USERS_FILE": "/run/pooler/users", "WEFT_VALUES_URL": "http://agent/values",
})
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bootstrap  # noqa: E402

# What Postgres 17 stored for the password `pw` (`pg_authid.rolpassword`).
STORED = (
    "SCRAM-SHA-256$4096:8pKmXNvoZw8v9qPgnGSNpg==$fKVddU1mC39C81y4v0D/H8hDtGTvdFJ1nMIQ+xRN6dY="
    ":oM1QsL/ZwfZ+Jt23YpdHrl8YTrU3x6hDncF3Ff/1p00="
)


class ScramTest(unittest.TestCase):
    def test_a_verifier_accepts_its_own_password_only(self):
        self.assertTrue(bootstrap.scram_accepts(STORED, "pw"))
        self.assertFalse(bootstrap.scram_accepts(STORED, "pW"))

    def test_no_verifier_or_another_kind_or_a_broken_one_is_a_no(self):
        for stored in (None, "", "md5" + "0" * 32, "SCRAM-SHA-256$broken", "SCRAM-SHA-256$x:!!$y:z"):
            self.assertFalse(bootstrap.scram_accepts(stored, "pw"), stored)


class PoolerUsersTest(unittest.TestCase):
    def test_the_program_signs_in_with_its_verifier_and_the_superuser_with_none(self):
        self.assertEqual(bootstrap.pooler_users({"app": STORED}), f'"app" "{STORED}"\n"weft" ""\n')

    def test_each_login_role_signs_in_with_its_own_verifier(self):
        self.assertEqual(
            bootstrap.pooler_users({"app": STORED, "tenant_site": "SCRAM-other"}),
            f'"app" "{STORED}"\n"tenant_site" "SCRAM-other"\n"weft" ""\n',
        )

    def test_only_the_program_and_the_login_roles_sign_in(self):
        self.assertEqual(bootstrap.SIGNS_IN, ["app", "tenant_site"])


class RolesTest(unittest.TestCase):
    def test_a_role_is_made_with_the_words_its_entry_picks_and_granted_to_the_program(self):
        site = bootstrap.role_script({"name": "tenant_site", "login": True, "bypassRowLevelSecurity": False})
        self.assertIn("NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION LOGIN NOBYPASSRLS;", site)
        self.assertIn('GRANT :"role" TO :"app";', site)
        self.assertNotIn("tenant_site", site, "the name travels as a psql variable, never in the SQL")
        admin = bootstrap.role_script({"name": "tenant_admin", "login": False, "bypassRowLevelSecurity": True})
        self.assertIn(" NOLOGIN BYPASSRLS;", admin)

    def test_the_node_s_roles_are_read_whole_or_refused(self):
        self.assertEqual(bootstrap.read_roles("[]"), [])
        for broken in ('{"name": "x"}', '[{"name": "x"}]', '[{"name": "", "login": false, "bypassRowLevelSecurity": false}]',
                       '[{"name": "x", "login": "yes", "bypassRowLevelSecurity": false}]'):
            with self.assertRaises(SystemExit, msg=broken):
                bootstrap.read_roles(broken)

    def test_each_button_and_each_taken_press_names_its_own_role(self):
        for kind in ("reset_password", "password_taken"):
            self.assertEqual(bootstrap.role_of(kind, kind), "app")
            self.assertEqual(bootstrap.role_of(f"{kind}:tenant_site", kind), "tenant_site")
            for nothing in (f"{kind}:tenant_app", f"{kind}:weft", f"{kind}:", "other", None, 3):
                self.assertIsNone(bootstrap.role_of(nothing, kind), nothing)
        self.assertIsNone(bootstrap.role_of("password_taken", "reset_password"))


class PasswordCardTest(unittest.TestCase):
    """Every password the node holds is shown the one way: readable until it
    is taken, then a line saying so, with its Reset button in both states."""

    def setUp(self):
        self.kept = {name: getattr(bootstrap, name) for name in ("stored_password", "sealed", "seal")}
        self.passwords = {"app": "pw", "tenant_site": "site-pw"}
        self.sealed = set()
        bootstrap.stored_password = lambda role: self.passwords.get(role)
        bootstrap.sealed = lambda role: role in self.sealed
        bootstrap.seal = lambda role: self.sealed.add(role)

    def tearDown(self):
        for name, value in self.kept.items():
            setattr(bootstrap, name, value)

    def test_a_password_is_readable_with_its_taken_press_until_taken(self):
        for role, label, mark in (("app", "Password", ""), ("tenant_site", "tenant_site password", ":tenant_site")):
            item = bootstrap.password_item(role)
            self.assertEqual((item["type"], item["label"], item["data"]), ("secret", label, self.passwords[role]))
            self.assertEqual(item["taken"], "password_taken" + mark)
            self.assertEqual(item["action"]["actionKind"], "reset_password" + mark)

    def test_only_the_password_itself_retires_it(self):
        self.assertIn("not the password", bootstrap.taken("tenant_site", "wrong"))
        self.assertIn("not the password", bootstrap.taken("tenant_site", None))
        self.assertEqual(self.sealed, set())
        self.assertIsNone(bootstrap.taken("tenant_site", "site-pw"))
        self.assertEqual(self.sealed, {"tenant_site"}, "the role taken, never the program's")
        item = bootstrap.password_item("tenant_site")
        self.assertEqual((item["type"], item["data"], item["handedOver"]), ("text", "handed over; reset to get a new one", True))
        self.assertNotIn("taken", item)
        self.assertEqual(item["action"]["actionKind"], "reset_password:tenant_site")

    def test_a_password_not_minted_yet_cannot_be_taken(self):
        self.passwords.pop("tenant_site")
        self.assertEqual(bootstrap.password_item("tenant_site")["data"], "not minted yet")
        self.assertIn("no password yet", bootstrap.taken("tenant_site", "x"))

    def test_the_card_holds_every_password_in_order(self):
        kept = bootstrap.connection_items
        bootstrap.connection_items = lambda: []
        try:
            labels = [item["label"] for item in bootstrap.live_items()]
            self.assertEqual(labels, ["Database", "User", "Password", "tenant_site password"])
        finally:
            bootstrap.connection_items = kept


class RowsTest(unittest.TestCase):
    def test_csv_reads_as_one_dict_per_row(self):
        printed = 'user,state,n\napp,active,3\n"we,ft",idle,1\n'
        self.assertEqual(
            bootstrap.rows(printed),
            [{"user": "app", "state": "active", "n": "3"}, {"user": "we,ft", "state": "idle", "n": "1"}],
        )
        self.assertEqual(bootstrap.rows(""), [])
        # A role with no password, as psql prints it.
        self.assertEqual(bootstrap.rows("rolpassword\n\n"), [{"rolpassword": ""}])


class CardTest(unittest.TestCase):
    def test_postgres_counts_every_connection_by_user_and_state(self):
        shown = bootstrap.postgres_connections(
            {"most": "23", "reserved": "3"},
            [{"user": "app", "state": "active", "n": "4"}, {"user": "app", "state": "idle", "n": "9"},
             {"user": "weft", "state": "active", "n": "1"}],
        )
        self.assertEqual(shown, "14 of 23 in use (3 of them kept for the superuser)\napp: 4 active, 9 idle\nweft: 1 active")

    def test_the_pooler_shows_each_door_with_who_waits(self):
        databases = [
            {"name": "shop", "pool_size": "15", "current_connections": "15"},
            {"name": "shop_watch", "pool_size": "4", "current_connections": "1"},
            {"name": "pgbouncer", "pool_size": "2", "current_connections": "0"},
        ]
        pools = [
            {"database": "shop", "user": "app", "cl_active": "15", "cl_waiting": "6", "maxwait": "3"},
            {"database": "shop_watch", "user": "app", "cl_active": "1", "cl_waiting": "0", "maxwait": "0"},
        ]
        self.assertEqual(
            bootstrap.pooler_lanes(databases, pools),
            "queries (shop): 15 client(s) served, 6 waiting (the longest for 3s); 15 of 15 connections open\n"
            "watchers (shop_watch): 1 client(s) served, 0 waiting; 1 of 4 connections open",
        )

    def test_a_door_counts_the_clients_of_every_role_signing_in_through_it(self):
        databases = [{"name": "shop", "pool_size": "15", "current_connections": "15"}]
        pools = [
            {"database": "shop", "user": "app", "cl_active": "10", "cl_waiting": "2", "maxwait": "1"},
            {"database": "shop", "user": "tenant_site", "cl_active": "5", "cl_waiting": "3", "maxwait": "4"},
        ]
        shown = bootstrap.pooler_lanes(databases, pools)
        self.assertTrue(shown.startswith("queries (shop): 15 client(s) served, 5 waiting (the longest for 4s)"), shown)

    def test_a_door_nobody_used_yet_reads_as_empty(self):
        shown = bootstrap.pooler_lanes([{"name": "shop", "pool_size": "15", "current_connections": "0"}], [])
        self.assertTrue(shown.startswith("queries (shop): 0 client(s) served, 0 waiting; 0 of 15 connections open"), shown)

    def test_clients_are_counted_by_where_they_come_from_and_the_console_is_left_out(self):
        clients = [
            {"database": "shop", "addr": "10.0.0.9", "application_name": ""},
            {"database": "shop", "addr": "10.0.0.5", "application_name": ""},
            {"database": "shop_watch", "addr": "10.0.0.5", "application_name": ""},
            {"database": "shop", "addr": "10.0.0.7", "application_name": "web"},
            {"database": "pgbouncer", "addr": "unix", "application_name": "psql"},
        ]
        self.assertEqual(bootstrap.pooler_clients(clients), "10.0.0.5: 2\n10.0.0.7 (web): 1\n10.0.0.9: 1")
        self.assertEqual(bootstrap.pooler_clients([]), "none")


class ConvergeTest(unittest.TestCase):
    """One pass of `converge` against a database that answers as a fake:
    every script it sends is kept, and the extension `nope` is refused."""

    def setUp(self):
        self.sent = []
        self.kept = {name: getattr(bootstrap, name) for name in ("psql", "wait_until", "stored_password", "verifier", "write_password")}
        self.passwords = {"app": "pw"}

        def psql(script, variables, database=None, pooler=False):
            self.sent.append((script, dict(variables)))
            if variables.get("name") == "nope":
                raise bootstrap.Refused('ERROR:  extension "nope" is not available')
            return ""

        bootstrap.psql = psql
        bootstrap.wait_until = lambda what, not_yet: None
        bootstrap.stored_password = lambda role: self.passwords.get(role)
        bootstrap.write_password = lambda role, password: self.passwords.__setitem__(role, password)
        bootstrap.verifier = lambda role: STORED

    def tearDown(self):
        for name, value in self.kept.items():
            setattr(bootstrap, name, value)

    def test_each_listed_extension_is_made_and_one_postgres_lacks_is_refused_by_name(self):
        refused = bootstrap.converge_once()
        made = [variables["name"] for script, variables in self.sent if "CREATE EXTENSION IF NOT EXISTS" in script]
        self.assertEqual(made, ["pg_trgm", "nope"])
        self.assertEqual(self.sent[0][1]["database"], "my shop", "the role and its ownership are set on the database's own name")
        self.assertIn("'nope'", refused)
        self.assertIn("is not available", refused)

    def test_each_role_is_made_and_a_login_role_gets_a_password_of_its_own_once(self):
        bootstrap.converge_once()
        made = [variables["role"] for script, variables in self.sent if "CREATE ROLE" in script and "role" in variables]
        self.assertEqual(made, ["tenant_app", "tenant_site", "tenant_admin"])
        minted = self.passwords["tenant_site"]
        self.assertNotEqual(minted, "pw")
        self.assertNotIn("tenant_app", self.passwords, "a role that does not sign in has no password")
        given = [variables["password"] for script, variables in self.sent if "PASSWORD" in script and variables.get("role") == "tenant_site"]
        self.assertEqual(given, [minted], "the role is given the password just minted, which the verifier did not accept")
        self.sent.clear()
        bootstrap.converge_once()
        self.assertEqual(self.passwords["tenant_site"], minted, "kept from then on")

    def test_the_roles_listed_no_more_lose_what_the_list_gave_them(self):
        bootstrap.converge_once()
        [(script, variables)] = [(script, variables) for script, variables in self.sent if "REVOKE" in script]
        self.assertEqual(json.loads(variables["listed"]), ["tenant_app", "tenant_site", "tenant_admin"])
        self.assertIn("NOLOGIN NOBYPASSRLS", script)
        self.assertIn("m.grantor", script, "only what the superuser granted, never a grant somebody else made")

    def test_a_role_the_database_refuses_is_named(self):
        def psql(script, variables, database=None, pooler=False):
            if variables.get("role") == "tenant_site" and "CREATE ROLE" in script:
                raise bootstrap.Refused("ERROR:  role \"app\" is a member of role \"tenant_site\"")
            return ""

        bootstrap.psql = psql
        refused = bootstrap.converge_once()
        self.assertIn("'tenant_site'", refused)
        self.assertIn("`roles`", refused)

    def test_a_pass_the_database_takes_answers_nothing(self):
        bootstrap.EXTENSIONS, kept = ["pg_trgm"], bootstrap.EXTENSIONS
        try:
            self.assertIsNone(bootstrap.converge_once())
        finally:
            bootstrap.EXTENSIONS = kept


class OldPostgresTest(unittest.TestCase):
    def test_a_new_database_is_never_made_on_a_postgres_that_keeps_idle_sessions(self):
        self.assertIsNone(bootstrap.too_old_for_a_new_database(True, False))
        self.assertIsNone(bootstrap.too_old_for_a_new_database(True, True))
        self.assertIsNone(bootstrap.too_old_for_a_new_database(False, True), "an existing database keeps running")
        self.assertIn("new database", bootstrap.too_old_for_a_new_database(False, False))

    def test_the_idle_limit_is_set_only_where_postgres_has_it(self):
        sent = []
        kept = {name: getattr(bootstrap, name) for name in ("psql", "wait_until", "stored_password", "verifier", "CLOSES_IDLE_SESSIONS", "EXTENSIONS")}
        bootstrap.psql = lambda script, variables, database=None, pooler=False: sent.append((script, variables)) or ""
        bootstrap.wait_until = lambda what, not_yet: None
        bootstrap.stored_password = lambda role: "pw"
        bootstrap.verifier = lambda role: STORED
        bootstrap.EXTENSIONS = []
        try:
            for closes, expected in ((True, True), (False, False)):
                sent.clear()
                bootstrap.CLOSES_IDLE_SESSIONS = closes
                self.assertIsNone(bootstrap.converge_once())
                closed = [variables["role"] for script, variables in sent if "idle_session_timeout" in script]
                self.assertEqual(closed, ["app", "tenant_site"] if expected else [], "every role that signs in")
        finally:
            for name, value in kept.items():
                setattr(bootstrap, name, value)

    def test_the_card_warns_of_an_old_postgres_first(self):
        kept = {name: getattr(bootstrap, name) for name in ("POSTGRES_NOTE", "connection_items", "stored_password")}
        bootstrap.POSTGRES_NOTE = "Postgres 13 does not close a connection a client left idle"
        bootstrap.connection_items = lambda: []
        bootstrap.stored_password = lambda role: None
        try:
            items = bootstrap.live_items()
            self.assertEqual(items[0], {"type": "text", "label": "Warning", "data": bootstrap.POSTGRES_NOTE})
            bootstrap.POSTGRES_NOTE = ""
            self.assertNotEqual(bootstrap.live_items()[0]["label"], "Warning")
        finally:
            for name, value in kept.items():
                setattr(bootstrap, name, value)


class ResetUndoTest(unittest.TestCase):
    """A reset whose password cannot be written gives the role back the
    exact verifier it held, which the pooler still signs in with."""

    def setUp(self):
        self.names = ("verifier", "set_password", "write_password", "stored_password", "tell_the_pooler", "push_password")
        self.kept = {name: getattr(bootstrap, name) for name in self.names}
        self.set = []
        bootstrap.verifier = lambda role: STORED
        bootstrap.set_password = lambda role, password: self.set.append((role, password))
        bootstrap.stored_password = lambda role: "pw"

        def full(role, password):
            raise OSError(28, "No space left on device")

        bootstrap.write_password = full
        bootstrap.tell_the_pooler = lambda: self.fail("the pooler is not told of a password that was undone")
        bootstrap.push_password = lambda password: self.fail("weft is not told of a password that was undone")

    def tearDown(self):
        for name, value in self.kept.items():
            setattr(bootstrap, name, value)

    def test_the_old_verifier_comes_back_as_it_was(self):
        for role in ("app", "tenant_site"):
            self.set.clear()
            refused = bootstrap.reset_password(role)
            self.assertEqual(len(self.set), 2)
            self.assertEqual(self.set[1], (role, STORED), "the verifier itself, never the password hashed again")
            self.assertIn("undone", refused)


class RoleResetTest(unittest.TestCase):
    """A login role's reset tells the pooler and makes its password readable
    again, and stops there: weft holds no connection for it."""

    def test_a_login_role_s_reset_unseals_it_and_tells_no_connection(self):
        names = ("verifier", "set_password", "write_password", "tell_the_pooler", "push_password", "sealed_file")
        kept = {name: getattr(bootstrap, name) for name in names}
        told = []
        bootstrap.verifier = lambda role: STORED
        bootstrap.set_password = lambda role, password: None
        bootstrap.write_password = lambda role, password: None
        bootstrap.tell_the_pooler = lambda: told.append("pooler")
        bootstrap.push_password = lambda password: self.fail("weft holds no connection for a login role")
        seal = os.path.join(tempfile.mkdtemp(), "sealed")
        open(seal, "w").close()
        bootstrap.sealed_file = lambda role: seal
        try:
            self.assertIsNone(bootstrap.reset_password("tenant_site"))
            self.assertEqual(told, ["pooler"])
            self.assertFalse(os.path.exists(seal), "readable again")
        finally:
            for name, value in kept.items():
                setattr(bootstrap, name, value)


if __name__ == "__main__":
    unittest.main()
