"""The credential container's pure parts: what it reads, what it writes
for the pooler, and what the card shows. Run with the standard library
alone (`python3 -m unittest`), so it needs no install."""

import os
import sys
import unittest

# The settings the node hands the container, so the module imports here
# as it does in the unit.
os.environ.update({
    "WEFT_SECRET_DIR": "/store/secret", "WEFT_PASSWORD_FILE": "/store/secret/password", "WEFT_CREDENTIAL_PORT": "8099",
    "WEFT_SOCKET_DIR": "/var/run/postgresql", "WEFT_POSTGRES_PORT": "5433", "WEFT_ADMIN_USER": "weft",
    "WEFT_APP_USER": "app", "WEFT_DATABASE": "my shop", "WEFT_POOLER_DATABASE": "shop", "WEFT_WATCH_DATABASE": "shop_watch",
    "WEFT_EXTENSIONS": '["pg_trgm", "nope"]', "WEFT_POSTGRES_MAJOR": "17", "WEFT_CLOSES_IDLE_SESSIONS_FROM": "14",
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
        self.assertEqual(bootstrap.pooler_users(STORED), f'"app" "{STORED}"\n"weft" ""\n')


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
        self.kept = {name: getattr(bootstrap, name) for name in ("psql", "wait_until", "stored_password", "app_verifier")}

        def psql(script, variables, database=None, pooler=False):
            self.sent.append((script, dict(variables)))
            if variables.get("name") == "nope":
                raise bootstrap.Refused('ERROR:  extension "nope" is not available')
            return ""

        bootstrap.psql = psql
        bootstrap.wait_until = lambda what, not_yet: None
        bootstrap.stored_password = lambda: "pw"
        bootstrap.app_verifier = lambda: STORED

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
        kept = {name: getattr(bootstrap, name) for name in ("psql", "wait_until", "stored_password", "app_verifier", "CLOSES_IDLE_SESSIONS", "EXTENSIONS")}
        bootstrap.psql = lambda script, variables, database=None, pooler=False: sent.append(script) or ""
        bootstrap.wait_until = lambda what, not_yet: None
        bootstrap.stored_password = lambda: "pw"
        bootstrap.app_verifier = lambda: STORED
        bootstrap.EXTENSIONS = []
        try:
            for closes, expected in ((True, True), (False, False)):
                sent.clear()
                bootstrap.CLOSES_IDLE_SESSIONS = closes
                self.assertIsNone(bootstrap.converge_once())
                self.assertEqual(any("idle_session_timeout" in script for script in sent), expected)
        finally:
            for name, value in kept.items():
                setattr(bootstrap, name, value)

    def test_the_card_warns_of_an_old_postgres_first(self):
        kept = {name: getattr(bootstrap, name) for name in ("POSTGRES_NOTE", "connection_items", "stored_password")}
        bootstrap.POSTGRES_NOTE = "Postgres 13 does not close a connection a client left idle"
        bootstrap.connection_items = lambda: []
        bootstrap.stored_password = lambda: None
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
        self.names = ("app_verifier", "set_app_password", "write_password", "stored_password", "tell_the_pooler", "push_password")
        self.kept = {name: getattr(bootstrap, name) for name in self.names}
        self.set = []
        bootstrap.app_verifier = lambda: STORED
        bootstrap.set_app_password = lambda password: self.set.append(password)
        bootstrap.stored_password = lambda: "pw"

        def full(password):
            raise OSError(28, "No space left on device")

        bootstrap.write_password = full
        bootstrap.tell_the_pooler = lambda: self.fail("the pooler is not told of a password that was undone")
        bootstrap.push_password = lambda password: self.fail("weft is not told of a password that was undone")

    def tearDown(self):
        for name, value in self.kept.items():
            setattr(bootstrap, name, value)

    def test_the_old_verifier_comes_back_as_it_was(self):
        refused = bootstrap.reset_password()
        self.assertEqual(len(self.set), 2)
        self.assertEqual(self.set[1], STORED, "the verifier itself, never the password hashed again")
        self.assertIn("undone", refused)


if __name__ == "__main__":
    unittest.main()
