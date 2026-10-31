"""The database's own credential: mint it, then hand it over once.

Two jobs, one script, chosen by argv:

  mint   Run as an init container, before Postgres starts. Writes a
         fresh password to the shared volume the FIRST time only. On
         every later boot the file is already there and nothing
         changes, which is what keeps the database answering to the
         password it was created with.

  serve  Run beside Postgres and the pooler. First it sets the database
         up for the program (`converge`): the program's own role, which
         owns the program's database and everything in it and is no
         superuser, signing in with the password on the shared volume;
         then it tells the pooler who may sign in. Every start does it
         again, so a database made before any of this existed is brought
         to the same shape. Until it is done, `/health` says not ready.

         Then it answers the password to whoever asks over the network,
         and stops answering once the asker proves it stored it, by
         sending the password back. Proof, not a bare say-so: otherwise
         anything able to reach this port could retire a password nobody
         holds, and the database would be locked away with no way back.

         That same proof doubles as the answer to "is what I hold
         still this database's password", which is how a caller finds
         out its disk was replaced underneath it.

         It also serves the graph: `/live` shows the database and user
         to sign in as, whether the password is still readable, and who
         is connected (to Postgres and to the pooler, read when the card
         asks, never on a timer); its one button, `/action` with
         `reset_password`, mints a new password, sets it on the program's
         role over the unix socket (which needs no password), tells the
         pooler, and makes it readable again. Then it tells weft the new
         password (`WEFT_VALUES_URL`, the agent beside this unit), which
         writes it into the connection the node published, so the
         program's next run signs in with it; the card shows it until the
         node's next run retires it.

Where the files live, which ports and names to use, all come from the
node that declares this container, so the two sides cannot drift.
"""

import csv
import hashlib
import hmac
import base64
import http.server
import io
import json
import os
import secrets
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

# SYNC: the credential container's settings <-> catalog/postgres/database/mod.rs (credential_env)
SECRET_DIR = os.environ.get("WEFT_SECRET_DIR")
PASSWORD_FILE = os.environ.get("WEFT_PASSWORD_FILE")
PORT = os.environ.get("WEFT_CREDENTIAL_PORT")
SOCKET_DIR = os.environ.get("WEFT_SOCKET_DIR")
POSTGRES_PORT = os.environ.get("WEFT_POSTGRES_PORT")
ADMIN_USER = os.environ.get("WEFT_ADMIN_USER")
APP_USER = os.environ.get("WEFT_APP_USER")
# The database as Postgres has it, and the names the pooler lets clients
# in by: the one a client signs in with and the watching queries' own.
DATABASE = os.environ.get("WEFT_DATABASE")
POOLER_DATABASE = os.environ.get("WEFT_POOLER_DATABASE")
WATCH_DATABASE = os.environ.get("WEFT_WATCH_DATABASE")
# The extensions the program's database needs, as a JSON list of names.
EXTENSIONS_JSON = os.environ.get("WEFT_EXTENSIONS")
# The Postgres this unit runs, the first major that closes idle sessions,
# and what to warn of when it is older (empty when it is not).
POSTGRES_MAJOR = os.environ.get("WEFT_POSTGRES_MAJOR")
CLOSES_IDLE_SESSIONS_FROM = os.environ.get("WEFT_CLOSES_IDLE_SESSIONS_FROM")
POSTGRES_NOTE = os.environ.get("WEFT_POSTGRES_NOTE")
DATA_DIR = os.environ.get("WEFT_DATA_DIR")
IDLE_SESSION_SECONDS = os.environ.get("WEFT_IDLE_SESSION_SECONDS")
POOLER_DIR = os.environ.get("WEFT_POOLER_DIR")
POOLER_PORT = os.environ.get("WEFT_POOLER_PORT")
POOLER_USERS_FILE = os.environ.get("WEFT_POOLER_USERS_FILE")
# Where this unit tells weft what changed of what the node handed it: set
# by weft on every container of an infra unit, never by the node.
# SYNC: WEFT_VALUES_URL <-> crates/weft-core/src/infra/resolve.rs (VALUES_URL_ENV)
VALUES_URL = os.environ.get("WEFT_VALUES_URL")
_FROM_THE_NODE = {
    "WEFT_SECRET_DIR": SECRET_DIR, "WEFT_PASSWORD_FILE": PASSWORD_FILE, "WEFT_CREDENTIAL_PORT": PORT,
    "WEFT_SOCKET_DIR": SOCKET_DIR, "WEFT_POSTGRES_PORT": POSTGRES_PORT, "WEFT_ADMIN_USER": ADMIN_USER,
    "WEFT_APP_USER": APP_USER, "WEFT_DATABASE": DATABASE, "WEFT_POOLER_DATABASE": POOLER_DATABASE,
    "WEFT_WATCH_DATABASE": WATCH_DATABASE, "WEFT_EXTENSIONS": EXTENSIONS_JSON, "WEFT_POSTGRES_MAJOR": POSTGRES_MAJOR,
    "WEFT_CLOSES_IDLE_SESSIONS_FROM": CLOSES_IDLE_SESSIONS_FROM, "WEFT_DATA_DIR": DATA_DIR,
    "WEFT_IDLE_SESSION_SECONDS": IDLE_SESSION_SECONDS, "WEFT_POOLER_DIR": POOLER_DIR,
    "WEFT_POOLER_PORT": POOLER_PORT, "WEFT_POOLER_USERS_FILE": POOLER_USERS_FILE,
}
_missing = [name for name, value in _FROM_THE_NODE.items() if not value]
if _missing:
    sys.exit(f"{', '.join(_missing)} must be set by the node")
if POSTGRES_NOTE is None:
    sys.exit("WEFT_POSTGRES_NOTE must be set by the node, empty when there is nothing to warn of")
if not VALUES_URL:
    sys.exit("WEFT_VALUES_URL must be set by weft on every container of an infra unit")
PORT = int(PORT)
CLOSES_IDLE_SESSIONS = int(POSTGRES_MAJOR) >= int(CLOSES_IDLE_SESSIONS_FROM)
EXTENSIONS = json.loads(EXTENSIONS_JSON)
if not isinstance(EXTENSIONS, list) or not all(isinstance(name, str) and name for name in EXTENSIONS):
    sys.exit(f"WEFT_EXTENSIONS must be a JSON list of names, got {EXTENSIONS_JSON}")
SEALED_FILE = os.path.join(SECRET_DIR, "sealed")

# SYNC: credential routes <-> catalog/postgres/database/mod.rs
#       CREDENTIAL_PATH / CREDENTIAL_STORED_PATH / HEALTH_PATH
CREDENTIAL_PATH = "/credential"
CREDENTIAL_STORED_PATH = "/credential/stored"
HEALTH_PATH = "/health"
# The dispatcher's routes (features.liveEndpoint names this container).
LIVE_PATH = "/live"
ACTION_PATH = "/action"
RESET_ACTION = "reset_password"

# Set once `converge` finished: the program's role is in place and the
# pooler knows who may sign in. `/health` answers ready from then on.
CONVERGED = threading.Event()
# Why the program cannot sign in yet, while `converge` waits or tries
# again: what `/health` answers until it is done, which the unit's
# readiness check carries to `weft infra status`.
NOT_READY = {"why": "the database is still being set up for the program"}
# How long `converge` waits before trying again after the database refused.
CONVERGE_RETRY_SECONDS = 30
# One change of the password at a time: two presses at once would leave
# the pooler told one password and the database holding the other.
CHANGING = threading.Lock()


def mint() -> None:
    refused = too_old_for_a_new_database(CLOSES_IDLE_SESSIONS, os.path.exists(os.path.join(DATA_DIR, "PG_VERSION")))
    if refused is not None:
        sys.exit(f"credential: {refused}")
    # Made on EVERY boot, before any other container starts, because
    # the container that serves the password is given this directory
    # alone rather than the whole disk. A directory that already
    # exists is handed over as it is; one the container runtime has to
    # create can land owned by root, locking out the container that
    # must write in it.
    os.makedirs(SECRET_DIR, mode=0o750, exist_ok=True)
    if os.path.exists(PASSWORD_FILE):
        return
    write_password(new_password())


def too_old_for_a_new_database(closes_idle_sessions: bool, has_data: bool) -> str | None:
    """Why this start must not make a database, or None. Run before Postgres
    starts, the one moment that can tell a new disk from one an older weft
    filled: an existing database on a Postgres that does not close idle
    sessions keeps running (`POSTGRES_NOTE` says what it lacks), a new one
    is never made on it."""
    if closes_idle_sessions or has_data:
        return None
    return (
        f"Postgres {POSTGRES_MAJOR} cannot be used for a new database: this node needs {CLOSES_IDLE_SESSIONS_FROM} or "
        f"later, which closes the connections a client leaves idle. Set the node's `version` to 17 and start it again"
    )


def new_password() -> str:
    # url-safe: the value travels in a connection string, and it is
    # quoted into SQL by the reset below, so no quote can be in it.
    return secrets.token_urlsafe(32)


def write_file(path: str, text: str) -> None:
    """Write `text` to `path`, whole or not at all, readable by the infra unit's group."""
    # Written under a temporary name and renamed once complete AND on
    # disk, so neither a crash mid-write nor a node reboot can leave a
    # half file, or lose a password Postgres has already adopted.
    tmp = path + ".partial"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    # Group-readable, because Postgres and the pooler run as other users
    # in the same infra unit and read these files. The group is the
    # unit's fsGroup, which only its own containers are in.
    os.chmod(tmp, 0o640)
    os.replace(tmp, path)
    dir_fd = os.open(os.path.dirname(path), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def write_password(password: str) -> None:
    write_file(PASSWORD_FILE, password)


def sealed() -> bool:
    return os.path.exists(SEALED_FILE)


class Refused(Exception):
    """What psql said when the database or the pooler refused."""


def psql(script: str, variables: dict, database: str | None = None, pooler: bool = False) -> str:
    """Run `script` as the superuser and hand back what it printed as CSV.

    Over Postgres's unix socket, which trusts a local connection without
    a password, or with `pooler`, over the pooler's own socket to its
    admin console. The script arrives on STDIN, not through `-c`:
    variables (`:'name'` quotes as a literal, `:"name"` as an identifier)
    are a psql feature, and psql only expands them while READING a
    script, so a value never touches the SQL text itself. Text handed to
    `-c` goes to the server as it stands, which is why the graph's reset
    button once failed with `syntax error at or near ":"`."""
    host, port, database = (POOLER_DIR, POOLER_PORT, "pgbouncer") if pooler else (SOCKET_DIR, POSTGRES_PORT, database or DATABASE)
    args = ["psql", "-h", host, "-p", port, "-U", ADMIN_USER, "-X", "-q", "--csv", "-v", "ON_ERROR_STOP=1"]
    for name, value in variables.items():
        args += ["-v", f"{name}={value}"]
    # The database by `PGDATABASE`, never `-d`: psql reads a `-d` holding
    # `=`, or starting `postgres://`, as a whole connection string, and a
    # database may be named anything.
    run = subprocess.run(args, input=script, capture_output=True, text=True, env={**os.environ, "PGDATABASE": database})
    if run.returncode != 0:
        raise Refused((run.stderr or run.stdout).strip() or f"psql exited {run.returncode}")
    return run.stdout


def rows(printed: str) -> list[dict]:
    """What psql printed as CSV for ONE statement, as one dict per row.
    Several results back to back cannot be told apart, so a caller that
    reads rows sends one statement at a time. A one-column row whose value
    is empty prints as an empty line, which reads as no fields at all."""
    records = list(csv.reader(io.StringIO(printed)))
    return [dict(zip(records[0], record or [""])) for record in records[1:]] if records else []


# Every object in the program's database the superuser owns that the
# program's role must own to use, change or drop the way the program
# made it: what an older database made before the program had its own
# role holds, and what a person operating the database as the superuser
# adds later. Members of an extension stay where the extension put them,
# a sequence owned by a column moves with its table, and a table's row
# type and an array type move with what they belong to.
OWNED_BY_ADMIN = r"""
WITH admin AS (SELECT oid FROM pg_roles WHERE rolname = :'admin'),
     member AS (SELECT classid, objid FROM pg_depend WHERE deptype = 'e'),
     mine AS (SELECT oid FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname <> 'information_schema')
SELECT format('ALTER SCHEMA %I OWNER TO %I', n.nspname, :'app')
  FROM pg_namespace n
 WHERE n.nspowner = (SELECT oid FROM admin) AND n.oid IN (SELECT oid FROM mine)
   AND (n.oid) NOT IN (SELECT objid FROM member WHERE classid = 'pg_namespace'::regclass)
UNION ALL
SELECT format('ALTER %s %s OWNER TO %I',
              CASE c.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' WHEN 'S' THEN 'SEQUENCE'
                             WHEN 'f' THEN 'FOREIGN TABLE' ELSE 'TABLE' END,
              c.oid::regclass, :'app')
  FROM pg_class c
 WHERE c.relowner = (SELECT oid FROM admin) AND c.relnamespace IN (SELECT oid FROM mine)
   AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
   AND c.oid NOT IN (SELECT objid FROM member WHERE classid = 'pg_class'::regclass)
   AND NOT (c.relkind = 'S' AND EXISTS (SELECT FROM pg_depend d
                                         WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid
                                           AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i')))
UNION ALL
SELECT format('ALTER ROUTINE %s OWNER TO %I', p.oid::regprocedure, :'app')
  FROM pg_proc p
 WHERE p.proowner = (SELECT oid FROM admin) AND p.pronamespace IN (SELECT oid FROM mine)
   AND p.oid NOT IN (SELECT objid FROM member WHERE classid = 'pg_proc'::regclass)
UNION ALL
SELECT format('ALTER %s %s OWNER TO %I', CASE t.typtype WHEN 'd' THEN 'DOMAIN' ELSE 'TYPE' END, t.oid::regtype, :'app')
  FROM pg_type t
 WHERE t.typowner = (SELECT oid FROM admin) AND t.typnamespace IN (SELECT oid FROM mine)
   AND (t.typtype IN ('d', 'e', 'r')
        OR (t.typtype = 'c' AND (SELECT relkind FROM pg_class WHERE oid = t.typrelid) = 'c'))
   AND t.oid NOT IN (SELECT objid FROM member WHERE classid = 'pg_type'::regclass)
UNION ALL
SELECT format('ALTER LARGE OBJECT %s OWNER TO %I', l.oid, :'app')
  FROM pg_largeobject_metadata l
 WHERE l.lomowner = (SELECT oid FROM admin)
UNION ALL
SELECT format('ALTER STATISTICS %s OWNER TO %I', format('%I.%I', ns.nspname, s.stxname), :'app')
  FROM pg_statistic_ext s JOIN pg_namespace ns ON ns.oid = s.stxnamespace
 WHERE s.stxowner = (SELECT oid FROM admin) AND s.stxnamespace IN (SELECT oid FROM mine)
UNION ALL
SELECT format('ALTER COLLATION %s OWNER TO %I', format('%I.%I', ns.nspname, co.collname), :'app')
  FROM pg_collation co JOIN pg_namespace ns ON ns.oid = co.collnamespace
 WHERE co.collowner = (SELECT oid FROM admin) AND co.collnamespace IN (SELECT oid FROM mine)
   AND co.oid NOT IN (SELECT objid FROM member WHERE classid = 'pg_collation'::regclass)
UNION ALL
SELECT format('ALTER CONVERSION %s OWNER TO %I', format('%I.%I', ns.nspname, cv.conname), :'app')
  FROM pg_conversion cv JOIN pg_namespace ns ON ns.oid = cv.connamespace
 WHERE cv.conowner = (SELECT oid FROM admin) AND cv.connamespace IN (SELECT oid FROM mine)
   AND cv.oid NOT IN (SELECT objid FROM member WHERE classid = 'pg_conversion'::regclass)
UNION ALL
SELECT format('ALTER OPERATOR %s OWNER TO %I', o.oid::regoperator, :'app')
  FROM pg_operator o
 WHERE o.oprowner = (SELECT oid FROM admin) AND o.oprnamespace IN (SELECT oid FROM mine)
   AND o.oid NOT IN (SELECT objid FROM member WHERE classid = 'pg_operator'::regclass)
UNION ALL
SELECT format('ALTER TEXT SEARCH CONFIGURATION %s OWNER TO %I', cfg.oid::regconfig, :'app')
  FROM pg_ts_config cfg
 WHERE cfg.cfgowner = (SELECT oid FROM admin) AND cfg.cfgnamespace IN (SELECT oid FROM mine)
   AND cfg.oid NOT IN (SELECT objid FROM member WHERE classid = 'pg_ts_config'::regclass)
UNION ALL
SELECT format('ALTER TEXT SEARCH DICTIONARY %s OWNER TO %I', dict.oid::regdictionary, :'app')
  FROM pg_ts_dict dict
 WHERE dict.dictowner = (SELECT oid FROM admin) AND dict.dictnamespace IN (SELECT oid FROM mine)
   AND dict.oid NOT IN (SELECT objid FROM member WHERE classid = 'pg_ts_dict'::regclass)
\gexec
"""

# The program's role, made when missing and held to what it must be on
# every start: it signs in and it is no superuser. The program's database
# is its own, with everything in it.
CONVERGE = r"""
SELECT format('CREATE ROLE %I LOGIN', :'app') WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'app')
\gexec
ALTER ROLE :"app" WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER DATABASE :"database" OWNER TO :"app";
""" + OWNED_BY_ADMIN

# Postgres closes the role's sessions once they sit idle, which is how a
# connection a client holds and does not use goes back to the pooler
# whoever the client is. Set on a Postgres that has the setting only: an
# older one keeps its idle sessions open, which its card warns of.
CLOSE_IDLE_SESSIONS = """ALTER ROLE :"app" SET idle_session_timeout = :'idle';\n"""


def scram_accepts(verifier: str | None, password: str) -> bool:
    """Whether Postgres's stored `verifier` (`pg_authid.rolpassword`) is one for `password`.

    A SCRAM-SHA-256 verifier keeps a hash of the key the password
    derives, so checking it never needs the database to try a sign-in.
    Anything else (no password, an md5 one) answers no."""
    if not verifier or not verifier.startswith("SCRAM-SHA-256$"):
        return False
    try:
        iterations_salt, keys = verifier[len("SCRAM-SHA-256$"):].split("$", 1)
        iterations, salt = iterations_salt.split(":", 1)
        stored_key = keys.split(":", 1)[0]
        salted = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), base64.b64decode(salt), int(iterations))
        client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
        return hmac.compare_digest(hashlib.sha256(client_key).digest(), base64.b64decode(stored_key))
    except (ValueError, TypeError):
        return False


def pooler_users(verifier: str) -> str:
    """The pooler's file of who may sign in: the program's role with its
    verifier (the pooler checks a client's password against it, and signs
    in to Postgres with what the client proved), and the superuser with no
    password at all, which the pooler's own rules let in only to its admin
    console over its unix socket."""
    quoted = lambda value: '"' + value.replace('"', '""') + '"'
    return f"{quoted(APP_USER)} {quoted(verifier)}\n{quoted(ADMIN_USER)} \"\"\n"


def app_verifier() -> str | None:
    """The program's role's stored password verifier, or None when it has none."""
    found = rows(psql("SELECT rolpassword FROM pg_authid WHERE rolname = :'app';\n", {"app": APP_USER}))
    if not found:
        return None
    return found[0]["rolpassword"] or None


def set_app_password(password: str) -> None:
    psql("ALTER ROLE :\"app\" PASSWORD :'password';\n", {"app": APP_USER, "password": password})


def tell_the_pooler() -> None:
    """Write who may sign in from the database's own records, then have
    the pooler read it again. Raises `Refused` when the pooler would not."""
    verifier = app_verifier()
    if verifier is None:
        raise Refused(f"the role {APP_USER} has no password in the database")
    write_file(POOLER_USERS_FILE, pooler_users(verifier))
    psql("RELOAD;\n", {}, pooler=True)


def wait_until(what: str, not_yet) -> None:
    """Wait until `not_yet()` answers None, keeping what it answered instead
    for `/health` and saying it in the log every half minute."""
    started = time.monotonic()
    said = started
    while (why := not_yet()) is not None:
        NOT_READY["why"] = f"waiting for {what}: {why}"
        if time.monotonic() - said >= 30:
            sys.stderr.write(f"credential: still waiting for {what} ({int(time.monotonic() - started)}s): {why}\n")
            said = time.monotonic()
        time.sleep(1)


def postgres_not_serving() -> str | None:
    # Over TCP, for the reason the node's readiness check gives: the
    # image runs a socket-only server while it initialises.
    check = subprocess.run(
        ["pg_isready", "-h", "127.0.0.1", "-p", POSTGRES_PORT, "-U", ADMIN_USER, "-d", "postgres"],
        capture_output=True,
        text=True,
    )
    return None if check.returncode == 0 else (check.stdout or check.stderr).strip()


def pooler_not_told() -> str | None:
    try:
        tell_the_pooler()
        return None
    except Refused as e:
        return str(e)


def converge() -> None:
    """Bring the database to the shape the node declares, then tell the pooler,
    trying again every half minute while the database refuses.

    A refusal is often over by itself (Postgres restarting under the
    container), and when it is not (an extension this Postgres does not
    have) only a change to the node fixes it, which starts the unit
    afresh. Meanwhile `/health` answers what was refused, so the unit's
    readiness check says it where the person looks, and the program is
    never handed a database it cannot sign in to or that lacks an
    extension it needs."""
    while (refused := converge_once()) is not None:
        NOT_READY["why"] = f"{refused}; trying again every {CONVERGE_RETRY_SECONDS}s"
        sys.stderr.write(f"credential: {refused}\n")
        time.sleep(CONVERGE_RETRY_SECONDS)
    CONVERGED.set()
    sys.stderr.write(f"credential: the program signs in as {APP_USER}, through the pooler\n")


def converge_once() -> str | None:
    """One pass of `converge`: what the database refused, or None once done.

    The password file is the one source of the program's password: the
    role is given it when it does not already have it, so a reset that
    stopped halfway is settled by the next start. Each extension the
    node lists is made by the superuser when it is not there yet."""
    wait_until("Postgres to accept connections", postgres_not_serving)
    password = stored_password()
    if password is None:
        sys.exit("credential: no password on the shared volume; the init container writes it before this one starts")
    try:
        psql(CONVERGE, {"app": APP_USER, "admin": ADMIN_USER, "database": DATABASE})
        if CLOSES_IDLE_SESSIONS:
            psql(CLOSE_IDLE_SESSIONS, {"app": APP_USER, "idle": f"{IDLE_SESSION_SECONDS}s"})
        if not scram_accepts(app_verifier(), password):
            set_app_password(password)
    except Refused as e:
        return f"the database refused to be set up for the program: {e}"
    for name in EXTENSIONS:
        try:
            psql('CREATE EXTENSION IF NOT EXISTS :"name";\n', {"name": name})
        except Refused as e:
            return f"the database could not make the extension '{name}' the node lists in `extensions`: {e}"
    wait_until("the pooler to take the sign-ins", pooler_not_told)
    return None


def reset_password() -> str | None:
    """Mint a new password, set it on the program's role, tell the pooler,
    make it readable again, and tell weft: what went wrong, or None.

    The database is told first, over its unix socket, which trusts a
    local connection without a password: a file that named a password
    the database did not have would lock every sign-in out. All of it
    happens under one lock, weft's part included, so of two presses the
    one that changed the database last is the one weft holds."""
    password = new_password()
    with CHANGING:
        # What the role holds now, exactly: an undo gives it this verifier
        # back as it is (Postgres keeps a SCRAM verifier it is handed), not
        # the old password hashed again under a new salt, which the pooler,
        # holding this one, could no longer sign in with.
        held = app_verifier()
        if held is None:
            return f"the role {APP_USER} has no password in the database, so the reset has nothing to replace; restart the database's infra"
        try:
            set_app_password(password)
        except Refused as e:
            sys.stderr.write(f"credential: reset refused by the database: {e}\n")
            return f"the database refused the new password: {e}"
        try:
            write_password(password)
        except OSError as e:
            # A write that failed after the file was swapped in (its
            # directory not synced) still left the new password there.
            try:
                written = stored_password() == password
            except OSError:
                written = False
            if not written:
                sys.stderr.write(f"credential: reset, but the password could not be written: {e}\n")
                try:
                    set_app_password(held)
                except Refused as undo:
                    return (
                        f"the database took a new password that could not be written to its disk ({e}), and would not "
                        f"take the old one back ({undo}), so no client can sign in. Free some room on the database's "
                        "disk (or whatever the error says) and press Reset password again, or restart the database's infra."
                    )
                return (
                    f"the new password could not be written to the database's disk ({e}), so the reset was undone and "
                    "the old password still signs in. Free some room on that disk (or whatever the error says) and press Reset password again."
                )
            sys.stderr.write(f"credential: the new password is written, but its directory could not be synced: {e}\n")
        try:
            tell_the_pooler()
        except (Refused, OSError) as e:
            sys.stderr.write(f"credential: reset, but the pooler was not told: {e}\n")
            return (
                f"the database has the new password, but its pooler could not be told ({e}), so no client can "
                "sign in until it is. Press Reset password again."
            )
        try:
            os.remove(SEALED_FILE)
        except FileNotFoundError:
            pass
        except OSError as e:
            sys.stderr.write(f"credential: reset, but the password stays sealed: {e}\n")
            return (
                f"the database and its pooler have a new password, but it could not be made readable ({e}), so "
                "neither the card nor a run can take it. Press Reset password again."
            )
        # The connection the node published still holds the old password
        # until weft is told the new one.
        unheard = push_password(password)
    if unheard is not None:
        return (
            f"the database has a new password, but weft could not be told ({unheard}), so the "
            "program still signs in with the old one. Run `weft infra rebake` on this node: "
            "its run reads the new password from the database."
        )
    sys.stderr.write("credential: password reset; readable again until a run stores it\n")
    return None


def push_password(password: str) -> str | None:
    """Tell weft the connection's new password: why it did not land, or None once it did."""
    # SYNC: push_values <-> crates/weft-core/src/infra/bake.rs (PushedValues), catalog/postgres/database/mod.rs (the published `password`)
    body = json.dumps({"connection": {"password": password}}).encode("utf-8")
    request = urllib.request.Request(VALUES_URL, data=body, method="POST", headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as answer:
            answer.read()
        return None
    except urllib.error.HTTPError as e:
        return e.read().decode("utf-8", "replace").strip() or f"weft answered {e.code}"
    except (urllib.error.URLError, OSError) as e:
        return str(e)


def stored_password() -> str | None:
    try:
        with open(PASSWORD_FILE, encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return None


class Handler(http.server.BaseHTTPRequestHandler):
    # A client that opens a connection and says nothing gives up its
    # thread after this, so a handful of silent clients cannot pile up
    # into every thread being held and the readiness probe failing.
    timeout = 5
    protocol_version = "HTTP/1.0"

    def _send(self, status: int, body: dict) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802 (the stdlib's spelling)
        if self.path == HEALTH_PATH:
            if CONVERGED.is_set():
                # The role the program signs in as: how the node tells
                # this unit from one an older weft set up.
                # SYNC: signsInAs <-> catalog/postgres/database/mod.rs (signs_in_as_app)
                self._send(200, {"ok": True, "signsInAs": APP_USER})
            else:
                self._send(503, {"error": NOT_READY["why"]})
            return
        if self.path == LIVE_PATH:
            self._send(200, {"items": live_items()})
            return
        if self.path != CREDENTIAL_PATH:
            self._send(404, {"error": "no such path"})
            return
        password = stored_password()
        if password is None:
            self._send(503, {"error": "no password on the shared volume yet"})
            return
        # "Already handed over" is an ANSWER, not a failure: the node
        # that asks needs to act on it (find its own connection, or
        # tell the user how to recover), and an HTTP error would reach
        # it as an opaque transport failure instead.
        self._send(200, {"sealed": True} if sealed() else {"password": password})

    def _body(self) -> dict:
        try:
            # Clamped at BOTH ends, because the length is the caller's
            # claim: above, because no body here is near this size;
            # below, because a negative length means "read to the end"
            # and a client that trickles forever would take the whole
            # container's memory with it.
            length = max(0, min(int(self.headers.get("Content-Length", "0")), 4096))
            body = json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, json.JSONDecodeError):
            return {}
        return body if isinstance(body, dict) else {}

    def do_POST(self) -> None:  # noqa: N802
        if self.path == ACTION_PATH:
            self._action(self._body())
            return
        if self.path != CREDENTIAL_STORED_PATH:
            self._send(404, {"error": "no such path"})
            return
        password = stored_password()
        if password is None:
            self._send(503, {"error": "no password on the shared volume yet"})
            return
        sent = self._body().get("password", "")
        # Only a caller that actually holds the password may retire it.
        # Compared as BYTES: comparing str refuses non-ASCII outright,
        # which would escape as a crash rather than a refusal.
        #
        # A mismatch is an ANSWER, not a failure. It is how the caller
        # learns that the password it holds is not this database's,
        # which happens when the disk was replaced under it: a fresh
        # disk means a fresh password, and the caller has to notice
        # rather than go on handing out one nothing accepts.
        if not secrets.compare_digest(str(sent).encode("utf-8"), password.encode("utf-8")):
            self._send(200, {"stored": False})
            return
        # Idempotent: confirming twice is the same as confirming once.
        try:
            with open(SEALED_FILE, "w", encoding="utf-8") as f:
                f.write("stored")
        except OSError as e:
            self._send(503, {"error": f"the password could not be retired, its disk refused the write: {e}"})
            return
        self._send(200, {"stored": True})

    def _action(self, body: dict) -> None:
        # The graph's button, in the dispatcher's envelope: a refusal
        # travels as `result.error`, which the user reads as it is.
        # SYNC: the /action envelope <-> crates/weft-core/src/infra/action.rs, catalog/bailey/bridge/images/bridge/src/actions.js,
        #       catalog/computer/computer/images/computer/computer.py
        action = body.get("action")
        if action != RESET_ACTION:
            self._send(200, {"result": {"error": f"no such action: {action!r}"}})
            return
        if not CONVERGED.is_set():
            self._send(200, {"result": {"error": "the database is still being set up for the program; press it again in a moment"}})
            return
        refused = reset_password()
        if refused is not None:
            self._send(200, {"result": {"error": refused}})
            return
        self._send(200, {"result": {"reset": True}})

    def log_message(self, fmt: str, *args: object) -> None:
        # One line per request on stderr, never the response body.
        sys.stderr.write("credential: %s\n" % (fmt % args))


def live_items() -> list:
    """What the node's card shows: the names to sign in with, the password
    itself while it is still readable or the state it has moved on to, with
    the one button that makes it readable again, then who is connected.

    The card is a legitimate first taker. The password is readable
    exactly once, by whoever asks first, and then sealed; a person
    looking at the graph is asking the same way a run does, so while it
    is unsealed the card SHOWS it, masked, with a way to copy it. That
    is how somebody outside the program (a frontend that needs its own
    tables in this database, a `psql` session) gets in without anybody
    digging in a container for it.

    Once a run has taken it the value is gone from here for good, and
    the way back is the button below: reset, and the fresh password is
    readable, and shown, again."""
    password = stored_password()
    if password is None:
        item = {
            "type": "text",
            "label": "Password",
            "data": "not minted yet",
        }
    elif sealed():
        item = {
            "type": "text",
            "label": "Password",
            "data": "handed over to a connection; reset to get a new one",
        }
    else:
        # Rendered masked, with reveal and copy beside it.
        item = {
            "type": "secret",
            "label": "Password",
            "data": password,
        }
    # The database and user a client signs in with, beside the
    # password. The pooler lets clients into this database only, under
    # this name.
    names = [
        {"type": "text", "label": "Database", "data": POOLER_DATABASE},
        {"type": "text", "label": "User", "data": APP_USER},
    ]
    item["action"] = {
        "label": "Reset password",
        "actionKind": RESET_ACTION,
        "confirm": (
            "Give the database a new password? A client already connected stays "
            "connected; every new sign-in with the old one is refused. The program's "
            "own next run picks the new one up by itself; anything outside it (a "
            "frontend, a psql session) takes it from this card."
        ),
    }
    warning = [{"type": "text", "label": "Warning", "data": POSTGRES_NOTE}] if POSTGRES_NOTE else []
    return warning + names + [item] + connection_items()


def connection_items() -> list:
    """Who is connected, read now: what Postgres serves, what the pooler
    does with its connections, and the clients behind it. Each part says
    why it could not be read, in its own place, rather than taking the
    rest down."""
    items = []
    try:
        limits = rows(psql(
            "SELECT current_setting('max_connections') AS most, "
            "current_setting('superuser_reserved_connections') AS reserved;\n",
            {},
            database="postgres",
        ))[0]
        activity = rows(psql(
            "SELECT usename AS user, coalesce(state, 'starting') AS state, count(*) AS n FROM pg_stat_activity "
            "WHERE backend_type = 'client backend' AND pid <> pg_backend_pid() GROUP BY 1, 2 ORDER BY 1, 2;\n",
            {},
            database="postgres",
        ))
        items.append({"type": "text", "label": "Postgres connections", "data": postgres_connections(limits, activity)})
    except Refused as e:
        items.append({"type": "text", "label": "Postgres connections", "data": f"could not be read: {e}"})
    try:
        databases = rows(psql("SHOW DATABASES;\n", {}, pooler=True))
        pools = rows(psql("SHOW POOLS;\n", {}, pooler=True))
        clients = rows(psql("SHOW CLIENTS;\n", {}, pooler=True))
        items.append({"type": "text", "label": "Pooler", "data": pooler_lanes(databases, pools)})
        items.append({"type": "text", "label": "Clients", "data": pooler_clients(clients)})
    except Refused as e:
        items.append({"type": "text", "label": "Pooler", "data": f"could not be read: {e}"})
    return items


def postgres_connections(limits: dict, activity: list[dict]) -> str:
    """Postgres's own count: in use of the most it takes, by user and state."""
    in_use = sum(int(row["n"]) for row in activity)
    lines = [f"{in_use} of {limits['most']} in use ({limits['reserved']} of them kept for the superuser)"]
    for user in sorted({row["user"] for row in activity}):
        parts = [f"{row['n']} {row['state']}" for row in activity if row["user"] == user]
        lines.append(f"{user}: {', '.join(parts)}")
    return "\n".join(lines)


def pooler_lanes(databases: list[dict], pools: list[dict]) -> str:
    """The pooler's two doors: clients served and waiting, connections open of its share."""
    lines = []
    for name, lane in ((POOLER_DATABASE, "queries"), (WATCH_DATABASE, "watchers")):
        db = next((row for row in databases if row.get("name") == name), {})
        pool = next((row for row in pools if row.get("database") == name and row.get("user") == APP_USER), {})
        served, waiting = int(pool.get("cl_active", 0)), int(pool.get("cl_waiting", 0))
        line = f"{lane} ({name}): {served} client(s) served, {waiting} waiting"
        if waiting:
            line += f" (the longest for {pool.get('maxwait', '0')}s)"
        line += f"; {db.get('current_connections', 0)} of {db.get('pool_size', '?')} connections open"
        lines.append(line)
    return "\n".join(lines)


def pooler_clients(clients: list[dict]) -> str:
    """The pooler's clients by where they come from, most first: which
    workers and frontends hold or wait for this database's connections."""
    counts: dict[str, int] = {}
    for client in clients:
        if client.get("database") not in (POOLER_DATABASE, WATCH_DATABASE):
            continue
        name = client.get("application_name") or ""
        where = client.get("addr", "?") + (f" ({name})" if name else "")
        counts[where] = counts.get(where, 0) + 1
    if not counts:
        return "none"
    return "\n".join(f"{where}: {n}" for where, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


def serve() -> None:
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    # Answering from the start, so the card and the readiness check hear
    # "still being set up" rather than nothing while `converge` runs.
    threading.Thread(target=server.serve_forever, daemon=True).start()
    converge()
    threading.Event().wait()


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("mint", "serve"):
        sys.exit("usage: bootstrap.py mint|serve")
    mint() if sys.argv[1] == "mint" else serve()
