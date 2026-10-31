# Start PgBouncer with the settings the node wrote.
#
# Every setting, and every path, comes from the node that declares this
# container, so this script and the node cannot disagree on any of them.
# The settings and the sign-in rules arrive as the arguments, one line
# each: `--config <lines> --hba <lines>`.
# SYNC: the pooler's settings <-> catalog/postgres/database/mod.rs (pooler_env, pooler_args)
set -eu

: "${WEFT_POOLER_CONFIG_FILE:?set by the node}" "${WEFT_POOLER_HBA_FILE:?set by the node}"
: "${WEFT_POOLER_USERS_FILE:?set by the node}" "${WEFT_POOLER_OPEN_FILES:?set by the node}"

if [ "${1:-}" != "--config" ]; then
    echo "pooler: expected the arguments --config <lines> --hba <lines> from the node" >&2
    exit 1
fi
shift
: > "$WEFT_POOLER_CONFIG_FILE"
: > "$WEFT_POOLER_HBA_FILE"
into="$WEFT_POOLER_CONFIG_FILE"
for line in "$@"; do
    if [ "$line" = "--hba" ]; then
        into="$WEFT_POOLER_HBA_FILE"
        continue
    fi
    printf '%s\n' "$line" >> "$into"
done
if [ "$into" != "$WEFT_POOLER_HBA_FILE" ]; then
    echo "pooler: no --hba among the arguments, so nobody could sign in" >&2
    exit 1
fi

# Every client and every connection to Postgres is an open file, and a
# container often starts allowed far fewer than it may raise itself to.
# A machine that does not allow enough is said here, at the start, rather
# than as clients refused under load.
ulimit -n "$(ulimit -Hn)"
allowed="$(ulimit -n)"
if [ "$allowed" != "unlimited" ] && [ "$allowed" -lt "$WEFT_POOLER_OPEN_FILES" ]; then
    echo "pooler: this machine lets the pooler open $allowed files, fewer than the $WEFT_POOLER_OPEN_FILES its clients need" >&2
    exit 1
fi

# The credential container writes who may sign in once the database has
# the program's role (`bootstrap.py`, `converge`). Started before that,
# PgBouncer would refuse every client as unknown.
if [ ! -s "$WEFT_POOLER_USERS_FILE" ]; then
    echo "pooler: waiting for the database to be set up before taking clients" >&2
    until [ -s "$WEFT_POOLER_USERS_FILE" ]; do sleep 1; done
fi

exec pgbouncer "$WEFT_POOLER_CONFIG_FILE"
