# PgBouncer, the one door every client goes through to reach the
# database. Its settings come from the node that declares this
# container, written out by the entrypoint; nothing is baked here.
FROM alpine:3.24

# The release the node's settings were written and checked against.
# Alpine's package decides the version, so a base that brought an older
# one stops the build here rather than starting a pooler that reads the
# settings differently.
RUN apk add --no-cache pgbouncer \
    && version="$(pgbouncer --version | head -n 1 | cut -d ' ' -f 2)" \
    && major="${version%%.*}" && rest="${version#*.}" && minor="${rest%%.*}" \
    && if [ "$major" -lt 1 ] || { [ "$major" -eq 1 ] && [ "$minor" -lt 25 ]; }; then \
         echo "pgbouncer $version is older than 1.25, the release this pooler is set up for" >&2; exit 1; \
       fi

COPY entrypoint.sh /entrypoint.sh

# Not root (PgBouncer refuses to run as root), and the same user as the
# credential container: both only need the infra unit's shared group to
# reach the directory they share.
USER 999:999
ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]
