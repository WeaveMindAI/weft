# Start a throwaway Postgres container and wait until it accepts
# connections. Shared by run-db-tests.sh and setup.sh's --migration
# block, so there is one way to do this.
#
#   start_throwaway_postgres <name-prefix>
#
# The container is named <name-prefix>-<pid> and listens on a
# loopback-only port the kernel picks, so concurrent runs (worktrees,
# CI jobs) never fight over a name or a port, and nothing off the
# machine can reach it. Fails loudly (with the container's logs) if
# Postgres never comes up or the container dies, and leaves the
# connection URL in $THROWAWAY_DATABASE_URL and the container name in
# $THROWAWAY_PG_CONTAINER. The CALLER owns cleanup (its own EXIT trap
# running `docker rm -f "$THROWAWAY_PG_CONTAINER"`): scripts already
# carry traps of their own, and a trap set here would silently replace
# them. That caller-side removal is also why there is no `--rm` here:
# a container that crashes must stay around long enough for the
# failure branch below to read its logs.

start_throwaway_postgres() {
  local container="$1-$$"
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is not on PATH, so no throwaway postgres can be started" >&2
    return 1
  fi
  docker rm -f "$container" >/dev/null 2>&1 || true
  # Exported BEFORE the container runs, so a Ctrl-C anywhere in the
  # readiness wait still leaves the caller's trap pointing at the right
  # container instead of an unset variable and an orphan.
  THROWAWAY_PG_CONTAINER="$container"
  # Host port 0 asks the kernel for a free loopback port; docker port
  # reports the one it got.
  # SYNC: postgres image tag <-> setup.sh (--purge --postgres, which
  #       reclaims exactly this image),
  #       deploy/k8s/postgres.yaml (the in-cluster image, 18-alpine)
  if ! docker run -d --name "$container" -p 127.0.0.1:0:5432 \
       -e POSTGRES_PASSWORD=postgres postgres:18 >/dev/null; then
    echo "could not start the throwaway postgres container '$container'" >&2
    return 1
  fi
  local port
  port=$(docker port "$container" 5432/tcp | head -n1)
  port="${port##*:}"
  if [ -z "$port" ]; then
    echo "docker reported no host port for '$container'; its logs:" >&2
    docker logs "$container" >&2 2>&1 || true
    docker rm -f "$container" >/dev/null 2>&1 || true
    return 1
  fi
  local tries=0
  until docker exec "$container" pg_isready -U postgres >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null)" != "true" ] \
       || [ "$tries" -gt 60 ]; then
      echo "the throwaway postgres '$container' never became ready; its logs:" >&2
      docker logs "$container" >&2 2>&1 || true
      docker rm -f "$container" >/dev/null 2>&1 || true
      return 1
    fi
    sleep 1
  done
  THROWAWAY_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:$port/postgres"
}
